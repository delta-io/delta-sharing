use std::mem::transmute;
use std::sync::Arc;

use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use arrow::error::ArrowError;
use arrow::ffi::to_ffi;
use arrow::pyarrow::PyArrowType;
use arrow::record_batch::{RecordBatch, RecordBatchIterator, RecordBatchReader};

use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::scan::ScanResult;
use delta_kernel::table_changes::scan::{
    TableChangesScan as KernelTableChangesScan,
    TableChangesScanBuilder as KernelTableChangesScanBuilder,
};
use delta_kernel::Error as KernelError;
use delta_kernel::{engine::arrow_data::ArrowEngineData, schema::StructType};
use delta_kernel::{DeltaResult, Engine};

use polars::error::PolarsError;
use polars::prelude::{concat, DataFrame, IntoLazy, Series, UnionArgs};

use polars_arrow::ffi::{import_array_from_c, import_field_from_c};

use pyo3_polars::{PyDataFrame, PyLazyFrame};

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use url::Url;

use std::collections::HashMap;

enum PyRustError {
    PyKernelError(KernelError),
    PyPolarsError(PolarsError),
}


impl From<PyRustError> for PyErr {
    fn from(error: PyRustError) -> Self {
        let msg = match error {
            PyRustError::PyKernelError(e) => format!("Kernel error: {}", e),
            PyRustError::PyPolarsError(e) => format!("Polars error: {}", e),
        };
        PyValueError::new_err(msg)
    }
}

impl From<KernelError> for PyRustError {
    fn from(delta_kernel_error: KernelError) -> Self {
        Self::PyKernelError(delta_kernel_error)
    }
}

impl From<PolarsError> for PyRustError {
    fn from(polars_error: PolarsError) -> Self {
        Self::PyPolarsError(polars_error)
    }
}

type DeltaPyResult<T> = std::result::Result<T, PyRustError>;

#[pyclass]
struct Table(delta_kernel::Table);

#[pymethods]
impl Table {
    #[new]
    fn new(location: &str) -> DeltaPyResult<Self> {
        let location = Url::parse(location).map_err(KernelError::InvalidUrl)?;
        let table = delta_kernel::Table::new(location);
        Ok(Table(table))
    }

    fn snapshot(&self, engine_interface: &PythonInterface) -> DeltaPyResult<Snapshot> {
        let snapshot = self.0.snapshot(engine_interface.0.as_ref(), None)?;
        Ok(Snapshot(Arc::new(snapshot)))
    }
}

#[pyclass]
struct Snapshot(Arc<delta_kernel::snapshot::Snapshot>);

#[pymethods]
impl Snapshot {
    fn version(&self) -> delta_kernel::Version {
        self.0.version()
    }
}

#[pyclass]
struct ScanBuilder(Option<delta_kernel::scan::ScanBuilder>);

#[pymethods]
impl ScanBuilder {
    #[new]
    fn new(snapshot: &Snapshot) -> ScanBuilder {
        let sb = delta_kernel::scan::ScanBuilder::new(snapshot.0.clone());
        ScanBuilder(Some(sb))
    }

    fn build(&mut self) -> DeltaPyResult<Scan> {
        let scan = self
            .0
            .take()
            .ok_or_else(|| KernelError::generic("Can only call build() once on ScanBuilder"))?
            .build()?;
        Ok(Scan(scan))
    }
}

fn try_get_schema(schema: &Arc<StructType>) -> Result<ArrowSchemaRef, KernelError> {
    Ok(Arc::new(schema.as_ref().try_into().map_err(|e| {
        KernelError::Generic(format!("Could not get result schema: {e}"))
    })?))
}

fn try_create_record_batch_iter(
    results: impl Iterator<Item = DeltaResult<ScanResult>>,
    result_schema: ArrowSchemaRef,
) -> RecordBatchIterator<impl Iterator<Item = Result<RecordBatch, ArrowError>>> {
    let record_batches = results.map(|res| {
        let scan_res = res.and_then(|res| Ok((res.full_mask(), res.raw_data?)));
        let (mask, data) = scan_res.map_err(|e| ArrowError::from_external_error(Box::new(e)))?;
        let record_batch: RecordBatch = data
            .into_any()
            .downcast::<ArrowEngineData>()
            .map_err(|_| ArrowError::CastError("Couldn't cast to ArrowEngineData".to_string()))?
            .into();
        if let Some(mask) = mask {
            let filtered_batch = filter_record_batch(&record_batch, &mask.into())?;
            Ok(filtered_batch)
        } else {
            Ok(record_batch)
        }
    });
    RecordBatchIterator::new(record_batches, result_schema)
}

unsafe fn record_batch_to_dataframe(batch: &RecordBatch) -> Result<DataFrame, PolarsError> {
    let mut columns = Vec::with_capacity(batch.num_columns());

    // Arrow stores data by columns, therefore need to be Zero-copied by column
    for (i, col) in batch.columns().iter().enumerate() {
        // Convert to ArrayData (arrow-rs)
        let array = col.to_data();

        // Convert to ffi with arrow-rs
        let (out_array, out_schema) = to_ffi(&array).unwrap();

        // Import field from ffi with polars
        let field = unsafe {
            import_field_from_c(transmute::<
                &arrow::ffi::FFI_ArrowSchema,
                &polars_arrow::ffi::ArrowSchema,
                >(&out_schema))
        }?;

        // Import data from ffi with polars
        let data = unsafe {
            import_array_from_c(
                transmute::<arrow::ffi::FFI_ArrowArray, polars_arrow::ffi::ArrowArray>(
                    out_array,
                ),
                field.dtype().clone(),
            )
        }?;

        // Create Polars series from arrow column
        columns.push(Series::from_arrow(
            batch.schema().field(i).name().into(),
            data,
        )?);
    }
    Ok(DataFrame::from_iter(columns))
}

#[pyclass]
struct Scan(delta_kernel::scan::Scan);

#[pymethods]
impl Scan {
    fn execute(
        &self,
        engine_interface: &PythonInterface,
    ) -> DeltaPyResult<PyArrowType<Box<dyn RecordBatchReader + Send>>> {
        let result_schema: ArrowSchemaRef = try_get_schema(self.0.schema())?;
        let results = self.0.execute(engine_interface.0.clone())?;
        let record_batch_iter = try_create_record_batch_iter(results, result_schema);
        Ok(PyArrowType(Box::new(record_batch_iter)))
    }

    fn execute_polars(
        &self,
        engine_interface: &PythonInterface,
    ) -> DeltaPyResult<PyDataFrame> {
        let result_schema: ArrowSchemaRef = try_get_schema(self.0.schema())?;
        let results = self.0.execute(engine_interface.0.clone())?;
        let record_batch_iter = try_create_record_batch_iter(results, result_schema);
        let mut dfs = Vec::new();
        for rb in record_batch_iter {
            unsafe {
                let df = record_batch_to_dataframe(&rb.map_err(KernelError::Arrow)?)?;
                dfs.push(df.lazy())
            };
        };
        let dfs_concat = concat(dfs, UnionArgs::default());
        Ok(PyDataFrame(dfs_concat?.collect()?))
    }
}

#[pyclass]
struct TableChangesScanBuilder(Option<KernelTableChangesScanBuilder>);

#[pymethods]
impl TableChangesScanBuilder {
    #[new]
    #[pyo3(signature = (table, engine_interface, start_version, end_version=None))]
    fn new(
        table: &Table,
        engine_interface: &PythonInterface,
        start_version: u64,
        end_version: Option<u64>,
    ) -> DeltaPyResult<TableChangesScanBuilder> {
        let table_changes = table
            .0
            .table_changes(engine_interface.0.as_ref(), start_version, end_version)?;
        Ok(TableChangesScanBuilder(Some(
            table_changes.into_scan_builder(),
        )))
    }

    fn build(&mut self) -> DeltaPyResult<TableChangesScan> {
        let scan = self
            .0
            .take()
            .ok_or_else(|| {
                KernelError::generic("Can only call build() once on TableChangesScanBuilder")
            })?
            .build()?;
        let schema: ArrowSchemaRef = try_get_schema(scan.schema())?;
        Ok(TableChangesScan { scan, schema })
    }
}

#[pyclass]
struct TableChangesScan {
    scan: KernelTableChangesScan,
    schema: ArrowSchemaRef,
}

#[pymethods]
impl TableChangesScan {
    fn execute(
        &self,
        engine_interface: &PythonInterface,
    ) -> DeltaPyResult<PyArrowType<Box<dyn RecordBatchReader + Send>>> {
        let result_schema = self.schema.clone();
        let results = self.scan.execute(engine_interface.0.clone())?;
        let record_batch_iter = try_create_record_batch_iter(results, result_schema);
        Ok(PyArrowType(Box::new(record_batch_iter)))
    }
}

#[pyclass]
struct PythonInterface(Arc<dyn Engine + Send>);

#[pymethods]
impl PythonInterface {
    #[new]
    fn new(location: &str) -> DeltaPyResult<Self> {
        let url = Url::parse(location).map_err(KernelError::InvalidUrl)?;
        let client = DefaultEngine::try_new(
            &url,
            HashMap::<String, String>::new(),
            Arc::new(TokioBackgroundExecutor::new()),
        )?;
        Ok(PythonInterface(Arc::new(client)))
    }
}

/// Define the delta_kernel_rust_sharing_wrapper module. The name of this function _must_ be
/// `delta_kernel_rust_sharing_wrapper`, and _must_ the `lib.name` setting in the `Cargo.toml`, otherwise Python
/// will not be able to import the module.
#[pymodule]
fn delta_kernel_rust_sharing_wrapper(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Table>()?;
    m.add_class::<PythonInterface>()?;
    m.add_class::<Snapshot>()?;
    m.add_class::<ScanBuilder>()?;
    m.add_class::<Scan>()?;
    m.add_class::<TableChangesScanBuilder>()?;
    m.add_class::<TableChangesScan>()?;
    Ok(())
}
