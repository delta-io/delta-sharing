use std::sync::Arc;

use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::pyarrow::PyArrowType;
use arrow::record_batch::{RecordBatch, RecordBatchIterator, RecordBatchReader};

use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::scan::ScanResult;
use delta_kernel::{engine::arrow_data::ArrowEngineData, schema::StructType};
use delta_kernel::{DeltaResult, Engine};

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use url::Url;

use std::collections::HashMap;

struct KernelError(delta_kernel::Error);

impl From<KernelError> for PyErr {
    fn from(error: KernelError) -> Self {
        PyValueError::new_err(format!("Kernel error: {}", error.0))
    }
}

impl From<delta_kernel::Error> for KernelError {
    fn from(delta_kernel_error: delta_kernel::Error) -> Self {
        Self(delta_kernel_error)
    }
}

type DeltaPyResult<T> = std::result::Result<T, KernelError>;

#[pyclass]
struct Table(delta_kernel::Table);

#[pymethods]
impl Table {
    #[new]
    fn new(location: &str) -> DeltaPyResult<Self> {
        let location = Url::parse(location).map_err(delta_kernel::Error::InvalidUrl)?;
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
        let scan = self.0.take().unwrap().build()?;
        Ok(Scan(scan))
    }
}

fn try_get_schema(schema: &Arc<StructType>) -> Result<SchemaRef, delta_kernel::Error> {
    Ok(Arc::new(schema.as_ref().try_into().map_err(|e| {
        delta_kernel::Error::Generic(format!("Could not get result schema: {e}"))
    })?))
}

fn try_create_record_batch_iter(
    results: impl Iterator<Item = DeltaResult<ScanResult>>,
    result_schema: SchemaRef,
) -> RecordBatchIterator<Vec<Result<RecordBatch, ArrowError>>> {
    let record_batches: Vec<_> = results
        .map(|res| {
            let scan_res = res.and_then(|res| Ok((res.full_mask(), res.raw_data?)));
            let (mask, data) =
                scan_res.map_err(|e| ArrowError::from_external_error(Box::new(e)))?;
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
        })
        .collect();
    RecordBatchIterator::new(record_batches, result_schema)
}

#[pyclass]
struct Scan(delta_kernel::scan::Scan);

#[pymethods]
impl Scan {
    fn execute(
        &self,
        engine_interface: &PythonInterface,
    ) -> DeltaPyResult<PyArrowType<Box<dyn RecordBatchReader + Send>>> {
        let result_schema: SchemaRef = try_get_schema(self.0.schema())?;
        let results = self.0.execute(engine_interface.0.as_ref())?;
        let record_batch_iter = try_create_record_batch_iter(results, result_schema);
        Ok(PyArrowType(Box::new(record_batch_iter)))
    }
}

#[pyclass]
struct TableChangesScanBuilder(
    Option<delta_kernel::table_changes::table_changes_scan::TableChangesScanBuilder>
);

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
        let table_changes =
            table
                .0
                .table_changes(engine_interface.0.as_ref(), start_version, end_version)?;
        Ok(TableChangesScanBuilder(Some(
            table_changes.into_scan_builder(),
        )))
    }

    fn build(&mut self) -> DeltaPyResult<TableChangesScan> {
        let scan = self.0.take().unwrap().build()?;
        Ok(TableChangesScan(scan))
    }
}

#[pyclass]
struct TableChangesScan(delta_kernel::table_changes::table_changes_scan::TableChangesScan);

#[pymethods]
impl TableChangesScan {
    fn execute(
        &self,
        engine_interface: &PythonInterface,
    ) -> DeltaPyResult<PyArrowType<Box<dyn RecordBatchReader + Send>>> {
        let result_schema: SchemaRef = try_get_schema(self.0.schema())?;
        let results = self.0.execute(engine_interface.0.as_ref())?;
        let record_batch_iter = try_create_record_batch_iter(results, result_schema);
        Ok(PyArrowType(Box::new(record_batch_iter)))
    }
}

#[pyclass]
struct PythonInterface(Box<dyn Engine + Send>);

#[pymethods]
impl PythonInterface {
    #[new]
    fn new(location: &str) -> DeltaPyResult<Self> {
        let url = Url::parse(location).map_err(delta_kernel::Error::InvalidUrl)?;
        let client = DefaultEngine::try_new(
            &url,
            HashMap::<String, String>::new(),
            Arc::new(TokioBackgroundExecutor::new()),
        )?;
        Ok(PythonInterface(Box::new(client)))
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
