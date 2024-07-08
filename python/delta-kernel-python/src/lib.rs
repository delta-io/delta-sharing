use std::sync::Arc;

use arrow::compute::filter_record_batch;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::pyarrow::PyArrowType;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::scan::ScanResult;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use url::Url;

use arrow::record_batch::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use delta_kernel::Engine;

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

#[pyclass]
struct Scan(delta_kernel::scan::Scan);

#[pymethods]
impl Scan {
    fn execute(
        &self,
        engine_interface: &PythonInterface,
    ) -> DeltaPyResult<PyArrowType<Box<dyn RecordBatchReader + Send>>> {
        let mut results = self.0.execute(engine_interface.0.as_ref())?;
        match results.pop() {
            Some(last) => {
                // TODO(nick): This is a terrible way to have to get the schema
                let rb: RecordBatch = last
                    .raw_data?
                    .into_any()
                    .downcast::<ArrowEngineData>()
                    .map_err(|_| {
                        delta_kernel::Error::engine_data_type("Couldn't cast to ArrowEngineData")
                    })?
                    .into();
                let schema: SchemaRef = rb.schema();
                results.push(ScanResult {
                    raw_data: Ok(Box::new(ArrowEngineData::new(rb))),
                    mask: last.mask,
                });
                let record_batch_iter = RecordBatchIterator::new(
                    results.into_iter().map(|res| {
                        let data = res
                            .raw_data
                            .map_err(|e| ArrowError::from_external_error(Box::new(e)))?;
                        let record_batch: RecordBatch = data
                            .into_any()
                            .downcast::<ArrowEngineData>()
                            .map_err(|_| {
                                ArrowError::CastError("Couldn't cast to ArrowEngineData".to_string())
                            })?
                            .into();
                        if let Some(mask) = res.mask {
                            let filtered_batch = filter_record_batch(&record_batch, &mask.into())?;
                            Ok(filtered_batch)
                        } else {
                            Ok(record_batch)
                        }
                    }),
                    schema,
                );
                Ok(PyArrowType(Box::new(record_batch_iter)))
            }
            None => {
                // no results, return empty iterator
                Ok(PyArrowType(Box::new(RecordBatchIterator::new(
                    vec![],
                    Arc::new(Schema::empty()),
                ))))
            }
        }
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

/// Define the delta_kernel_python module. The name of this function _must_ be
/// `delta_kernel_python`, and _must_ the `lib.name` setting in the `Cargo.toml`, otherwise Python
/// will not be able to import the module.
#[pymodule]
fn delta_kernel_python(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Table>()?;
    m.add_class::<PythonInterface>()?;
    m.add_class::<Snapshot>()?;
    m.add_class::<ScanBuilder>()?;
    m.add_class::<Scan>()?;
    Ok(())
}
