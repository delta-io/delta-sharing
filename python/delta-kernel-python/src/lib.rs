use std::sync::Arc;

use arrow::compute::filter_record_batch;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::pyarrow::PyArrowType;
use deltakernel::client::DefaultTableClient;
use deltakernel::executor::tokio::TokioBackgroundExecutor;
use deltakernel::scan::ScanResult;
use deltakernel::simple_client::data::SimpleData;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use url::Url;

use arrow::record_batch::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use deltakernel::EngineInterface;

use std::collections::HashMap;

struct KernelError(deltakernel::Error);

impl From<KernelError> for PyErr {
    fn from(error: KernelError) -> Self {
        PyValueError::new_err(format!("Kernel error: {}", error.0))
    }
}

impl From<deltakernel::Error> for KernelError {
    fn from(delta_kernel_error: deltakernel::Error) -> Self {
        Self(delta_kernel_error)
    }
}

type DeltaPyResult<T> = std::result::Result<T, KernelError>;

#[pyclass]
struct Table(deltakernel::Table);

#[pymethods]
impl Table {
    #[new]
    fn new(location: &str) -> DeltaPyResult<Self> {
        let location = Url::parse(location).map_err(deltakernel::Error::InvalidUrl)?;
        let table = deltakernel::Table::new(location);
        Ok(Table(table))
    }

    fn snapshot(&self, engine_interface: &PythonInterface) -> DeltaPyResult<Snapshot> {
        let snapshot = self.0.snapshot(engine_interface.0.as_ref(), None)?;
        Ok(Snapshot(snapshot))
    }
}

#[pyclass]
struct Snapshot(Arc<deltakernel::snapshot::Snapshot>);

#[pymethods]
impl Snapshot {
    fn version(&self) -> deltakernel::Version {
        self.0.version()
    }
}

#[pyclass]
struct ScanBuilder(deltakernel::scan::ScanBuilder);

#[pymethods]
impl ScanBuilder {
    #[new]
    fn new(snapshot: &Snapshot) -> ScanBuilder {
        let sb = deltakernel::scan::ScanBuilder::new(snapshot.0.clone());
        ScanBuilder(sb)
    }

    fn build(&self) -> Scan {
        let scan = self.0.clone().build();
        Scan(scan)
    }
}

#[pyclass]
struct Scan(deltakernel::scan::Scan);

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
                    .downcast::<SimpleData>()
                    .map_err(|_| {
                        deltakernel::Error::engine_data_type("Couldn't cast to SimpleData")
                    })?
                    .into();
                let schema: SchemaRef = rb.schema();
                results.push(ScanResult {
                    raw_data: Ok(Box::new(SimpleData::new(rb))),
                    mask: last.mask,
                });
                let record_batch_iter = RecordBatchIterator::new(
                    results.into_iter().map(|res| {
                        let data = res
                            .raw_data
                            .map_err(|e| ArrowError::from_external_error(Box::new(e)))?;
                        let record_batch: RecordBatch = data
                            .into_any()
                            .downcast::<SimpleData>()
                            .map_err(|_| {
                                ArrowError::CastError("Couldn't cast to SimpleData".to_string())
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
struct PythonInterface(Box<dyn EngineInterface + Send>);

#[pymethods]
impl PythonInterface {
    #[new]
    fn new(location: &str) -> DeltaPyResult<Self> {
        let url = Url::parse(location).map_err(deltakernel::Error::InvalidUrl)?;
        let client = DefaultTableClient::try_new(
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
