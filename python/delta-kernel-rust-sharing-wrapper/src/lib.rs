use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use arrow::error::ArrowError;
use arrow::pyarrow::PyArrowType;
use arrow::record_batch::{RecordBatch, RecordBatchIterator, RecordBatchReader};

use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::table_changes::scan::{
    TableChangesScan as KernelTableChangesScan,
    TableChangesScanBuilder as KernelTableChangesScanBuilder,
};
use delta_kernel::Error as KernelError;
use delta_kernel::{engine::arrow_data::ArrowEngineData, schema::StructType};
use delta_kernel::{DeltaResult, Engine};

use object_store::gcp::{GcpCredential, GoogleCloudStorageBuilder};
use object_store::{ObjectStore, StaticCredentialProvider};

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use url::Url;

// Synthetic storage option recognized only by this wrapper. `object_store`'s string-keyed
// `parse_url_opts` has no key for a raw GCS OAuth bearer token, so the Python connector passes the
// token under this key and we build the GCS store directly with a credential provider.
const GCS_BEARER_TOKEN_OPTION: &str = "google_bearer_token";

/// Build an object store for `url`, honoring `storage_options` when present.
///
/// GCS is special-cased: a raw OAuth bearer token (passed under `google_bearer_token`) cannot be
/// expressed through `parse_url_opts`, so we construct the store with a static credential provider.
fn build_object_store(
    url: &Url,
    storage_options: Option<HashMap<String, String>>,
) -> Result<Arc<dyn ObjectStore>, object_store::Error> {
    match storage_options {
        Some(options) => {
            if matches!(url.scheme(), "gs" | "gcs") {
                if let Some(bearer) = options.get(GCS_BEARER_TOKEN_OPTION) {
                    return build_gcs_with_bearer_token(url, bearer);
                }
            }
            let (object_store, _path) = object_store::parse_url_opts(url, options)?;
            Ok(object_store.into())
        }
        None => {
            let (object_store, _path) = object_store::parse_url(url)?;
            Ok(object_store.into())
        }
    }
}

fn build_gcs_with_bearer_token(
    url: &Url,
    bearer: &str,
) -> Result<Arc<dyn ObjectStore>, object_store::Error> {
    let credential = Arc::new(StaticCredentialProvider::new(GcpCredential {
        bearer: bearer.to_string(),
    }));
    // For gs://bucket/path the bucket is the URL host. build() surfaces a clear error if it is
    // missing, so no manual validation is needed here.
    let mut builder = GoogleCloudStorageBuilder::new().with_credentials(credential);
    if let Some(bucket) = url.host_str() {
        builder = builder.with_bucket_name(bucket);
    }
    let store = builder.build()?;
    Ok(Arc::new(store))
}

struct PyKernelError(KernelError);

impl From<PyKernelError> for PyErr {
    fn from(error: PyKernelError) -> Self {
        PyValueError::new_err(format!("Kernel error: {}", error.0))
    }
}

impl From<KernelError> for PyKernelError {
    fn from(delta_kernel_error: KernelError) -> Self {
        Self(delta_kernel_error)
    }
}

type DeltaPyResult<T> = std::result::Result<T, PyKernelError>;

#[pyclass]
struct Table(Url);

#[pymethods]
impl Table {
    #[new]
    fn new(location: &str) -> DeltaPyResult<Self> {
        // location must end in a trailing / so that it gets treated as a dir
        let location = if location.ends_with('/') {
            location
        } else {
            &format!("{location}/")
        };
        let location = Url::parse(location).map_err(KernelError::InvalidUrl)?;
        Ok(Table(location))
    }

    fn snapshot(&self, engine_interface: &PythonInterface) -> DeltaPyResult<Snapshot> {
        let snapshot = delta_kernel::Snapshot::builder_for(self.0.clone())
            .build(engine_interface.0.as_ref())?;
        Ok(Snapshot(snapshot))
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
    Ok(Arc::new(schema.as_ref().try_into_arrow().map_err(|e| {
        KernelError::Generic(format!("Could not get result schema: {e}"))
    })?))
}

fn try_create_record_batch_iter(
    results: impl Iterator<Item = DeltaResult<Box<dyn delta_kernel::EngineData>>>,
    result_schema: ArrowSchemaRef,
) -> RecordBatchIterator<impl Iterator<Item = Result<RecordBatch, ArrowError>>> {
    let record_batches = results.map(|data| {
        let record_batch: RecordBatch = data
            .map_err(|e| ArrowError::from_external_error(Box::new(e)))?
            .into_any()
            .downcast::<ArrowEngineData>()
            .map_err(|_| ArrowError::CastError("Couldn't cast to ArrowEngineData".to_string()))?
            .into();
        Ok(record_batch)
    });
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
        let result_schema: ArrowSchemaRef = try_get_schema(self.0.logical_schema())?;
        let results = self.0.execute(engine_interface.0.clone())?;
        let record_batch_iter = try_create_record_batch_iter(results, result_schema);
        Ok(PyArrowType(Box::new(record_batch_iter)))
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
        let table_changes = delta_kernel::table_changes::TableChanges::try_new(
            table.0.clone(),
            engine_interface.0.as_ref(),
            start_version,
            end_version,
        )?;
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
        let schema: ArrowSchemaRef = try_get_schema(scan.logical_schema())?;
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
    /// Build an engine that reads from `location`.
    ///
    /// `storage_options`, when provided, carries the cloud credentials and configuration used to read
    /// a table directly from object storage (Delta Sharing directory-based access). Most keys are the
    /// standard `object_store` configuration keys (e.g. `access_key_id`, `secret_access_key`, `token`,
    /// `endpoint`, `region`, `account_name`, `azure_storage_sas_token`); a raw GCS OAuth bearer token
    /// is passed under the wrapper-specific `google_bearer_token` key. When omitted, the store is
    /// built from the URL alone, which is what URL-based access (pre-signed file URLs read from a
    /// local temporary log) relies on.
    #[new]
    #[pyo3(signature = (location, storage_options=None))]
    fn new(
        location: &str,
        storage_options: Option<HashMap<String, String>>,
    ) -> DeltaPyResult<Self> {
        let url = Url::parse(location).map_err(KernelError::InvalidUrl)?;
        let object_store = build_object_store(&url, storage_options).map_err(|e| {
            KernelError::InvalidTableLocation(format!("Failed to parse table location {url}: {e}"))
        })?;
        let engine = DefaultEngineBuilder::new(object_store).build();
        Ok(PythonInterface(Arc::new(engine)))
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
