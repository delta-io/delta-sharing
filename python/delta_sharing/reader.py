#
# Copyright (C) 2021 The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from collections import defaultdict
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple
from urllib.parse import urlparse
from json import loads, dump
from urllib.request import getproxies

import delta_kernel_rust_sharing_wrapper
import fsspec
import logging
import os
import pandas as pd
import pyarrow as pa
import tempfile
import time
from pyarrow.dataset import dataset
from pyarrow.parquet import ParquetFile

from delta_sharing.converter import get_empty_table, to_arrow_schema, to_arrow_type, to_converters
from delta_sharing.credentials import (
    DIR_ACCESS_MODE,
    TemporaryTableCredentialsProvider,
    choose_access_mode,
    is_dir_readable_scheme,
    to_object_store_options,
)
from delta_sharing.protocol import AddCdcFile, CdfOptions, FileAction, Table
from delta_sharing.rest_client import DataSharingRestClient
from delta_sharing.fake_checkpoint import get_fake_checkpoint_byte_array


class DeltaSharingReader:
    def __init__(
        self,
        table: Table,
        rest_client: DataSharingRestClient,
        *,
        predicateHints: Optional[Sequence[str]] = None,
        jsonPredicateHints: Optional[str] = None,
        limit: Optional[int] = None,
        version: Optional[int] = None,
        timestamp: Optional[str] = None,
        use_delta_format: Optional[bool] = None,
        convert_in_batches: bool = False,
        access_mode: Optional[str] = None,
    ):
        self._table = table
        self._rest_client = rest_client

        if predicateHints is not None:
            assert isinstance(predicateHints, Sequence)
            assert all(isinstance(predicateHint, str) for predicateHint in predicateHints)
        self._predicateHints = predicateHints
        self._jsonPredicateHints = jsonPredicateHints

        if limit is not None:
            assert isinstance(limit, int) and limit >= 0, "'limit' must be a non-negative int"
        self._limit = limit
        self._version = version
        self._timestamp = timestamp
        self._use_delta_format = use_delta_format
        self._convert_in_batches = convert_in_batches
        self._access_mode = access_mode
        self._dir_credentials_provider_cache: Optional[TemporaryTableCredentialsProvider] = None

    @property
    def table(self) -> Table:
        return self._table

    def predicateHints(self, predicateHints: Optional[Sequence[str]]) -> "DeltaSharingReader":
        return self._copy(
            predicateHints=predicateHints,
            jsonPredicateHints=self._jsonPredicateHints,
            limit=self._limit,
            version=self._version,
            timestamp=self._timestamp,
        )

    def jsonPredicateHints(self, jsonPredicateHints: Optional[str]) -> "DeltaSharingReader":
        return self._copy(
            predicateHints=self._predicateHints,
            jsonPredicateHints=jsonPredicateHints,
            limit=self._limit,
            version=self._version,
            timestamp=self._timestamp,
        )

    def limit(self, limit: Optional[int]) -> "DeltaSharingReader":
        return self._copy(
            predicateHints=self._predicateHints,
            jsonPredicateHints=self._jsonPredicateHints,
            limit=limit,
            version=self._version,
            timestamp=self._timestamp,
        )

    def __snapshot_scan_kernel(self):
        """
        Build a delta-kernel snapshot scan. The caller owns the returned temp dir
        and must keep it alive until the scan result is fully consumed.
        """
        temp_dir = tempfile.TemporaryDirectory()
        try:
            self._rest_client.set_delta_format_header()
            try:
                response = self._rest_client.list_files_in_table(
                    self._table,
                    predicateHints=self._predicateHints,
                    jsonPredicateHints=self._jsonPredicateHints,
                    limitHint=self._limit,
                    version=self._version,
                    timestamp=self._timestamp,
                )
            finally:
                self._rest_client.remove_delta_format_header()

            lines = list(response.lines)
            table_path = self.__write_temp_delta_log_snapshot(temp_dir.name, lines)
            num_files = len(lines)

            interface = delta_kernel_rust_sharing_wrapper.PythonInterface(table_path)
            table = delta_kernel_rust_sharing_wrapper.Table(table_path)
            snapshot = table.snapshot(interface)
            scan = delta_kernel_rust_sharing_wrapper.ScanBuilder(snapshot).build()
            return temp_dir, scan.execute(interface), num_files
        except Exception:
            temp_dir.cleanup()
            raise

    def __to_pandas_kernel(self):
        """
        This function calls delta-kernel-rust python wrapper to load a df for a table
        with advanced reader features. It sets the header of the request to delta format
        with client reader features added. It then saves the resposne into a temporary
        json file stored in temporary storage. It calls delta-kernel-rust python wrapper
        to return the df.

        Returns: a pandas df
        """
        temp_dir, batches, num_files = self.__snapshot_scan_kernel()
        try:
            # The table is empty so use the schema to return an empty table with correct col names
            if num_files == 0:
                schema = batches.schema
                return pd.DataFrame(columns=schema.names)

            if self._convert_in_batches:
                pdfs = [batch.to_pandas(self_destruct=True) for batch in batches]
                print(f"Received {len(pdfs)} batches of data.")
                result = pd.concat(pdfs, axis=0, ignore_index=True, copy=False)
            else:
                result = pa.Table.from_batches(batches).to_pandas(self_destruct=True)

            # Apply residual limit that was not handled from server pushdown
            return result.head(self._limit)
        finally:
            # Delete the temp folder explicitly.
            temp_dir.cleanup()

    def __record_batches_kernel(self) -> Tuple[pa.Schema, Iterator[pa.RecordBatch]]:
        temp_dir, scan_result, _ = self.__snapshot_scan_kernel()

        def iterator() -> Iterator[pa.RecordBatch]:
            left = self._limit
            try:
                for batch in scan_result:
                    if left is not None and left == 0:
                        return
                    if left is not None and batch.num_rows > left:
                        batch = batch.slice(0, left)
                    yield batch
                    if left is not None:
                        left -= batch.num_rows
            finally:
                temp_dir.cleanup()

        return scan_result.schema, iterator()

    def _resolve_access_mode(self) -> Optional[str]:
        """The caller's access-mode preference, falling back to the DELTA_SHARING_ACCESS_MODE env
        var, or None (auto / URL-based) when neither is set."""
        if self._access_mode is not None:
            return self._access_mode.lower()
        env = os.environ.get("DELTA_SHARING_ACCESS_MODE")
        return env.lower() if env else None

    def _dir_credentials_provider(self) -> TemporaryTableCredentialsProvider:
        if self._dir_credentials_provider_cache is None:
            self._dir_credentials_provider_cache = TemporaryTableCredentialsProvider(
                self._rest_client, self._table
            )
        return self._dir_credentials_provider_cache

    def __to_pandas_dir_negotiated(self) -> pd.DataFrame:
        """Read the table via directory-based access after confirming the server offers it.

        Raises a descriptive error (rather than silently downgrading) when 'dir' was explicitly
        requested but cannot be served, per the protocol's Access Modes compatibility table.
        """
        # Directory-based access is delta-format only, and tables with advanced reader features
        # (e.g. deletion vectors) reject a parquet-format metadata query, so request delta caps.
        self._rest_client.set_sharing_capabilities_header()
        try:
            metadata = self._rest_client.query_table_metadata(self._table).metadata
        finally:
            self._rest_client.remove_sharing_capabilities_header()
        location = metadata.location
        if metadata.auxiliary_locations:
            # The table's files span locations beyond `location`; dir access reads only `location`
            # and would return incomplete results, so require the URL path for these tables.
            raise NotImplementedError(
                "Directory-based access does not yet support tables with auxiliary storage "
                "locations; use access_mode='url'."
            )
        version_requested = self._version is not None or self._timestamp is not None
        choose_access_mode(
            DIR_ACCESS_MODE,
            metadata.access_modes,
            location,
            version_requested=version_requested,
            location_readable=is_dir_readable_scheme(location),
        )
        return self.__to_pandas_dir(location)

    def __to_pandas_dir(self, location: str) -> pd.DataFrame:
        """Read a Delta table directly from object storage using temporary cloud credentials.

        Unlike the URL-based kernel path, no per-file pre-signed URLs are fetched and no temporary
        delta log is materialized: the server issues prefix-scoped credentials and delta-kernel reads
        the real delta log and data files from `location` via the cloud storage API.
        """
        creds = self._dir_credentials_provider().get(location)
        storage_options = to_object_store_options(creds, location)
        table_location = creds.location or location

        interface = delta_kernel_rust_sharing_wrapper.PythonInterface(
            table_location, storage_options
        )
        table = delta_kernel_rust_sharing_wrapper.Table(table_location)
        snapshot = table.snapshot(interface)
        scan = delta_kernel_rust_sharing_wrapper.ScanBuilder(snapshot).build()

        reader = scan.execute(interface)
        schema = reader.schema
        if self._convert_in_batches:
            pdfs = [batch.to_pandas(self_destruct=True) for batch in reader]
            if not pdfs:
                return pd.DataFrame(columns=schema.names)
            result = pd.concat(pdfs, axis=0, ignore_index=True, copy=False)
        else:
            result = pa.Table.from_batches(list(reader), schema).to_pandas(self_destruct=True)

        if self._limit is not None:
            return result.head(self._limit)
        return result

    def to_pandas(self) -> pd.DataFrame:
        # Opt-in directory-based access (access_mode="dir" or DELTA_SHARING_ACCESS_MODE=dir).
        if self._resolve_access_mode() == DIR_ACCESS_MODE:
            return self.__to_pandas_dir_negotiated()

        response_format = ""
        # If client does not specify which format to use, autoresolve it.
        # Otherwise use the specified format.
        if self._use_delta_format is None:
            response_format = self._rest_client.autoresolve_query_format(self._table)
        elif self._use_delta_format:
            response_format = response_format = DataSharingRestClient.DELTA_FORMAT

        # If the response format is delta, use delta kernel rust
        if response_format == DataSharingRestClient.DELTA_FORMAT:
            return self.__to_pandas_kernel()

        # Otherwise use the standard approach
        response, schema_json = self._list_files()

        if len(response.add_files) == 0 or self._limit == 0:
            return get_empty_table(schema_json)

        converters = to_converters(schema_json)

        if self._limit is None:
            pdfs = [
                DeltaSharingReader._to_pandas(
                    file, converters, False, None, self._convert_in_batches
                )
                for file in response.add_files
            ]
        else:
            left = self._limit
            pdfs = []
            for file in response.add_files:
                pdf = DeltaSharingReader._to_pandas(
                    file, converters, False, left, self._convert_in_batches
                )
                pdfs.append(pdf)
                left -= len(pdf)
                assert (
                    left >= 0
                ), f"'_to_pandas' returned too many rows. Required: {left}, returned: {len(pdf)}"
                if left == 0:
                    break

        merged = pd.concat(
            pdfs,
            axis=0,
            ignore_index=True,
            copy=False,
        )

        col_map = {}
        for col in merged.columns:
            col_map[col.lower()] = col

        return merged[[col_map[field["name"].lower()] for field in schema_json["fields"]]]

    def to_arrow(self) -> pa.Table:
        schema, batches = self._to_arrow_stream()
        return pa.Table.from_batches(list(batches), schema=schema)

    def to_record_batches(self) -> Iterator[pa.RecordBatch]:
        """
        Batches are produced lazily; consume the iterator fully (or close it) so
        that temporary resources backing the stream are released promptly.
        """
        _, batches = self._to_arrow_stream()
        return batches

    def to_record_batch_reader(self) -> pa.RecordBatchReader:
        """
        Batches are produced lazily; read the reader fully (or close it) so that
        temporary resources backing the stream are released promptly.
        """
        schema, batches = self._to_arrow_stream()
        return pa.RecordBatchReader.from_batches(schema, batches)

    def _to_arrow_stream(self) -> Tuple[pa.Schema, Iterator[pa.RecordBatch]]:
        response_format = ""
        if self._use_delta_format is None:
            response_format = self._rest_client.autoresolve_query_format(self._table)
        elif self._use_delta_format:
            response_format = DataSharingRestClient.DELTA_FORMAT

        if response_format == DataSharingRestClient.DELTA_FORMAT:
            return self.__record_batches_kernel()

        response, schema_json = self._list_files()
        schema = to_arrow_schema(schema_json)

        if len(response.add_files) == 0 or self._limit == 0:
            return schema, iter(())

        def iterator() -> Iterator[pa.RecordBatch]:
            left = self._limit
            for file in response.add_files:
                file_limit = left
                for batch in DeltaSharingReader._to_record_batches(file, schema_json, file_limit):
                    yield batch
                    if left is not None:
                        left -= batch.num_rows
                        if left < 0:
                            raise RuntimeError(
                                "'_to_record_batches' returned more rows than the "
                                f"requested limit of {file_limit}"
                            )
                        if left == 0:
                            return

        return schema, iterator()

    def _list_files(self):
        response = self._rest_client.list_files_in_table(
            self._table,
            predicateHints=self._predicateHints,
            jsonPredicateHints=self._jsonPredicateHints,
            limitHint=self._limit,
            version=self._version,
            timestamp=self._timestamp,
        )
        schema_json = loads(response.metadata.schema_string)
        return response, schema_json

    def __write_temp_delta_log_snapshot(self, temp_dir: str, lines: List[str]) -> str:
        delta_log_dir_name = temp_dir
        table_path = "file:///" + delta_log_dir_name

        # Create a new directory named '_delta_log' within the temporary directory
        log_dir = os.path.join(delta_log_dir_name, "_delta_log")
        os.makedirs(log_dir)

        # Create a new .json file within the '_delta_log' directory
        json_file_name = "0".zfill(20) + ".json"
        json_file_path = os.path.join(log_dir, json_file_name)
        json_file = open(json_file_path, "w+")

        # Write the protocol action to the log file
        protocol_json = loads(lines.pop(0))
        deltaProtocol = {"protocol": protocol_json["protocol"]["deltaProtocol"]}
        dump(deltaProtocol, json_file)
        json_file.write("\n")

        # Write the metadata action to the log file
        metadata_json = loads(lines.pop(0))
        deltaMetadata = {"metaData": metadata_json["metaData"]["deltaMetadata"]}
        dump(deltaMetadata, json_file)
        json_file.write("\n")

        # Write the add file actions to the log file
        for line in lines:
            line_json = loads(line)
            dump(line_json["file"]["deltaSingleAction"], json_file)
            json_file.write("\n")

        # Close the file
        json_file.close()
        return table_path

    def __write_temp_delta_log_cdf(
        self,
        log_dir: str,
        delta_protocol: dict,
        min_version: int,
        max_version: int,
        version_to_metadata: Dict[int, Any],
        version_to_actions: Dict[int, Any],
        version_to_timestamp: Dict[int, int],
    ):
        min_version_file_name = str(min_version).zfill(20) + ".json"
        min_version_path = os.path.join(log_dir, min_version_file_name)
        with open(min_version_path, "w+") as min_version_file:
            dump(delta_protocol, min_version_file)
            min_version_file.write("\n")

        num_versions_with_action = len(version_to_actions)
        for version in range(min_version, max_version + 1):
            log_file_name = str(version).zfill(20) + ".json"
            log_file_path = os.path.join(log_dir, log_file_name)
            with open(log_file_path, "a+") as log_file:
                if version in version_to_metadata:
                    dump(version_to_metadata[version], log_file)
                    log_file.write("\n")
                for action in version_to_actions[version]:
                    dump(action, log_file)
                    log_file.write("\n")
            # Ensure log file modification time matches the version timestamp
            # _commit_timestamp of an action is populated by log file modification time
            if version in version_to_timestamp:
                # os.utime accepts seconds while delta log timestamp is in ms
                os.utime(log_file_path, times=(0, version_to_timestamp[version] // 1000))

        if min_version > 0 and num_versions_with_action > 0:
            # Fake checkpoint so kernel reads logs from the start version
            checkpoint_version = min_version - 1
            checkpoint_file_name = str(checkpoint_version).zfill(20) + ".checkpoint.parquet"
            with open(os.path.join(log_dir, checkpoint_file_name), "w+b") as checkpoint_file:
                checkpoint_file.write(get_fake_checkpoint_byte_array())
                checkpoint_file.close()

            # Ensure _last_checkpoint points to the fake checkpoint
            last_checkpoint_content = (
                f'{{"version":{min_version - 1},"size":{len(get_fake_checkpoint_byte_array())}}}'
            )
            last_checkpoint_path = os.path.join(log_dir, "_last_checkpoint")
            with open(last_checkpoint_path, "w+") as last_checkpoint_file:
                last_checkpoint_file.write(last_checkpoint_content)
                last_checkpoint_file.close()

    def __table_changes_to_pandas_kernel(self, cdfOptions: CdfOptions) -> pd.DataFrame:
        # Create a temporary directory using the tempfile module
        temp_dir = tempfile.TemporaryDirectory()
        self._rest_client.set_delta_format_header(for_cdf=True)
        try:
            response = self._rest_client.list_table_changes(self._table, cdfOptions)
            lines = response.lines

            # first line is protocol
            protocol_json = loads(lines.pop(0))
            delta_protocol = {"protocol": protocol_json["protocol"]["deltaProtocol"]}
            start_version = cdfOptions.starting_version

            min_version = start_version if start_version is not None else (10**20 - 1)
            max_version = 0
            version_to_actions = defaultdict(list)
            version_to_metadata = {}
            version_to_timestamp = {}

            # Construct map from version to actions that took place in that version
            line_count = 1
            for line in lines:
                line_count += 1
                line_json = loads(line)
                if "file" in line_json:
                    file = line_json["file"]
                    action = file["deltaSingleAction"]
                    version = file["version"]
                    min_version = min(min_version, version)
                    max_version = max(max_version, version)
                    version_to_timestamp[version] = file["timestamp"]
                    version_to_actions[version].append(action)
                elif "metaData" in line_json:
                    metadata = line_json["metaData"]
                    delta_metadata = {"metaData": metadata["deltaMetadata"]}
                    version = metadata["version"]
                    min_version = min(min_version, version)
                    max_version = max(max_version, version)
                    version_to_metadata[version] = delta_metadata
                else:
                    raise Exception(f"Invalid JSON object:\n{line}\nIs neither metadata nor file.")

            num_versions_with_action = len(version_to_actions)
            print(
                f"table_changes stats: min_version={min_version}, "
                f"max_version={max_version}, "
                f"num_versions_with_action={num_versions_with_action}, "
                f"num_versions_with_metadata={len(version_to_metadata)}, "
                f"lines_in_response={line_count}, "
            )
            delta_log_dir_name = temp_dir.name
            table_path = "file:///" + delta_log_dir_name

            # Create a new directory named '_delta_log' within the temporary directory
            log_dir = os.path.join(delta_log_dir_name, "_delta_log")
            os.makedirs(log_dir)
            self.__write_temp_delta_log_cdf(
                log_dir,
                delta_protocol,
                min_version,
                max_version,
                version_to_metadata,
                version_to_actions,
                version_to_timestamp,
            )

            # Invoke delta-kernel-rust to return the pandas dataframe
            interface = delta_kernel_rust_sharing_wrapper.PythonInterface(table_path)
            table = delta_kernel_rust_sharing_wrapper.Table(table_path)
            scan = delta_kernel_rust_sharing_wrapper.TableChangesScanBuilder(
                table, interface, min_version, max_version
            ).build()

            scan_result = scan.execute(interface)
            if num_versions_with_action == 0:
                schema = scan_result.schema
                result = pd.DataFrame(columns=schema.names)
            elif self._convert_in_batches:
                pdfs = [batch.to_pandas(self_destruct=True) for batch in scan_result]
                result = pd.concat(pdfs, axis=0, ignore_index=True, copy=False)
            else:
                result = pa.Table.from_batches(scan_result).to_pandas(self_destruct=True)
        finally:
            # Delete the temp folder explicitly and remove the delta format from header
            temp_dir.cleanup()
            self._rest_client.remove_delta_format_header()

        return result

    def table_changes_to_pandas(self, cdfOptions: CdfOptions) -> pd.DataFrame:
        if self._resolve_access_mode() == DIR_ACCESS_MODE:
            raise NotImplementedError(
                "Directory-based access (access_mode='dir') is not yet supported for table changes "
                "(CDF); use the default URL-based access."
            )

        # Only use delta format if explicitly specified
        if self._use_delta_format:
            return self.__table_changes_to_pandas_kernel(cdfOptions)

        response = self._rest_client.list_table_changes(self._table, cdfOptions)

        schema_json = loads(response.metadata.schema_string)
        converters = to_converters(schema_json)
        schema_with_cdf = self._add_special_cdf_schema(schema_json)

        if len(response.actions) == 0:
            return get_empty_table(schema_with_cdf)

        pdfs = []
        for action in response.actions:
            pdf = DeltaSharingReader._to_pandas(
                action, converters, True, None, self._convert_in_batches
            )
            pdfs.append(pdf)

        merged = pd.concat(pdfs, axis=0, ignore_index=True, copy=False)

        col_map = {}
        for col in merged.columns:
            col_map[col.lower()] = col

        return merged[[col_map[field["name"].lower()] for field in schema_with_cdf["fields"]]]

    def _copy(
        self,
        *,
        predicateHints: Optional[Sequence[str]],
        jsonPredicateHints: Optional[str],
        limit: Optional[int],
        version: Optional[int],
        timestamp: Optional[str],
    ) -> "DeltaSharingReader":
        return DeltaSharingReader(
            table=self._table,
            rest_client=self._rest_client,
            predicateHints=predicateHints,
            limit=limit,
            version=version,
            timestamp=timestamp,
        )

    _RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
    _RETRYABLE_MARKERS = (
        "slowdown",
        "service unavailable",
        "too many requests",
        "reduce your request rate",
        "connection reset",
        "connection aborted",
        "timed out",
    )

    @staticmethod
    def _is_transient_error(error: Exception) -> bool:
        # aiohttp ClientResponseError exposes `.status`; requests HTTPError exposes `.response`.
        status = getattr(error, "status", None)
        if status is None:
            status = getattr(getattr(error, "response", None), "status_code", None)
        if status in DeltaSharingReader._RETRYABLE_STATUS:
            return True
        message = str(error).lower()
        return any(marker in message for marker in DeltaSharingReader._RETRYABLE_MARKERS)

    @staticmethod
    def _read_file_with_retry(read_fn, num_retries: int = 5, initial_sleep_ms: int = 250):
        sleep_ms = initial_sleep_ms
        for attempt in range(num_retries + 1):
            try:
                return read_fn()
            except Exception as e:
                if attempt >= num_retries or not DeltaSharingReader._is_transient_error(e):
                    raise
                logging.warning(f"Retrying file read in {sleep_ms}ms after transient error: {e}")
                time.sleep(sleep_ms / 1000)
                sleep_ms *= 2

    @staticmethod
    def _to_pandas(
        action: FileAction,
        converters: Dict[str, Callable[[str], Any]],
        for_cdf: bool,
        limit: Optional[int],
        convert_in_batches: bool,
    ) -> pd.DataFrame:
        filesystem = DeltaSharingReader._parquet_filesystem(action.url)

        def read_file() -> pd.DataFrame:
            # Pre-signed cloud URLs can return transient 429/5xx (e.g. S3 throttling when a query
            # spans many files); fsspec's http reader does not retry, so wrap the whole read.
            if convert_in_batches:
                pa_file = ParquetFile(action.url, filesystem=filesystem)
                pdfs = []
                rows_read = 0
                for batch in pa_file.iter_batches():
                    rows_read += len(batch)
                    pdfs.append(
                        batch.to_pandas(
                            date_as_object=True,
                            use_threads=False,
                            split_blocks=False,
                            self_destruct=True,
                        )
                    )
                    if limit is not None and rows_read >= limit:
                        break

                print(f"Received {len(pdfs)} batches of data.")
                out = pd.concat(pdfs, axis=0, ignore_index=True, copy=False)
                return out.head(limit) if limit is not None else out

            pa_dataset = dataset(source=action.url, format="parquet", filesystem=filesystem)
            pa_table = pa_dataset.head(limit) if limit is not None else pa_dataset.to_table()
            return pa_table.to_pandas(
                date_as_object=True, use_threads=False, split_blocks=False, self_destruct=True
            )

        pdf = DeltaSharingReader._read_file_with_retry(read_file)

        lowered_cols = set()
        for col in pdf.columns:
            lowered_cols.add(col.lower())

        for col, converter in converters.items():
            lowered = col.lower()
            if lowered not in lowered_cols:
                if col in action.partition_values:
                    if converter is not None:
                        pdf[col] = converter(action.partition_values[col])
                    else:
                        raise ValueError("Cannot partition on binary or complex columns")
                else:
                    pdf[col] = None

        if for_cdf:
            # Add the change type col name to non cdc actions.
            if not isinstance(action, AddCdcFile):
                pdf[DeltaSharingReader._change_type_col_name()] = action.get_change_type_col_value()

            # If available, add timestamp and version columns from the action.
            # All rows of the dataframe will get the same value.
            if action.version is not None:
                assert DeltaSharingReader._commit_version_col_name() not in pdf.columns
                pdf[DeltaSharingReader._commit_version_col_name()] = action.version

            if action.timestamp is not None:
                assert DeltaSharingReader._commit_timestamp_col_name() not in pdf.columns
                pdf[DeltaSharingReader._commit_timestamp_col_name()] = action.timestamp
        return pdf

    @staticmethod
    def _to_record_batches(
        action: FileAction,
        schema_json: dict,
        limit: Optional[int],
    ) -> Iterator[pa.RecordBatch]:
        filesystem = DeltaSharingReader._parquet_filesystem(action.url)
        pa_dataset = dataset(source=action.url, format="parquet", filesystem=filesystem)
        scanner = pa_dataset.scanner()
        rows_read = 0

        for batch in scanner.to_batches():
            if limit is not None and rows_read == limit:
                return

            if limit is not None and rows_read + batch.num_rows > limit:
                batch = batch.slice(0, limit - rows_read)

            yield DeltaSharingReader._normalize_record_batch(batch, action, schema_json)
            rows_read += batch.num_rows

    @staticmethod
    def _parquet_filesystem(action_url: str):
        url = urlparse(action_url)
        if "storage.googleapis.com" in (url.netloc.lower()):
            import delta_sharing._yarl_patch  # noqa: F401

        protocol = url.scheme
        proxy = getproxies()
        if len(proxy) != 0:
            filesystem = fsspec.filesystem(protocol, client_kwargs={"trust_env": True})
        else:
            filesystem = fsspec.filesystem(protocol)

        return filesystem

    @staticmethod
    def _normalize_record_batch(
        batch: pa.RecordBatch,
        action: FileAction,
        schema_json: dict,
    ) -> pa.RecordBatch:
        columns = []
        names = []
        lower_to_index = {name.lower(): index for index, name in enumerate(batch.schema.names)}
        num_rows = batch.num_rows

        for field in schema_json["fields"]:
            field_name = field["name"]
            lower_name = field_name.lower()
            names.append(field_name)
            field_type = to_arrow_type(field["type"])

            if lower_name in lower_to_index:
                column = batch.column(lower_to_index[lower_name])
                if column.type != field_type:
                    column = column.cast(field_type)
                columns.append(column)
                continue

            if field_name in action.partition_values:
                schema_type = field["type"]
                if schema_type == "binary" or isinstance(schema_type, dict):
                    raise ValueError("Cannot partition on binary or complex columns")
                value = action.partition_values[field_name]
                if value is None or value == "":
                    columns.append(pa.nulls(num_rows, type=field_type))
                    continue

                values = pa.array([value] * num_rows)
                if schema_type == "timestamp":
                    values = values.cast(pa.timestamp("us"))
                columns.append(values.cast(field_type))
            else:
                columns.append(pa.nulls(num_rows, type=field_type))

        return pa.RecordBatch.from_arrays(columns, names=names)

    # The names of special delta columns for cdf.

    @staticmethod
    def _change_type_col_name():
        return "_change_type"

    @staticmethod
    def _commit_timestamp_col_name():
        return "_commit_timestamp"

    @staticmethod
    def _commit_version_col_name():
        return "_commit_version"

    @staticmethod
    def _add_special_cdf_schema(schema_json: dict) -> dict:
        fields = schema_json["fields"]
        fields.append({"name": DeltaSharingReader._change_type_col_name(), "type": "string"})
        fields.append({"name": DeltaSharingReader._commit_version_col_name(), "type": "long"})
        fields.append({"name": DeltaSharingReader._commit_timestamp_col_name(), "type": "long"})
        return schema_json
