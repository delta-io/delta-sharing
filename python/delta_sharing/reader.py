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
from typing import Any, Callable, Dict, List, Optional, Sequence
from urllib.parse import urlparse
from json import loads, dump
from urllib.request import getproxies

import delta_kernel_rust_sharing_wrapper
import fsspec
import os
import pandas as pd
import pyarrow as pa
import tempfile
from pyarrow.dataset import dataset

from delta_sharing.converter import to_converters, get_empty_table
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

    @property
    def table(self) -> Table:
        return self._table

    def predicateHints(self, predicateHints: Optional[Sequence[str]]) -> "DeltaSharingReader":
        return self._copy(
            predicateHints=predicateHints,
            jsonPredicateHints=self._jsonPredicateHints,
            limit=self._limit,
            version=self._version,
            timestamp=self._timestamp
        )

    def jsonPredicateHints(self, jsonPredicateHints: Optional[str]) -> "DeltaSharingReader":
        return self._copy(
            predicateHints=self._predicateHints,
            jsonPredicateHints=jsonPredicateHints,
            limit=self._limit,
            version=self._version,
            timestamp=self._timestamp
        )

    def limit(self, limit: Optional[int]) -> "DeltaSharingReader":
        return self._copy(
            predicateHints=self._predicateHints,
            jsonPredicateHints=self._jsonPredicateHints,
            limit=limit,
            version=self._version,
            timestamp=self._timestamp
        )

    def __to_pandas_kernel(self):
        """
        This function calls delta-kernel-rust python wrapper to load a df for a table
        with advanced reader features. It sets the header of the request to delta format
        with client reader features added. It then saves the resposne into a temporary
        json file stored in temporary storage. It calls delta-kernel-rust python wrapper
        to return the df.

        Returns: a pandas df
        """
        self._rest_client.set_delta_format_header()
        response = self._rest_client.list_files_in_table(
            self._table,
            predicateHints=self._predicateHints,
            jsonPredicateHints=self._jsonPredicateHints,
            limitHint=self._limit,
            version=self._version,
            timestamp=self._timestamp
        )

        lines = response.lines
        # Create a temporary directory using the tempfile module
        temp_dir = tempfile.TemporaryDirectory()
        table_path = self.__write_temp_delta_log_snapshot(temp_dir.name, lines)
        num_files = len(lines)

        # Invoke delta-kernel-rust to return the pandas dataframe
        interface = delta_kernel_rust_sharing_wrapper.PythonInterface(table_path)
        table = delta_kernel_rust_sharing_wrapper.Table(table_path)
        snapshot = table.snapshot(interface)
        scan = delta_kernel_rust_sharing_wrapper.ScanBuilder(snapshot).build()

        # The table is empty so use the schema to return an empty table with correct col names
        if (num_files == 0):
            schema = scan.execute(interface).schema
            return pd.DataFrame(columns=schema.names)

        table = pa.Table.from_batches(scan.execute(interface))
        result = table.to_pandas()

        # Apply residual limit that was not handled from server pushdown
        result = result.head(self._limit)

        # Delete the temp folder explicitly and remove the delta format from header
        temp_dir.cleanup()
        self._rest_client.remove_delta_format_header()

        return result

    def to_pandas(self) -> pd.DataFrame:
        response_format = ""
        # If client does not specify which format to use, autoresolve it.
        # Otherwise use the specified format.
        if self._use_delta_format is None:
            response_format = self._rest_client.autoresolve_query_format(self._table)
        elif self._use_delta_format:
            response_format = response_format = DataSharingRestClient.DELTA_FORMAT

        # If the response format is delta, use delta kernel rust
        if (response_format == DataSharingRestClient.DELTA_FORMAT):
            return self.__to_pandas_kernel()

        # Otherwise use the standard approach
        response = self._rest_client.list_files_in_table(
            self._table,
            predicateHints=self._predicateHints,
            jsonPredicateHints=self._jsonPredicateHints,
            limitHint=self._limit,
            version=self._version,
            timestamp=self._timestamp
        )

        schema_json = loads(response.metadata.schema_string)

        if len(response.add_files) == 0 or self._limit == 0:
            return get_empty_table(schema_json)

        converters = to_converters(schema_json)

        if self._limit is None:
            pdfs = [
                DeltaSharingReader._to_pandas(
                    file, converters, False, None) for file in response.add_files
            ]
        else:
            left = self._limit
            pdfs = []
            for file in response.add_files:
                pdf = DeltaSharingReader._to_pandas(file, converters, False, left)
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

    def __write_temp_delta_log_snapshot(self, temp_dir: str, lines: List[str]) -> str:
        delta_log_dir_name = temp_dir
        table_path = "file:///" + delta_log_dir_name

        # Create a new directory named '_delta_log' within the temporary directory
        log_dir = os.path.join(delta_log_dir_name, '_delta_log')
        os.makedirs(log_dir)

        # Create a new .json file within the '_delta_log' directory
        json_file_name = "0".zfill(20) + ".json"
        json_file_path = os.path.join(log_dir, json_file_name)
        json_file = open(json_file_path, 'w+')

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
        version_to_timestamp: Dict[int, int]
    ):
        min_version_file_name = str(min_version).zfill(20) + ".json"
        min_version_path = os.path.join(log_dir, min_version_file_name)
        with open(min_version_path, 'w+') as min_version_file:
            dump(delta_protocol, min_version_file)
            min_version_file.write("\n")

        num_versions_with_action = len(version_to_actions)
        for version in range(min_version, max_version + 1):
            log_file_name = str(version).zfill(20) + ".json"
            log_file_path = os.path.join(log_dir, log_file_name)
            with open(log_file_path, 'a+') as log_file:
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
            with open(os.path.join(log_dir, checkpoint_file_name), 'w+b') as checkpoint_file:
                checkpoint_file.write(get_fake_checkpoint_byte_array())
                checkpoint_file.close()

            # Ensure _last_checkpoint points to the fake checkpoint
            last_checkpoint_content = \
                f'{{"version":{min_version - 1},"size":{len(get_fake_checkpoint_byte_array())}}}'
            last_checkpoint_path = os.path.join(log_dir, '_last_checkpoint')
            with open(last_checkpoint_path, 'w+') as last_checkpoint_file:
                last_checkpoint_file.write(last_checkpoint_content)
                last_checkpoint_file.close()

    def __table_changes_to_pandas_kernel(self, cdfOptions: CdfOptions) -> pd.DataFrame:
        self._rest_client.set_delta_format_header()
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
                raise Exception(
                    f"Invalid JSON object:\n{line}\nIs neither metadata nor file."
                )

        num_versions_with_action = len(version_to_actions)
        print(
            f"table_changes stats: min_version={min_version}, "
            f"max_version={max_version}, "
            f"num_versions_with_action={num_versions_with_action}, "
            f"num_versions_with_metadata={len(version_to_metadata)}, "
            f"lines_in_response={line_count}, "
        )
        # Create a temporary directory using the tempfile module
        temp_dir = tempfile.TemporaryDirectory()
        try:
            delta_log_dir_name = temp_dir.name
            table_path = "file:///" + delta_log_dir_name

            # Create a new directory named '_delta_log' within the temporary directory
            log_dir = os.path.join(delta_log_dir_name, '_delta_log')
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

            if num_versions_with_action == 0:
                schema = scan.execute(interface).schema
                result = pd.DataFrame(columns=schema.names)
            else:
                table = pa.Table.from_batches(scan.execute(interface))
                result = table.to_pandas()
        finally:
            # Delete the temp folder explicitly and remove the delta format from header
            temp_dir.cleanup()
            self._rest_client.remove_delta_format_header()

        return result

    def table_changes_to_pandas(self, cdfOptions: CdfOptions) -> pd.DataFrame:
        # Only use delta format if explicitly specified
        if self._use_delta_format:
            return self.__table_changes_to_pandas_kernel(cdfOptions)

        response = self._rest_client.list_table_changes(self._table, cdfOptions)

        schema_json = loads(response.metadata.schema_string)

        if len(response.actions) == 0:
            return get_empty_table(self._add_special_cdf_schema(schema_json))

        converters = to_converters(schema_json)
        pdfs = []
        for action in response.actions:
            pdf = DeltaSharingReader._to_pandas(action, converters, True, None)
            pdfs.append(pdf)

        return pd.concat(pdfs, axis=0, ignore_index=True, copy=False)

    def _copy(
        self,
        *,
        predicateHints: Optional[Sequence[str]],
        jsonPredicateHints: Optional[str],
        limit: Optional[int],
        version: Optional[int],
        timestamp: Optional[str]
    ) -> "DeltaSharingReader":
        return DeltaSharingReader(
            table=self._table,
            rest_client=self._rest_client,
            predicateHints=predicateHints,
            limit=limit,
            version=version,
            timestamp=timestamp
        )

    @staticmethod
    def _to_pandas(
        action: FileAction,
        converters: Dict[str, Callable[[str], Any]],
        for_cdf: bool,
        limit: Optional[int]
    ) -> pd.DataFrame:
        url = urlparse(action.url)
        if "storage.googleapis.com" in (url.netloc.lower()):
            # Apply the yarl patch for GCS pre-signed urls
            import delta_sharing._yarl_patch  # noqa: F401

        protocol = url.scheme
        proxy = getproxies()
        if len(proxy) != 0:
            filesystem = fsspec.filesystem(protocol, client_kwargs={"trust_env":True})
        else:
            filesystem = fsspec.filesystem(protocol)

        pa_dataset = dataset(source=action.url, format="parquet", filesystem=filesystem)
        pa_table = pa_dataset.head(limit) if limit is not None else pa_dataset.to_table()
        pdf = pa_table.to_pandas(
            date_as_object=True, use_threads=False, split_blocks=True, self_destruct=True
        )

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
        fields.append({"name" : DeltaSharingReader._change_type_col_name(), "type" : "string"})
        fields.append({"name" : DeltaSharingReader._commit_version_col_name(), "type" : "long"})
        fields.append({"name" : DeltaSharingReader._commit_timestamp_col_name(), "type" : "long"})
        return schema_json
