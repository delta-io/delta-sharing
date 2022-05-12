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
from typing import Any, Callable, Dict, Optional, Sequence
from urllib.parse import urlparse
from json import loads

import fsspec
import pandas as pd
from pyarrow.dataset import dataset

from delta_sharing.converter import to_converters, get_empty_table
from delta_sharing.protocol import AddFile, Table, CdfOptions, FileAction
from delta_sharing.rest_client import DataSharingRestClient


class DeltaSharingReader:
    def __init__(
        self,
        table: Table,
        rest_client: DataSharingRestClient,
        *,
        predicateHints: Optional[Sequence[str]] = None,
        limit: Optional[int] = None,
    ):
        self._table = table
        self._rest_client = rest_client

        if predicateHints is not None:
            assert isinstance(predicateHints, Sequence)
            assert all(isinstance(predicateHint, str) for predicateHint in predicateHints)
        self._predicateHints = predicateHints

        if limit is not None:
            assert isinstance(limit, int) and limit >= 0, "'limit' must be a non-negative int"
        self._limit = limit

    @property
    def table(self) -> Table:
        return self._table

    def predicateHints(self, predicateHints: Optional[Sequence[str]]) -> "DeltaSharingReader":
        return self._copy(predicateHints=predicateHints, limit=self._limit)

    def limit(self, limit: Optional[int]) -> "DeltaSharingReader":
        return self._copy(predicateHints=self._predicateHints, limit=limit)

    def to_pandas(self) -> pd.DataFrame:
        response = self._rest_client.list_files_in_table(
            self._table, predicateHints=self._predicateHints, limitHint=self._limit
        )

        schema_json = loads(response.metadata.schema_string)

        if len(response.add_files) == 0 or self._limit == 0:
            return get_empty_table(schema_json)

        converters = to_converters(schema_json)

        if self._limit is None:
            pdfs = [
                DeltaSharingReader._to_pandas(file, converters, None) for file in response.add_files
            ]
        else:
            left = self._limit
            pdfs = []
            for file in response.add_files:
                pdf = DeltaSharingReader._to_pandas(file, converters, left)
                pdfs.append(pdf)
                left -= len(pdf)
                assert (
                    left >= 0
                ), f"'_to_pandas' returned too many rows. Required: {left}, returned: {len(pdf)}"
                if left == 0:
                    break

        return pd.concat(
            pdfs,
            axis=0,
            ignore_index=True,
            copy=False,
        )[[field["name"] for field in schema_json["fields"]]]

    def table_changes_to_pandas(self, cdfOptions: CdfOptions) -> pd.DataFrame:
        response = self._rest_client.list_table_changes(self._table, cdfOptions)

        schema_json = loads(response.metadata.schema_string)

        if len(response.actions) == 0 or self._limit == 0:
            return get_empty_table(schema_json)

        converters = to_converters(schema_json)
        pdfs = []
        for action in response.actions:
            pdf = DeltaSharingReader._to_pandas(action, converters, None)
            pdfs.append(pdf)

        return pd.concat(pdfs, axis=0, ignore_index=True, copy=False)

    def _copy(
        self, *, predicateHints: Optional[Sequence[str]], limit: Optional[int]
    ) -> "DeltaSharingReader":
        return DeltaSharingReader(
            table=self._table,
            rest_client=self._rest_client,
            predicateHints=predicateHints,
            limit=limit,
        )

    @staticmethod
    def _to_pandas(
        action: FileAction, converters: Dict[str, Callable[[str], Any]], limit: Optional[int]
    ) -> pd.DataFrame:
        url = urlparse(action.url)
        if "storage.googleapis.com" in (url.netloc.lower()):
            # Apply the yarl patch for GCS pre-signed urls
            import delta_sharing._yarl_patch  # noqa: F401

        protocol = url.scheme
        filesystem = fsspec.filesystem(protocol)

        pa_dataset = dataset(source=action.url, format="parquet", filesystem=filesystem)
        pa_table = pa_dataset.head(limit) if limit is not None else pa_dataset.to_table()
        pdf = pa_table.to_pandas(
            date_as_object=True, use_threads=False, split_blocks=True, self_destruct=True
        )

        for col, converter in converters.items():
            if col not in pdf.columns:
                if col in action.partition_values:
                    if converter is not None:
                        pdf[col] = converter(action.partition_values[col])
                    else:
                        raise ValueError("Cannot partition on binary or complex columns")
                else:
                    pdf[col] = None

        # If available, add timestamp and version columns from the action.
        # All rows of the dataframe will get the same value.
        if action.timestamp is not None:
            assert DeltaSharingReader._current_timestamp_col_name() not in pdf.columns
            pdf[DeltaSharingReader._current_timestamp_col_name()] = action.timestamp

        if action.version is not None:
            assert DeltaSharingReader._current_version_col_name() not in pdf.columns
            pdf[DeltaSharingReader._current_version_col_name()] = action.version

        return pdf

    @staticmethod
    def _current_timestamp_col_name():
        return "_current_timestamp"

    @staticmethod
    def _current_version_col_name():
        return "_current_version"
