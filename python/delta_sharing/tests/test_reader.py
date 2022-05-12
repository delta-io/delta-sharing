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
import pytest

from dataclasses import dataclass
from datetime import date
from typing import Optional, Sequence

import pandas as pd

from delta_sharing.protocol import AddFile, AddCdcFile, CdfOptions, Metadata, RemoveFile, Table
from delta_sharing.reader import DeltaSharingReader
from delta_sharing.rest_client import (
    ListFilesInTableResponse,
    ListTableChangesResponse,
    DataSharingRestClient,
)
from delta_sharing.tests.conftest import ENABLE_INTEGRATION, SKIP_MESSAGE


def test_to_pandas_non_partitioned(tmp_path):
    pdf1 = pd.DataFrame({"a": [1, 2, 3], "b": ["a", "b", "c"]})
    pdf2 = pd.DataFrame({"a": [4, 5, 6], "b": ["d", "e", "f"]})

    pdf1.to_parquet(tmp_path / "pdf1.parquet")
    pdf2.to_parquet(tmp_path / "pdf2.parquet")

    class RestClientMock:
        def list_files_in_table(
            self,
            table: Table,
            *,
            predicateHints: Optional[Sequence[str]] = None,
            limitHint: Optional[int] = None,
        ) -> ListFilesInTableResponse:
            assert table == Table("table_name", "share_name", "schema_name")

            metadata = Metadata(
                schema_string=(
                    '{"fields":['
                    '{"metadata":{},"name":"a","nullable":true,"type":"long"},'
                    '{"metadata":{},"name":"b","nullable":true,"type":"string"}'
                    '],"type":"struct"}'
                )
            )
            add_files = [
                AddFile(
                    url=str(tmp_path / "pdf1.parquet"),
                    id="pdf1",
                    partition_values={},
                    size=0,
                    stats="",
                ),
                AddFile(
                    url=str(tmp_path / "pdf2.parquet"),
                    id="pdf2",
                    partition_values={},
                    size=0,
                    stats="",
                ),
            ]
            return ListFilesInTableResponse(protocol=None, metadata=metadata, add_files=add_files)

    reader = DeltaSharingReader(Table("table_name", "share_name", "schema_name"), RestClientMock())
    pdf = reader.to_pandas()

    expected = pd.concat([pdf1, pdf2]).reset_index(drop=True)

    pd.testing.assert_frame_equal(pdf, expected)


def test_to_pandas_partitioned(tmp_path):
    pdf1 = pd.DataFrame({"a": [1, 2, 3]})
    pdf2 = pd.DataFrame({"a": [4, 5, 6]})

    pdf1.to_parquet(tmp_path / "pdf1.parquet")
    pdf2.to_parquet(tmp_path / "pdf2.parquet")

    class RestClientMock:
        def list_files_in_table(
            self,
            table: Table,
            *,
            predicateHints: Optional[Sequence[str]] = None,
            limitHint: Optional[int] = None,
        ) -> ListFilesInTableResponse:
            assert table == Table("table_name", "share_name", "schema_name")

            metadata = Metadata(
                schema_string=(
                    '{"fields":['
                    '{"metadata":{},"name":"a","nullable":true,"type":"long"},'
                    '{"metadata":{},"name":"b","nullable":true,"type":"string"}'
                    '],"type":"struct"}'
                )
            )
            add_files = [
                AddFile(
                    url=str(tmp_path / "pdf1.parquet"),
                    id="pdf1",
                    partition_values={"b": "x"},
                    size=0,
                    stats="",
                ),
                AddFile(
                    url=str(tmp_path / "pdf2.parquet"),
                    id="pdf2",
                    partition_values={"b": "y"},
                    size=0,
                    stats="",
                ),
            ]
            return ListFilesInTableResponse(protocol=None, metadata=metadata, add_files=add_files)

    reader = DeltaSharingReader(Table("table_name", "share_name", "schema_name"), RestClientMock())
    pdf = reader.to_pandas()

    expected1 = pdf1.copy()
    expected1["b"] = "x"
    expected2 = pdf2.copy()
    expected2["b"] = "y"
    expected = pd.concat([expected1, expected2]).reset_index(drop=True)

    pd.testing.assert_frame_equal(pdf, expected)


def test_to_pandas_partitioned_different_schemas(tmp_path):
    pdf1 = pd.DataFrame({"a": [1, 2, 3]})
    pdf2 = pd.DataFrame({"a": [4.0, 5.0, 6.0], "b": ["d", "e", "f"]})

    pdf1.to_parquet(tmp_path / "pdf1.parquet")
    pdf2.to_parquet(tmp_path / "pdf2.parquet")

    class RestClientMock:
        def list_files_in_table(
            self,
            table: Table,
            *,
            predicateHints: Optional[Sequence[str]] = None,
            limitHint: Optional[int] = None,
        ) -> ListFilesInTableResponse:
            assert table == Table("table_name", "share_name", "schema_name")

            metadata = Metadata(
                schema_string=(
                    '{"fields":['
                    '{"metadata":{},"name":"a","nullable":true,"type":"long"},'
                    '{"metadata":{},"name":"b","nullable":true,"type":"string"},'
                    '{"metadata":{},"name":"c","nullable":true,"type":"date"}'
                    '],"type":"struct"}'
                )
            )
            add_files = [
                AddFile(
                    url=str(tmp_path / "pdf1.parquet"),
                    id="pdf1",
                    partition_values={"c": "2021-01-01"},
                    size=0,
                    stats="",
                ),
                AddFile(
                    url=str(tmp_path / "pdf2.parquet"),
                    id="pdf2",
                    partition_values={"c": "2021-01-02"},
                    size=0,
                    stats="",
                ),
            ]
            return ListFilesInTableResponse(protocol=None, metadata=metadata, add_files=add_files)

    reader = DeltaSharingReader(Table("table_name", "share_name", "schema_name"), RestClientMock())
    pdf = reader.to_pandas()

    expected1 = pdf1.copy()
    expected1["c"] = date(2021, 1, 1)
    expected2 = pdf2.copy()
    expected2["c"] = date(2021, 1, 2)
    expected = pd.concat([expected1, expected2])[["a", "b", "c"]].reset_index(drop=True)

    pd.testing.assert_frame_equal(pdf, expected)


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_to_pandas_empty(rest_client: DataSharingRestClient):
    class RestClientMock:
        def list_files_in_table(
            self,
            table: Table,
            *,
            predicateHints: Optional[Sequence[str]] = None,
            limitHint: Optional[int] = None,
        ) -> ListFilesInTableResponse:
            assert table == Table("table_name", "share_name", "schema_name")

            metadata = Metadata(
                schema_string=(
                    '{"fields":['
                    '{"metadata":{},"name":"a","nullable":true,"type":"boolean"},'
                    '{"metadata":{},"name":"b","nullable":true,"type":"byte"},'
                    '{"metadata":{},"name":"c","nullable":true,"type":"short"},'
                    '{"metadata":{},"name":"d","nullable":true,"type":"integer"},'
                    '{"metadata":{},"name":"e","nullable":true,"type":"long"},'
                    '{"metadata":{},"name":"f","nullable":true,"type":"float"},'
                    '{"metadata":{},"name":"g","nullable":true,"type":"double"},'
                    '{"metadata":{},"name":"h","nullable":true,"type":"decimal(5,2)"},'
                    '{"metadata":{},"name":"i","nullable":true,"type":"string"},'
                    '{"metadata":{},"name":"j","nullable":true,"type":"binary"},'
                    '{"metadata":{},"name":"k","nullable":true,"type":"timestamp"},'
                    '{"metadata":{},"name":"l","nullable":true,"type":"date"},'
                    '{"metadata":{},"name":"m","nullable":true,"type":{"type":"array",'
                    '"elementType":"string","containsNull":true}},'
                    '{"metadata":{},"name":"n","nullable":true,"type":{"type":"struct","fields":'
                    '[{"name":"foo","type":"string","nullable":true,"metadata":{}},'
                    '{"name":"bar","type":"integer","nullable":true,"metadata":{}}]}},'
                    '{"metadata":{},"name":"o","nullable":true,"type":{"type":"map",'
                    '"keyType":"string","valueType":"integer","valueContainsNull":true}}'
                    '],"type":"struct"}'
                )
            )
            add_files: Sequence[AddFile] = []
            return ListFilesInTableResponse(protocol=None, metadata=metadata, add_files=add_files)

    reader = DeltaSharingReader(
        Table("table_name", "share_name", "schema_name"), RestClientMock()  # type: ignore
    )
    pdf = reader.to_pandas()

    reader = DeltaSharingReader(Table(name="table7", share="share1", schema="default"), rest_client)
    expected = reader.to_pandas().iloc[0:0]

    pd.testing.assert_frame_equal(pdf, expected)


def test_table_changes_to_pandas_non_partitioned(tmp_path):
    # Create basic data frame.
    pdf1 = pd.DataFrame({"a": [1, 2, 3], "b": ["a", "b", "c"]})
    pdf2 = pd.DataFrame({"a": [4, 5, 6], "b": ["d", "e", "f"]})
    pdf3 = pd.DataFrame({"a": [7, 8, 9], "b": ["x", "y", "z"]})
    pdf4 = pd.DataFrame({"a": [7, 8, 9], "b": ["x", "y", "z"]})

    # Add change type (which is present in the parquet files).
    pdf1["_change_type"] = "Insert"
    pdf2["_change_type"] = "Delete"
    pdf3["_change_type"] = "update_preimage"
    pdf4["_change_type"] = "update_postimage"

    # Save.
    pdf1.to_parquet(tmp_path / "pdf1.parquet")
    pdf2.to_parquet(tmp_path / "pdf2.parquet")
    pdf3.to_parquet(tmp_path / "pdf3.parquet")
    pdf4.to_parquet(tmp_path / "pdf4.parquet")

    # Version and timestamp are not in the parquet files; but are expected by the conversion.
    timestamp1 = "2022-04-27T15:32:21.000-0800"
    timestamp2 = "2022-04-28T15:32:21.000-0800"
    timestamp3 = "2022-04-29T15:32:21.000-0800"
    timestamp4 = "2022-04-29T15:32:21.000-0800"

    version1 = 1
    version2 = 2
    version3 = 3
    version4 = 4

    pdf1[DeltaSharingReader._current_timestamp_col_name()] = timestamp1
    pdf2[DeltaSharingReader._current_timestamp_col_name()] = timestamp2
    pdf3[DeltaSharingReader._current_timestamp_col_name()] = timestamp3
    pdf4[DeltaSharingReader._current_timestamp_col_name()] = timestamp4

    pdf1[DeltaSharingReader._current_version_col_name()] = version1
    pdf2[DeltaSharingReader._current_version_col_name()] = version2
    pdf3[DeltaSharingReader._current_version_col_name()] = version3
    pdf4[DeltaSharingReader._current_version_col_name()] = version4

    class RestClientMock:
        def list_table_changes(
            self, table: Table, cdfOptions: CdfOptions
        ) -> ListTableChangesResponse:
            assert table == Table("table_name", "share_name", "schema_name")

            metadata = Metadata(
                schema_string=(
                    '{"fields":['
                    '{"metadata":{},"name":"a","nullable":true,"type":"long"},'
                    '{"metadata":{},"name":"b","nullable":true,"type":"string"}'
                    '],"type":"struct"}'
                )
            )
            actions = [
                AddFile(
                    url=str(tmp_path / "pdf1.parquet"),
                    id="pdf1",
                    partition_values={},
                    size=0,
                    stats="",
                    timestamp=timestamp1,
                    version=version1,
                ),
                RemoveFile(
                    url=str(tmp_path / "pdf2.parquet"),
                    id="pdf2",
                    partition_values={},
                    size=0,
                    timestamp=timestamp2,
                    version=version2,
                ),
                AddCdcFile(
                    url=str(tmp_path / "pdf3.parquet"),
                    id="pdf3",
                    partition_values={},
                    size=0,
                    timestamp=timestamp3,
                    version=version3,
                ),
                AddCdcFile(
                    url=str(tmp_path / "pdf4.parquet"),
                    id="pdf4",
                    partition_values={},
                    size=0,
                    timestamp=timestamp4,
                    version=version4,
                ),
            ]
            return ListTableChangesResponse(protocol=None, metadata=metadata, actions=actions)

    reader = DeltaSharingReader(Table("table_name", "share_name", "schema_name"), RestClientMock())
    pdf = reader.table_changes_to_pandas(CdfOptions())

    expected = pd.concat([pdf1, pdf2, pdf3, pdf4]).reset_index(drop=True)
    pd.testing.assert_frame_equal(pdf, expected)


def test_table_changes_to_pandas_partitioned(tmp_path):
    pdf1 = pd.DataFrame({"a": [1, 2, 3]})
    pdf2 = pd.DataFrame({"a": [4, 5, 6]})

    pdf1["_change_type"] = "update_preimage"
    pdf2["_change_type"] = "update_postimage"

    pdf1.to_parquet(tmp_path / "pdf1.parquet")
    pdf2.to_parquet(tmp_path / "pdf2.parquet")

    # Version, timestamp, and partition are not in the parquet files; but will be populated.
    timestamp = "2022-04-27T15:32:21.000-0800"
    version = 10
    pdf1["b"] = "x"
    pdf2["b"] = "x"
    pdf1[DeltaSharingReader._current_timestamp_col_name()] = timestamp
    pdf2[DeltaSharingReader._current_timestamp_col_name()] = timestamp
    pdf1[DeltaSharingReader._current_version_col_name()] = version
    pdf2[DeltaSharingReader._current_version_col_name()] = version

    class RestClientMock:
        def list_table_changes(
            self,
            table: Table,
            cdfOptions: CdfOptions,
        ) -> ListTableChangesResponse:
            assert table == Table("table_name", "share_name", "schema_name")

            metadata = Metadata(
                schema_string=(
                    '{"fields":['
                    '{"metadata":{},"name":"a","nullable":true,"type":"long"},'
                    '{"metadata":{},"name":"b","nullable":true,"type":"string"}'
                    '],"type":"struct"}'
                )
            )
            actions = [
                AddCdcFile(
                    url=str(tmp_path / "pdf1.parquet"),
                    id="pdf1",
                    partition_values={"b": "x"},
                    size=0,
                    timestamp=timestamp,
                    version=version,
                ),
                AddCdcFile(
                    url=str(tmp_path / "pdf2.parquet"),
                    id="pdf2",
                    partition_values={"b": "x"},
                    size=0,
                    timestamp=timestamp,
                    version=version,
                ),
            ]
            return ListTableChangesResponse(protocol=None, metadata=metadata, actions=actions)

    reader = DeltaSharingReader(Table("table_name", "share_name", "schema_name"), RestClientMock())
    pdf = reader.table_changes_to_pandas(CdfOptions())

    expected = pd.concat([pdf1, pdf2]).reset_index(drop=True)
    pd.testing.assert_frame_equal(pdf, expected)
