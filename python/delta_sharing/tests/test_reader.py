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

from datetime import date
from typing import Optional, Sequence

import pandas as pd

from delta_sharing.protocol import AddFile, Metadata, Table
from delta_sharing.reader import DeltaSharingReader
from delta_sharing.rest_client import ListFilesInTableResponse, DataSharingRestClient
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
