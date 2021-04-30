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

from delta_exchange.protocol import (
    AddFile,
    Format,
    Metadata,
    Protocol,
    Schema,
    Share,
    Table,
)
from delta_exchange.rest_client import DataSharingRestClient
from delta_exchange.tests.conftest import SKIP, SKIP_MESSAGE


@pytest.mark.skipif(SKIP, reason=SKIP_MESSAGE)
def test_list_shares(rest_client: DataSharingRestClient):
    response = rest_client.list_shares()
    assert response.shares == [Share(name="share1"), Share(name="share2")]


@pytest.mark.skipif(SKIP, reason=SKIP_MESSAGE)
def test_list_schemas(rest_client: DataSharingRestClient):
    response = rest_client.list_schemas(Share(name="share1"))
    assert response.schemas == [Schema(name="default", share="share1")]

    response = rest_client.list_schemas(Share(name="share2"))
    assert response.schemas == [Schema(name="default", share="share2")]


@pytest.mark.skipif(SKIP, reason=SKIP_MESSAGE)
def test_list_tables(rest_client: DataSharingRestClient):
    response = rest_client.list_tables(Schema(name="default", share="share1"))
    assert response.tables == [
        Table(name="table1", share="share1", schema="default"),
        Table(name="table3", share="share1", schema="default"),
    ]

    response = rest_client.list_tables(Schema(name="default", share="share2"))
    assert response.tables == [Table(name="table2", share="share2", schema="default")]


@pytest.mark.skipif(SKIP, reason=SKIP_MESSAGE)
def test_query_table_metadata_1(rest_client: DataSharingRestClient):
    response = rest_client.query_table_metadata(
        Table(name="table1", share="share1", schema="default")
    )
    assert response.protocol == Protocol(min_reader_version=1, min_writer_version=2)
    assert response.metadata == Metadata(
        id="ed96aa41-1d81-4b7f-8fb5-846878b4b0cf",
        format=Format(provider="parquet", options={}),
        schema_string=(
            '{"type":"struct","fields":['
            '{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},'
            '{"name":"date","type":"date","nullable":true,"metadata":{}}'
            "]}"
        ),
        partition_columns=[],
        configuration={},
    )


@pytest.mark.skipif(SKIP, reason=SKIP_MESSAGE)
def test_query_table_metadata_2(rest_client: DataSharingRestClient):
    response = rest_client.query_table_metadata(
        Table(name="table2", share="share2", schema="default")
    )
    assert response.protocol == Protocol(min_reader_version=1, min_writer_version=2)
    assert response.metadata == Metadata(
        id="f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2",
        format=Format(provider="parquet", options={}),
        schema_string=(
            '{"type":"struct","fields":['
            '{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},'
            '{"name":"date","type":"date","nullable":true,"metadata":{}}'
            "]}"
        ),
        partition_columns=["date"],
        configuration={},
    )


@pytest.mark.skipif(SKIP, reason=SKIP_MESSAGE)
def test_query_table_metadata_3(rest_client: DataSharingRestClient):
    response = rest_client.query_table_metadata(
        Table(name="table3", share="share1", schema="default")
    )
    assert response.protocol == Protocol(min_reader_version=1, min_writer_version=2)
    assert response.metadata == Metadata(
        id="7ba6d727-a578-4234-a138-953f790b427c",
        format=Format(provider="parquet", options={}),
        schema_string=(
            '{"type":"struct","fields":['
            '{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},'
            '{"name":"date","type":"date","nullable":true,"metadata":{}},'
            '{"name":"type","type":"string","nullable":true,"metadata":{}}'
            "]}"
        ),
        partition_columns=["date"],
        configuration={},
    )


@pytest.mark.skipif(SKIP, reason=SKIP_MESSAGE)
def test_list_files_in_table_1(rest_client: DataSharingRestClient):
    response = rest_client.list_files_in_table(
        Table(name="table1", share="share1", schema="default")
    )
    assert response.protocol == Protocol(min_reader_version=1, min_writer_version=2)
    assert response.metadata == Metadata(
        id="ed96aa41-1d81-4b7f-8fb5-846878b4b0cf",
        format=Format(provider="parquet", options={}),
        schema_string=(
            '{"type":"struct","fields":['
            '{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},'
            '{"name":"date","type":"date","nullable":true,"metadata":{}}'
            "]}"
        ),
        partition_columns=[],
        configuration={},
    )
    assert response.add_files == [
        AddFile(
            path=response.add_files[0].path,
            partition_values={},
            size=781,
            data_change=False,
            tags={},
            stats={
                "numRecords": 1,
                "minValues": {"eventTime": "2021-04-28T06:32:22.421Z", "date": "2021-04-28"},
                "maxValues": {"eventTime": "2021-04-28T06:32:22.421Z", "date": "2021-04-28"},
                "nullCount": {"eventTime": 0, "date": 0},
            },
        ),
        AddFile(
            path=response.add_files[1].path,
            partition_values={},
            size=781,
            data_change=False,
            tags={},
            stats={
                "numRecords": 1,
                "minValues": {"eventTime": "2021-04-28T06:32:02.070Z", "date": "2021-04-28"},
                "maxValues": {"eventTime": "2021-04-28T06:32:02.070Z", "date": "2021-04-28"},
                "nullCount": {"eventTime": 0, "date": 0},
            },
        ),
    ]


@pytest.mark.skipif(SKIP, reason=SKIP_MESSAGE)
def test_list_files_in_table_2(rest_client: DataSharingRestClient):
    response = rest_client.list_files_in_table(
        Table(name="table2", share="share2", schema="default")
    )
    assert response.protocol == Protocol(min_reader_version=1, min_writer_version=2)
    assert response.metadata == Metadata(
        id="f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2",
        format=Format(provider="parquet", options={}),
        schema_string=(
            '{"type":"struct","fields":['
            '{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},'
            '{"name":"date","type":"date","nullable":true,"metadata":{}}'
            "]}"
        ),
        partition_columns=["date"],
        configuration={},
    )
    assert response.add_files == [
        AddFile(
            path=response.add_files[0].path,
            partition_values={"date": "2021-04-28"},
            size=573,
            data_change=False,
            tags={},
            stats={
                "numRecords": 1,
                "minValues": {"eventTime": "2021-04-28T23:33:57.955Z"},
                "maxValues": {"eventTime": "2021-04-28T23:33:57.955Z"},
                "nullCount": {"eventTime": 0},
            },
        ),
        AddFile(
            path=response.add_files[1].path,
            partition_values={"date": "2021-04-28"},
            size=573,
            data_change=False,
            tags={},
            stats={
                "numRecords": 1,
                "minValues": {"eventTime": "2021-04-28T23:33:48.719Z"},
                "maxValues": {"eventTime": "2021-04-28T23:33:48.719Z"},
                "nullCount": {"eventTime": 0},
            },
        ),
    ]


@pytest.mark.skipif(SKIP, reason=SKIP_MESSAGE)
def test_list_files_in_table_3(rest_client: DataSharingRestClient):
    response = rest_client.list_files_in_table(
        Table(name="table3", share="share1", schema="default")
    )
    assert response.protocol == Protocol(min_reader_version=1, min_writer_version=2)
    assert response.metadata == Metadata(
        id="7ba6d727-a578-4234-a138-953f790b427c",
        format=Format(provider="parquet", options={}),
        schema_string=(
            '{"type":"struct","fields":['
            '{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},'
            '{"name":"date","type":"date","nullable":true,"metadata":{}},'
            '{"name":"type","type":"string","nullable":true,"metadata":{}}'
            "]}"
        ),
        partition_columns=["date"],
        configuration={},
    )
    assert response.add_files == [
        AddFile(
            path=response.add_files[0].path,
            partition_values={"date": "2021-04-28"},
            size=778,
            data_change=False,
            tags={},
            stats={
                "numRecords": 1,
                "minValues": {"eventTime": "2021-04-28T23:36:51.945Z", "type": "bar"},
                "maxValues": {"eventTime": "2021-04-28T23:36:51.945Z", "type": "bar"},
                "nullCount": {"eventTime": 0, "type": 0},
            },
        ),
        AddFile(
            path=response.add_files[1].path,
            partition_values={"date": "2021-04-28"},
            size=778,
            data_change=False,
            tags={},
            stats={
                "numRecords": 1,
                "minValues": {"eventTime": "2021-04-28T23:36:47.599Z", "type": "foo"},
                "maxValues": {"eventTime": "2021-04-28T23:36:47.599Z", "type": "foo"},
                "nullCount": {"eventTime": 0, "type": 0},
            },
        ),
        AddFile(
            path=response.add_files[2].path,
            partition_values={"date": "2021-04-28"},
            size=573,
            data_change=False,
            tags={},
            stats={
                "numRecords": 1,
                "minValues": {"eventTime": "2021-04-28T23:35:53.156Z"},
                "maxValues": {"eventTime": "2021-04-28T23:35:53.156Z"},
                "nullCount": {"eventTime": 0},
            },
        ),
    ]
