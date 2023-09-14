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

from requests.models import Response
from requests.exceptions import HTTPError, ConnectionError

from delta_sharing.protocol import (
    AddCdcFile,
    AddFile,
    CdfOptions,
    Format,
    Metadata,
    Protocol,
    RemoveFile,
    Schema,
    Share,
    Table,
)
from delta_sharing.rest_client import (
    DataSharingRestClient,
    retry_with_exponential_backoff,
)
from delta_sharing.tests.conftest import ENABLE_INTEGRATION, SKIP_MESSAGE


def test_retry(rest_client: DataSharingRestClient):
    class TestWrapper(DataSharingRestClient):
        def __init__(self):
            # inherit from DataSharingRestClient to make sure all the helper methods are the same
            super().__init__(rest_client._profile)
            self.sleeps = []
            self._sleeper = self.sleeps.append

            http_error = HTTPError()
            response = Response()
            response.status_code = 429
            http_error.response = response
            self.http_error = http_error

            self.connection_error = ConnectionError()

        @retry_with_exponential_backoff
        def success(self):
            return True

        @retry_with_exponential_backoff
        def all_fail_http(self):
            raise self.http_error

        @retry_with_exponential_backoff
        def all_fail_connection(self):
            raise self.connection_error

        @retry_with_exponential_backoff
        def fail_before_success(self):
            if len(self.sleeps) < 4:
                raise self.http_error
            else:
                return True

    wrapper = TestWrapper()
    assert wrapper.success()
    assert not wrapper.sleeps

    try:
        wrapper.all_fail_http()
    except Exception as e:
        assert isinstance(e, HTTPError)
    assert wrapper.sleeps == [100, 200, 400, 800, 1600, 3200, 6400, 12800, 25600, 51200]
    wrapper.sleeps.clear()

    try:
        wrapper.all_fail_connection()
    except Exception as e:
        assert isinstance(e, ConnectionError)
    assert wrapper.sleeps == [100, 200, 400, 800, 1600, 3200, 6400, 12800, 25600, 51200]
    wrapper.sleeps.clear()

    assert wrapper.fail_before_success()
    assert wrapper.sleeps == [100, 200, 400, 800]
    wrapper.sleeps.clear()


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_read_endpoint(rest_client: DataSharingRestClient):
    assert not rest_client._profile.endpoint.endswith("/")


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_list_shares(rest_client: DataSharingRestClient):
    response = rest_client.list_shares()
    assert response.shares == [
        Share(name="share1"),
        Share(name="share2"),
        Share(name="share3"),
        Share(name="share4"),
        Share(name="share5"),
        Share(name="share6"),
        Share(name="share7"),
        Share(name="share_azure"),
        Share(name="share_gcp"),
        Share(name="share8")
    ]


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_list_schemas(rest_client: DataSharingRestClient):
    response = rest_client.list_schemas(Share(name="share1"))
    assert response.schemas == [Schema(name="default", share="share1")]

    response = rest_client.list_schemas(Share(name="share2"))
    assert response.schemas == [Schema(name="default", share="share2")]


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_list_tables(rest_client: DataSharingRestClient):
    response = rest_client.list_tables(Schema(name="default", share="share1"))
    assert response.tables == [
        Table(name="table1", share="share1", schema="default"),
        Table(name="table3", share="share1", schema="default"),
        Table(name="table7", share="share1", schema="default")
    ]

    response = rest_client.list_tables(Schema(name="default", share="share2"))
    assert response.tables == [Table(name="table2", share="share2", schema="default")]


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_list_tables_with_pagination(rest_client: DataSharingRestClient):
    response = rest_client.list_tables(Schema(name="default", share="share1"), max_results=1)
    assert response.tables == [
        Table(name="table1", share="share1", schema="default"),
    ]
    response = rest_client.list_tables(
        Schema(name="default", share="share1"), page_token=response.next_page_token
    )
    assert response.tables == [
        Table(name="table3", share="share1", schema="default"),
        Table(name="table7", share="share1", schema="default"),
    ]


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_query_table_metadata_non_partitioned(rest_client: DataSharingRestClient):
    response = rest_client.query_table_metadata(
        Table(name="table1", share="share1", schema="default")
    )
    assert response.delta_table_version > 1
    assert response.protocol == Protocol(min_reader_version=1)
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
    )


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_query_table_metadata_partitioned(rest_client: DataSharingRestClient):
    response = rest_client.query_table_metadata(
        Table(name="table2", share="share2", schema="default")
    )
    assert response.delta_table_version > 1
    assert response.protocol == Protocol(min_reader_version=1)
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
    )


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_query_table_metadata_partitioned_different_schemas(
    rest_client: DataSharingRestClient,
):
    response = rest_client.query_table_metadata(
        Table(name="table3", share="share1", schema="default")
    )
    assert response.delta_table_version > 1
    assert response.protocol == Protocol(min_reader_version=1)
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
    )


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_query_existed_table_version(rest_client: DataSharingRestClient):
    response = rest_client.query_table_version(
        Table(name="table1", share="share1", schema="default")
    )
    assert isinstance(response.delta_table_version, int)
    assert response.delta_table_version > 0


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_query_table_version_with_timestamp(rest_client: DataSharingRestClient):
    response = rest_client.query_table_version(
        Table(name="cdf_table_cdf_enabled", share="share8", schema="default"),
        starting_timestamp="2020-01-01T00:00:00-08:00"
    )
    assert isinstance(response.delta_table_version, int)
    assert response.delta_table_version == 0


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_query_table_version_with_timestamp_exception(rest_client: DataSharingRestClient):
    try:
        rest_client.query_table_version(
            Table(name="table1", share="share1", schema="default"),
            starting_timestamp="2020-01-1T00:00:00-08:00"
        )
    except Exception as e:
        assert isinstance(e, HTTPError)
        assert "Reading table by version or timestamp is not supported" in (str(e))


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_query_nonexistent_table_version(rest_client: DataSharingRestClient):
    with pytest.raises(HTTPError):
        rest_client.query_table_version(
            Table(name="nonexistenttable", share="share1", schema="default")
        )


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_list_files_in_table_non_partitioned(rest_client: DataSharingRestClient):
    response = rest_client.list_files_in_table(
        Table(name="table1", share="share1", schema="default"),
        predicateHints=["date = '2021-01-31'"],
    )
    assert response.delta_table_version > 1
    assert response.protocol == Protocol(min_reader_version=1)
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
    )
    assert response.add_files == [
        AddFile(
            url=response.add_files[0].url,
            id="061cb3683a467066995f8cdaabd8667d",
            partition_values={},
            size=781,
            stats=(
                r'{"numRecords":1,'
                r'"minValues":{"eventTime":"2021-04-28T06:32:22.421Z","date":"2021-04-28"},'
                r'"maxValues":{"eventTime":"2021-04-28T06:32:22.421Z","date":"2021-04-28"},'
                r'"nullCount":{"eventTime":0,"date":0}}'
            ),
        ),
        AddFile(
            url=response.add_files[1].url,
            id="e268cbf70dbaa6143e7e9fa3e2d3b00e",
            partition_values={},
            size=781,
            stats=(
                r'{"numRecords":1,'
                r'"minValues":{"eventTime":"2021-04-28T06:32:02.070Z","date":"2021-04-28"},'
                r'"maxValues":{"eventTime":"2021-04-28T06:32:02.070Z","date":"2021-04-28"},'
                r'"nullCount":{"eventTime":0,"date":0}}'
            ),
        ),
    ]


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_list_files_in_table_partitioned(rest_client: DataSharingRestClient):
    response = rest_client.list_files_in_table(
        Table(name="table2", share="share2", schema="default"),
        predicateHints=["date = '2021-01-31'"],
        limitHint=123,
    )
    assert response.delta_table_version > 1
    assert response.protocol == Protocol(min_reader_version=1)
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
    )
    assert response.add_files == [
        AddFile(
            url=response.add_files[0].url,
            id="9f1a49539c5cffe1ea7f9e055d5c003c",
            partition_values={"date": "2021-04-28"},
            size=573,
            stats=(
                r'{"numRecords":1,'
                r'"minValues":{"eventTime":"2021-04-28T23:33:57.955Z"},'
                r'"maxValues":{"eventTime":"2021-04-28T23:33:57.955Z"},'
                r'"nullCount":{"eventTime":0}}'
            ),
        ),
        AddFile(
            url=response.add_files[1].url,
            id="cd2209b32f5ed5305922dd50f5908a75",
            partition_values={"date": "2021-04-28"},
            size=573,
            stats=(
                r'{"numRecords":1,'
                r'"minValues":{"eventTime":"2021-04-28T23:33:48.719Z"},'
                r'"maxValues":{"eventTime":"2021-04-28T23:33:48.719Z"},'
                r'"nullCount":{"eventTime":0}}'
            ),
        ),
    ]


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_list_files_in_table_partitioned_different_schemas(
    rest_client: DataSharingRestClient,
):
    response = rest_client.list_files_in_table(
        Table(name="table3", share="share1", schema="default")
    )
    assert response.delta_table_version > 1
    assert response.protocol == Protocol(min_reader_version=1)
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
    )
    assert response.add_files == [
        AddFile(
            url=response.add_files[0].url,
            id="db213271abffec6fd6c7fc2aad9d4b3f",
            partition_values={"date": "2021-04-28"},
            size=778,
            stats=(
                r'{"numRecords":1,'
                r'"minValues":{"eventTime":"2021-04-28T23:36:51.945Z","type":"bar"},'
                r'"maxValues":{"eventTime":"2021-04-28T23:36:51.945Z","type":"bar"},'
                r'"nullCount":{"eventTime":0,"type":0}}'
            ),
        ),
        AddFile(
            url=response.add_files[1].url,
            id="f1f8be229d8b18eb6d6a34255f2d7089",
            partition_values={"date": "2021-04-28"},
            size=778,
            stats=(
                r'{"numRecords":1,'
                r'"minValues":{"eventTime":"2021-04-28T23:36:47.599Z","type":"foo"},'
                r'"maxValues":{"eventTime":"2021-04-28T23:36:47.599Z","type":"foo"},'
                r'"nullCount":{"eventTime":0,"type":0}}'
            ),
        ),
        AddFile(
            url=response.add_files[2].url,
            id="a892a55d770ee70b34ffb2ebf7dc2fd0",
            partition_values={"date": "2021-04-28"},
            size=573,
            stats=(
                r'{"numRecords":1,'
                r'"minValues":{"eventTime":"2021-04-28T23:35:53.156Z"},'
                r'"maxValues":{"eventTime":"2021-04-28T23:35:53.156Z"},'
                r'"nullCount":{"eventTime":0}}'
            ),
        ),
    ]


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_list_files_in_table_version(
    rest_client: DataSharingRestClient,
):
    response = rest_client.list_files_in_table(
        Table(name="cdf_table_cdf_enabled", share="share8", schema="default"),
        version=1
    )
    assert response.delta_table_version == 1
    assert response.protocol == Protocol(min_reader_version=1)
    assert response.metadata == Metadata(
        id="16736144-3306-4577-807a-d3f899b77670",
        format=Format(provider="parquet", options={}),
        schema_string=(
            '{"type":"struct","fields":['
            '{"name":"name","type":"string","nullable":true,"metadata":{}},'
            '{"name":"age","type":"integer","nullable":true,"metadata":{}},'
            '{"name":"birthday","type":"date","nullable":true,"metadata":{}}'
            "]}"
        ),
        configuration={"enableChangeDataFeed": "true"},
        partition_columns=[],
    )
    assert response.add_files == [
        AddFile(
            url=response.add_files[0].url,
            id="60d0cf57f3e4367db154aa2c36152a1f",
            partition_values={},
            size=1030,
            stats=(
                r'{"numRecords":1,'
                r'"minValues":{"name":"1","age":1,"birthday":"2020-01-01"},'
                r'"maxValues":{"name":"1","age":1,"birthday":"2020-01-01"},'
                r'"nullCount":{"name":0,"age":0,"birthday":0}}'
            ),
            version=1,
            timestamp=1651272635000
        ),
        AddFile(
            url=response.add_files[1].url,
            id="d7ed708546dd70fdff9191b3e3d6448b",
            partition_values={},
            size=1030,
            stats=(
                r'{"numRecords":1,'
                r'"minValues":{"name":"3","age":3,"birthday":"2020-01-01"},'
                r'"maxValues":{"name":"3","age":3,"birthday":"2020-01-01"},'
                r'"nullCount":{"name":0,"age":0,"birthday":0}}'
            ),
            version=1,
            timestamp=1651272635000
        ),
        AddFile(
            url=response.add_files[2].url,
            id="a6dc5694a4ebcc9a067b19c348526ad6",
            partition_values={},
            size=1030,
            stats=(
                r'{"numRecords":1,'
                r'"minValues":{"name":"2","age":2,"birthday":"2020-01-01"},'
                r'"maxValues":{"name":"2","age":2,"birthday":"2020-01-01"},'
                r'"nullCount":{"name":0,"age":0,"birthday":0}}'
            ),
            version=1,
            timestamp=1651272635000
        ),
    ]


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_list_files_with_json_predicates(
    rest_client: DataSharingRestClient,
):
    # This function invokes the client with the specified json predicate hints
    # on the test table which is partitioned by date. The function validates
    # that the retuned files contains the expected dates.
    def test_hints(hints, expected_dates):
        response = rest_client.list_files_in_table(
            Table(name="cdf_table_with_partition", share="share8", schema="default"),
            jsonPredicateHints=hints
        )
        assert response.protocol == Protocol(min_reader_version=1)
        assert response.metadata == Metadata(
            id="e21eb083-6976-4159-90f2-ad88d06b7c7f",
            format=Format(provider="parquet", options={}),
            schema_string=(
                '{"type":"struct","fields":['
                '{"name":"name","type":"string","nullable":true,"metadata":{}},'
                '{"name":"age","type":"integer","nullable":true,"metadata":{}},'
                '{"name":"birthday","type":"date","nullable":true,"metadata":{}}'
                "]}"
            ),
            configuration={"enableChangeDataFeed": "true"},
            partition_columns=["birthday"]
        )

        # validate that we get back all expected files.
        assert len(expected_dates) == len(response.add_files)
        for date in expected_dates:
            found = False
            for file in response.add_files:
                if date in file.url:
                    found = True
            assert found

    # Without predicates, we should get back all the files.
    test_hints(None, ["2020-01-01", "2020-02-02"])

    # These predicates should return file with date 2020-01-01.
    hints1 = (
        '{"op":"and","children":['
        '{"op":"not","children":['
        '{"op":"isNull","children":['
        '{"op":"column","name":"birthday","valueType":"date"}]}]},'
        '{"op":"equal","children":['
        '{"op":"column","name":"birthday","valueType":"date"},'
        '{"op":"literal","value":"2020-01-01","valueType":"date"}]}'
        ']}'
    )
    test_hints(hints1, ["2020-01-01"])

    # These predicates should return file with date 2020-02-02.
    hints2 = (
        '{"op":"and","children":['
        '{"op":"not","children":['
        '{"op":"isNull","children":['
        '{"op":"column","name":"birthday","valueType":"date"}]}]},'
        '{"op":"greaterThan","children":['
        '{"op":"column","name":"birthday","valueType":"date"},'
        '{"op":"literal","value":"2020-01-01","valueType":"date"}]}'
        ']}'
    )
    test_hints(hints2, ["2020-02-02"])

    # Invalid predicates should return all the files.
    test_hints("bad-predicates", ["2020-01-01", "2020-02-02"])


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_list_files_in_table_version_exception(
    rest_client: DataSharingRestClient,
):
    try:
        rest_client.list_files_in_table(
            Table(name="table1", share="share1", schema="default"),
            version=1
        )
    except Exception as e:
        assert isinstance(e, HTTPError)
        assert "Reading table by version or timestamp is not supported" in (str(e))


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_list_files_in_table_timestamp(
    rest_client: DataSharingRestClient
):
    try:
        rest_client.list_files_in_table(
            Table(name="table1", share="share1", schema="default"),
            timestamp="random_str"
        )
    except Exception as e:
        assert isinstance(e, HTTPError)
        assert "Reading table by version or timestamp is not supported" in (str(e))

    cdf_table = Table(name="cdf_table_with_partition", share="share8", schema="default")

    # Only one of version and timestamp is supported
    try:
        rest_client.list_files_in_table(cdf_table, version=1, timestamp="random_str")
    except Exception as e:
        assert isinstance(e, HTTPError)
        assert "Please only provide one of" in (str(e))

    # Use a random string, and look for an appropriate error.
    # This will ensure that the timestamp is pass to server.
    try:
        rest_client.list_files_in_table(cdf_table, timestamp="randomTimestamp")
        assert False
    except Exception as e:
        assert isinstance(e, HTTPError)
        assert "Invalid timestamp:" in (str(e))
        assert "randomTimestamp" in (str(e))

    # Use a really old start time, and look for an appropriate error.
    # This will ensure that the timestamp is parsed correctly.
    try:
        rest_client.list_files_in_table(cdf_table, timestamp="2000-01-01T00:00:00Z")
        assert False
    except Exception as e:
        assert isinstance(e, HTTPError)
        assert "Please use a timestamp greater" in (str(e))

    # Use an end time far away, and look for an appropriate error.
    try:
        rest_client.list_files_in_table(cdf_table, timestamp="9000-01-01T00:00:00Z")
        assert False
    except Exception as e:
        assert isinstance(e, HTTPError)
        assert "Please use a timestamp less" in str(e)


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_list_table_changes(
    rest_client: DataSharingRestClient,
):
    # The following table query will return all types of actions.
    cdf_table = Table(name="cdf_table_with_partition", share="share8", schema="default")
    response = rest_client.list_table_changes(
        cdf_table,
        CdfOptions(starting_version=1, ending_version=3)
    )

    assert response.protocol == Protocol(min_reader_version=1)
    assert response.metadata == Metadata(
        id="e21eb083-6976-4159-90f2-ad88d06b7c7f",
        format=Format(provider="parquet", options={}),
        schema_string=(
            '{"type":"struct","fields":['
            '{"name":"name","type":"string","nullable":true,"metadata":{}},'
            '{"name":"age","type":"integer","nullable":true,"metadata":{}},'
            '{"name":"birthday","type":"date","nullable":true,"metadata":{}}'
            "]}"
        ),
        configuration={"enableChangeDataFeed": "true"},
        partition_columns=["birthday"],
        version=3
    )
    assert response.actions == [
        AddFile(
            url=response.actions[0].url,
            id='a04d61f17541fac1f9b5df5b8d26fff8',
            partition_values={'birthday': '2020-01-01'},
            size=791,
            timestamp=1651614980000,
            version=1,
            stats=(
                r'{"numRecords":1,'
                r'"minValues":{"name":"1","age":1},'
                r'"maxValues":{"name":"1","age":1},'
                r'"nullCount":{"name":0,"age":0}}'
            ),
        ),
        AddFile(
            url=response.actions[1].url,
            id='f206a7168597c4db5956b2b11ed5cbb2',
            partition_values={'birthday': '2020-01-01'},
            size=791,
            timestamp=1651614980000,
            version=1,
            stats=(
                r'{"numRecords":1,'
                r'"minValues":{"name":"2","age":2},'
                r'"maxValues":{"name":"2","age":2},'
                r'"nullCount":{"name":0,"age":0}}'
            ),
        ),
        AddFile(
            url=response.actions[2].url,
            id='9410d65d7571842eb7fb6b1ac01af372',
            partition_values={'birthday': '2020-03-03'},
            size=791,
            timestamp=1651614980000,
            version=1,
            stats=(
                r'{"numRecords":1,'
                r'"minValues":{"name":"3","age":3},'
                r'"maxValues":{"name":"3","age":3},'
                r'"nullCount":{"name":0,"age":0}}'
            ),
        ),
        AddCdcFile(
            url=response.actions[3].url,
            id="d5fc796b3c044f7061702525435e43b7",
            partition_values={"birthday": "2020-01-01"},
            size=1125,
            timestamp=1651614986000,
            version=2,
        ),
        AddCdcFile(
            url=response.actions[4].url,
            id="6f8084143b699c5b808eb00db211c85c",
            partition_values={"birthday": "2020-02-02"},
            size=1132,
            timestamp=1651614986000,
            version=2,
        ),
        RemoveFile(
            url=response.actions[5].url,
            id='9410d65d7571842eb7fb6b1ac01af372',
            partition_values={'birthday': '2020-03-03'},
            size=791,
            timestamp=1651614994000,
            version=3
        ),
    ]


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_list_table_changes_with_more_metadata(
    rest_client: DataSharingRestClient,
):
    # The table streaming_notnull_to_null contains metadata for different version, but won't be
    # returned for cdf queries from python connector because the User-Agent header is not set to
    # include spark structured streaming. So the query will still work: old connector is compatible
    # with new server.
    cdf_table = Table(name="streaming_notnull_to_null", share="share8", schema="default")
    response = rest_client.list_table_changes(
        cdf_table,
        CdfOptions(starting_version=0)
    )

    assert response.protocol == Protocol(min_reader_version=1)
    assert len(response.actions) == 2
    assert response.actions == [
        AddFile(
            url=response.actions[0].url,
            id=response.actions[0].id,
            partition_values={},
            size=568,
            timestamp=1668327046000,
            version=1,
            stats=(
                r'{"numRecords":1,'
                r'"minValues":{"name":"1"},'
                r'"maxValues":{"name":"1"},'
                r'"nullCount":{"name":0}}'
            ),
        ),
        AddFile(
            url=response.actions[1].url,
            id=response.actions[1].id,
            partition_values={},
            size=568,
            timestamp=1668327050000,
            version=3,
            stats=(
                r'{"numRecords":2,'
                r'"minValues":{"name":"2"},'
                r'"maxValues":{"name":"2"},'
                r'"nullCount":{"name":1}}'
            ),
        )
    ]


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_list_table_changes_with_timestamp(
    rest_client: DataSharingRestClient
):
    cdf_table = Table(name="cdf_table_with_partition", share="share8", schema="default")

    # Use a really old start time, and look for an appropriate error.
    # This will ensure that the timestamps are parsed correctly.
    try:
        rest_client.list_table_changes(
            cdf_table,
            CdfOptions(starting_timestamp="2000-05-03T00:00:00-08:00")
        )
        assert False
    except Exception as e:
        assert isinstance(e, HTTPError)
        assert "Please use a timestamp greater" in str(e)

    # Use an end time far away, and look for an appropriate error.
    try:
        rest_client.list_table_changes(
            cdf_table,
            CdfOptions(starting_version=0, ending_timestamp="2100-05-03T00:00:00Z")
        )
        assert False
    except Exception as e:
        assert isinstance(e, HTTPError)
        assert "Please use a timestamp less" in str(e)
