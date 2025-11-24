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
from datetime import date, datetime
from typing import Optional, Sequence

import pandas as pd
import pytest

from delta_sharing.delta_sharing import (
    DeltaSharingProfile,
    SharingClient,
    get_table_metadata,
    get_table_protocol,
    get_table_version,
    load_as_pandas,
    load_as_spark,
    load_table_changes_as_spark,
    load_table_changes_as_pandas,
    _parse_url,
)
from delta_sharing.protocol import Format, Metadata, Protocol, Schema, Share, Table
from delta_sharing.rest_client import (
    DataSharingRestClient,
    ListAllTablesResponse,
    retry_with_exponential_backoff,
)
from delta_sharing.tests.conftest import ENABLE_INTEGRATION, SKIP_MESSAGE

from requests.models import Response
from requests.exceptions import HTTPError


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_list_shares(sharing_client: SharingClient):
    shares = sharing_client.list_shares()
    assert shares == [
        Share(name="share1"),
        Share(name="share2"),
        Share(name="share3"),
        Share(name="share4"),
        Share(name="share5"),
        Share(name="share6"),
        Share(name="share7"),
        Share(name="share_azure"),
        Share(name="share_gcp"),
        Share(name="share8"),
    ]


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_list_schemas(sharing_client: SharingClient):
    schemas = sharing_client.list_schemas(Share(name="share1"))
    assert schemas == [Schema(name="default", share="share1")]

    schemas = sharing_client.list_schemas(Share(name="share2"))
    assert schemas == [Schema(name="default", share="share2")]


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_list_tables(sharing_client: SharingClient):
    tables = sharing_client.list_tables(Schema(name="default", share="share1"))
    assert tables == [
        Table(name="table1", share="share1", schema="default"),
        Table(name="table3", share="share1", schema="default"),
        Table(name="table7", share="share1", schema="default"),
    ]

    tables = sharing_client.list_tables(Schema(name="default", share="share2"))
    assert tables == [Table(name="table2", share="share2", schema="default")]


def _verify_all_tables_result(tables: Sequence[Table]):
    assert tables == [
        Table(name="table1", share="share1", schema="default"),
        Table(name="table3", share="share1", schema="default"),
        Table(name="table7", share="share1", schema="default"),
        Table(name="table2", share="share2", schema="default"),
        Table(name="table4", share="share3", schema="default"),
        Table(name="table5", share="share3", schema="default"),
        Table(name="test_gzip", share="share4", schema="default"),
        Table(name="table8", share="share7", schema="schema1"),
        Table(name="table9", share="share7", schema="schema2"),
        Table(name="table_wasb", share="share_azure", schema="default"),
        Table(name="table_abfs", share="share_azure", schema="default"),
        Table(name="table_gcs", share="share_gcp", schema="default"),
        Table(name="12k_rows", share="share8", schema="default"),
        Table(name="add_columns_partitioned_cdf", share="share8", schema="default"),
        Table(name="add_columns_non_partitioned_cdf", share="share8", schema="default"),
        Table(name="timestampntz_cdf_table", share="share8", schema="default"),
        Table(name="cdf_table_cdf_enabled", share="share8", schema="default"),
        Table(name="cdf_table_with_partition", share="share8", schema="default"),
        Table(name="cdf_table_with_vacuum", share="share8", schema="default"),
        Table(name="cdf_table_missing_log", share="share8", schema="default"),
        Table(name="streaming_table_with_optimize", share="share8", schema="default"),
        Table(name="streaming_table_metadata_protocol", share="share8", schema="default"),
        Table(name="streaming_notnull_to_null", share="share8", schema="default"),
        Table(name="streaming_null_to_notnull", share="share8", schema="default"),
        Table(name="streaming_cdf_null_to_notnull", share="share8", schema="default"),
        Table(name="streaming_cdf_table", share="share8", schema="default"),
        Table(name="table_reader_version_increased", share="share8", schema="default"),
        Table(name="table_with_no_metadata", share="share8", schema="default"),
        Table(name="table_data_loss_with_checkpoint", share="share8", schema="default"),
        Table(name="table_data_loss_no_checkpoint", share="share8", schema="default"),
        Table(name="table_with_cm_name", share="share8", schema="default"),
        Table(name="table_with_cm_id", share="share8", schema="default"),
        Table(name="deletion_vectors_with_dvs_dv_property_on", share="share8", schema="default"),
        Table(name="dv_and_cm_table", share="share8", schema="default"),
    ]


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_list_all_tables(sharing_client: SharingClient):
    tables = sharing_client.list_all_tables()
    _verify_all_tables_result(tables)


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_list_all_tables_with_fallback(profile: DeltaSharingProfile):
    class TestDataSharingRestClient(DataSharingRestClient):
        """
        A special DataSharingRestClient whose list_all_tables always fails with 404. We use this to
        test the fallback logic for old servers.
        """

        def __init__(self):
            super().__init__(profile)

        @retry_with_exponential_backoff
        def list_all_tables(
            self,
            share: Share,
            *,
            max_results: Optional[int] = None,
            page_token: Optional[str] = None,
        ) -> ListAllTablesResponse:
            http_error = HTTPError()
            response = Response()
            response.status_code = 404
            http_error.response = response
            raise http_error

    sharing_client = SharingClient(profile)
    sharing_client._rest_client = TestDataSharingRestClient()
    tables = sharing_client.list_all_tables()
    _verify_all_tables_result(tables)


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
@pytest.mark.parametrize(
    "fragments,starting_timestamp,error,expected_version",
    [
        pytest.param(
            "share1.default.table1",
            None,
            None,
            2,
            id="table1 spark",
        ),
        pytest.param(
            "share1.default.table1",
            "random_timestamp",
            "random",
            -1,
            id="table1 starting_timestamp not valid",
        ),
        pytest.param(
            "share8.default.cdf_table_cdf_enabled",
            "2022-01-01T00:00:00Z",
            None,
            0,
            id="cdf_table_cdf_enabled version 0",
        ),
        pytest.param(
            "share8.default.cdf_table_cdf_enabled",
            "2100-01-01T00:00:00Z",
            "is after the latest available version",
            -1,
            id="cdf_table_cdf_enabled timestamp too late",
        ),
    ],
)
def test_get_table_version(
    profile_path: str,
    fragments: str,
    starting_timestamp: Optional[str],
    error: Optional[str],
    expected_version: int,
):
    if error is None:
        actual_version = get_table_version(f"{profile_path}#{fragments}", starting_timestamp)
        assert expected_version == actual_version
    else:
        try:
            get_table_version(f"{profile_path}#{fragments}", starting_timestamp)
            assert False
        except Exception as e:
            assert error in str(e)


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
@pytest.mark.parametrize(
    "fragments,expected",
    [
        pytest.param(
            "share1.default.table1",
            Metadata(
                id="ed96aa41-1d81-4b7f-8fb5-846878b4b0cf",
                format=Format(provider="parquet", options={}),
                schema_string=(
                    '{"type":"struct","fields":['
                    '{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},'
                    '{"name":"date","type":"date","nullable":true,"metadata":{}}'
                    "]}"
                ),
                partition_columns=[],
            ),
            id="non partitioned",
        ),
        pytest.param(
            "share2.default.table2",
            Metadata(
                id="f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2",
                format=Format(provider="parquet", options={}),
                schema_string=(
                    '{"type":"struct","fields":['
                    '{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},'
                    '{"name":"date","type":"date","nullable":true,"metadata":{}}'
                    "]}"
                ),
                partition_columns=["date"],
            ),
            id="partitioned",
        ),
        pytest.param(
            "share1.default.table3",
            Metadata(
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
            ),
            id="partitioned and different schemas",
        ),
    ],
)
def test_get_table_metadata(profile_path: str, fragments: str, expected: Metadata):
    actual = get_table_metadata(f"{profile_path}#{fragments}")
    assert expected == actual
    actual_using_parquet = get_table_metadata(f"{profile_path}#{fragments}", use_delta_format=False)
    assert expected == actual_using_parquet


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_get_table_protocol(profile_path: str):
    actual = get_table_protocol(f"{profile_path}#share1.default.table1")
    assert Protocol(min_reader_version=1) == actual
    actual_using_parquet = get_table_protocol(
        f"{profile_path}#share1.default.table1", use_delta_format=False
    )
    assert Protocol(min_reader_version=1) == actual_using_parquet


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
@pytest.mark.parametrize(
    "fragments,limit,version,expected",
    [
        pytest.param(
            "share1.default.table1",
            None,
            None,
            pd.DataFrame(
                {
                    "eventTime": [
                        pd.Timestamp("2021-04-28 06:32:22.421"),
                        pd.Timestamp("2021-04-28 06:32:02.070"),
                    ],
                    "date": [date(2021, 4, 28), date(2021, 4, 28)],
                }
            ),
            id="non partitioned",
        ),
        pytest.param(
            "share2.default.table2",
            None,
            None,
            pd.DataFrame(
                {
                    "eventTime": [
                        pd.Timestamp("2021-04-28 23:33:57.955"),
                        pd.Timestamp("2021-04-28 23:33:48.719"),
                    ],
                    "date": [date(2021, 4, 28), date(2021, 4, 28)],
                }
            ),
            id="partitioned",
        ),
        pytest.param(
            "share1.default.table3",
            None,
            None,
            pd.DataFrame(
                {
                    "eventTime": [
                        pd.Timestamp("2021-04-28 23:36:51.945"),
                        pd.Timestamp("2021-04-28 23:35:53.156"),
                        pd.Timestamp("2021-04-28 23:36:47.599"),
                    ],
                    "date": [
                        date(2021, 4, 28),
                        date(2021, 4, 28),
                        date(2021, 4, 28),
                    ],
                    "type": [
                        "bar",
                        None,
                        "foo",
                    ],
                }
            ),
            id="partitioned and different schemas",
        ),
        pytest.param(
            "share1.default.table3",
            0,
            None,
            pd.DataFrame(
                {
                    "eventTime": [pd.Timestamp("2021-04-28 23:36:51.945")],
                    "date": [date(2021, 4, 28)],
                    "type": ["bar"],
                }
            ).iloc[0:0],
            id="limit 0",
        ),
        pytest.param(
            "share1.default.table3",
            1,
            None,
            pd.DataFrame(
                {
                    "eventTime": [pd.Timestamp("2021-04-28 23:36:51.945")],
                    "date": [date(2021, 4, 28)],
                    "type": ["bar"],
                }
            ),
            id="limit 1",
        ),
        pytest.param(
            "share1.default.table3",
            2,
            None,
            pd.DataFrame(
                {
                    "eventTime": [
                        pd.Timestamp("2021-04-28 23:36:51.945"),
                        pd.Timestamp("2021-04-28 23:35:53.156"),
                    ],
                    "date": [date(2021, 4, 28), date(2021, 4, 28)],
                    "type": ["bar", None],
                }
            ),
            id="limit 2",
        ),
        pytest.param(
            "share1.default.table3",
            3,
            None,
            pd.DataFrame(
                {
                    "eventTime": [
                        pd.Timestamp("2021-04-28 23:36:51.945"),
                        pd.Timestamp("2021-04-28 23:35:53.156"),
                        pd.Timestamp("2021-04-28 23:36:47.599"),
                    ],
                    "date": [date(2021, 4, 28), date(2021, 4, 28), date(2021, 4, 28)],
                    "type": ["bar", None, "foo"],
                }
            ),
            id="limit 3",
        ),
        pytest.param(
            "share1.default.table3",
            4,
            None,
            pd.DataFrame(
                {
                    "eventTime": [
                        pd.Timestamp("2021-04-28 23:36:51.945"),
                        pd.Timestamp("2021-04-28 23:35:53.156"),
                        pd.Timestamp("2021-04-28 23:36:47.599"),
                    ],
                    "date": [date(2021, 4, 28), date(2021, 4, 28), date(2021, 4, 28)],
                    "type": ["bar", None, "foo"],
                }
            ),
            id="limit 4",
        ),
        pytest.param(
            "share8.default.cdf_table_cdf_enabled",
            None,
            None,
            pd.DataFrame(
                {
                    "name": ["2", "1"],
                    "age": pd.Series([2, 1], dtype="int32"),
                    "birthday": [date(2020, 2, 2), date(2020, 1, 1)],
                }
            ),
            id="cdf_table_cdf_enabled",
        ),
        pytest.param(
            "share8.default.cdf_table_cdf_enabled",
            None,
            1,
            pd.DataFrame(
                {
                    "name": ["1", "2", "3"],
                    "age": pd.Series([1, 2, 3], dtype="int32"),
                    "birthday": [date(2020, 1, 1), date(2020, 1, 1), date(2020, 1, 1)],
                }
            ),
            id="cdf_table_cdf_enabled version 1",
        ),
        pytest.param(
            "share3.default.table4",
            None,
            None,
            pd.DataFrame(
                {
                    "type": [None, None],
                    "eventTime": [
                        pd.Timestamp("2021-04-28 23:33:48.719"),
                        pd.Timestamp("2021-04-28 23:33:57.955"),
                    ],
                    "date": [date(2021, 4, 28), date(2021, 4, 28)],
                }
            ),
            id="table column order is not the same as parquet files",
        ),
        pytest.param(
            "share4.default.test_gzip",
            None,
            None,
            pd.DataFrame({"a": [True], "b": pd.Series([1], dtype="int32"), "c": ["Hi"]}),
            id="table column order is not the same as parquet files",
        ),
        pytest.param(
            "share_azure.default.table_wasb",
            None,
            None,
            pd.DataFrame(
                {
                    "c1": ["foo bar"],
                    "c2": ["foo bar"],
                }
            ),
            id="Azure Blob Storage",
        ),
        pytest.param(
            "share_azure.default.table_abfs",
            None,
            None,
            pd.DataFrame(
                {
                    "c1": ["foo bar"],
                    "c2": ["foo bar"],
                }
            ),
            id="Azure Data Lake Storage Gen2",
        ),
        pytest.param(
            "share_gcp.default.table_gcs",
            None,
            None,
            pd.DataFrame(
                {
                    "c1": ["foo bar"],
                    "c2": ["foo bar"],
                }
            ),
            id="Google Cloud Storage",
        ),
    ],
)
def test_load_as_pandas_success(
    profile_path: str,
    fragments: str,
    limit: Optional[int],
    version: Optional[int],
    expected: pd.DataFrame,
):
    pdf = load_as_pandas(f"{profile_path}#{fragments}", limit, version, None)
    pd.testing.assert_frame_equal(pdf, expected)


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
@pytest.mark.parametrize(
    "fragments,limit,version,expected",
    [
        pytest.param(
            "share8.default.deletion_vectors_with_dvs_dv_property_on",
            None,
            None,
            pd.DataFrame(
                {
                    "id": [3, 4, 5, 6, 7, 8, 9],
                    "value": ["3", "4", "5", "6", "7", "8", "9"],
                    "timestamp": [
                        pd.Timestamp("2023-05-31T18:58:33.633+00:00"),
                        pd.Timestamp("2023-05-31T18:58:33.633+00:00"),
                        pd.Timestamp("2023-05-31T18:58:33.633+00:00"),
                        pd.Timestamp("2023-05-31T18:58:33.633+00:00"),
                        pd.Timestamp("2023-05-31T18:58:33.633+00:00"),
                        pd.Timestamp("2023-05-31T18:58:33.633+00:00"),
                        pd.Timestamp("2023-05-31T18:58:33.633+00:00"),
                    ],
                    "rand": [
                        0.7918174793484931,
                        0.9281049271981882,
                        0.27796520310701633,
                        0.15263801464228832,
                        0.1981143710215575,
                        0.3069439236599195,
                        0.5175919190815845,
                    ],
                }
            ),
            id="dv",
        ),
        pytest.param(
            "share8.default.deletion_vectors_with_dvs_dv_property_on",
            6,
            None,
            pd.DataFrame(
                {
                    "id": [3, 4, 5, 6, 7, 8],
                    "value": ["3", "4", "5", "6", "7", "8"],
                    "timestamp": [
                        pd.Timestamp("2023-05-31T18:58:33.633+00:00"),
                        pd.Timestamp("2023-05-31T18:58:33.633+00:00"),
                        pd.Timestamp("2023-05-31T18:58:33.633+00:00"),
                        pd.Timestamp("2023-05-31T18:58:33.633+00:00"),
                        pd.Timestamp("2023-05-31T18:58:33.633+00:00"),
                        pd.Timestamp("2023-05-31T18:58:33.633+00:00"),
                    ],
                    "rand": [
                        0.7918174793484931,
                        0.9281049271981882,
                        0.27796520310701633,
                        0.15263801464228832,
                        0.1981143710215575,
                        0.3069439236599195,
                    ],
                }
            ),
            id="dv",
        ),
        pytest.param(
            "share8.default.deletion_vectors_with_dvs_dv_property_on",
            3,
            None,
            pd.DataFrame(
                {
                    "id": [3, 4, 5],
                    "value": ["3", "4", "5"],
                    "timestamp": [
                        pd.Timestamp("2023-05-31T18:58:33.633+00:00"),
                        pd.Timestamp("2023-05-31T18:58:33.633+00:00"),
                        pd.Timestamp("2023-05-31T18:58:33.633+00:00"),
                    ],
                    "rand": [0.7918174793484931, 0.9281049271981882, 0.27796520310701633],
                }
            ),
            id="dv",
        ),
        pytest.param(
            "share8.default.deletion_vectors_with_dvs_dv_property_on",
            2,
            None,
            pd.DataFrame(
                {
                    "id": [3, 4],
                    "value": ["3", "4"],
                    "timestamp": [
                        pd.Timestamp("2023-05-31T18:58:33.633+00:00"),
                        pd.Timestamp("2023-05-31T18:58:33.633+00:00"),
                    ],
                    "rand": [0.7918174793484931, 0.9281049271981882],
                }
            ),
            id="dv",
        ),
    ],
)
def test_load_as_pandas_success_dv(
    profile_path: str,
    fragments: str,
    limit: Optional[int],
    version: Optional[int],
    expected: pd.DataFrame,
):
    pdf = load_as_pandas(f"{profile_path}#{fragments}", limit, version, None)
    expected["timestamp"] = expected["timestamp"].astype("datetime64[us, UTC]")
    pd.testing.assert_frame_equal(pdf, expected)


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
@pytest.mark.parametrize(
    "fragments,limit,version,expected",
    [
        pytest.param(
            "share8.default.table_with_cm_name",
            None,
            None,
            pd.DataFrame(
                {
                    "date": [date(2024, 6, 25), date(2024, 6, 25), date(2024, 6, 25)],
                    "eventTime": [
                        pd.Timestamp("2024-06-25T00:00:00.000+00:00"),
                        pd.Timestamp("2024-06-25T01:00:00.000+00:00"),
                        pd.Timestamp("2024-06-25T00:00:00.000+00:00"),
                    ],
                }
            ),
            id="column map name",
        ),
        pytest.param(
            "share8.default.table_with_cm_name",
            2,
            None,
            pd.DataFrame(
                {
                    "date": [date(2024, 6, 25), date(2024, 6, 25)],
                    "eventTime": [
                        pd.Timestamp("2024-06-25T00:00:00.000+00:00"),
                        pd.Timestamp("2024-06-25T01:00:00.000+00:00"),
                    ],
                }
            ),
            id="column map name",
        ),
        pytest.param(
            "share8.default.table_with_cm_name",
            1,
            None,
            pd.DataFrame(
                {
                    "date": [date(2024, 6, 25)],
                    "eventTime": [pd.Timestamp("2024-06-25T00:00:00.000+00:00")],
                }
            ),
            id="column map name",
        ),
        pytest.param(
            "share8.default.table_with_cm_id",
            None,
            None,
            pd.DataFrame(
                {
                    "date": [date(2024, 6, 23), date(2024, 6, 24), date(2024, 6, 25)],
                    "eventTime": [
                        pd.Timestamp("2024-06-23T00:00:00.000+00:00"),
                        pd.Timestamp("2024-06-24T01:00:00.000+00:00"),
                        pd.Timestamp("2024-06-25T00:00:00.000+00:00"),
                    ],
                }
            ),
            id="column map id",
        ),
    ],
)
def test_load_as_pandas_success_cm(
    profile_path: str,
    fragments: str,
    limit: Optional[int],
    version: Optional[int],
    expected: pd.DataFrame,
):
    pdf = load_as_pandas(f"{profile_path}#{fragments}", limit, version, None)
    expected["eventTime"] = expected["eventTime"].astype("datetime64[us, UTC]")
    pd.testing.assert_frame_equal(pdf, expected)


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
@pytest.mark.parametrize(
    "fragments,limit,version,expected",
    [
        pytest.param(
            "share8.default.dv_and_cm_table",
            5,
            None,
            pd.DataFrame(
                {
                    "id": [0, 2, 3, 4, 5],
                    "rand": [74, 45, 37, 69, 58],
                    "partition_col": [3, 3, 3, 3, 3],
                }
            ),
            id="deletion vector and column map name",
        )
    ],
)
def test_load_as_pandas_success_dv_and_cm(
    profile_path: str,
    fragments: str,
    limit: Optional[int],
    version: Optional[int],
    expected: pd.DataFrame,
):
    pdf = load_as_pandas(f"{profile_path}#{fragments}", limit, version, None)
    expected["rand"] = expected["rand"].astype("int32")
    expected["partition_col"] = expected["partition_col"].astype("int32")
    pd.testing.assert_frame_equal(pdf, expected)

    # Test client specifying explicit delta format
    pdf = load_as_pandas(f"{profile_path}#{fragments}", limit, version, None, None, True)
    pd.testing.assert_frame_equal(pdf, expected)


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
@pytest.mark.parametrize(
    "fragments,limit,version,expected",
    [
        pytest.param(
            "share8.default.dv_and_cm_table",
            0,
            None,
            pd.DataFrame(columns=["id", "rand", "partition_col"]),
            id="test empty table share",
        )
    ],
)
def test_load_as_pandas_success_empty_dv_and_cm(
    profile_path: str,
    fragments: str,
    limit: Optional[int],
    version: Optional[int],
    expected: pd.DataFrame,
):
    pdf = load_as_pandas(f"{profile_path}#{fragments}", limit, version, None)
    pd.testing.assert_frame_equal(pdf, expected)


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
@pytest.mark.parametrize(
    "fragments,expected",
    [
        pytest.param(
            "share8.default.timestampntz_cdf_table",
            pd.DataFrame(
                {
                    "id": [2, 3],
                    "time": [
                        pd.Timestamp("2024-12-10T13:17:40.123456"),
                        pd.Timestamp("1000-01-01T00:00:00.000000"),
                    ],
                }
            ),
            id="test read timestampntz",
        )
    ],
)
def test_load_as_pandas_success_timestampntz(
    profile_path: str, fragments: str, expected: pd.DataFrame
):
    pdf = load_as_pandas(f"{profile_path}#{fragments}")
    expected["time"] = expected["time"].values.astype("datetime64[us]")
    expected["id"] = expected["id"].astype("int32")
    pd.testing.assert_frame_equal(pdf, expected)


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
@pytest.mark.parametrize(
    "fragments,limit,version,expected",
    [
        pytest.param(
            "share1.default.table1",
            None,
            None,
            pd.DataFrame(
                {
                    "eventTime": [
                        pd.Timestamp("2021-04-28T06:32:22.421+00:00"),
                        pd.Timestamp("2021-04-28T06:32:02.070+00:00"),
                    ],
                    "date": [date(2021, 4, 28), date(2021, 4, 28)],
                }
            ),
            id="non partitioned",
        )
    ],
)
def test_load_as_pandas_success_client_delta_kernel_enabled_with_normal_table(
    profile_path: str,
    fragments: str,
    limit: Optional[int],
    version: Optional[int],
    expected: pd.DataFrame,
):
    pdf = load_as_pandas(f"{profile_path}#{fragments}", limit, version, None, None, True)
    expected["eventTime"] = expected["eventTime"].astype("datetime64[us, UTC]")
    pd.testing.assert_frame_equal(pdf, expected)


# We will test predicates with the table share8.default.cdf_table_with_partition
# This table is partitioned by birthday column of type date.
# There are two partitions: 2020-02-02, and 2020-01-01.
# Each partition has one row.
@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
@pytest.mark.parametrize(
    "fragments,jsonPredicateHints,expected",
    [
        # No predicates specified, so both rows are returned.
        pytest.param(
            "share8.default.cdf_table_with_partition",
            None,
            pd.DataFrame(
                {
                    "name": ["2", "1"],
                    "age": pd.Series([2, 1], dtype="int32"),
                    "birthday": [date(2020, 2, 2), date(2020, 1, 1)],
                }
            ),
            id="no predicates",
        ),
        # Equality predicate returns only one row.
        pytest.param(
            "share8.default.cdf_table_with_partition",
            (
                '{"op":"equal", "children":['
                '  {"op":"column","name":"birthday","valueType":"date"},'
                '  {"op":"literal","value":"2020-02-02","valueType":"date"}]}'
            ),
            pd.DataFrame(
                {
                    "name": ["2"],
                    "age": pd.Series([2], dtype="int32"),
                    "birthday": [date(2020, 2, 2)],
                }
            ),
            id="equal 2020-02-02",
        ),
        # Equality predicate returns the other row.
        pytest.param(
            "share8.default.cdf_table_with_partition",
            (
                '{"op":"equal", "children":['
                '  {"op":"column","name":"birthday","valueType":"date"},'
                '  {"op":"literal","value":"2020-01-01","valueType":"date"}]}'
            ),
            pd.DataFrame(
                {
                    "name": ["1"],
                    "age": pd.Series([1], dtype="int32"),
                    "birthday": [date(2020, 1, 1)],
                }
            ),
            id="equal 2020-01-01",
        ),
        # Equality predicate returns zero rows.
        pytest.param(
            "share8.default.cdf_table_with_partition",
            (
                '{"op":"equal", "children":['
                '  {"op":"column","name":"birthday","valueType":"date"},'
                '  {"op":"literal","value":"2022-02-02","valueType":"date"}]}'
            ),
            pd.DataFrame(
                {
                    "name": pd.Series([], dtype="str"),
                    "age": pd.Series([], dtype="int32"),
                    "birthday": pd.Series([], dtype="object"),
                }
            ),
            id="equal 2022-02-02",
        ),
        # GT predicate returns all rows.
        pytest.param(
            "share8.default.cdf_table_with_partition",
            (
                '{"op":"greaterThan", "children":['
                '  {"op":"column","name":"birthday","valueType":"date"},'
                '  {"op":"literal","value":"2019-01-01","valueType":"date"}]}'
            ),
            pd.DataFrame(
                {
                    "name": ["2", "1"],
                    "age": pd.Series([2, 1], dtype="int32"),
                    "birthday": [date(2020, 2, 2), date(2020, 1, 1)],
                }
            ),
            id="greatherThan 2019-01-01",
        ),
    ],
)
def test_load_as_pandas_with_json_predicates(
    profile_path: str, fragments: str, jsonPredicateHints: Optional[str], expected: pd.DataFrame
):
    pdf = load_as_pandas(f"{profile_path}#{fragments}", None, None, None, jsonPredicateHints)
    pd.testing.assert_frame_equal(pdf, expected)


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
@pytest.mark.parametrize(
    "fragments,version,timestamp,error",
    [
        pytest.param(
            "share1.default.table1",
            1,
            None,
            "Reading table by version or timestamp is not supported",
            id="version not supported",
        ),
        pytest.param(
            "share1.default.table1",
            None,
            "random_timestamp",
            "Reading table by version or timestamp is not supported",
            id="timestamp not supported",
        ),
        pytest.param(
            "share8.default.cdf_table_cdf_enabled",
            1,
            "random_timestamp",
            "Please only provide one of",
            id="only one is supported",
        ),
        pytest.param(
            "share8.default.cdf_table_cdf_enabled",
            None,
            "2000-01-01T00:00:00Z",
            "greater than or equal to",
            id="timestamp too early ",
        ),
    ],
)
def test_load_as_pandas_exception(
    profile_path: str,
    fragments: str,
    version: Optional[int],
    timestamp: Optional[str],
    error: Optional[str],
):
    try:
        load_as_pandas(f"{profile_path}#{fragments}", None, version, timestamp)
        assert False
    except Exception as e:
        assert isinstance(e, HTTPError)
        assert error in str(e)


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
@pytest.mark.parametrize(
    "fragments,version,timestamp,error",
    [
        pytest.param(
            "share8.default.table_with_cm_name",
            None,
            None,
            "Unsupported Delta Table Properties",
            id="column mapping id not supported",
        ),
    ],
)
def test_load_as_pandas_exception_client_delta_kernel_disabled_with_delta_table(
    profile_path: str,
    fragments: str,
    version: Optional[int],
    timestamp: Optional[str],
    error: Optional[str],
):
    try:
        load_as_pandas(f"{profile_path}#{fragments}", None, version, timestamp, None, False)
        assert False
    except Exception as e:
        assert error in str(e)


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
@pytest.mark.parametrize(
    "fragments,starting_version,ending_version,starting_timestamp,ending_timestamp,error,expected",
    [
        pytest.param(
            "share8.default.cdf_table_cdf_enabled",
            0,
            3,
            None,
            None,
            None,
            pd.DataFrame(
                {
                    "name": ["1", "2", "3", "3", "2", "2"],
                    "age": pd.Series([1, 2, 3, 3, 2, 2], dtype="int32"),
                    "birthday": [
                        date(2020, 1, 1),
                        date(2020, 1, 1),
                        date(2020, 1, 1),
                        date(2020, 1, 1),
                        date(2020, 1, 1),
                        date(2020, 2, 2),
                    ],
                    "_change_type": [
                        "insert",
                        "insert",
                        "insert",
                        "delete",
                        "update_preimage",
                        "update_postimage",
                    ],
                    "_commit_version": [1, 1, 1, 2, 3, 3],
                    "_commit_timestamp": [
                        1651272635000,
                        1651272635000,
                        1651272635000,
                        1651272655000,
                        1651272660000,
                        1651272660000,
                    ],
                }
            ),
            id="cdf_table_cdf_enabled table changes:[0, 3]",
        ),
        pytest.param(
            "share8.default.cdf_table_cdf_enabled",
            5,
            None,
            None,
            None,
            None,
            pd.DataFrame(
                {
                    "name": pd.Series([], dtype="object"),
                    "age": pd.Series([], dtype="int32"),
                    "birthday": pd.Series([], dtype="object"),
                    "_change_type": pd.Series([], dtype="object"),
                    "_commit_version": pd.Series([], dtype="long"),
                    "_commit_timestamp": pd.Series([], dtype="long"),
                }
            ),
            id="cdf_table_cdf_enabled table changes:[5, ]",
        ),
        pytest.param(
            "share8.default.cdf_table_cdf_enabled",
            None,
            None,
            "2000-01-01T00:00:00Z",
            None,
            "Please use a timestamp greater",
            pd.DataFrame({"not_used": []}),
            id="cdf_table_cdf_enabled table changes with starting_timestamp",
        ),
        pytest.param(
            "share8.default.cdf_table_cdf_enabled",
            0,
            None,
            None,
            "2100-01-01T00:00:00Z",
            "Please use a timestamp less",
            pd.DataFrame({"not_used": []}),
            id="cdf_table_cdf_enabled table changes with ending_timestamp",
        ),
        pytest.param(
            "share1.default.table1",
            0,
            1,
            None,
            None,
            "cdf is not enabled on table share1.default.table1",
            pd.DataFrame({"not_used": []}),
            id="table1 table changes not supported",
        ),
    ],
)
def test_load_table_changes(
    profile_path: str,
    fragments: str,
    starting_version: Optional[int],
    ending_version: Optional[int],
    starting_timestamp: Optional[str],
    ending_timestamp: Optional[str],
    error: Optional[str],
    expected: pd.DataFrame,
):
    if error is None:
        pdf = load_table_changes_as_pandas(
            f"{profile_path}#{fragments}",
            starting_version,
            ending_version,
            starting_timestamp,
            ending_timestamp,
        )
        pd.testing.assert_frame_equal(pdf, expected)
    else:
        try:
            load_table_changes_as_pandas(
                f"{profile_path}#{fragments}",
                starting_version,
                ending_version,
                starting_timestamp,
                ending_timestamp,
            )
            assert False
        except Exception as e:
            assert isinstance(e, HTTPError)
            assert error in str(e)


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
@pytest.mark.parametrize(
    "fragments,starting_version,ending_version,starting_timestamp,ending_timestamp,error,expected",
    [
        pytest.param(
            "share8.default.cdf_table_cdf_enabled",
            0,
            3,
            None,
            None,
            None,
            pd.DataFrame(
                {
                    "name": ["1", "2", "3", "3", "2", "2"],
                    "age": pd.Series([1, 2, 3, 3, 2, 2], dtype="int32"),
                    "birthday": [
                        date(2020, 1, 1),
                        date(2020, 1, 1),
                        date(2020, 1, 1),
                        date(2020, 1, 1),
                        date(2020, 1, 1),
                        date(2020, 2, 2),
                    ],
                    "_change_type": [
                        "insert",
                        "insert",
                        "insert",
                        "delete",
                        "update_preimage",
                        "update_postimage",
                    ],
                    "_commit_version": [1, 1, 1, 2, 3, 3],
                    "_commit_timestamp": [
                        1651272635000,
                        1651272635000,
                        1651272635000,
                        1651272655000,
                        1651272660000,
                        1651272660000,
                    ],
                }
            ),
            id="cdf_table_cdf_enabled table changes:[0, 3]",
        ),
        pytest.param(
            "share8.default.cdf_table_cdf_enabled",
            5,
            None,
            None,
            None,
            None,
            pd.DataFrame(
                columns=[
                    "name",
                    "age",
                    "birthday",
                    "_change_type",
                    "_commit_version",
                    "_commit_timestamp",
                ]
            ),
            id="cdf_table_cdf_enabled table changes:[5, ]",
        ),
        pytest.param(
            "share8.default.cdf_table_cdf_enabled",
            None,
            None,
            "2000-01-01T00:00:00Z",
            None,
            "Please use a timestamp greater",
            pd.DataFrame({"not_used": []}),
            id="cdf_table_cdf_enabled table changes with starting_timestamp",
        ),
        pytest.param(
            "share8.default.cdf_table_cdf_enabled",
            0,
            None,
            None,
            "2100-01-01T00:00:00Z",
            "Please use a timestamp less",
            pd.DataFrame({"not_used": []}),
            id="cdf_table_cdf_enabled table changes with ending_timestamp",
        ),
        pytest.param(
            "share1.default.table1",
            0,
            1,
            None,
            None,
            "cdf is not enabled on table share1.default.table1",
            pd.DataFrame({"not_used": []}),
            id="table1 table changes not supported",
        ),
    ],
)
def test_load_table_changes_kernel(
    profile_path: str,
    fragments: str,
    starting_version: Optional[int],
    ending_version: Optional[int],
    starting_timestamp: Optional[str],
    ending_timestamp: Optional[str],
    error: Optional[str],
    expected: pd.DataFrame,
):
    if error is None:
        pdf = load_table_changes_as_pandas(
            f"{profile_path}#{fragments}",
            starting_version,
            ending_version,
            starting_timestamp,
            ending_timestamp,
            use_delta_format=True,
        )
        if len(pdf) > 0:
            pdf["_commit_timestamp"] = pdf["_commit_timestamp"].astype("int") // 1000
        pd.testing.assert_frame_equal(pdf, expected)
    else:
        try:
            load_table_changes_as_pandas(
                f"{profile_path}#{fragments}",
                starting_version,
                ending_version,
                starting_timestamp,
                ending_timestamp,
            )
            assert False
        except Exception as e:
            assert isinstance(e, HTTPError)
            assert error in str(e)


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
@pytest.mark.parametrize(
    "fragments,starting_version,ending_version,starting_timestamp,ending_timestamp,error,expected",
    [
        pytest.param(
            "share8.default.cdf_table_with_partition",
            1,
            3,
            None,
            None,
            None,
            pd.DataFrame(
                {
                    "name": ["1", "2", "3", "2", "2", "3"],
                    "age": pd.Series([1, 2, 3, 2, 2, 3], dtype="int32"),
                    "birthday": [
                        date(2020, 1, 1),
                        date(2020, 1, 1),
                        date(2020, 3, 3),
                        date(2020, 1, 1),
                        date(2020, 2, 2),
                        date(2020, 3, 3),
                    ],
                    "_change_type": [
                        "insert",
                        "insert",
                        "insert",
                        "update_preimage",
                        "update_postimage",
                        "delete",
                    ],
                    "_commit_version": [1, 1, 1, 2, 2, 3],
                    "_commit_timestamp": [
                        1651614980000,
                        1651614980000,
                        1651614980000,
                        1651614986000,
                        1651614986000,
                        1651614994000,
                    ],
                }
            ),
            id="cdf_table_with_partition table changes:[0, 3]",
        ),
        pytest.param(
            "share8.default.cdf_table_cdf_enabled",
            5,
            None,
            None,
            None,
            None,
            pd.DataFrame(
                columns=[
                    "name",
                    "age",
                    "birthday",
                    "_change_type",
                    "_commit_version",
                    "_commit_timestamp",
                ]
            ),
            id="cdf_table_with_partition table changes:[5, ]",
        ),
        pytest.param(
            "share8.default.cdf_table_with_partition",
            None,
            None,
            "2022-05-03T21:56:25Z",
            "2022-05-03T21:56:30Z",
            None,
            pd.DataFrame(
                {
                    "name": ["2", "2"],
                    "age": pd.Series([2, 2], dtype="int32"),
                    "birthday": [
                        date(2020, 1, 1),
                        date(2020, 2, 2),
                    ],
                    "_change_type": [
                        "update_preimage",
                        "update_postimage",
                    ],
                    "_commit_version": [2, 2],
                    "_commit_timestamp": [
                        1651614986000,
                        1651614986000,
                    ],
                }
            ),
            id=(
                "cdf_table_with_partition table changes with"
                "starting_timestamp and ending_timestamp"
            ),
        ),
    ],
)
def test_load_table_changes_partition_kernel(
    profile_path: str,
    fragments: str,
    starting_version: Optional[int],
    ending_version: Optional[int],
    starting_timestamp: Optional[str],
    ending_timestamp: Optional[str],
    error: Optional[str],
    expected: pd.DataFrame,
):
    if error is None:
        pdf = load_table_changes_as_pandas(
            f"{profile_path}#{fragments}",
            starting_version,
            ending_version,
            starting_timestamp,
            ending_timestamp,
            use_delta_format=True,
        )
        if len(pdf) > 0:
            pdf["_commit_timestamp"] = pdf["_commit_timestamp"].astype("int") // 1000
        pd.testing.assert_frame_equal(pdf, expected)
    else:
        try:
            load_table_changes_as_pandas(
                f"{profile_path}#{fragments}",
                starting_version,
                ending_version,
                starting_timestamp,
                ending_timestamp,
            )
            assert False
        except Exception as e:
            assert isinstance(e, HTTPError)
            assert error in str(e)


# TODO: Enable once timestampntz + CDF support is enabled for both
@pytest.mark.skipif(
    True, reason="timestampNtz + CDF not supported in OSS server or delta-kernel-rs yet"
)
@pytest.mark.parametrize(
    "fragments,expected",
    [
        pytest.param(
            "share8.default.timestampntz_cdf_table",
            pd.DataFrame(
                {
                    "id": [2, 1, 2, 2, 3, 1],
                    "time": [
                        pd.Timestamp("2024-12-10T13:17:40.000000"),
                        pd.Timestamp("2021-07-01T08:43:28.000000"),
                        pd.Timestamp("2024-12-10T13:17:40.123456"),
                        pd.Timestamp("2024-12-10T13:17:40.000000"),
                        pd.Timestamp("1000-01-01T00:00:00.000000"),
                        pd.Timestamp("2021-07-01T08:43:28.000000"),
                    ],
                    "_change_type": [
                        "delete",
                        "delete",
                        "insert",
                        "insert",
                        "insert",
                        "insert",
                    ],
                    "_commit_version": [3, 4, 3, 2, 2, 1],
                    "_commit_timestamp": [
                        1741140666000,
                        1741144375000,
                        1741140666000,
                        1741140657000,
                        1741140657000,
                        1741140565000,
                    ],
                }
            ),
            id="test read timestampntz",
        )
    ],
)
def test_load_table_changes_as_pandas_timestampntz(
    profile_path: str, fragments: str, expected: pd.DataFrame
):
    pdf = load_table_changes_as_pandas(
        f"{profile_path}#{fragments}", starting_version=0, use_delta_format=True
    )
    expected["time"] = expected["time"].values.astype("datetime64[us]")
    expected["id"] = expected["id"].astype("int32")
    pdf["_commit_timestamp"] = pdf["_commit_timestamp"].astype("int") // 1000
    pd.testing.assert_frame_equal(pdf, expected)


def test_parse_url():
    def check_invalid_url(url: str):
        with pytest.raises(ValueError, match=f"Invalid 'url': {url}"):
            _parse_url(url)

    check_invalid_url("")
    check_invalid_url("#")
    check_invalid_url("foo")
    check_invalid_url("foo#")
    check_invalid_url("foo#share")
    check_invalid_url("foo#share.schema")
    check_invalid_url("foo#share.schema.")

    assert _parse_url("profile#share.schema.table") == ("profile", "share", "schema", "table")
    assert _parse_url("foo#bar#share.schema.table") == ("foo#bar", "share", "schema", "table")


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
@pytest.mark.parametrize(
    "fragments,version,timestamp,error,expected_data,expected_schema_str",
    [
        pytest.param(
            "share1.default.table1",
            None,
            None,
            None,
            [
                (datetime(2021, 4, 27, 23, 32, 22, 421000), date(2021, 4, 28)),
                (datetime(2021, 4, 27, 23, 32, 2, 70000), date(2021, 4, 28)),
            ],
            "eventTime: timestamp, date: date",
            id="table1 spark",
        ),
        pytest.param(
            "share1.default.table1",
            1,
            None,
            "not supported",
            [],
            "not-used-schema-str",
            id="table1 version not supported",
        ),
        pytest.param(
            "share1.default.table1",
            None,
            "random_timestamp",
            "not supported",
            [],
            "not-used-schema-str",
            id="table1 timestamp not supported",
        ),
        pytest.param(
            "share8.default.cdf_table_cdf_enabled",
            1,
            None,
            None,
            [
                ("1", 1, date(2020, 1, 1)),
                ("3", 3, date(2020, 1, 1)),
                ("2", 2, date(2020, 1, 1)),
            ],
            "name: string, age: int, birthday: date",
            id="cdf_table_cdf_enabled version 1 spark",
        ),
        pytest.param(
            "share8.default.cdf_table_cdf_enabled",
            None,
            "2000-01-01T00:00:00Z",
            "Please use a timestamp greater",
            [],
            "not-used-schema-str",
            id="cdf_table_cdf_enabled timestamp too early",
        ),
        pytest.param(
            "share8.default.cdf_table_cdf_enabled",
            1,
            "2000-01-01T00:00:00Z",
            "Please either provide",
            [],
            "not-used-schema-str",
            id="cdf_table_cdf_enabled timestamp too early",
        ),
    ],
)
def test_load_as_spark(
    profile_path: str,
    fragments: str,
    version: Optional[int],
    timestamp: Optional[str],
    error: Optional[str],
    expected_data: list,
    expected_schema_str: str,
):
    try:
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.appName("delta-sharing-test")
            .master("local[*]")
            .config("spark.jars.packages", "io.delta:delta-sharing-spark_2.12:1.0.0-SNAPSHOT")
            .config("spark.delta.sharing.network.sslTrustAll", "true")
            .getOrCreate()
        )

        if error is None:
            expected_df = spark.createDataFrame(expected_data, expected_schema_str)
            actual_df = load_as_spark(f"{profile_path}#{fragments}", version, timestamp)
            assert expected_df.schema == actual_df.schema
            assert expected_df.collect() == actual_df.collect()
        else:
            try:
                load_as_spark(f"{profile_path}#{fragments}", version, timestamp).collect()
                assert False
            except Exception as e:
                assert error in str(e)
    except ImportError:
        with pytest.raises(
            ImportError, match="Unable to import pyspark. `load_as_spark` requires PySpark."
        ):
            load_as_spark("not-used")


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
@pytest.mark.parametrize(
    "fragments,starting_version,ending_version,starting_timestamp,ending_timestamp,error,"
    + "expected_data,expected_schema_str",
    [
        pytest.param(
            "share8.default.cdf_table_cdf_enabled",
            0,
            3,
            None,
            None,
            None,
            [
                ("1", 1, date(2020, 1, 1), 1, 1651272635000, "insert"),
                ("2", 2, date(2020, 1, 1), 1, 1651272635000, "insert"),
                ("3", 3, date(2020, 1, 1), 1, 1651272635000, "insert"),
                ("2", 2, date(2020, 1, 1), 3, 1651272660000, "update_preimage"),
                ("2", 2, date(2020, 2, 2), 3, 1651272660000, "update_postimage"),
                ("3", 3, date(2020, 1, 1), 2, 1651272655000, "delete"),
            ],
            "name: string, age: int, birthday:date, _commit_version: long, _commit_timestamp"
            + ": long, _change_type: string",
            id="cdf_table_cdf_enabled table changes",
        ),
        pytest.param(
            "share8.default.cdf_table_cdf_enabled",
            None,
            None,
            "2000-01-01T00:00:00Z",
            None,
            "Please use a timestamp greater",
            [],
            "unused-schema-str",
            id="cdf_table_cdf_enabled starting_timestamp correctly passed",
        ),
        pytest.param(
            "share8.default.cdf_table_cdf_enabled",
            0,
            None,
            None,
            "2100-01-01T00:00:00Z",
            "Please use a timestamp less than",
            [],
            "unused-schema-str",
            id="cdf_table_cdf_enabled ending_timestamp correctly passed",
        ),
        pytest.param(
            "share1.default.table1",
            0,
            3,
            None,
            None,
            "cdf is not enabled on table share1.default.table1",
            [],
            "name: string, age: int, birthday:date, _commit_version: long, _commit_timestamp"
            + ": long, _change_type: string",
            id="table1 table changes not enabled",
        ),
    ],
)
def test_load_table_changes_as_spark(
    profile_path: str,
    fragments: str,
    starting_version: Optional[int],
    ending_version: Optional[int],
    starting_timestamp: Optional[str],
    ending_timestamp: Optional[str],
    error: Optional[str],
    expected_data: list,
    expected_schema_str: str,
):
    try:
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.appName("delta-sharing-test")
            .master("local[*]")
            .config("spark.jars.packages", "io.delta:delta-sharing-spark_2.12:1.0.0-SNAPSHOT")
            .config("spark.delta.sharing.network.sslTrustAll", "true")
            .getOrCreate()
        )

        if error is None:
            expected_df = spark.createDataFrame(expected_data, expected_schema_str)

            actual_df = load_table_changes_as_spark(
                f"{profile_path}#{fragments}",
                starting_version=starting_version,
                ending_version=ending_version,
                starting_timestamp=starting_timestamp,
                ending_timestamp=ending_timestamp,
            )
            assert expected_df.schema == actual_df.schema
            assert expected_df.collect() == actual_df.collect()
        else:
            try:
                load_table_changes_as_spark(
                    f"{profile_path}#{fragments}",
                    starting_version=starting_version,
                    ending_version=ending_version,
                    starting_timestamp=starting_timestamp,
                    ending_timestamp=ending_timestamp,
                )
            except Exception as e:
                assert isinstance(e, HTTPError)
                assert error in str(e)

    except ImportError:
        with pytest.raises(
            ImportError,
            match="Unable to import pyspark. `load_table_changes_as_spark` requires" + " PySpark.",
        ):
            load_table_changes_as_spark("not-used")


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
@pytest.mark.parametrize(
    "fragments",
    [pytest.param("share8.default.12k_rows")],
)
def test_load_as_pandas_delta_batch_convert(
    profile_path: str,
    fragments: str,
):
    ids = list(range(12000))
    expected = pd.DataFrame(
        {
            "id": ids,
            "name": [f"str_{n}" for n in ids],
            "time": [pd.Timestamp(n * 10**5, unit="s", tz="UTC") for n in ids],
            "val": [n**0.5 for n in ids],
        }
    )
    expected["time"] = expected["time"].astype("datetime64[us, UTC]")

    pdf = (
        load_as_pandas(
            f"{profile_path}#{fragments}", use_delta_format=True, convert_in_batches=True
        )
        .sort_values(by="id")
        .reset_index(drop=True)
    )
    pd.testing.assert_frame_equal(pdf, expected)
    pdf = load_as_pandas(
        f"{profile_path}#{fragments}", use_delta_format=True, convert_in_batches=True, limit=500
    )
    assert len(pdf) == 500
    pdf = load_as_pandas(
        f"{profile_path}#{fragments}", use_delta_format=True, convert_in_batches=True, limit=3000
    )
    assert len(pdf) == 3000


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
@pytest.mark.parametrize(
    "fragments",
    [pytest.param("share8.default.12k_rows")],
)
def test_load_table_changes_as_pandas_delta_batch_convert(
    profile_path: str,
    fragments: str,
):
    rows_to_insert = list(range(100, 1600, 100))  # adds up to 12000

    version_to_timestamp = {
        1: pd.Timestamp("2025-05-29T02:51:51.000+00:00"),
        2: pd.Timestamp("2025-05-29T02:51:53.000+00:00"),
        3: pd.Timestamp("2025-05-29T02:51:54.000+00:00"),
        4: pd.Timestamp("2025-05-29T02:51:56.000+00:00"),
        5: pd.Timestamp("2025-05-29T02:51:57.000+00:00"),
        6: pd.Timestamp("2025-05-29T02:51:58.000+00:00"),
        7: pd.Timestamp("2025-05-29T02:52:00.000+00:00"),
        8: pd.Timestamp("2025-05-29T02:52:01.000+00:00"),
        9: pd.Timestamp("2025-05-29T02:52:03.000+00:00"),
        10: pd.Timestamp("2025-05-29T02:52:04.000+00:00"),
        11: pd.Timestamp("2025-05-29T02:52:05.000+00:00"),
        12: pd.Timestamp("2025-05-29T02:52:07.000+00:00"),
        13: pd.Timestamp("2025-05-29T02:52:09.000+00:00"),
        14: pd.Timestamp("2025-05-29T02:52:11.000+00:00"),
        15: pd.Timestamp("2025-05-29T02:52:12.000+00:00"),
    }

    rows_inserted = 0
    version = 1
    expected_pdfs = []
    for row_count in rows_to_insert:
        ids = list(range(rows_inserted, rows_inserted + row_count))
        pdf_added = pd.DataFrame(
            {
                "id": ids,
                "name": [f"str_{n}" for n in ids],
                "time": [pd.Timestamp(n * 10**5, unit="s", tz="UTC") for n in ids],
                "val": [n**0.5 for n in ids],
                "_change_type": "insert",
                "_commit_version": version,
                "_commit_timestamp": version_to_timestamp[version],
            }
        )
        expected_pdfs.append(pdf_added)
        rows_inserted += row_count
        version += 1

    expected = (
        pd.concat(expected_pdfs, axis=0)
        .sort_values(by=["_commit_timestamp", "id"])
        .reset_index(drop=True)
    )
    expected["_commit_timestamp"] = expected["_commit_timestamp"].astype("datetime64[us, UTC]")
    expected["time"] = expected["time"].astype("datetime64[us, UTC]")
    pdf = (
        load_table_changes_as_pandas(
            f"{profile_path}#{fragments}",
            starting_version=0,
            use_delta_format=True,
            convert_in_batches=True,
        )
        .sort_values(by=["_commit_timestamp", "id"])
        .reset_index(drop=True)
    )
    pd.testing.assert_frame_equal(pdf, expected)


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
@pytest.mark.parametrize(
    "fragments,version,expected",
    [
        pytest.param(
            "share8.default.add_columns_non_partitioned_cdf",
            3,
            # table initially contains c1 = [1, 2] at version 1
            # then we add int column c2 at version 2
            # then we insert a row with c1 = 3, c2 = 4
            pd.DataFrame({"c1": pd.Series([1, 2, 3], dtype="int32"), "c2": [None, None, 4]}),
            id="version 3",
        ),
        pytest.param(
            "share8.default.add_columns_non_partitioned_cdf",
            4,
            # v4: change c2 to 5 where c1 = 1
            pd.DataFrame({"c1": pd.Series([1, 2, 3], dtype="int32"), "c2": [5.0, None, 4.0]}),
            id="version 4",
        ),
        pytest.param(
            "share8.default.add_columns_non_partitioned_cdf",
            5,
            # v5: add float columns c3 and c4
            pd.DataFrame(
                {
                    "c1": pd.Series([1, 2, 3], dtype="int32"),
                    "c2": [5.0, None, 4.0],
                    "c3": [None, None, None],
                    "c4": [None, None, None],
                }
            ),
            id="version 5",
        ),
        pytest.param(
            "share8.default.add_columns_non_partitioned_cdf",
            6,
            # v6: add rows, some with non-null values for c3 and c4
            pd.DataFrame(
                {
                    "c1": pd.Series([1, 2, 3, 5, 6, 7], dtype="int32"),
                    "c2": [5, None, 4, 6, None, 8],
                    "c3": pd.Series([None, None, None, 0.1, 0.2, None], dtype="float32"),
                    "c4": pd.Series([None, None, None, 0.1, 0.2, None], dtype="float32"),
                }
            ),
            id="version 6",
        ),
        pytest.param(
            "share8.default.add_columns_non_partitioned_cdf",
            7,
            # v7: delete rows where c1 = 2 or c1 = 6
            pd.DataFrame(
                {
                    "c1": pd.Series([1, 3, 5, 7], dtype="int32"),
                    "c2": pd.Series([5, 4, 6, 8], dtype="int32"),
                    "c3": pd.Series([None, None, 0.1, None], dtype="float32"),
                    "c4": pd.Series([None, None, 0.1, None], dtype="float32"),
                }
            ),
            id="version 7",
        ),
        pytest.param(
            "share8.default.add_columns_non_partitioned_cdf",
            8,
            # v8: update c3 to 0.0 where c1 = 3 or c2 = 8
            pd.DataFrame(
                {
                    "c1": pd.Series([1, 3, 5, 7], dtype="int32"),
                    "c2": pd.Series([5, 4, 6, 8], dtype="int32"),
                    "c3": pd.Series([None, 0, 0.1, 0], dtype="float32"),
                    "c4": pd.Series([None, None, 0.1, None], dtype="float32"),
                }
            ),
            id="version 8",
        ),
        pytest.param(
            "share8.default.add_columns_non_partitioned_cdf",
            9,
            # v9: set c4 to NULL where c1 < 7
            pd.DataFrame(
                {
                    "c1": pd.Series([1, 3, 5, 7], dtype="int32"),
                    "c2": pd.Series([5, 4, 6, 8], dtype="int32"),
                    "c3": pd.Series([None, 0, 0.1, 0], dtype="float32"),
                    "c4": pd.Series([None, None, None, None], dtype="float32"),
                }
            ),
            id="version 9",
        ),
        pytest.param(
            "share8.default.add_columns_non_partitioned_cdf",
            None,
            pd.DataFrame(
                {
                    "c1": pd.Series([1, 3, 5, 7], dtype="int32"),
                    "c2": pd.Series([5, 4, 6, 8], dtype="int32"),
                    "c3": pd.Series([None, 0, 0.1, 0], dtype="float32"),
                    "c4": pd.Series([None, None, None, None], dtype="float32"),
                }
            ),
            id="latest",
        ),
    ],
)
def test_add_column_non_partitioned(
    profile_path: str,
    fragments: str,
    version: Optional[int],
    expected: pd.DataFrame,
):
    pdf = load_as_pandas(f"{profile_path}#{fragments}", version=version, use_delta_format=False)
    # sort to eliminate row order inconsistencies
    pdf = pdf.sort_values(by="c1").reset_index(drop=True)
    # check_dtype=False to deal with minor type discrepancies like float32 vs float64
    pd.testing.assert_frame_equal(pdf, expected, check_dtype=False)

    pdf_delta = load_as_pandas(
        f"{profile_path}#{fragments}", version=version, use_delta_format=True
    )
    pdf_delta = pdf_delta.sort_values(by="c1").reset_index(drop=True)
    pd.testing.assert_frame_equal(pdf_delta, expected, check_dtype=False)


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
@pytest.mark.parametrize(
    "fragments,version,expected",
    [
        pytest.param(
            "share8.default.add_columns_partitioned_cdf",
            1,
            # initial data at version 1
            pd.DataFrame(
                {
                    "p1": pd.Series([0.1, 0.1, 0.1, 0.1, None], dtype="float32"),
                    "p2": ["a", "a", "a", "b", "a"],
                    "c1": [0, 1, 2, None, 0],
                }
            ),
            id="version 1",
        ),
        pytest.param(
            "share8.default.add_columns_partitioned_cdf",
            2,
            # v2: add some rows containing NULL partition values
            pd.DataFrame(
                {
                    "p1": pd.Series([0.1, 0.1, 0.1, 0.1, None, None, None], dtype="float32"),
                    "p2": ["a", "a", "a", "b", "a", None, None],
                    "c1": [0, 1, 2, None, 0, 0, None],
                }
            ),
            id="version 2",
        ),
        pytest.param(
            "share8.default.add_columns_partitioned_cdf",
            3,
            # v3: add int columns c2 and c3
            pd.DataFrame(
                {
                    "p1": pd.Series([0.1, 0.1, 0.1, 0.1, None, None, None], dtype="float32"),
                    "p2": ["a", "a", "a", "b", "a", None, None],
                    "c1": [0, 1, 2, None, 0, 0, None],
                    "c2": [None, None, None, None, None, None, None],
                    "c3": [None, None, None, None, None, None, None],
                }
            ),
            id="version 3",
        ),
        pytest.param(
            "share8.default.add_columns_partitioned_cdf",
            4,
            # v4: set c2 = 10 where c1 = 0
            pd.DataFrame(
                {
                    "p1": pd.Series([0.1, 0.1, 0.1, 0.1, None, None, None], dtype="float32"),
                    "p2": ["a", "a", "a", "b", "a", None, None],
                    "c1": [0, 1, 2, None, 0, 0, None],
                    "c2": [10, None, None, None, 10, 10, None],
                    "c3": [None, None, None, None, None, None, None],
                }
            ),
            id="version 4",
        ),
        pytest.param(
            "share8.default.add_columns_partitioned_cdf",
            5,
            # v5: add more rows, containing values in c2 and c3
            pd.DataFrame(
                {
                    "p1": pd.Series(
                        [0.1, 0.1, 0.1, 0.1, 0.1, 0.2, 0.3, None, None, None], dtype="float32"
                    ),
                    "p2": ["a", "a", "a", "b", "b", "a", "a", "a", None, None],
                    "c1": [0, 1, 2, 3, None, 2, 2, 0, 0, None],
                    "c2": [10, None, None, 30, None, 20, 20, 10, 10, None],
                    "c3": pd.Series(
                        [None, None, None, 300, None, 200, 200, None, None, None], dtype="object"
                    ),
                }
            ),
            id="version 5",
        ),
        pytest.param(
            "share8.default.add_columns_partitioned_cdf",
            6,
            # v6: delete partitions where p2 is NULL
            pd.DataFrame(
                {
                    "p1": pd.Series([0.1, 0.1, 0.1, 0.1, 0.1, 0.2, 0.3, None], dtype="float32"),
                    "p2": ["a", "a", "a", "b", "b", "a", "a", "a"],
                    "c1": [0, 1, 2, 3, None, 2, 2, 0],
                    "c2": [10, None, None, 30, None, 20, 20, 10],
                    "c3": pd.Series([None, None, None, 300, None, 200, 200, None], dtype="object"),
                }
            ),
            id="version 6",
        ),
        pytest.param(
            "share8.default.add_columns_partitioned_cdf",
            7,
            # v7: update c1 = c3, c2 =-1 where p1 is NULL or p2 = "a"
            pd.DataFrame(
                {
                    "p1": pd.Series([0.1, 0.1, 0.1, 0.1, 0.1, 0.2, 0.3, None], dtype="float32"),
                    "p2": ["a", "a", "a", "b", "b", "a", "a", "a"],
                    "c1": [10, None, None, 3, None, 20, 20, 10],
                    "c2": [10, None, None, 30, None, 20, 20, 10],
                    "c3": pd.Series([-1, -1, -1, 300, None, -1, -1, -1], dtype="object"),
                }
            ),
            id="version 7",
        ),
        pytest.param(
            "share8.default.add_columns_partitioned_cdf",
            None,
            pd.DataFrame(
                {
                    "p1": pd.Series([0.1, 0.1, 0.1, 0.1, 0.1, 0.2, 0.3, None], dtype="float32"),
                    "p2": ["a", "a", "a", "b", "b", "a", "a", "a"],
                    "c1": [10, None, None, 3, None, 20, 20, 10],
                    "c2": [10, None, None, 30, None, 20, 20, 10],
                    "c3": pd.Series([-1, -1, -1, 300, None, -1, -1, -1], dtype="object"),
                }
            ),
            id="latest",
        ),
    ],
)
def test_add_column_partitioned(
    profile_path: str,
    fragments: str,
    version: Optional[int],
    expected: pd.DataFrame,
):
    pdf = load_as_pandas(f"{profile_path}#{fragments}", version=version, use_delta_format=False)
    # sort to eliminate row order inconsistencies
    pdf = pdf.sort_values(by=["p1", "p2", "c1"]).reset_index(drop=True)
    # check_dtype=False to deal with minor type discrepancies like float32 vs float64
    pd.testing.assert_frame_equal(pdf, expected, check_dtype=False)

    pdf_delta = load_as_pandas(
        f"{profile_path}#{fragments}", version=version, use_delta_format=True
    )
    pdf_delta = pdf_delta.sort_values(by=["p1", "p2", "c1"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(pdf_delta, expected, check_dtype=False)


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
@pytest.mark.parametrize(
    "fragments,starting_version,ending_version,expected",
    [
        pytest.param(
            "share8.default.add_columns_non_partitioned_cdf",
            0,
            None,
            # see test_add_column_partitioned for specific changes in each version
            pd.DataFrame(
                [
                    [1, None, None, None, "insert", 1, "2025-11-20T02:38:48.000+00:00"],
                    [2, None, None, None, "insert", 1, "2025-11-20T02:38:48.000+00:00"],
                    [3, 4, None, None, "insert", 3, "2025-11-20T02:39:24.000+00:00"],
                    [1, 5, None, None, "update_postimage", 4, "2025-11-20T02:40:07.000+00:00"],
                    [1, None, None, None, "update_preimage", 4, "2025-11-20T02:40:07.000+00:00"],
                    [5, 6, 0.1, 0.1, "insert", 6, "2025-11-20T02:40:46.000+00:00"],
                    [6, None, 0.2, 0.2, "insert", 6, "2025-11-20T02:40:46.000+00:00"],
                    [7, 8, None, None, "insert", 6, "2025-11-20T02:40:46.000+00:00"],
                    [2, None, None, None, "delete", 7, "2025-11-20T02:41:04.000+00:00"],
                    [6, None, 0.2, 0.2, "delete", 7, "2025-11-20T02:41:04.000+00:00"],
                    [3, 4, 0, None, "update_postimage", 8, "2025-11-20T02:41:09.000+00:00"],
                    [7, 8, 0, None, "update_postimage", 8, "2025-11-20T02:41:09.000+00:00"],
                    [3, 4, None, None, "update_preimage", 8, "2025-11-20T02:41:09.000+00:00"],
                    [7, 8, None, None, "update_preimage", 8, "2025-11-20T02:41:09.000+00:00"],
                    [1, 5, None, None, "update_postimage", 9, "2025-11-20T02:41:15.000+00:00"],
                    [3, 4, 0, None, "update_postimage", 9, "2025-11-20T02:41:15.000+00:00"],
                    [5, 6, 0.1, None, "update_postimage", 9, "2025-11-20T02:41:15.000+00:00"],
                    [1, 5, None, None, "update_preimage", 9, "2025-11-20T02:41:15.000+00:00"],
                    [3, 4, 0, None, "update_preimage", 9, "2025-11-20T02:41:15.000+00:00"],
                    [5, 6, 0.1, 0.1, "update_preimage", 9, "2025-11-20T02:41:15.000+00:00"],
                ],
                columns=[
                    "c1",
                    "c2",
                    "c3",
                    "c4",
                    "_change_type",
                    "_commit_version",
                    "_commit_timestamp",
                ],
            ),
            id="Full CDF with columns added",
        ),
    ],
)
def test_add_column_non_partitioned_cdf(
    profile_path: str,
    fragments: str,
    starting_version: Optional[int],
    ending_version: Optional[int],
    expected: pd.DataFrame,
):
    expected["c1"] = expected["c1"].astype("int32")
    expected["c3"] = expected["c3"].astype("float32")
    expected["c4"] = expected["c4"].astype("float32")
    # convert _commit_timestamp from date string to unix timestamp
    expected["_commit_timestamp"] = (
        pd.to_datetime(expected["_commit_timestamp"]).map(pd.Timestamp.timestamp).astype("int64")
        * 1000
    )
    pdf = load_table_changes_as_pandas(
        f"{profile_path}#{fragments}", starting_version, ending_version, use_delta_format=False
    )
    pdf = pdf.sort_values(by=["_commit_timestamp", "_change_type", "c1"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(pdf, expected)
    # TODO: enable once delta-kernel-rs supports schema changes during version range
    # pdf_delta = load_table_changes_as_pandas(
    #     f"{profile_path}#{fragments}", starting_version, ending_version, use_delta_format=True
    # )
    # pdf_delta = pdf_delta.sort_values(by=["_commit_timestamp", "_change_type", "c1"]).reset_index(
    #     drop=True
    # )
    # pd.testing.assert_frame_equal(pdf_delta, expected)


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
@pytest.mark.parametrize(
    "fragments,starting_version,ending_version,expected",
    [
        pytest.param(
            "share8.default.add_columns_partitioned_cdf",
            0,
            None,
            # see test_add_column_non_partitioned for specific changes in each version
            pd.DataFrame(
                [
                    [0.1, "a", 0, None, None, "insert", 1, "2025-11-20T03:08:37.000+00:00"],
                    [0.1, "a", 1, None, None, "insert", 1, "2025-11-20T03:08:37.000+00:00"],
                    [0.1, "a", 2, None, None, "insert", 1, "2025-11-20T03:08:37.000+00:00"],
                    [0.1, "b", None, None, None, "insert", 1, "2025-11-20T03:08:37.000+00:00"],
                    [None, "a", 0, None, None, "insert", 1, "2025-11-20T03:08:37.000+00:00"],
                    [None, None, 0, None, None, "insert", 2, "2025-11-20T03:08:39.000+00:00"],
                    [None, None, None, None, None, "insert", 2, "2025-11-20T03:08:39.000+00:00"],
                    [0.1, "a", 0, 10, None, "update_postimage", 4, "2025-11-20T03:08:45.000+00:00"],
                    [
                        None,
                        "a",
                        0,
                        10,
                        None,
                        "update_postimage",
                        4,
                        "2025-11-20T03:08:45.000+00:00",
                    ],
                    [
                        None,
                        None,
                        0,
                        10,
                        None,
                        "update_postimage",
                        4,
                        "2025-11-20T03:08:45.000+00:00",
                    ],
                    [
                        0.1,
                        "a",
                        0,
                        None,
                        None,
                        "update_preimage",
                        4,
                        "2025-11-20T03:08:45.000+00:00",
                    ],
                    [
                        None,
                        "a",
                        0,
                        None,
                        None,
                        "update_preimage",
                        4,
                        "2025-11-20T03:08:45.000+00:00",
                    ],
                    [
                        None,
                        None,
                        0,
                        None,
                        None,
                        "update_preimage",
                        4,
                        "2025-11-20T03:08:45.000+00:00",
                    ],
                    [0.1, "b", 3, 30, 300, "insert", 5, "2025-11-20T03:08:48.000+00:00"],
                    [0.2, "a", 2, 20, 200, "insert", 5, "2025-11-20T03:08:48.000+00:00"],
                    [0.3, "a", 2, 20, 200, "insert", 5, "2025-11-20T03:08:48.000+00:00"],
                    [None, None, 0, 10, None, "delete", 6, "2025-11-20T03:08:50.000+00:00"],
                    [None, None, None, None, None, "delete", 6, "2025-11-20T03:08:50.000+00:00"],
                    [0.1, "a", 10, 10, -1, "update_postimage", 7, "2025-11-20T03:08:52.000+00:00"],
                    [
                        0.1,
                        "a",
                        None,
                        None,
                        -1,
                        "update_postimage",
                        7,
                        "2025-11-20T03:08:52.000+00:00",
                    ],
                    [
                        0.1,
                        "a",
                        None,
                        None,
                        -1,
                        "update_postimage",
                        7,
                        "2025-11-20T03:08:52.000+00:00",
                    ],
                    [0.2, "a", 20, 20, -1, "update_postimage", 7, "2025-11-20T03:08:52.000+00:00"],
                    [0.3, "a", 20, 20, -1, "update_postimage", 7, "2025-11-20T03:08:52.000+00:00"],
                    [None, "a", 10, 10, -1, "update_postimage", 7, "2025-11-20T03:08:52.000+00:00"],
                    [0.1, "a", 0, 10, None, "update_preimage", 7, "2025-11-20T03:08:52.000+00:00"],
                    [
                        0.1,
                        "a",
                        1,
                        None,
                        None,
                        "update_preimage",
                        7,
                        "2025-11-20T03:08:52.000+00:00",
                    ],
                    [
                        0.1,
                        "a",
                        2,
                        None,
                        None,
                        "update_preimage",
                        7,
                        "2025-11-20T03:08:52.000+00:00",
                    ],
                    [0.2, "a", 2, 20, 200, "update_preimage", 7, "2025-11-20T03:08:52.000+00:00"],
                    [0.3, "a", 2, 20, 200, "update_preimage", 7, "2025-11-20T03:08:52.000+00:00"],
                    [None, "a", 0, 10, None, "update_preimage", 7, "2025-11-20T03:08:52.000+00:00"],
                ],
                columns=[
                    "p1",
                    "p2",
                    "c1",
                    "c2",
                    "c3",
                    "_change_type",
                    "_commit_version",
                    "_commit_timestamp",
                ],
            ),
            id="Full CDF with columns added",
        ),
    ],
)
def test_add_column_partitioned_cdf(
    profile_path: str,
    fragments: str,
    starting_version: Optional[int],
    ending_version: Optional[int],
    expected: pd.DataFrame,
):
    expected["p1"] = expected["p1"].astype("float32")
    # convert _commit_timestamp from date string to unix timestamp
    expected["_commit_timestamp"] = (
        pd.to_datetime(expected["_commit_timestamp"]).map(pd.Timestamp.timestamp).astype("int64")
        * 1000
    )
    pdf = load_table_changes_as_pandas(
        f"{profile_path}#{fragments}", starting_version, ending_version, use_delta_format=False
    )
    pdf = pdf.sort_values(by=["_commit_timestamp", "_change_type", "p1", "p2", "c1"]).reset_index(
        drop=True
    )
    print(pdf)
    pd.testing.assert_frame_equal(pdf, expected)
    # TODO: enable once delta-kernel-rs supports schema changes during version range
    # pdf_delta = load_table_changes_as_pandas(
    #     f"{profile_path}#{fragments}", starting_version, ending_version, use_delta_format=True
    # )
    # pdf_delta = pdf_delta.sort_values(by=["_commit_timestamp", "_change_type", "c1"]).reset_index(
    #     drop=True
    # )
    # pd.testing.assert_frame_equal(pdf_delta, expected)
