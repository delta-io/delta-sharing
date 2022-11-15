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
    load_as_pandas,
    load_as_spark,
    load_table_changes_as_spark,
    load_table_changes_as_pandas,
    _parse_url,
)
from delta_sharing.protocol import Schema, Share, Table
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
        Share(name="share8")
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
        Table(name="table7", share="share1", schema="default")
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
        Table(name="table_data_loss_no_checkpoint", share="share8", schema="default")
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
                        pd.Timestamp("2021-04-28 23:36:47.599"),
                        pd.Timestamp("2021-04-28 23:35:53.156"),
                    ],
                    "date": [date(2021, 4, 28), date(2021, 4, 28), date(2021, 4, 28)],
                    "type": ["bar", "foo", None],
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
                        pd.Timestamp("2021-04-28 23:36:47.599"),
                    ],
                    "date": [date(2021, 4, 28), date(2021, 4, 28)],
                    "type": ["bar", "foo"],
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
                        pd.Timestamp("2021-04-28 23:36:47.599"),
                        pd.Timestamp("2021-04-28 23:35:53.156"),
                    ],
                    "date": [date(2021, 4, 28), date(2021, 4, 28), date(2021, 4, 28)],
                    "type": ["bar", "foo", None],
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
                        pd.Timestamp("2021-04-28 23:36:47.599"),
                        pd.Timestamp("2021-04-28 23:35:53.156"),
                    ],
                    "date": [date(2021, 4, 28), date(2021, 4, 28), date(2021, 4, 28)],
                    "type": ["bar", "foo", None],
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
                    "name": ["1", "2"],
                    "age": pd.Series([1, 2], dtype="int32"),
                    "birthday": [date(2020, 1, 1), date(2020, 2, 2)],
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
                    "name": ["1", "3", "2"],
                    "age": pd.Series([1, 3, 2], dtype="int32"),
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
                        pd.Timestamp("2021-04-28 23:33:57.955"),
                        pd.Timestamp("2021-04-28 23:33:48.719"),
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
    expected: pd.DataFrame
):
    pdf = load_as_pandas(f"{profile_path}#{fragments}", limit, version, None)
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
            "2000-01-01 00:00:00",
            "Please use a timestamp greater",
            id="timestap too early ",
        ),
    ],
)
def test_load_as_pandas_exception(
    profile_path: str,
    fragments: str,
    version: Optional[int],
    timestamp: Optional[str],
    error: Optional[str]
):
    try:
        load_as_pandas(f"{profile_path}#{fragments}", None, version, timestamp)
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
                    "name": ["3", "2", "2", "1", "2", "3"],
                    "age": pd.Series([3, 2, 2, 1, 2, 3], dtype="int32"),
                    "birthday": [
                        date(2020, 1, 1),
                        date(2020, 1, 1),
                        date(2020, 2, 2),
                        date(2020, 1, 1),
                        date(2020, 1, 1),
                        date(2020, 1, 1),
                    ],
                    "_change_type": [
                        "delete",
                        "update_preimage",
                        "update_postimage",
                        "insert",
                        "insert",
                        "insert",
                    ],
                    "_commit_version": [2, 3, 3, 1, 1, 1],
                    "_commit_timestamp": [
                        1651272655000,
                        1651272660000,
                        1651272660000,
                        1651272635000,
                        1651272635000,
                        1651272635000,
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
            "2000-01-01 00:00:00",
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
            "2100-01-01 00:00:00",
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
    expected: pd.DataFrame
):
    if error is None:
        pdf = load_table_changes_as_pandas(
            f"{profile_path}#{fragments}",
            starting_version,
            ending_version,
            starting_timestamp,
            ending_timestamp
        )
        pd.testing.assert_frame_equal(pdf, expected)
    else:
        try:
            load_table_changes_as_pandas(
                f"{profile_path}#{fragments}",
                starting_version,
                ending_version,
                starting_timestamp,
                ending_timestamp
            )
            assert False
        except Exception as e:
            assert isinstance(e, HTTPError)
            assert error in str(e)


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
            "2000-01-01 00:00:00",
            "Please use a timestamp greater",
            [],
            "not-used-schema-str",
            id="cdf_table_cdf_enabled timestamp too early",
        ),
        pytest.param(
            "share8.default.cdf_table_cdf_enabled",
            1,
            "2000-01-01 00:00:00",
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
        spark = SparkSession.builder \
            .appName("delta-sharing-test") \
            .master("local[*]") \
            .config("spark.jars.packages", "io.delta:delta-sharing-spark_2.12:1.0.0-SNAPSHOT") \
            .config("spark.delta.sharing.network.sslTrustAll", "true") \
            .getOrCreate()

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
    "fragments,starting_version,ending_version,starting_timestamp,ending_timestamp,error," +
    "expected_data,expected_schema_str",
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
            "name: string, age: int, birthday:date, _commit_version: long, _commit_timestamp" +
            ": long, _change_type: string",
            id="cdf_table_cdf_enabled table changes",
        ),
        pytest.param(
            "share8.default.cdf_table_cdf_enabled",
            None,
            None,
            "2000-01-01 00:00:00",
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
            "2100-01-01 00:00:00",
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
            "name: string, age: int, birthday:date, _commit_version: long, _commit_timestamp" +
            ": long, _change_type: string",
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
    expected_schema_str: str
):
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("delta-sharing-test") \
            .master("local[*]") \
            .config("spark.jars.packages", "io.delta:delta-sharing-spark_2.12:1.0.0-SNAPSHOT") \
            .config("spark.delta.sharing.network.sslTrustAll", "true") \
            .getOrCreate()

        if error is None:
            expected_df = spark.createDataFrame(expected_data, expected_schema_str)

            actual_df = load_table_changes_as_spark(
                f"{profile_path}#{fragments}",
                starting_version=starting_version,
                ending_version=ending_version,
                starting_timestamp=starting_timestamp,
                ending_timestamp=ending_timestamp
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
                    ending_timestamp=ending_timestamp
                )
            except Exception as e:
                assert isinstance(e, HTTPError)
                assert error in str(e)

    except ImportError:
        with pytest.raises(
            ImportError, match="Unable to import pyspark. `load_table_changes_as_spark` requires" +
            " PySpark."
        ):
            load_table_changes_as_spark("not-used")
