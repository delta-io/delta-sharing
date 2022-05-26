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
from datetime import date
from typing import Optional, Sequence

import pandas as pd
import pytest

from delta_sharing.delta_sharing import (
    DeltaSharingProfile,
    SharingClient,
    load_as_pandas,
    load_as_spark,
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
        Table(name="cdf_table_cdf_enabled", share="share1", schema="default"),
        Table(name="cdf_table_with_partition", share="share1", schema="default"),
    ]

    tables = sharing_client.list_tables(Schema(name="default", share="share2"))
    assert tables == [Table(name="table2", share="share2", schema="default")]


def _verify_all_tables_result(tables: Sequence[Table]):
    assert tables == [
        Table(name="table1", share="share1", schema="default"),
        Table(name="table3", share="share1", schema="default"),
        Table(name="table7", share="share1", schema="default"),
        Table(name="cdf_table_cdf_enabled", share="share1", schema="default"),
        Table(name="cdf_table_with_partition", share="share1", schema="default"),
        Table(name="table2", share="share2", schema="default"),
        Table(name="table4", share="share3", schema="default"),
        Table(name="table5", share="share3", schema="default"),
        Table(name="test_gzip", share="share4", schema="default"),
        Table(name="table8", share="share7", schema="schema1"),
        Table(name="table9", share="share7", schema="schema2"),
        Table(name="table_wasb", share="share_azure", schema="default"),
        Table(name="table_abfs", share="share_azure", schema="default"),
        Table(name="table_gcs", share="share_gcp", schema="default"),
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
            "share1.default.cdf_table_cdf_enabled",
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
            "share1.default.cdf_table_cdf_enabled",
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
def test_load(
    profile_path: str,
    fragments: str,
    limit: Optional[int],
    version: Optional[int],
    expected: pd.DataFrame
):
    pdf = load_as_pandas(f"{profile_path}#{fragments}", limit, version)
    pd.testing.assert_frame_equal(pdf, expected)


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
@pytest.mark.parametrize(
    "fragments,starting_version,ending_version,starting_timestamp,ending_timestamp,error,expected",
    [
        pytest.param(
            "share1.default.cdf_table_cdf_enabled",
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
            "share1.default.cdf_table_cdf_enabled",
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
            "share1.default.cdf_table_cdf_enabled",
            None,
            None,
            "2000-01-01 00:00:00",
            None,
            "Please use a timestamp greater",
            pd.DataFrame({"not_used": []}),
            id="cdf_table_cdf_enabled table changes with starting_timestamp",
        ),
        pytest.param(
            "share1.default.cdf_table_cdf_enabled",
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


def test_load_as_spark():
    try:
        import pyspark  # noqa: F401

        with pytest.raises(
            AssertionError,
            match="No active SparkSession was found. "
            "`load_as_spark` requires running in a PySpark application.",
        ):
            load_as_spark("not-used")
    except ImportError:
        with pytest.raises(
            ImportError, match="Unable to import pyspark. `load_as_spark` requires PySpark."
        ):
            load_as_spark("not-used")
