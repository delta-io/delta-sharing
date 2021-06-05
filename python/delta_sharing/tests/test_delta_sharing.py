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

import pandas as pd
import pytest

from delta_sharing.delta_sharing import SharingClient, load_as_pandas, load_as_spark, _parse_url
from delta_sharing.protocol import Schema, Share, Table
from delta_sharing.tests.conftest import ENABLE_INTEGRATION, SKIP_MESSAGE


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_list_shares(sharing_client: SharingClient):
    shares = sharing_client.list_shares()
    assert shares == [Share(name="share1"), Share(name="share2"), Share(name="share3")]


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
    ]

    tables = sharing_client.list_tables(Schema(name="default", share="share2"))
    assert tables == [Table(name="table2", share="share2", schema="default")]


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_list_all_tables(sharing_client: SharingClient):
    tables = sharing_client.list_all_tables()
    assert tables == [
        Table(name="table1", share="share1", schema="default"),
        Table(name="table3", share="share1", schema="default"),
        Table(name="table2", share="share2", schema="default"),
        Table(name="table4", share="share3", schema="default"),
        Table(name="table5", share="share3", schema="default"),
    ]


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
@pytest.mark.parametrize(
    "fragments,expected",
    [
        pytest.param(
            "share1.default.table1",
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
            "share3.default.table4",
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
    ],
)
def test_load(profile_path: str, fragments: str, expected: pd.DataFrame):
    pdf = load_as_pandas(f"{profile_path}#{fragments}")
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
