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

from delta_exchange.delta_exchange import DeltaExchange
from delta_exchange.protocol import Schema, Share, Table
from delta_exchange.tests.conftest import ENABLE_INTEGRATION, SKIP_MESSAGE


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_list_shares(exchange: DeltaExchange):
    shares = exchange.list_shares()
    assert shares == [Share(name="share1"), Share(name="share2")]


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_list_schemas(exchange: DeltaExchange):
    schemas = exchange.list_schemas(Share(name="share1"))
    assert schemas == [Schema(name="default", share="share1")]

    schemas = exchange.list_schemas(Share(name="share2"))
    assert schemas == [Schema(name="default", share="share2")]


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_list_tables(exchange: DeltaExchange):
    tables = exchange.list_tables(Schema(name="default", share="share1"))
    assert tables == [
        Table(name="table1", share="share1", schema="default"),
        Table(name="table3", share="share1", schema="default"),
    ]

    tables = exchange.list_tables(Schema(name="default", share="share2"))
    assert tables == [Table(name="table2", share="share2", schema="default")]


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_list_all_tables(exchange: DeltaExchange):
    tables = exchange.list_all_tables()
    assert tables == [
        Table(name="table1", share="share1", schema="default"),
        Table(name="table3", share="share1", schema="default"),
        Table(name="table2", share="share2", schema="default"),
    ]


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
@pytest.mark.parametrize(
    "fragments,table,expected",
    [
        pytest.param(
            "share1.default.table1",
            Table(name="table1", share="share1", schema="default"),
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
            Table(name="table2", share="share2", schema="default"),
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
            Table(name="table3", share="share1", schema="default"),
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
    ],
)
def test_load(profile_path: str, fragments: str, table: Table, expected: pd.DataFrame):
    reader = DeltaExchange.load(f"{profile_path}#{fragments}")
    assert reader.table == table

    pdf = reader.to_pandas()
    pd.testing.assert_frame_equal(pdf, expected)

    pdf = DeltaExchange(profile_path).load_as_pandas(table)
    pd.testing.assert_frame_equal(pdf, expected)
