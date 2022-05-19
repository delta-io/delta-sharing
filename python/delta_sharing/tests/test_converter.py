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
from decimal import Decimal
from json import loads
from typing import Any

import numpy as np
import pandas as pd
import pytest

from delta_sharing.converter import to_converter, get_empty_table


def test_to_converter_boolean():
    converter = to_converter("boolean")
    assert converter("true") is True
    assert converter("false") is False
    assert converter("") is None


@pytest.mark.parametrize(
    "type_str,expected",
    [
        pytest.param("byte", np.int8(1), id="byte"),
        pytest.param("short", np.int16(1), id="short"),
        pytest.param("integer", np.int32(1), id="integer"),
        pytest.param("long", np.int64(1), id="long"),
        pytest.param("float", np.float32(1), id="float"),
        pytest.param("double", np.float64(1), id="double"),
    ],
)
def test_to_converter_numeric(type_str: str, expected: Any):
    converter = to_converter(type_str)
    assert converter("1") == expected
    assert np.isnan(converter(""))


def test_to_converter_decimal():
    converter = to_converter("decimal(10,0)")
    assert converter("1") == Decimal(1)
    assert converter("") is None


def test_to_converter_string():
    converter = to_converter("string")
    assert converter("abc") == "abc"
    assert converter("") is None


def test_to_converter_date():
    converter = to_converter("date")
    assert converter("2021-01-01") == date(2021, 1, 1)
    assert converter("") is None


def test_to_converter_timestamp():
    converter = to_converter("timestamp")
    assert converter("2021-04-28 23:36:47.599") == pd.Timestamp("2021-04-28 23:36:47.599")
    assert converter("") is pd.NaT


def test_get_empty_table():
    schema_string = (
        '{"fields": ['
        '{"metadata": {},"name": "a","nullable": true,"type": "long"},'
        '{"metadata": {},"name": "b","nullable": true,"type": "string"}'
        '],"type":"struct"}'
    )
    schema_json = loads(schema_string)
    pdf = get_empty_table(schema_json)
    assert pdf.empty
    assert pdf.columns.values.size == 2
    assert pdf.columns.values[0] == "a"
    assert pdf.columns.values[1] == "b"
