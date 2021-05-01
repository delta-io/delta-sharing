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
from typing import Any

import numpy as np
import pandas as pd
import pytest

from delta_exchange.converter import to_converter


def test_to_converter_boolean():
    converter = to_converter("boolean")
    assert converter("true") is True
    assert converter("false") is False


@pytest.mark.parametrize(
    "type_str,expected",
    [
        ("byte", np.int8(1)),
        ("short", np.int16(1)),
        ("integer", np.int32(1)),
        ("long", np.int64(1)),
        ("float", np.float32(1)),
        ("double", np.float64(1)),
        ("decimal(10,0)", Decimal(1)),
    ],
)
def test_to_converter_numeric(type_str: str, expected: Any):
    converter = to_converter(type_str)
    assert converter("1") == expected


def test_to_converter_string():
    converter = to_converter("string")
    assert converter("abc") == "abc"


def test_to_converter_date():
    converter = to_converter("date")
    assert converter("2021-01-01") == date(2021, 1, 1)


def test_to_converter_timestamp():
    converter = to_converter("timestamp")
    assert converter("2021-04-28T23:36:47.599Z") == pd.Timestamp("2021-04-28 23:36:47.599")
