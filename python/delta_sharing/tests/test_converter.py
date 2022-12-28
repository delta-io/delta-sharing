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
import pyarrow as pa
import pytest

from delta_sharing.converter import to_converter, get_empty_pandas_table, _to_pyarrow_schema


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
    pdf = get_empty_pandas_table(schema_json)
    assert pdf.empty
    assert pdf.columns.values.size == 2
    assert pdf.columns.values[0] == "a"
    assert pdf.columns.values[1] == "b"


def test_pyarrow_schema_map():
    base_schema_dict = {"type": "struct", "fields": []}

    simple_map_col = {
        "name": "simple_map",
        "type": {"type": "map", "keyType": "string", "valueType": "string"},
        "nullable": True,
        "metadata": {},
    }

    base_schema_dict["fields"] = [simple_map_col]
    base_w_map_schema = _to_pyarrow_schema(base_schema_dict)

    assert base_w_map_schema.field("simple_map").nullable
    assert base_w_map_schema.field("simple_map").type == pa.map_(pa.string(), pa.string())

    with pytest.raises(TypeError) as exp_info1:
        timestamp_map_col = {
            "name": "timestamp_map_col",
            "type": {"type": "map", "keyType": "timestamp", "valueType": "string"},
            "nullable": False,
            "metadata": {},
        }

        base_schema_dict_tmp = base_schema_dict.copy()
        base_schema_dict_tmp["fields"] = [timestamp_map_col]

        _to_pyarrow_schema(base_schema_dict_tmp)

    assert (
        str(exp_info1.value)
        == "Could not parse map field with timestamp key or value types to PyArrow type."
    )

    with pytest.raises(TypeError) as exp_info2:
        struct_map_col = {
            "name": "struct_map_col",
            "type": {
                "type": "map",
                "keyType": "string",
                "valueType": {
                    "type": "struct",
                    "fields": [
                        {"name": "firstname", "type": "string", "nullable": True, "metadata": {}},
                        {"name": "middlename", "type": "string", "nullable": False, "metadata": {}},
                        {"name": "lastname", "type": "string", "nullable": True, "metadata": {}},
                    ],
                },
            },
            "nullable": False,
            "metadata": {},
        }

        base_schema_dict_tmp = base_schema_dict.copy()
        base_schema_dict_tmp["fields"] = [struct_map_col]

        _to_pyarrow_schema(base_schema_dict_tmp)

    assert (
        str(exp_info2.value)
        == "Could not parse map field with struct key or value types to PyArrow type."
    )


def test_pyarrow_schema_struct():
    base_schema_dict = {"type": "struct", "fields": []}

    simple_struct_col = {
        "name": "simple_struct",
        "type": {
            "type": "struct",
            "fields": [
                {"name": "firstname", "type": "string", "nullable": True, "metadata": {}},
                {"name": "middlename", "type": "string", "nullable": False, "metadata": {}},
                {"name": "lastname", "type": "string", "nullable": True, "metadata": {}},
            ],
        },
        "nullable": False,
        "metadata": {},
    }

    base_schema_dict["fields"] = [simple_struct_col]
    base_w_st_schema = _to_pyarrow_schema(base_schema_dict)

    assert not base_w_st_schema.field("simple_struct").nullable
    assert base_w_st_schema.field("simple_struct").type == pa.struct(
        [
            pa.field("firstname", pa.string(), nullable=True),
            pa.field("middlename", pa.string(), nullable=False),
            pa.field("lastname", pa.string(), nullable=True),
        ]
    )

    with pytest.raises(TypeError) as exp_info1:
        complex_struct_col = {
            "name": "complex_struct",
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "test": "test",
                        "type": {
                            "type": "struct",
                            "fields": [
                                {
                                    "test": "nested_struct_field",
                                    "type": "string",
                                    "nullable": True,
                                    "metadata": {},
                                }
                            ],
                        },
                        "nullable": True,
                        "metadata": {},
                    }
                ],
            },
            "nullable": False,
            "metadata": {},
        }

        base_schema_dict_tmp = base_schema_dict.copy()
        base_schema_dict_tmp["fields"] = [complex_struct_col]

        _to_pyarrow_schema(base_schema_dict_tmp)

    assert str(exp_info1.value) == "Nested StructType cannot be converted to PyArrow type."


def test_pyarrow_schema_array():
    base_schema_dict = {"type": "struct", "fields": []}

    simple_arr_col = {
        "name": "simple_arr",
        "type": {"type": "array", "elementType": "string", "containsNull": True},
        "nullable": True,
        "metadata": {},
    }

    complex_arr_col_1 = {
        "name": "complex_arr",
        "type": {
            "type": "array",
            "elementType": {
                "type": "struct",
                "fields": [
                    {"name": "middlename", "type": "string", "nullable": False, "metadata": {}},
                ],
            },
            "containsNull": True,
        },
        "nullable": False,
        "metadata": {},
    }

    base_schema_dict["fields"].extend([simple_arr_col, complex_arr_col_1])

    base_w_arr_schema = _to_pyarrow_schema(base_schema_dict)

    assert base_w_arr_schema.field("simple_arr").nullable
    assert base_w_arr_schema.field("simple_arr").type == pa.list_(pa.string())

    assert not base_w_arr_schema.field("complex_arr").nullable
    assert base_w_arr_schema.field("complex_arr").type == pa.list_(
        pa.struct([pa.field("middlename", pa.string(), nullable=False)])
    )

    with pytest.raises(TypeError) as exp_info1:
        complex_arr_col_2 = {
            "name": "complex_arr",
            "type": {
                "type": "array",
                "elementType": {
                    "type": "struct",
                    "fields": [
                        {
                            "name": "middlename",
                            "type": {"type": "struct", "fields": []},
                            "nullable": False,
                            "metadata": {},
                        },
                    ],
                },
                "containsNull": True,
            },
            "nullable": False,
            "metadata": {},
        }

        base_schema_dict_tmp = base_schema_dict.copy()
        base_schema_dict_tmp["fields"] = [complex_arr_col_2]

        _to_pyarrow_schema(base_schema_dict_tmp)

    assert str(exp_info1.value) == "Nested StructType cannot be converted to PyArrow type."

    with pytest.raises(TypeError) as exp_info2:
        complex_arr_col_2 = {
            "name": "complex_arr",
            "type": {"type": "array", "elementType": "timestamp", "containsNull": True},
            "nullable": False,
            "metadata": {},
        }

        base_schema_dict_tmp = base_schema_dict.copy()
        base_schema_dict_tmp["fields"] = [complex_arr_col_2]

        _to_pyarrow_schema(base_schema_dict_tmp)

    assert (
        str(exp_info2.value) == "Could not parse map field types: array<timestamp> to PyArrow type."
    )

    with pytest.raises(TypeError) as exp_info3:
        base_schema_dict_tmp = base_schema_dict.copy()
        base_schema_dict_tmp["fields"] = [
            {"name": "test", "type": "xyz", "nullable": False, "metadata": {}}
        ]
        _to_pyarrow_schema(base_schema_dict_tmp)

    assert str(exp_info3.value) == "Could not parse type: xyz to PyArrow type."


def test_pyarrow_schema_base():
    base_schema_dict = {
        "type": "struct",
        "fields": [
            {"name": "age", "type": "integer", "nullable": False, "metadata": {"foo": "bar"}},
            {"name": "gender", "type": "string", "nullable": True, "metadata": {}},
            {"name": "is_active", "type": "boolean", "nullable": False, "metadata": {}},
            {"name": "date_of_birth", "type": "date", "nullable": True, "metadata": {}},
            {"name": "salary", "type": "double", "nullable": True, "metadata": {}},
            {"name": "salary_band", "type": "decimal(38,0)", "nullable": True, "metadata": {}},
            {"name": "created_timestamp", "type": "timestamp", "nullable": False, "metadata": {}},
            {"name": "test", "type": "void", "nullable": True, "metadata": {}},
        ],
    }

    base_schema = _to_pyarrow_schema(base_schema_dict)

    # Metadata tests
    assert not base_schema.field("age").metadata == {"foo": "bar"}

    # Nullability tests
    assert base_schema.field("gender").nullable
    assert not base_schema.field("age").nullable
    assert not base_schema.field("created_timestamp").nullable
    assert base_schema.field("date_of_birth").nullable
    assert not base_schema.field("is_active").nullable

    # Type conversion tests
    assert base_schema.field("gender").type == pa.string()
    assert base_schema.field("age").type == pa.int32()
    assert base_schema.field("salary").type == pa.float64()
    assert base_schema.field("salary_band").type == pa.decimal128(38, 0)
    assert base_schema.field("is_active").type == pa.bool_()
    assert base_schema.field("date_of_birth").type == pa.date32()
    assert base_schema.field("created_timestamp").type == pa.timestamp("us")
    assert pa.null() in base_schema.types
