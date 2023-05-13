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
from decimal import Decimal
from typing import Any, Callable, Dict

import numpy as np
import pandas as pd
import pyarrow as pa
from pyarrow import Schema as PyArrowSchema
from pyarrow import Table as PyArrowTable
from pyarrow.dataset import Dataset as PyArrowDataset
from pyarrow.dataset import dataset


def _get_dummy_column(schema_type):
    """
    Return a dummy column with the data type specified in schema_type.
    The dummy column is used to populate the dtype fields in empty tables.
    :param schema_type: str or json representing a data type
    :return: dummy pandas Series to be inserted into an empty table
    """
    if schema_type == "boolean":
        return pd.Series([False])
    elif schema_type == "byte":
        return pd.Series([0], dtype="int8")
    elif schema_type == "short":
        return pd.Series([0], dtype="int16")
    elif schema_type == "integer":
        return pd.Series([0], dtype="int32")
    elif schema_type == "long":
        return pd.Series([0], dtype="int64")
    elif schema_type == "float":
        return pd.Series([0], dtype="float32")
    elif schema_type == "double":
        return pd.Series([0], dtype="float64")
    elif isinstance(schema_type, str) and schema_type.startswith("decimal"):
        return pd.Series([0], dtype=np.dtype("O"))
    elif schema_type == "string":
        return pd.Series([0], dtype=np.dtype("O"))
    elif schema_type == "date":
        return pd.Series([pd.Timestamp(0).date()])
    elif schema_type == "timestamp":
        return pd.Series([pd.Timestamp(0)], dtype=np.dtype("datetime64[ns]"))
    elif schema_type == "binary":
        return pd.Series([0], dtype=np.dtype("O"))
    elif isinstance(schema_type, dict) and schema_type["type"] in ("array", "struct", "map"):
        return pd.Series([0], dtype=np.dtype("O"))

    raise ValueError(f"Could not parse datatype: {schema_type}")


def _to_pyarrow_schema(schema_value: dict) -> PyArrowSchema:
    """
    Converts delta table schema to a valid PyArrow Schema with
    field metadata and nullability preserved.
    This implementation closely follows PySpark and supports columns
    with complex data types like `struct`, `map` and `array`.

    :param schema_value: Representation of delta table schema as a dict
    :return: pyarrow.Schema
    """
    assert schema_value is not None and schema_value["type"] == "struct", "Invalid schema found."

    def _to_arrow_type(f_type) -> pa.DataType:
        is_nested = isinstance(f_type, dict)

        if not is_nested:
            f_type = f_type.lower()

        if not is_nested and f_type == "boolean":
            return pa.bool_()

        elif not is_nested and f_type == "byte":
            return pa.int8()

        elif not is_nested and f_type == "short":
            return pa.int16()

        elif not is_nested and f_type == "integer":
            return pa.int32()

        elif not is_nested and f_type == "long":
            return pa.int64()

        elif not is_nested and f_type == "float":
            return pa.float32()

        elif not is_nested and f_type == "double":
            return pa.float64()

        elif not is_nested and f_type.startswith("decimal"):
            try:
                import re

                decimal_pattern = re.compile(r"(\([^\)]+\))")
                res = [
                    int(v.strip())
                    for v in decimal_pattern.findall(f_type)[0].lstrip("(").rstrip(")").split(",")
                ]
                precision = res[0]
                scale = res[1]
            except:
                precision = 10
                scale = 0
            return pa.decimal128(precision, scale)

        elif not is_nested and f_type == "string":
            return pa.string()

        elif not is_nested and f_type == "binary":
            return pa.binary()

        elif not is_nested and f_type == "date":
            return pa.date32()

        elif not is_nested and f_type == "timestamp":
            return pa.timestamp("us")

        elif not is_nested and f_type in ["void", "null"]:
            return pa.null()

        elif is_nested and f_type["type"] == "array":
            element_type = f_type["elementType"]
            if isinstance(element_type, str) and element_type == "timestamp":
                raise TypeError(
                    f"Could not parse map field types: array<timestamp> to PyArrow type."
                )
            elif isinstance(element_type, dict) and element_type["type"] == "struct":
                if any(
                    isinstance(struct_field["type"], dict)
                    and struct_field["type"]["type"] == "struct"
                    for struct_field in element_type["fields"]
                ):
                    raise TypeError("Nested StructType cannot be converted to PyArrow type.")
            return pa.list_(_to_arrow_type(element_type))

        elif is_nested and f_type["type"] == "map":
            key_type = f_type["keyType"]
            value_type = f_type["valueType"]
            if (isinstance(key_type, str) and key_type == "timestamp") or (
                isinstance(value_type, str) and value_type == "timestamp"
            ):
                raise TypeError(
                    f"Could not parse map field with timestamp key or value types to PyArrow type."
                )
            elif (isinstance(key_type, dict) and key_type["type"] == "struct") or (
                isinstance(value_type, dict) and value_type["type"] == "struct"
            ):
                raise TypeError(
                    f"Could not parse map field with struct key or value types to PyArrow type."
                )
            return pa.map_(_to_arrow_type(key_type), _to_arrow_type(value_type))

        elif is_nested and f_type["type"] == "struct":
            if any(
                isinstance(struct_field["type"], dict) and struct_field["type"]["type"] == "struct"
                for struct_field in f_type["fields"]
            ):
                raise TypeError("Nested StructType cannot be converted to PyArrow type.")
            struct_fields = [
                pa.field(
                    struct_field["name"],
                    _to_arrow_type(struct_field["type"]),
                    nullable=(str(struct_field["nullable"]).lower() == "true"),
                    metadata=struct_field["metadata"],
                )
                for struct_field in f_type["fields"]
            ]
            return pa.struct(struct_fields)
        else:
            raise TypeError(f"Could not parse type: {f_type} to PyArrow type.")

    fields = []
    for field in schema_value["fields"]:
        field_name = str(field["name"])
        field_type = field["type"]
        field_nullable = str(field["nullable"]).lower() == "true"
        field_metadata = field["metadata"]

        fields.append(
            pa.field(
                field_name,
                _to_arrow_type(field_type),
                nullable=field_nullable,
                metadata=field_metadata,
            )
        )

    return pa.schema(fields)


def get_empty_pyarrow_table(schema_json: dict) -> PyArrowTable:
    schema = _to_pyarrow_schema(schema_json)
    return schema.empty_table()


def get_empty_pyarrow_dataset(schema_json: dict) -> PyArrowDataset:
    schema = _to_pyarrow_schema(schema_json)
    return dataset([], schema=schema)


def get_empty_pandas_table(schema_json: dict) -> pd.DataFrame:
    """
    For empty tables, we use dummy columns from `_get_dummy_column` and then
    drop all rows to generate a table with the correct column names and
    data types.
    :param schema_json: json object representing the table schema
    :return: empty table with columns specified in schema_json
    """
    assert schema_json["type"] == "struct"

    dummy_table = pd.DataFrame(
        {field["name"]: _get_dummy_column(field["type"]) for field in schema_json["fields"]}
    )
    return dummy_table.iloc[0:0]


def to_converters(schema_json: dict) -> Dict[str, Callable[[str], Any]]:
    assert schema_json["type"] == "struct"

    return {field["name"]: to_converter(field["type"]) for field in schema_json["fields"]}


def to_converter(schema_type) -> Callable[[str], Any]:
    """
    For types that support partitioning, a lambda to parse data into the
    corresponding type is returned. For data types that cannot be partitioned
    on, we return None. The caller is expected to check if the value is None before using.
    :param schema_type: str or json representing a data type
    :return: converter function or None
    """
    if schema_type == "boolean":
        return lambda x: None if (x is None or x == "") else (x is True or x == "true")
    elif schema_type == "byte":
        return lambda x: np.nan if (x is None or x == "") else np.int8(x)
    elif schema_type == "short":
        return lambda x: np.nan if (x is None or x == "") else np.int16(x)
    elif schema_type == "integer":
        return lambda x: np.nan if (x is None or x == "") else np.int32(x)
    elif schema_type == "long":
        return lambda x: np.nan if (x is None or x == "") else np.int64(x)
    elif schema_type == "float":
        return lambda x: np.nan if (x is None or x == "") else np.float32(x)
    elif schema_type == "double":
        return lambda x: np.nan if (x is None or x == "") else np.float64(x)
    elif isinstance(schema_type, str) and schema_type.startswith("decimal"):
        return lambda x: None if (x is None or x == "") else Decimal(x)
    elif schema_type == "string":
        return lambda x: None if (x is None or x == "") else str(x)
    elif schema_type == "date":
        return lambda x: None if (x is None or x == "") else pd.Timestamp(x).date()
    elif schema_type == "timestamp":
        return lambda x: pd.NaT if (x is None or x == "") else pd.Timestamp(x)
    elif schema_type == "binary":
        return None  # partition on binary column not supported
    elif isinstance(schema_type, dict) and schema_type["type"] in ("array", "struct", "map"):
        return None  # partition on complex column not supported

    raise ValueError(f"Could not parse datatype: {schema_type}")
