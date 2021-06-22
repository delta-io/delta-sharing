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


def get_empty_table(schema_json: dict) -> pd.DataFrame:
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
