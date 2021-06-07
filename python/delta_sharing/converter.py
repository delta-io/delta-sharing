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
from json import loads
from typing import Any, Callable, Dict

import numpy as np
import pandas as pd


def _get_dummy_column(type_string: str):
    if type_string == "boolean":
        return pd.Series([False])
    elif type_string == "byte":
        return pd.Series([0], dtype="int8")
    elif type_string == "short":
        return pd.Series([0], dtype="int16")
    elif type_string == "integer":
        return pd.Series([0], dtype="int32")
    elif type_string == "long":
        return pd.Series([0], dtype="int64")
    elif type_string == "float":
        return pd.Series([0], dtype="float32")
    elif type_string == "double":
        return pd.Series([0], dtype="float64")
    elif isinstance(type_string, str) and type_string.startswith("decimal"):
        return pd.Series([Decimal("1.2")])
    elif type_string == "string":
        return pd.Series(["dummy"], dtype="string")
    elif type_string == "date":
        return pd.Series([pd.Timestamp(0).date()], dtype="datetime64[ns]")
    elif type_string == "timestamp":
        return pd.Series([pd.Timestamp(0)], dtype="datetime64[ns]")


def get_empty_table(schema_string: str) -> pd.DataFrame:
    schema_json = loads(schema_string)
    assert schema_json["type"] == "struct"

    dummy_table = pd.DataFrame({field["name"]:
                                _get_dummy_column(field["type"])
                                for field in schema_json["fields"]})
    return dummy_table.iloc[0:0]


def to_converters(schema_string: str) -> Dict[str, Callable[[str], Any]]:
    schema_json = loads(schema_string)
    assert schema_json["type"] == "struct"

    return {field["name"]: to_converter(field["type"]) for field in schema_json["fields"]}


def to_converter(json) -> Callable[[str], Any]:
    if json == "boolean":
        return lambda x: None if (x is None or x == "") else (x is True or x == "true")
    elif json == "byte":
        return lambda x: np.nan if (x is None or x == "") else np.int8(x)
    elif json == "short":
        return lambda x: np.nan if (x is None or x == "") else np.int16(x)
    elif json == "integer":
        return lambda x: np.nan if (x is None or x == "") else np.int32(x)
    elif json == "long":
        return lambda x: np.nan if (x is None or x == "") else np.int64(x)
    elif json == "float":
        return lambda x: np.nan if (x is None or x == "") else np.float32(x)
    elif json == "double":
        return lambda x: np.nan if (x is None or x == "") else np.float64(x)
    elif isinstance(json, str) and json.startswith("decimal"):
        return lambda x: None if (x is None or x == "") else Decimal(x)
    elif json == "string":
        return lambda x: None if (x is None or x == "") else str(x)
    elif json == "date":
        return lambda x: None if (x is None or x == "") else pd.Timestamp(x).date()
    elif json == "timestamp":
        return lambda x: pd.NaT if (x is None or x == "") else pd.Timestamp(x)

    # TODO: binary

    raise ValueError(f"Could not parse datatype: {json}")
