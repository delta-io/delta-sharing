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
