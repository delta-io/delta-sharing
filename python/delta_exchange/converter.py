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
from typing import Any, Callable, Dict

import numpy as np
import pandas as pd


def to_converters(schema_string: str) -> Dict[str, Callable[[str], Any]]:
    schema_json = loads(schema_string)
    assert schema_json["type"] == "struct"

    return {field["name"]: to_converter(field["type"]) for field in schema_json["fields"]}


def to_converter(json) -> Callable[[str], Any]:
    if json == "boolean":
        return lambda x: x is True or x == "true"
    elif json == "byte":
        return np.int8
    elif json == "short":
        return np.int16
    elif json == "integer":
        return np.int32
    elif json == "long":
        return np.int64
    elif json == "float":
        return np.float32
    elif json == "double":
        return np.float64
    elif isinstance(json, str) and json.startswith("decimal"):
        return Decimal
    elif json == "string":
        return str
    elif json == "date":
        return date.fromisoformat
    elif json == "timestamp":
        return lambda x: pd.Timestamp(x).tz_localize(None)

    # TODO: binary, array, map, struct?

    raise ValueError(f"Could not parse datatype: {json}")
