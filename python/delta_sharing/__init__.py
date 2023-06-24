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

from delta_sharing.delta_sharing import SharingClient, load_as_pandas, load_as_spark
from delta_sharing.delta_sharing import get_table_metadata, get_table_protocol, get_table_version
from delta_sharing.delta_sharing import load_table_changes_as_pandas, load_table_changes_as_spark
from delta_sharing.protocol import Share, Schema, Table
from delta_sharing.version import __version__


__all__ = [
    "SharingClient",
    "Share",
    "Schema",
    "Table",
    "get_table_metadata",
    "get_table_protocol",
    "get_table_version",
    "load_as_pandas",
    "load_as_spark",
    "load_table_changes_as_pandas",
    "load_table_changes_as_spark",
    "__version__",
]
