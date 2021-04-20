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
import pytest

from delta_exchange.remote_delta_log import RemoteDeltaTable


def test_remote_delta_table():
    path = "delta://uuid:token@databricks.com"
    table = RemoteDeltaTable.from_path_string(path)
    assert table == RemoteDeltaTable("https://databricks.com", "token", "uuid")

    path = "https://uuid:token@databricks.com"
    with pytest.raises(AssertionError):
        RemoteDeltaTable.from_path_string(path)
