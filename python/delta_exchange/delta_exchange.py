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
import pandas as pd

from delta_exchange.remote_delta_log import RemoteDeltaLog, RemoteDeltaTable
from delta_exchange.rest_client import DeltaLogRestClient


class DeltaExchange:
    def __init__(self, path: str):
        self._path = path

        delta_table = RemoteDeltaTable.from_path_string(path)
        rest_client = DeltaLogRestClient(delta_table.api_url, delta_table.api_token)
        table_info = rest_client.get_table_info(delta_table.uuid)
        self._delta_log = RemoteDeltaLog(
            delta_table.uuid, table_info.version, table_info.path, rest_client
        )

    def to_pandas(self) -> pd.DataFrame:
        return self._delta_log.to_pandas()
