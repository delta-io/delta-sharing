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
import pytest

from delta_exchange.remote_delta_log import RemoteDeltaLog, RemoteDeltaTable
from delta_exchange.rest_client import Files


def test_remote_delta_table():
    path = "delta://uuid:token@databricks.com"
    table = RemoteDeltaTable.from_path_string(path)
    assert table == RemoteDeltaTable("https://databricks.com", "token", "uuid")

    path = "https://uuid:token@databricks.com"
    with pytest.raises(AssertionError):
        RemoteDeltaTable.from_path_string(path)


def test_to_pandas(tmp_path):
    pdf1 = pd.DataFrame({"a": [1, 2, 3], "b": ["a", "b", "c"]})
    pdf2 = pd.DataFrame({"a": [4, 5, 6], "b": ["d", "e", "f"]})

    pdf1.to_parquet(tmp_path / "pdf1.parquet")
    pdf2.to_parquet(tmp_path / "pdf2.parquet")

    class RestClientMock:
        def get_files(self, uuid: str, version: int) -> Files:
            assert uuid == "uuid"
            assert version == 0
            return Files([str(tmp_path / "pdf1.parquet"), str(tmp_path / "pdf2.parquet")])

    delta_log = RemoteDeltaLog(uuid="uuid", version=0, path="path", rest_client=RestClientMock())
    pdf = delta_log.to_pandas()
    pd.testing.assert_frame_equal(pdf, pd.concat([pdf1, pdf2]).reset_index(drop=True))
