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
from typing import NamedTuple, Sequence
from urllib.parse import urlparse

import fsspec
import pandas as pd
from pyarrow.dataset import dataset

from delta_exchange.client import DeltaLogRestClient


class RemoteDeltaTable(NamedTuple):
    api_url: str
    api_token: str
    uuid: str

    @staticmethod
    def from_path_string(path: str):
        url = urlparse(path)
        assert url.scheme == "delta"
        api_url = "https://{hostname}".format(hostname=url.hostname)
        api_token = url.password
        uuid = url.username
        return RemoteDeltaTable(api_url, api_token, uuid)


class RemoteDeltaLog(NamedTuple):
    uuid: str
    version: int
    path: str
    client: DeltaLogRestClient

    @property
    def files(self) -> Sequence[str]:
        return self.client.get_files(uuid=self.uuid, version=self.version).files

    def to_pandas(self) -> pd.DataFrame:
        files = self.files
        if len(files) > 0:
            scheme = urlparse(files[0]).scheme
            assert all(urlparse(f).scheme == scheme for f in files[1:])
            filesystem = fsspec.get_filesystem_class(scheme)
        else:
            filesystem = fsspec.get_filesystem_class("https")
        return dataset(source=files, filesystem=filesystem()).to_table().to_pandas()
