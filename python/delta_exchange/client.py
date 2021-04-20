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
import json
from typing import Any, Dict, NamedTuple, Sequence
from urllib.parse import urljoin, urlparse

import requests


class TableInfo(NamedTuple):
    path: str
    version: int


class Metadata(NamedTuple):
    metadata: str


class Files(NamedTuple):
    files: Sequence[str]


class DeltaLogRestClient:
    def __init__(self, api_url: str, api_token: str):
        self._api_url = api_url
        self._api_token = api_token

        self._session = requests.Session()
        if urlparse(api_url).netloc in ("localhost", "localhost:443"):
            self._session.verify = False

    def get_table_info(self, uuid: str) -> TableInfo:
        response = self._get_internal("/s3commit/table/info", {"uuid": uuid})
        return TableInfo(response["path"], int(response["version"]))

    def get_metadata(self, uuid: str, version: int) -> Metadata:
        response = self._get_internal(
            "/s3commit/table/metadata", {"uuid": uuid, "version": version}
        )
        return Metadata(response["metadata"])

    def get_files(self, uuid: str, version: int) -> Files:
        response = self._get_internal("/s3commit/table/files", {"uuid": uuid, "version": version})
        return Files([json.loads(r)["path"] for r in response["file"]])

    def close(self):
        self._session.close()

    def _get_internal(self, target: str, data: Dict[str, Any]) -> Dict[str, str]:
        url = urljoin(self._api_url, DeltaLogRestClient._get_path(target))
        headers = {"Authorization": "Bearer {api_token}".format(api_token=self._api_token)}
        response = self._session.get(url, headers=headers, json=data)
        try:
            response.raise_for_status()
            return response.json()
        finally:
            response.close()

    @staticmethod
    def _get_path(target: str) -> str:
        return "/api/2.0{target}".format(target=target)
