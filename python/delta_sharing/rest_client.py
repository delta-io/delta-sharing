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
import collections
from contextlib import contextmanager
from dataclasses import dataclass
import json
from typing import Any, Dict, Optional, Sequence
from urllib.parse import urlparse

import requests

from delta_sharing.protocol import (
    AddFile,
    DeltaSharingProfile,
    Metadata,
    Protocol,
    Share,
    Schema,
    Table,
)


@dataclass(frozen=True)
class ListSharesRespons:
    shares: Sequence[Share]
    next_page_token: Optional[str]


@dataclass(frozen=True)
class ListSchemasResponse:
    schemas: Sequence[Schema]
    next_page_token: Optional[str]


@dataclass(frozen=True)
class ListTablesResponse:
    tables: Sequence[Table]
    next_page_token: Optional[str]


@dataclass(frozen=True)
class QueryTableMetadataResponse:
    protocol: Protocol
    metadata: Metadata


@dataclass(frozen=True)
class ListFilesInTableResponse:
    protocol: Protocol
    metadata: Metadata
    add_files: Sequence[AddFile]


class DataSharingRestClient:
    def __init__(self, profile: DeltaSharingProfile):
        self._profile = profile

        self._session = requests.Session()
        self._session.headers.update({"Authorization": f"Bearer {profile.bearer_token}"})
        if urlparse(profile.endpoint).hostname == "localhost":
            self._session.verify = False

    def list_shares(
        self, *, max_results: Optional[int] = None, page_token: Optional[str] = None
    ) -> ListSharesRespons:
        data: Dict = {}
        if max_results is not None:
            data["maxResults"] = max_results
        if page_token is not None:
            data["pageToken"] = page_token

        with self._get_internal("/shares", data) as lines:
            shares_json = json.loads(next(lines))
            return ListSharesRespons(
                shares=[Share.from_json(share_json) for share_json in shares_json["items"]],
                next_page_token=shares_json.get("nextPageToken", None),
            )

    def list_schemas(
        self, share: Share, *, max_results: Optional[int] = None, page_token: Optional[str] = None
    ) -> ListSchemasResponse:
        data: Dict = {}
        if max_results is not None:
            data["maxResults"] = max_results
        if page_token is not None:
            data["pageToken"] = page_token

        with self._get_internal(f"/shares/{share.name}/schemas", data) as lines:
            schemas_json = json.loads(next(lines))
            return ListSchemasResponse(
                schemas=[Schema.from_json(schema_json) for schema_json in schemas_json["items"]],
                next_page_token=schemas_json.get("nextPageToken", None),
            )

    def list_tables(
        self, schema: Schema, *, max_results: Optional[int] = None, page_token: Optional[str] = None
    ) -> ListTablesResponse:
        data: Dict = {}
        if max_results is not None:
            data["maxResults"] = max_results
        if page_token is not None:
            data["pageToken"] = page_token

        with self._get_internal(
            f"/shares/{schema.share}/schemas/{schema.name}/tables", data
        ) as lines:
            tables_json = json.loads(next(lines))
            return ListTablesResponse(
                tables=[Table.from_json(table_json) for table_json in tables_json["items"]],
                next_page_token=tables_json.get("nextPageToken", None),
            )

    def query_table_metadata(self, table: Table) -> QueryTableMetadataResponse:
        with self._get_internal(
            f"/shares/{table.share}/schemas/{table.schema}/tables/{table.name}/metadata"
        ) as lines:
            protocol_json = json.loads(next(lines))
            metadata_json = json.loads(next(lines))
            return QueryTableMetadataResponse(
                protocol=Protocol.from_json(protocol_json["protocol"]),
                metadata=Metadata.from_json(metadata_json["metaData"]),
            )

    def list_files_in_table(
        self,
        table: Table,
        *,
        predicateHints: Optional[Sequence[str]] = None,
        limitHint: Optional[int] = None,
    ) -> ListFilesInTableResponse:
        data: Dict = {}
        if predicateHints is not None:
            data["predicateHints"] = predicateHints
        if limitHint is not None:
            data["limitHint"] = limitHint

        with self._post_internal(
            f"/shares/{table.share}/schemas/{table.schema}/tables/{table.name}/query", data=data,
        ) as lines:
            protocol_json = json.loads(next(lines))
            metadata_json = json.loads(next(lines))
            return ListFilesInTableResponse(
                protocol=Protocol.from_json(protocol_json["protocol"]),
                metadata=Metadata.from_json(metadata_json["metaData"]),
                add_files=[AddFile.from_json(json.loads(file)["file"]) for file in lines],
            )

    def close(self):
        self._session.close()

    def _get_internal(self, target: str, data: Optional[Dict[str, Any]] = None):
        return self._request_internal(request=self._session.get, target=target, data=data)

    def _post_internal(self, target: str, data: Optional[Dict[str, Any]] = None):
        return self._request_internal(request=self._session.post, target=target, data=data)

    @contextmanager
    def _request_internal(self, request, target: str, data: Optional[Dict[str, Any]]):
        response = request(f"{self._profile.endpoint}{target}", json=data)
        try:
            response.raise_for_status()
            lines = response.iter_lines(decode_unicode=True)
            try:
                yield lines
            finally:
                collections.deque(lines, maxlen=0)
        finally:
            response.close()
