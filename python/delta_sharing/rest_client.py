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
from typing import Any, ClassVar, Dict, List, Optional, Sequence
from urllib.parse import quote, urlparse
import time
import logging
import pprint
from datetime import datetime

import requests
from requests.exceptions import HTTPError, ConnectionError

from delta_sharing.protocol import (
    AddFile,
    FileAction,
    CdfOptions,
    DeltaSharingProfile,
    Metadata,
    Protocol,
    Share,
    Schema,
    Table,
)


@dataclass(frozen=True)
class ListSharesResponse:
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
class ListAllTablesResponse:
    tables: Sequence[Table]
    next_page_token: Optional[str]


@dataclass(frozen=True)
class QueryTableMetadataResponse:
    delta_table_version: int
    protocol: Protocol
    metadata: Metadata


@dataclass(frozen=True)
class QueryTableVersionResponse:
    delta_table_version: int


@dataclass(frozen=True)
class ListFilesInTableResponse:
    delta_table_version: int
    protocol: Protocol
    metadata: Metadata
    add_files: Sequence[AddFile]


@dataclass(frozen=True)
class ListTableChangesResponse:
    protocol: Protocol
    metadata: Metadata
    actions: Sequence[FileAction]


def retry_with_exponential_backoff(func):
    def func_with_retry(self, *arg, **kwargs):
        times_retried = 0
        sleep_ms = 100
        while True:
            times_retried += 1
            try:
                return func(self, *arg, **kwargs)
            except Exception as e:
                if self._should_retry(e) and times_retried <= self._num_retries:
                    logging.info(f"Sleeping {sleep_ms} to retry because of error {e}")
                    self._sleeper(sleep_ms)
                    sleep_ms *= 2
                elif self._error_on_expired_token(e):
                    raise HTTPError(
                        "It may be caused by an expired token as it has expired at "
                        + f"{self._profile.expiration_time}"
                    ) from e
                else:
                    raise e

    return func_with_retry


def _client_user_agent() -> str:
    try:
        from delta_sharing.version import __version__
        import pandas
        import pyarrow
        import platform

        return (
            f"Delta-Sharing-Python/{__version__}"
            + f" pandas/{pandas.__version__}"
            + f" PyArrow/{pyarrow.__version__}"
            + f" Python/{platform.python_version()}"
            + f" System/{platform.platform()}"
        )
    except Exception as e:
        logging.warning(
            f"Unable to load version information for Delta Sharing because of error {e}"
        )
        return "Delta-Sharing-Python/<unknown>"


class DataSharingRestClient:
    USER_AGENT: ClassVar[str] = _client_user_agent()

    def __init__(self, profile: DeltaSharingProfile, num_retries=10):
        self._profile = profile
        self._num_retries = num_retries
        self._sleeper = lambda sleep_ms: time.sleep(sleep_ms / 1000)

        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {profile.bearer_token}",
                "User-Agent": DataSharingRestClient.USER_AGENT,
            }
        )
        if urlparse(profile.endpoint).hostname == "localhost":
            self._session.verify = False

    @retry_with_exponential_backoff
    def list_shares(
        self, *, max_results: Optional[int] = None, page_token: Optional[str] = None
    ) -> ListSharesResponse:
        data: Dict = {}
        if max_results is not None:
            data["maxResults"] = max_results
        if page_token is not None:
            data["pageToken"] = page_token

        with self._get_internal("/shares", data) as lines:
            shares_json = json.loads(next(lines))
            return ListSharesResponse(
                shares=[Share.from_json(share_json) for share_json in shares_json.get("items", [])],
                next_page_token=shares_json.get("nextPageToken", None),
            )

    @retry_with_exponential_backoff
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
                schemas=[
                    Schema.from_json(schema_json) for schema_json in schemas_json.get("items", [])
                ],
                next_page_token=schemas_json.get("nextPageToken", None),
            )

    @retry_with_exponential_backoff
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
                tables=[Table.from_json(table_json) for table_json in tables_json.get("items", [])],
                next_page_token=tables_json.get("nextPageToken", None),
            )

    @retry_with_exponential_backoff
    def list_all_tables(
        self, share: Share, *, max_results: Optional[int] = None, page_token: Optional[str] = None
    ) -> ListAllTablesResponse:
        data: Dict = {}
        if max_results is not None:
            data["maxResults"] = max_results
        if page_token is not None:
            data["pageToken"] = page_token

        with self._get_internal(f"/shares/{share.name}/all-tables", data) as lines:
            tables_json = json.loads(next(lines))
            return ListAllTablesResponse(
                tables=[Table.from_json(table_json) for table_json in tables_json.get("items", [])],
                next_page_token=tables_json.get("nextPageToken", None),
            )

    @retry_with_exponential_backoff
    def query_table_metadata(self, table: Table) -> QueryTableMetadataResponse:
        with self._get_internal(
            f"/shares/{table.share}/schemas/{table.schema}/tables/{table.name}/metadata",
            return_headers=True
        ) as values:
            headers = values[0]
            # it's a bug in the server if it doesn't return delta-table-version in the header
            if "delta-table-version" not in headers:
                raise LookupError("Missing delta-table-version header")
            lines = values[1]
            protocol_json = json.loads(next(lines))
            metadata_json = json.loads(next(lines))
            return QueryTableMetadataResponse(
                delta_table_version=int(headers.get("delta-table-version")),
                protocol=Protocol.from_json(protocol_json["protocol"]),
                metadata=Metadata.from_json(metadata_json["metaData"]),
            )

    @retry_with_exponential_backoff
    def query_table_version(
        self,
        table: Table,
        starting_timestamp: Optional[str] = None,
    ) -> QueryTableVersionResponse:
        query_str = f"/shares/{table.share}/schemas/{table.schema}/tables/{table.name}/version"
        if starting_timestamp is not None:
            query_str += f"?startingTimestamp={quote(starting_timestamp)}"
        with self._get_internal(
            query_str,
            return_headers=True
        ) as values:
            headers = values[0]

            # it's a bug in the server if it doesn't return delta-table-version in the header
            if "delta-table-version" not in headers:
                raise LookupError("Missing delta-table-version header")

            table_version = int(headers.get("delta-table-version"))
            return QueryTableVersionResponse(delta_table_version=table_version)

    @retry_with_exponential_backoff
    def list_files_in_table(
        self,
        table: Table,
        *,
        predicateHints: Optional[Sequence[str]] = None,
        limitHint: Optional[int] = None,
        version: Optional[int] = None,
        timestamp: Optional[str] = None,
    ) -> ListFilesInTableResponse:
        data: Dict = {}
        if predicateHints is not None:
            data["predicateHints"] = predicateHints
        if limitHint is not None:
            data["limitHint"] = limitHint
        if version is not None:
            data["version"] = version
        if timestamp is not None:
            data["timestamp"] = timestamp

        with self._post_internal(
            f"/shares/{table.share}/schemas/{table.schema}/tables/{table.name}/query",
            data=data,
            return_headers=True
        ) as values:
            headers = values[0]
            # it's a bug in the server if it doesn't return delta-table-version in the header
            if "delta-table-version" not in headers:
                raise LookupError("Missing delta-table-version header")

            lines = values[1]
            protocol_json = json.loads(next(lines))
            metadata_json = json.loads(next(lines))

            return ListFilesInTableResponse(
                delta_table_version=int(headers.get("delta-table-version")),
                protocol=Protocol.from_json(protocol_json["protocol"]),
                metadata=Metadata.from_json(metadata_json["metaData"]),
                add_files=[AddFile.from_json(json.loads(file)["file"]) for file in lines],
            )

    @retry_with_exponential_backoff
    def list_table_changes(self, table: Table, cdfOptions: CdfOptions) -> ListTableChangesResponse:
        query_str = f"/shares/{table.share}/schemas/{table.schema}/tables/{table.name}/changes?"

        # Add cdf options.
        # We do not validate the CDF options here since the server will perform validations anyways.
        params = []
        if cdfOptions.starting_version is not None:
            params.append(f"startingVersion={cdfOptions.starting_version}")
        if cdfOptions.starting_timestamp is not None:
            params.append(f"startingTimestamp={quote(cdfOptions.starting_timestamp)}")
        if cdfOptions.ending_version is not None:
            params.append(f"endingVersion={cdfOptions.ending_version}")
        if cdfOptions.ending_timestamp is not None:
            params.append(f"endingTimestamp={quote(cdfOptions.ending_timestamp)}")
        query_str += "&".join(params)

        with self._get_internal(query_str) as lines:
            protocol_json = json.loads(next(lines))
            metadata_json = json.loads(next(lines))
            actions: List[FileAction] = []
            for line in lines:
                actions.append(FileAction.from_json(json.loads(line)))

            return ListTableChangesResponse(
                protocol=Protocol.from_json(protocol_json["protocol"]),
                metadata=Metadata.from_json(metadata_json["metaData"]),
                actions=actions,
            )

    def close(self):
        self._session.close()

    def _get_internal(
        self,
        target: str,
        data: Optional[Dict[str, Any]] = None,
        return_headers: bool = False,
    ):
        return self._request_internal(
            request=self._session.get, return_headers=return_headers, target=target, params=data)

    def _post_internal(
        self,
        target: str,
        data: Optional[Dict[str, Any]] = None,
        return_headers: bool = False,
    ):
        return self._request_internal(
            request=self._session.post, return_headers=return_headers, target=target, json=data)

    @contextmanager
    def _request_internal(
        self,
        request,
        return_headers,
        target: str,
        **kwargs,
    ):
        assert target.startswith("/"), "Targets should start with '/'"
        response = request(f"{self._profile.endpoint}{target}", **kwargs)
        try:
            response.raise_for_status()
            lines = response.iter_lines(decode_unicode=True)
            try:
                if return_headers:
                    yield response.headers, lines
                else:
                    yield lines
            finally:
                collections.deque(lines, maxlen=0)
        except HTTPError as e:
            message = e.args[0]
            try:
                reason = pprint.pformat(json.loads(response.text), indent=2)
                message += "\n Response from server: \n {}".format(reason)
            except ValueError:
                pass
            raise HTTPError(message, response=e.response) from None
        finally:
            response.close()

    def _should_retry(self, error):
        if isinstance(error, HTTPError):
            error_code = error.response.status_code
            if error_code == 429:  # Too Many Requests
                return True
            elif 500 <= error_code < 600:  # Internal Error
                return True
            else:
                return False
        elif isinstance(error, ConnectionError):  # Unable to connect to service
            return True
        else:
            return False

    def _error_on_expired_token(self, error):
        if isinstance(error, HTTPError) and error.response.status_code == 401:
            try:
                expiration_time = datetime.strptime(
                    self._profile.expiration_time, "%Y-%m-%dT%H:%M:%S.%fZ"
                )
                return datetime.now() > expiration_time
            except Exception:
                return False
        else:
            return False
