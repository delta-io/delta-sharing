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
from itertools import chain
from typing import BinaryIO, List, Optional, Sequence, TextIO, Union
from pathlib import Path
import requests

from delta_sharing.protocol import DeltaSharingProfile, Schema, Share, Table
from delta_sharing.rest_client import DataSharingRestClient

from requests.exceptions import HTTPError


class SharingClient:
    """
    A Delta Sharing client to query shares/schemas/tables from a Delta Sharing Server.

    :param profile: The path to the profile file or a DeltaSharingProfile object.
    :param session: An optional requests.Session object to use for HTTP requests.
                    You can use this to customize proxy settings, authentication, etc.
    """
    def __init__(
        self,
        profile: Union[str, BinaryIO, TextIO, Path, DeltaSharingProfile],
        session: Optional[requests.Session] = None
    ):
        if not isinstance(profile, DeltaSharingProfile):
            profile = DeltaSharingProfile.read_from_file(profile)
        self._profile = profile
        self._rest_client = DataSharingRestClient(profile, session=session)

    @property
    def rest_client(self) -> DataSharingRestClient:
        """
        Get the underlying DataSharingRestClient used by this SharingClient.

        :return: The DataSharingRestClient instance.
        """
        return self._rest_client

    def __list_all_tables_in_share(self, share: Share) -> Sequence[Table]:
        tables: List[Table] = []
        page_token: Optional[str] = None
        while True:
            response = self._rest_client.list_all_tables(share=share, page_token=page_token)
            tables.extend(response.tables)
            page_token = response.next_page_token
            if page_token is None or page_token == "":
                return tables

    def list_shares(self) -> Sequence[Share]:
        """
        List shares that can be accessed by you in a Delta Sharing Server.

        :return: the shares that can be accessed.
        """
        shares: List[Share] = []
        page_token: Optional[str] = None
        while True:
            response = self._rest_client.list_shares(page_token=page_token)
            shares.extend(response.shares)
            page_token = response.next_page_token
            if page_token is None or page_token == "":
                return shares

    def list_schemas(self, share: Share) -> Sequence[Schema]:
        """
        List schemas in a share that can be accessed by you in a Delta Sharing Server.

        :param share: the share to list.
        :return: the schemas in a share.
        """
        schemas: List[Schema] = []
        page_token: Optional[str] = None
        while True:
            response = self._rest_client.list_schemas(share=share, page_token=page_token)
            schemas.extend(response.schemas)
            page_token = response.next_page_token
            if page_token is None or page_token == "":
                return schemas

    def list_tables(self, schema: Schema) -> Sequence[Table]:
        """
        List tables in a schema that can be accessed by you in a Delta Sharing Server.

        :param schema: the schema to list.
        :return: the tables in a schema.
        """
        tables: List[Table] = []
        page_token: Optional[str] = None
        while True:
            response = self._rest_client.list_tables(schema=schema, page_token=page_token)
            tables.extend(response.tables)
            page_token = response.next_page_token
            if page_token is None or page_token == "":
                return tables

    def list_all_tables(self) -> Sequence[Table]:
        """
        List all tables that can be accessed by you in a Delta Sharing Server.

        :return: all tables that can be accessed.
        """
        shares = self.list_shares()
        try:
            return list(chain(*(self.__list_all_tables_in_share(share) for share in shares)))
        except HTTPError as e:
            if e.response.status_code == 404:
                # The server doesn't support all-tables API. Fallback to the old APIs instead.
                schemas = chain(*(self.list_schemas(share) for share in shares))
                return list(chain(*(self.list_tables(schema) for schema in schemas)))
            else:
                raise e
