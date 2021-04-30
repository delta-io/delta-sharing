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
from typing import BinaryIO, Sequence, TextIO, Union
from pathlib import Path

from delta_exchange.protocol import Schema, Share, ShareProfile, Table
from delta_exchange.rest_client import DataSharingRestClient


class DeltaExchange:
    def __init__(self, profile: Union[str, BinaryIO, TextIO, Path, ShareProfile]):
        if not isinstance(profile, ShareProfile):
            profile = ShareProfile.read_from_file(profile)
        self._profile = profile
        self._rest_client = DataSharingRestClient(profile)

    def list_shares(self) -> Sequence[Share]:
        return self._rest_client.list_shares().shares

    def list_schemas(self, share: Share) -> Sequence[Schema]:
        return self._rest_client.list_schemas(share=share).schemas

    def list_tables(self, schema: Schema) -> Sequence[Table]:
        return self._rest_client.list_tables(schema=schema).tables

    def list_all_tables(self) -> Sequence[Table]:
        shares = self.list_shares()
        schemas = chain(*(self.list_schemas(share) for share in shares))
        return list(chain(*(self.list_tables(schema) for schema in schemas)))
