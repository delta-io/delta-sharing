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
from urllib.parse import urlparse

import pandas as pd

try:
    from pyspark.sql import DataFrame as PySparkDataFrame
except ImportError:
    pass

from delta_sharing.protocol import Schema, Share, ShareProfile, Table
from delta_sharing.reader import DeltaSharingReader
from delta_sharing.rest_client import DataSharingRestClient


def load_as_pandas(url: str) -> pd.DataFrame:
    profile_json = url.split("#")[0]
    profile = ShareProfile.read_from_file(profile_json)

    parsed = urlparse(url)
    fragments = parsed.fragment.split(".")
    if len(fragments) != 3:
        raise ValueError("table")
    share, schema, table = fragments

    return DeltaSharingReader(
        table=Table(name=table, share=share, schema=schema),
        rest_client=DataSharingRestClient(profile),
    ).to_pandas()


def load_as_spark(url: str) -> "PySparkDataFrame":
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    assert spark is not None, (
        "No active SparkSession was found. "
        "`DeltaSharing.load_as_spark` needs SparkSession with DeltaSharing data source enabled."
    )
    return spark.read.format("deltaSharing").load(url)


class SharingClient:
    def __init__(self, profile: Union[str, BinaryIO, TextIO, Path, ShareProfile]):
        if not isinstance(profile, ShareProfile):
            profile = ShareProfile.read_from_file(profile)
        self._profile = profile
        self._rest_client = DataSharingRestClient(profile)

    def list_shares(self) -> Sequence[Share]:
        shares: List[Share] = []
        page_token: Optional[str] = None
        while True:
            response = self._rest_client.list_shares(page_token=page_token)
            shares.extend(response.shares)
            page_token = response.next_page_token
            if page_token is None:
                return shares

    def list_schemas(self, share: Share) -> Sequence[Schema]:
        schemas: List[Schema] = []
        page_token: Optional[str] = None
        while True:
            response = self._rest_client.list_schemas(share=share, page_token=page_token)
            schemas.extend(response.schemas)
            page_token = response.next_page_token
            if page_token is None:
                return schemas

    def list_tables(self, schema: Schema) -> Sequence[Table]:
        tables: List[Table] = []
        page_token: Optional[str] = None
        while True:
            response = self._rest_client.list_tables(schema=schema, page_token=page_token)
            tables.extend(response.tables)
            page_token = response.next_page_token
            if page_token is None:
                return tables

    def list_all_tables(self) -> Sequence[Table]:
        shares = self.list_shares()
        schemas = chain(*(self.list_schemas(share) for share in shares))
        return list(chain(*(self.list_tables(schema) for schema in schemas)))
