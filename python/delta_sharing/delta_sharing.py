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
from typing import BinaryIO, List, Optional, Sequence, TextIO, Tuple, Union
from pathlib import Path

import pandas as pd

try:
    from pyspark.sql import DataFrame as PySparkDataFrame
except ImportError:
    pass

from delta_sharing.protocol import DeltaSharingProfile, Schema, Share, Table
from delta_sharing.reader import DeltaSharingReader
from delta_sharing.rest_client import DataSharingRestClient


def _parse_url(url: str) -> Tuple[str, str, str, str]:
    """
    :param url: a url under the format "<profile>#<share>.<schema>.<table>"
    :return: a tuple with parsed (profile, share, schema, table)
    """
    shape_index = url.rfind("#")
    if shape_index < 0:
        raise ValueError(f"Invalid 'url': {url}")
    profile = url[0:shape_index]
    fragments = url[shape_index + 1 :].split(".")
    if len(fragments) != 3:
        raise ValueError(f"Invalid 'url': {url}")
    share, schema, table = fragments
    if len(profile) == 0 or len(share) == 0 or len(schema) == 0 or len(table) == 0:
        raise ValueError(f"Invalid 'url': {url}")
    return (profile, share, schema, table)


def load_as_pandas(url: str) -> pd.DataFrame:
    """
    Load the shared table using the give url as a pandas DataFrame.

    :param url: a url under the format "<profile>#<share>.<schema>.<table>"
    :return: A pandas DataFrame representing the shared table.
    """
    profile_json, share, schema, table = _parse_url(url)
    profile = DeltaSharingProfile.read_from_file(profile_json)
    return DeltaSharingReader(
        table=Table(name=table, share=share, schema=schema),
        rest_client=DataSharingRestClient(profile),
    ).to_pandas()


def load_as_spark(url: str) -> "PySparkDataFrame":  # noqa: F821
    """
    Load the shared table using the give url as a Spark DataFrame. `PySpark` must be installed, and
    the application must be a PySpark application with the Apache Spark Connector for Delta Sharing
    installed.

    :param url: a url under the format "<profile>#<share>.<schema>.<table>"
    :return: A Spark DataFrame representing the shared table.
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        raise ImportError("Unable to import pyspark. `load_as_spark` requires PySpark.")

    spark = SparkSession.getActiveSession()
    assert spark is not None, (
        "No active SparkSession was found. "
        "`load_as_spark` requires running in a PySpark application."
    )
    return spark.read.format("deltaSharing").load(url)


class SharingClient:
    """
    A Delta Sharing client to query shares/schemas/tables from a Delta Sharing Server.
    """

    def __init__(self, profile: Union[str, BinaryIO, TextIO, Path, DeltaSharingProfile]):
        if not isinstance(profile, DeltaSharingProfile):
            profile = DeltaSharingProfile.read_from_file(profile)
        self._profile = profile
        self._rest_client = DataSharingRestClient(profile)

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
            if page_token is None:
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
            if page_token is None:
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
            if page_token is None:
                return tables

    def list_all_tables(self) -> Sequence[Table]:
        """
        List all tables that can be accessed by you in a Delta Sharing Server.

        :return: all tables that can be accessed.
        """
        shares = self.list_shares()
        schemas = chain(*(self.list_schemas(share) for share in shares))
        return list(chain(*(self.list_tables(schema) for schema in schemas)))
