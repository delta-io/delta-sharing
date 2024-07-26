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

from delta_sharing.protocol import CdfOptions, Protocol, Metadata

try:
    from pyspark.sql import DataFrame as PySparkDataFrame
except ImportError:
    pass

from delta_sharing.protocol import DeltaSharingProfile, Schema, Share, Table
from delta_sharing.reader import DeltaSharingReader
from delta_sharing.rest_client import DataSharingRestClient

from requests.exceptions import HTTPError


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


def get_table_version(
    url: str,
    starting_timestamp: Optional[str] = None
) -> int:
    """
    Get the shared table version using the given url.

    :param url: a url under the format "<profile>#<share>.<schema>.<table>"
    :param starting_timestamp: a string in the format of YYYY-MM-DDThh:mm:ssZ. Get the version at or
      after the given timestamp. The latest table version will be returned if this is not specified.
    """
    profile_json, share, schema, table = _parse_url(url)
    profile = DeltaSharingProfile.read_from_file(profile_json)
    rest_client = DataSharingRestClient(profile)
    response = rest_client.query_table_version(
        Table(name=table, share=share, schema=schema),
        starting_timestamp
    )
    return response.delta_table_version


def get_table_protocol(url: str) -> Protocol:
    """
    Get the shared table protocol using the given url.

    :param url: a url under the format "<profile>#<share>.<schema>.<table>"
    """
    profile_json, share, schema, table = _parse_url(url)
    profile = DeltaSharingProfile.read_from_file(profile_json)
    rest_client = DataSharingRestClient(profile)
    response = rest_client.query_table_metadata(Table(name=table, share=share, schema=schema))
    return response.protocol


def get_table_metadata(url: str) -> Metadata:
    """
    Get the shared table metadata using the given url.

    :param url: a url under the format "<profile>#<share>.<schema>.<table>"
    """
    profile_json, share, schema, table = _parse_url(url)
    profile = DeltaSharingProfile.read_from_file(profile_json)
    rest_client = DataSharingRestClient(profile)
    response = rest_client.query_table_metadata(Table(name=table, share=share, schema=schema))
    return response.metadata


def load_as_pandas(
    url: str,
    limit: Optional[int] = None,
    version: Optional[int] = None,
    timestamp: Optional[str] = None,
    jsonPredicateHints: Optional[str] = None,
    use_delta_format: Optional[bool] = None,
) -> pd.DataFrame:
    """
    Load the shared table using the given url as a pandas DataFrame.

    :param url: a url under the format "<profile>#<share>.<schema>.<table>"
    :param limit: a non-negative int. Load only the ``limit`` rows if the parameter is specified.
      Use this optional parameter to explore the shared table without loading the entire table to
      the memory.
    :param version: an optional non-negative int. Load the snapshot of table at version
    :param jsonPredicateHints: Predicate hints to be applied to the table. For more details see:
      https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#json-predicates-for-filtering
    :return: A pandas DataFrame representing the shared table.
    """
    profile_json, share, schema, table = _parse_url(url)
    profile = DeltaSharingProfile.read_from_file(profile_json)
    return DeltaSharingReader(
        table=Table(name=table, share=share, schema=schema),
        rest_client=DataSharingRestClient(profile),
        jsonPredicateHints=jsonPredicateHints,
        limit=limit,
        version=version,
        timestamp=timestamp,
        use_delta_format=use_delta_format
    ).to_pandas()


def load_as_spark(
    url: str,
    version: Optional[int] = None,
    timestamp: Optional[str] = None
) -> "PySparkDataFrame":  # noqa: F821
    """
    Load the shared table using the given url as a Spark DataFrame. `PySpark` must be installed,
    and the application must be a PySpark application with the Apache Spark Connector for Delta
    Sharing installed. Only one of version/timestamp is supported at one time.

    :param url: a url under the format "<profile>#<share>.<schema>.<table>".
    :param version: an optional non-negative int. Load the snapshot of table at version.
    :param timestamp: an optional string. Load the snapshot of table at version corresponding
      to the timestamp.
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
    df = spark.read.format("deltaSharing")
    if version is not None:
        df.option("versionAsOf", version)
    if timestamp is not None:
        df.option("timestampAsOf", timestamp)
    return df.load(url)


def load_table_changes_as_spark(
    url: str,
    starting_version: Optional[int] = None,
    ending_version: Optional[int] = None,
    starting_timestamp: Optional[str] = None,
    ending_timestamp: Optional[str] = None
) -> "PySparkDataFrame":  # noqa: F821
    """
    Load the table changes of a shared table as a Spark DataFrame using the given url.
    `PySpark` must be installed, and the application must be a PySpark application with
    the Apache Spark Connector for Delta Sharing installed.
    Either starting_version or starting_timestamp need to be provided. And only one starting/ending
    parameter is accepted by the server. If the end parameter is not provided, the API will use the
    latest table version for it. The parameter range is inclusive in the query.

    :param url: a url under the format "<profile>#<share>.<schema>.<table>".
    :param starting_version: The starting version of table changes.
    :param ending_version: The ending version of table changes.
    :param starting_timestamp: The starting timestamp of table changes.
    :param ending_timestamp: The ending timestamp of table changes.
    :return: A Spark DataFrame representing the table changes.
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        raise ImportError(
            "Unable to import pyspark. `load_table_changes_as_spark` requires PySpark.")

    spark = SparkSession.getActiveSession()
    assert spark is not None, (
        "No active SparkSession was found. "
        "`load_table_changes_as_spark` requires running in a PySpark application."
    )
    df = spark.read.format("deltaSharing").option("readChangeFeed", "true")
    if starting_version is not None:
        df.option("startingVersion", starting_version)
    if ending_version is not None:
        df.option("endingVersion", ending_version)
    if starting_timestamp is not None:
        df.option("startingTimestamp", starting_timestamp)
    if ending_timestamp is not None:
        df.option("endingTimestamp", ending_timestamp)
    return df.load(url)


def load_table_changes_as_pandas(
    url: str,
    starting_version: Optional[int] = None,
    ending_version: Optional[int] = None,
    starting_timestamp: Optional[str] = None,
    ending_timestamp: Optional[str] = None
) -> pd.DataFrame:
    """
    Load the table changes of shared table as a pandas DataFrame using the given url.
    Either starting_version or starting_timestamp need to be provided. And only one starting/ending
    parameter is accepted by the server. If the end parameter is not provided, the API will use the
    latest table version for it. The parameter range is inclusive in the query.

    :param url: a url under the format "<profile>#<share>.<schema>.<table>".
    :param starting_version: The starting version of table changes.
    :param ending_version: The ending version of table changes.
    :param starting_timestamp: The starting timestamp of table changes.
    :param ending_timestamp: The ending timestamp of table changes.
    :return: A pandas DataFrame representing the shared table.
    """
    profile_json, share, schema, table = _parse_url(url)
    profile = DeltaSharingProfile.read_from_file(profile_json)
    return DeltaSharingReader(
        table=Table(name=table, share=share, schema=schema),
        rest_client=DataSharingRestClient(profile),
    ).table_changes_to_pandas(CdfOptions(
        starting_version=starting_version,
        ending_version=ending_version,
        starting_timestamp=starting_timestamp,
        ending_timestamp=ending_timestamp,
    ))


class SharingClient:
    """
    A Delta Sharing client to query shares/schemas/tables from a Delta Sharing Server.
    """

    def __init__(self, profile: Union[str, BinaryIO, TextIO, Path, DeltaSharingProfile]):
        if not isinstance(profile, DeltaSharingProfile):
            profile = DeltaSharingProfile.read_from_file(profile)
        self._profile = profile
        self._rest_client = DataSharingRestClient(profile)

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
