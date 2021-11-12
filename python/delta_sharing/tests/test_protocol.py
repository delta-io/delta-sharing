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
import io

import pytest

from delta_sharing.protocol import (
    AddFile,
    DeltaSharingProfile,
    Format,
    Metadata,
    Protocol,
    Schema,
    Share,
    Table,
)


def test_share_profile(tmp_path):
    json = """
        {
            "shareCredentialsVersion": 1,
            "endpoint": "https://localhost/delta-sharing/",
            "bearerToken": "token"
        }
        """
    profile = DeltaSharingProfile.from_json(json)
    assert profile == DeltaSharingProfile(1, "https://localhost/delta-sharing", "token")

    json = """
        {
            "shareCredentialsVersion": 1,
            "endpoint": "https://localhost/delta-sharing/",
            "bearerToken": "token",
            "expirationTime": "2021-11-12T00:12:29.0Z"
        }
        """
    profile = DeltaSharingProfile.from_json(json)
    assert profile == DeltaSharingProfile(
        1, "https://localhost/delta-sharing", "token", "2021-11-12T00:12:29.0Z"
    )

    profile = DeltaSharingProfile.read_from_file(io.StringIO(json))
    assert profile == DeltaSharingProfile(
        1, "https://localhost/delta-sharing", "token", "2021-11-12T00:12:29.0Z"
    )

    profile_path = tmp_path / "test_profile.json"
    with open(profile_path, "w") as f:
        f.write(json)

    profile = DeltaSharingProfile.read_from_file(str(profile_path))
    assert profile == DeltaSharingProfile(
        1, "https://localhost/delta-sharing", "token", "2021-11-12T00:12:29.0Z"
    )

    profile = DeltaSharingProfile.read_from_file(profile_path.as_uri())
    assert profile == DeltaSharingProfile(
        1, "https://localhost/delta-sharing", "token", "2021-11-12T00:12:29.0Z"
    )

    profile = DeltaSharingProfile.read_from_file(profile_path)
    assert profile == DeltaSharingProfile(
        1, "https://localhost/delta-sharing", "token", "2021-11-12T00:12:29.0Z"
    )

    profile = DeltaSharingProfile.read_from_file(io.FileIO(profile_path))
    assert profile == DeltaSharingProfile(
        1, "https://localhost/delta-sharing", "token", "2021-11-12T00:12:29.0Z"
    )

    json = """
        {
            "shareCredentialsVersion": 100,
            "endpoint": "https://localhost/delta-sharing/",
            "bearerToken": "token",
            "expirationTime": "2021-11-12T00:12:29.0Z"
        }
        """
    with pytest.raises(
        ValueError, match="'shareCredentialsVersion' in the profile is 100 which is too new."
    ):
        DeltaSharingProfile.read_from_file(io.StringIO(json))


def test_share():
    json = """
        {
            "name" : "share_name"
        }
        """
    share = Share.from_json(json)
    assert share == Share("share_name")


def test_schema():
    json = """
        {
            "name" : "schema_name",
            "share" : "share_name"
        }
        """
    schema = Schema.from_json(json)
    assert schema == Schema("schema_name", "share_name")


def test_table():
    json = """
        {
            "name" : "table_name",
            "share" : "share_name",
            "schema" : "schema_name"
        }
        """
    table = Table.from_json(json)
    assert table == Table("table_name", "share_name", "schema_name")


def test_protocol():
    json = """
        {
            "minReaderVersion" : 1
        }
        """
    protocol = Protocol.from_json(json)
    assert protocol == Protocol(1)

    json = """
        {
            "minReaderVersion" : 100
        }
        """
    with pytest.raises(ValueError, match="The table requires a newer version 100 to read."):
        Protocol.from_json(json)


def test_metadata():
    schema_string = (
        r"{\"type\":\"struct\",\"fields\":["
        r"{\"name\":\"_1\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},"
        r"{\"name\":\"_2\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}"
    )
    json = f"""
        {{
            "id" : "testId",
            "format" : {{
                "provider" : "parquet",
                "options" : {{}}
            }},
            "schemaString" : "{schema_string}",
            "partitionColumns" : []
        }}
        """
    metadata = Metadata.from_json(json)
    assert metadata == Metadata(
        id="testId",
        format=Format(),
        schema_string=schema_string.replace(r"\"", '"'),
        partition_columns=[],
    )


@pytest.mark.parametrize(
    "json,expected",
    [
        pytest.param(
            """
            {
                "url" : "https://localhost/path/to/file.parquet",
                "id" : "id",
                "partitionValues" : {},
                "size" : 120,
                "stats" : "{\\"numRecords\\":2}"
            }
            """,
            AddFile(
                url="https://localhost/path/to/file.parquet",
                id="id",
                partition_values={},
                size=120,
                stats=r'{"numRecords":2}',
            ),
            id="non partitioned",
        ),
        pytest.param(
            """
            {
                "url" : "https://localhost/path/to/file.parquet",
                "id" : "id",
                "partitionValues" : {"b": "x"},
                "size" : 120,
                "stats" : "{\\"numRecords\\":2}"
            }
            """,
            AddFile(
                url="https://localhost/path/to/file.parquet",
                id="id",
                partition_values={"b": "x"},
                size=120,
                stats=r'{"numRecords":2}',
            ),
            id="partitioned",
        ),
        pytest.param(
            """
            {
                "url" : "https://localhost/path/to/file.parquet",
                "id" : "id",
                "partitionValues" : {"b": "x"},
                "size" : 120
            }
            """,
            AddFile(
                url="https://localhost/path/to/file.parquet",
                id="id",
                partition_values={"b": "x"},
                size=120,
                stats=None,
            ),
            id="no stats",
        ),
    ],
)
def test_add_file(json: str, expected: AddFile):
    assert AddFile.from_json(json) == expected
