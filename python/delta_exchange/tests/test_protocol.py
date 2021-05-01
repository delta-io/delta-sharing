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

from delta_exchange.protocol import (
    AddFile,
    Format,
    Metadata,
    Protocol,
    Schema,
    Share,
    ShareProfile,
    Table,
)


def test_share_profile(tmp_path):
    json = """
        {
            "endpoint": "https://localhost/delta-exchange/",
            "token": "token"
        }
        """
    profile = ShareProfile.from_json(json)
    assert profile == ShareProfile("https://localhost/delta-exchange/", "token")

    profile = ShareProfile.read_from_file(io.StringIO(json))
    assert profile == ShareProfile("https://localhost/delta-exchange/", "token")

    profile_path = tmp_path / "test_profile.json"
    with open(profile_path, "w") as f:
        f.write(json)

    profile = ShareProfile.read_from_file(str(profile_path))
    assert profile == ShareProfile("https://localhost/delta-exchange/", "token")

    profile = ShareProfile.read_from_file(profile_path.as_uri())
    assert profile == ShareProfile("https://localhost/delta-exchange/", "token")

    profile = ShareProfile.read_from_file(profile_path)
    assert profile == ShareProfile("https://localhost/delta-exchange/", "token")

    profile = ShareProfile.read_from_file(io.FileIO(profile_path))
    assert profile == ShareProfile("https://localhost/delta-exchange/", "token")


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
            "minReaderVersion" : 1,
            "minWriterVersion" : 2
        }
        """
    protocol = Protocol.from_json(json)
    assert protocol == Protocol(1, 2)


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
            "partitionColumns" : [],
            "configuration" : {{}},
            "createdTime" : 1616958611482
        }}
        """
    metadata = Metadata.from_json(json)
    assert metadata == Metadata(
        id="testId",
        format=Format(),
        schema_string=schema_string.replace(r"\"", '"'),
        partition_columns=[],
        configuration={},
    )


@pytest.mark.parametrize(
    "json,expected",
    [
        pytest.param(
            """
            {
                "path" : "https://localhost/path/to/file.parquet",
                "partitionValues" : {},
                "size" : 120,
                "modificationTime" : 1616958613000,
                "dataChange" : false,
                "stats" : {
                    "numRecords" : 2,
                    "minValues" : {
                        "a" : 0,
                        "b" : "a"
                    },
                    "maxValues" : {
                        "a" : 10,
                        "b" : "z"
                    },
                    "nullCount" : {
                        "a" : 0,
                        "b" : 0
                    }
                }
            }
            """,
            AddFile(
                path="https://localhost/path/to/file.parquet",
                partition_values={},
                size=120,
                data_change=False,
                tags={},
                stats={
                    "numRecords": 2,
                    "minValues": {"a": 0, "b": "a"},
                    "maxValues": {"a": 10, "b": "z"},
                    "nullCount": {"a": 0, "b": 0},
                },
            ),
            id="non partitioned",
        ),
        pytest.param(
            """
            {
                "path" : "https://localhost/path/to/file.parquet",
                "partitionValues" : {"b": "x"},
                "size" : 120,
                "modificationTime" : 1616958613000,
                "dataChange" : false,
                "stats" : {
                    "numRecords" : 2,
                    "minValues" : {
                        "a" : 0
                    },
                    "maxValues" : {
                        "a" : 10
                    },
                    "nullCount" : {
                        "a" : 0
                    }
                }
            }
            """,
            AddFile(
                path="https://localhost/path/to/file.parquet",
                partition_values={"b": "x"},
                size=120,
                data_change=False,
                tags={},
                stats={
                    "numRecords": 2,
                    "minValues": {"a": 0},
                    "maxValues": {"a": 10},
                    "nullCount": {"a": 0},
                },
            ),
            id="partitioned",
        ),
    ],
)
def test_add_file(json: str, expected: AddFile):
    assert AddFile.from_json(json) == expected
