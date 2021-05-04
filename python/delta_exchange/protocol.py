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
from json import loads
from pathlib import Path
from typing import Any, Dict, IO, NamedTuple, Optional, Sequence, Union

import fsspec


class ShareProfile(NamedTuple):
    endpoint: str
    token: str

    @staticmethod
    def read_from_file(profile: Union[str, IO, Path]) -> "ShareProfile":
        if isinstance(profile, str):
            infile = fsspec.open(profile).open()
        elif isinstance(profile, Path):
            infile = fsspec.open(profile.as_uri()).open()
        else:
            infile = profile
        try:
            return ShareProfile.from_json(infile.read())
        finally:
            infile.close()

    @staticmethod
    def from_json(json) -> "ShareProfile":
        if isinstance(json, (str, bytes, bytearray)):
            json = loads(json)
        return ShareProfile(endpoint=json["endpoint"], token=json["token"])


class Share(NamedTuple):
    name: str

    @staticmethod
    def from_json(json) -> "Share":
        if isinstance(json, (str, bytes, bytearray)):
            json = loads(json)
        return Share(name=json["name"])


class Schema(NamedTuple):
    name: str
    share: str

    @staticmethod
    def from_json(json) -> "Schema":
        if isinstance(json, (str, bytes, bytearray)):
            json = loads(json)
        return Schema(name=json["name"], share=json["share"])


class Table(NamedTuple):
    name: str
    share: str
    schema: str

    @staticmethod
    def from_json(json) -> "Table":
        if isinstance(json, (str, bytes, bytearray)):
            json = loads(json)
        return Table(name=json["name"], share=json["share"], schema=json["schema"])


class Protocol(NamedTuple):
    min_reader_version: int
    min_writer_version: int

    @staticmethod
    def from_json(json) -> "Protocol":
        if isinstance(json, (str, bytes, bytearray)):
            json = loads(json)
        return Protocol(
            min_reader_version=int(json["minReaderVersion"]),
            min_writer_version=int(json["minWriterVersion"]),
        )


class Format(NamedTuple):
    provider: str = "parquet"
    options: Dict[str, str] = {}

    @staticmethod
    def from_json(json) -> "Format":
        if isinstance(json, (str, bytes, bytearray)):
            json = loads(json)
        return Format(provider=json.get("provider", "parquet"), options=json.get("options", {}))


class Metadata(NamedTuple):
    id: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    format: Format = Format()
    schema_string: Optional[str] = None
    partition_columns: Sequence[str] = []
    configuration: Dict[str, str] = {}

    @staticmethod
    def from_json(json) -> "Metadata":
        if isinstance(json, (str, bytes, bytearray)):
            json = loads(json)
        return Metadata(
            id=json["id"],
            name=json.get("name", None),
            description=json.get("description", None),
            format=Format.from_json(json["format"]),
            schema_string=json["schemaString"],
            partition_columns=json["partitionColumns"],
            configuration=json["configuration"],
        )


class AddFile(NamedTuple):
    path: str
    partition_values: Dict[str, str]
    size: int
    data_change: bool
    tags: Dict[str, str] = {}
    stats: Dict[str, Any] = {}

    @staticmethod
    def from_json(json) -> "AddFile":
        if isinstance(json, (str, bytes, bytearray)):
            json = loads(json)
        return AddFile(
            path=json["path"],
            partition_values=json["partitionValues"],
            size=int(json["size"]),
            data_change=bool(json["dataChange"]),
            tags=json.get("tags", {}),
            stats=json.get("stats", {}),
        )
