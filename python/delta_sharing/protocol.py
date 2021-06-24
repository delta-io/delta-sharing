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
from dataclasses import dataclass, field
from json import loads
from pathlib import Path
from typing import ClassVar, Dict, IO, Optional, Sequence, Union

import fsspec


@dataclass(frozen=True)
class DeltaSharingProfile:
    CURRENT: ClassVar[int] = 1

    share_credentials_version: int
    endpoint: str
    bearer_token: str

    def __post_init__(self):
        if self.share_credentials_version > DeltaSharingProfile.CURRENT:
            raise ValueError(
                "'shareCredentialsVersion' in the profile is "
                f"{self.share_credentials_version} which is too new. "
                f"The current release supports version {DeltaSharingProfile.CURRENT} and below. "
                "Please upgrade to a newer release."
            )

    @staticmethod
    def read_from_file(profile: Union[str, IO, Path]) -> "DeltaSharingProfile":
        if isinstance(profile, str):
            infile = fsspec.open(profile).open()
        elif isinstance(profile, Path):
            infile = fsspec.open(profile.as_uri()).open()
        else:
            infile = profile
        try:
            return DeltaSharingProfile.from_json(infile.read())
        finally:
            infile.close()

    @staticmethod
    def from_json(json) -> "DeltaSharingProfile":
        if isinstance(json, (str, bytes, bytearray)):
            json = loads(json)
        endpoint = json["endpoint"]
        if endpoint.endswith("/"):
            endpoint = endpoint[:-1]
        return DeltaSharingProfile(
            share_credentials_version=int(json["shareCredentialsVersion"]),
            endpoint=endpoint,
            bearer_token=json["bearerToken"],
        )


@dataclass(frozen=True)
class Share:
    name: str

    @staticmethod
    def from_json(json) -> "Share":
        if isinstance(json, (str, bytes, bytearray)):
            json = loads(json)
        return Share(name=json["name"])


@dataclass(frozen=True)
class Schema:
    name: str
    share: str

    @staticmethod
    def from_json(json) -> "Schema":
        if isinstance(json, (str, bytes, bytearray)):
            json = loads(json)
        return Schema(name=json["name"], share=json["share"])


@dataclass(frozen=True)
class Table:
    name: str
    share: str
    schema: str

    @staticmethod
    def from_json(json) -> "Table":
        if isinstance(json, (str, bytes, bytearray)):
            json = loads(json)
        return Table(name=json["name"], share=json["share"], schema=json["schema"])


@dataclass(frozen=True)
class Protocol:
    CURRENT: ClassVar[int] = 1

    min_reader_version: int

    def __post_init__(self):
        if self.min_reader_version > Protocol.CURRENT:
            raise ValueError(
                f"The table requires a newer version {self.min_reader_version} to read. "
                f"But the current release supports version {Protocol.CURRENT} and below. "
                f"Please upgrade to a newer release."
            )

    @staticmethod
    def from_json(json) -> "Protocol":
        if isinstance(json, (str, bytes, bytearray)):
            json = loads(json)
        return Protocol(min_reader_version=int(json["minReaderVersion"]))


@dataclass(frozen=True)
class Format:
    provider: str = "parquet"
    options: Dict[str, str] = field(default_factory=dict)

    @staticmethod
    def from_json(json) -> "Format":
        if isinstance(json, (str, bytes, bytearray)):
            json = loads(json)
        return Format(provider=json.get("provider", "parquet"), options=json.get("options", {}))


@dataclass(frozen=True)
class Metadata:
    id: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    format: Format = field(default_factory=Format)
    schema_string: Optional[str] = None
    partition_columns: Sequence[str] = field(default_factory=list)

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
        )


@dataclass(frozen=True)
class AddFile:
    url: str
    id: str
    partition_values: Dict[str, str]
    size: int
    stats: Optional[str] = None

    @staticmethod
    def from_json(json) -> "AddFile":
        if isinstance(json, (str, bytes, bytearray)):
            json = loads(json)
        return AddFile(
            url=json["url"],
            id=json["id"],
            partition_values=json["partitionValues"],
            size=int(json["size"]),
            stats=json.get("stats", None),
        )
