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
from typing import ClassVar, Dict, IO, List, Optional, Sequence, Union, TypedDict

import fsspec


class PrivateKeyConfig(TypedDict, total=False):
    privateKeyFile: Optional[str]
    keyId: Optional[str]
    algorithm: Optional[str]


@dataclass(frozen=True)
class DeltaSharingProfile:
    CURRENT: ClassVar[int] = 2

    share_credentials_version: int
    endpoint: str
    bearer_token: Optional[str] = None
    expiration_time: Optional[str] = None
    type: Optional[str] = None
    token_endpoint: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    scope: Optional[str] = None
    issuer: Optional[str] = None
    audience: Optional[str] = None
    private_key: Optional[Dict[str, str]] = field(default=None, hash=False, compare=False)

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

        share_credentials_version = int(json["shareCredentialsVersion"])
        endpoint = json["endpoint"]
        if endpoint is not None and endpoint.endswith("/"):
            endpoint = endpoint[:-1]

        if share_credentials_version == 1:
            return DeltaSharingProfile(
                share_credentials_version=share_credentials_version,
                endpoint=endpoint,
                bearer_token=json["bearerToken"],
                expiration_time=json.get("expirationTime"),
            )
        elif share_credentials_version == 2:
            type = json["type"]
            if type == "oauth_jwt_bearer_private_key_jwt":
                # New nested format with auth object
                auth = json["auth"]
                token_endpoint = auth["tokenEndpoint"]
                if token_endpoint is not None and token_endpoint.endswith("/"):
                    token_endpoint = token_endpoint[:-1]

                # Extract privateKey from nested structure
                private_key_config = auth.get("privateKey", {})
                private_key_dict = {
                    "privateKeyFile": private_key_config.get("privateKeyFile"),
                    "keyId": private_key_config.get("keyId"),
                    "algorithm": private_key_config.get("algorithm"),
                }

                return DeltaSharingProfile(
                    share_credentials_version=share_credentials_version,
                    type=type,
                    endpoint=endpoint,
                    token_endpoint=token_endpoint,
                    issuer=auth["issuer"],
                    client_id=auth["clientId"],
                    private_key=private_key_dict,
                    audience=auth["audience"],
                    scope=auth.get("scope"),
                )
            elif type == "oauth_client_credentials":
                token_endpoint = json["tokenEndpoint"]
                if token_endpoint is not None and token_endpoint.endswith("/"):
                    token_endpoint = token_endpoint[:-1]
                return DeltaSharingProfile(
                    share_credentials_version=share_credentials_version,
                    type=type,
                    endpoint=endpoint,
                    token_endpoint=token_endpoint,
                    client_id=json["clientId"],
                    client_secret=json["clientSecret"],
                    scope=json.get("scope"),
                )
            elif type == "bearer_token":
                return DeltaSharingProfile(
                    share_credentials_version=share_credentials_version,
                    type=type,
                    endpoint=endpoint,
                    bearer_token=json["bearerToken"],
                    expiration_time=json.get("expirationTime"),
                )
            elif type == "basic":
                return DeltaSharingProfile(
                    share_credentials_version=share_credentials_version,
                    type=type,
                    endpoint=endpoint,
                    username=json["username"],
                    password=json["password"],
                )
            else:
                raise ValueError(
                    f"The current release does not supports {type} type. " "Please check type."
                )
        else:
            raise ValueError(
                "'shareCredentialsVersion' in the profile is "
                f"{share_credentials_version} which is too new. "
                f"The current release supports version {DeltaSharingProfile.CURRENT} and below. "
                "Please upgrade to a newer release."
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
    CURRENT: ClassVar[int] = 3

    min_reader_version: int
    min_writer_version: Optional[int] = None
    reader_features: Optional[List[str]] = None
    writer_features: Optional[List[str]] = None

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
        if "deltaProtocol" in json:
            delta_protocol = json["deltaProtocol"]
            return Protocol(
                min_reader_version=int(delta_protocol["minReaderVersion"]),
                min_writer_version=int(delta_protocol["minWriterVersion"]),
                reader_features=delta_protocol.get("readerFeatures", None),
                writer_features=delta_protocol.get("writerFeatures", None),
            )
        else:
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
    configuration: Dict[str, str] = field(default_factory=dict)
    partition_columns: Sequence[str] = field(default_factory=list)
    version: Optional[int] = None
    size: Optional[int] = None
    num_files: Optional[int] = None
    created_time: Optional[int] = None
    # Directory-based access fields (see PROTOCOL.md "Access Modes"). `location` is the table root
    # where the delta log lives; `access_modes` lists the modes the server supports ("url"/"dir").
    location: Optional[str] = None
    auxiliary_locations: Optional[Sequence[str]] = None
    access_modes: Optional[Sequence[str]] = None

    @staticmethod
    def from_json(json) -> "Metadata":
        if isinstance(json, (str, bytes, bytearray)):
            json = loads(json)
        if "deltaMetadata" in json:
            delta_metadata = json["deltaMetadata"]
            configuration = delta_metadata.get("configuration", {})
            return Metadata(
                id=delta_metadata["id"],
                name=delta_metadata.get("name", None),
                description=delta_metadata.get("description", None),
                format=Format.from_json(delta_metadata["format"]),
                schema_string=delta_metadata["schemaString"],
                configuration=configuration,
                partition_columns=delta_metadata["partitionColumns"],
                version=json.get("version", None),
                size=json.get("size", None),
                num_files=json.get("numFiles", None),
                created_time=delta_metadata.get("createdTime", None),
                # Sharing-level fields sit alongside `deltaMetadata`, not inside it.
                location=json.get("location", None),
                auxiliary_locations=json.get("auxiliaryLocations", None),
                access_modes=json.get("accessModes", None),
            )
        else:
            configuration = json.get("configuration", {})
            return Metadata(
                id=json["id"],
                name=json.get("name", None),
                description=json.get("description", None),
                format=Format.from_json(json["format"]),
                schema_string=json["schemaString"],
                configuration=configuration,
                partition_columns=json["partitionColumns"],
                version=json.get("version", None),
                size=json.get("size", None),
                num_files=json.get("numFiles", None),
                location=json.get("location", None),
                auxiliary_locations=json.get("auxiliaryLocations", None),
                access_modes=json.get("accessModes", None),
            )


@dataclass(frozen=True)
class FileAction:
    url: str
    id: str
    partition_values: Dict[str, str]
    size: int
    timestamp: Optional[int] = None
    version: Optional[int] = None

    def get_change_type_col_value(self) -> str:
        raise ValueError(f"_change_type not supported for {self.url}")

    @staticmethod
    def from_json(action_json) -> "FileAction":
        if "add" in action_json:
            return AddFile.from_json(action_json["add"])
        elif "cdf" in action_json:
            return AddCdcFile.from_json(action_json["cdf"])
        elif "remove" in action_json:
            return RemoveFile.from_json(action_json["remove"])
        else:
            return None


@dataclass(frozen=True)
class AddFile(FileAction):
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
            timestamp=json.get("timestamp", None),
            version=json.get("version", None),
        )

    def get_change_type_col_value(self) -> str:
        return "insert"


@dataclass(frozen=True)
class AddCdcFile(FileAction):
    @staticmethod
    def from_json(json) -> "AddCdcFile":
        if isinstance(json, (str, bytes, bytearray)):
            json = loads(json)
        return AddCdcFile(
            url=json["url"],
            id=json["id"],
            partition_values=json["partitionValues"],
            size=int(json["size"]),
            timestamp=json["timestamp"],
            version=json["version"],
        )


@dataclass(frozen=True)
class RemoveFile(FileAction):
    @staticmethod
    def from_json(json) -> "RemoveFile":
        if isinstance(json, (str, bytes, bytearray)):
            json = loads(json)
        return RemoveFile(
            url=json["url"],
            id=json["id"],
            partition_values=json["partitionValues"],
            size=int(json["size"]),
            timestamp=json.get("timestamp", None),
            version=json.get("version", None),
        )

    def get_change_type_col_value(self) -> str:
        return "delete"


@dataclass(frozen=True)
class CdfOptions:
    starting_version: Optional[int] = None
    ending_version: Optional[int] = None
    starting_timestamp: Optional[str] = None
    ending_timestamp: Optional[str] = None
    include_historical_metadata: Optional[bool] = None


@dataclass(frozen=True)
class AwsTempCredentials:
    access_key_id: str
    secret_access_key: str
    session_token: str

    @staticmethod
    def from_json(json) -> "AwsTempCredentials":
        if isinstance(json, (str, bytes, bytearray)):
            json = loads(json)
        return AwsTempCredentials(
            access_key_id=json["accessKeyId"],
            secret_access_key=json["secretAccessKey"],
            session_token=json["sessionToken"],
        )

    def __repr__(self) -> str:
        return (
            "AwsTempCredentials(access_key_id='***', "
            "secret_access_key='***', session_token='***')"
        )


@dataclass(frozen=True)
class AzureUserDelegationSas:
    sas_token: str

    @staticmethod
    def from_json(json) -> "AzureUserDelegationSas":
        if isinstance(json, (str, bytes, bytearray)):
            json = loads(json)
        return AzureUserDelegationSas(sas_token=json["sasToken"])

    def __repr__(self) -> str:
        return "AzureUserDelegationSas(sas_token='***')"


@dataclass(frozen=True)
class GcpOauthToken:
    oauth_token: str

    @staticmethod
    def from_json(json) -> "GcpOauthToken":
        if isinstance(json, (str, bytes, bytearray)):
            json = loads(json)
        return GcpOauthToken(oauth_token=json["oauthToken"])

    def __repr__(self) -> str:
        return "GcpOauthToken(oauth_token='***')"


@dataclass(frozen=True)
class R2Credentials:
    access_key_id: str
    secret_access_key: str
    session_token: str

    @staticmethod
    def from_json(json) -> "R2Credentials":
        if isinstance(json, (str, bytes, bytearray)):
            json = loads(json)
        return R2Credentials(
            access_key_id=json["accessKeyId"],
            secret_access_key=json["secretAccessKey"],
            session_token=json["sessionToken"],
        )

    def __repr__(self) -> str:
        return "R2Credentials(access_key_id='***', " "secret_access_key='***', session_token='***')"


@dataclass(frozen=True)
class TemporaryTableCredentials:
    """Short-lived, prefix-scoped cloud credentials for directory-based table access.

    Returned by the ``temporary-table-credentials`` endpoint. Exactly one of the cloud-specific
    credential fields is populated. The credentials are short-lived: callers must respect
    ``expiration_time`` (epoch milliseconds) and refresh before it elapses. ``location`` is the
    storage prefix the credentials grant read access to.
    """

    location: Optional[str] = None
    expiration_time: Optional[int] = None
    aws_temp_credentials: Optional[AwsTempCredentials] = None
    azure_user_delegation_sas: Optional[AzureUserDelegationSas] = None
    gcp_oauth_token: Optional[GcpOauthToken] = None
    r2_credentials: Optional[R2Credentials] = None

    @staticmethod
    def from_json(json) -> "TemporaryTableCredentials":
        if isinstance(json, (str, bytes, bytearray)):
            json = loads(json)
        # The endpoint nests the payload under "credentials"; tolerate either shape.
        creds = json.get("credentials", json)
        aws = creds.get("awsTempCredentials")
        azure = creds.get("azureUserDelegationSas")
        gcp = creds.get("gcpOauthToken")
        r2 = creds.get("r2Credentials")
        return TemporaryTableCredentials(
            location=creds.get("location"),
            expiration_time=creds.get("expirationTime"),
            aws_temp_credentials=AwsTempCredentials.from_json(aws) if aws else None,
            azure_user_delegation_sas=AzureUserDelegationSas.from_json(azure) if azure else None,
            gcp_oauth_token=GcpOauthToken.from_json(gcp) if gcp else None,
            r2_credentials=R2Credentials.from_json(r2) if r2 else None,
        )

    def __repr__(self) -> str:
        kinds = {
            "aws": self.aws_temp_credentials,
            "azure": self.azure_user_delegation_sas,
            "gcp": self.gcp_oauth_token,
            "r2": self.r2_credentials,
        }
        kind = next((name for name, value in kinds.items() if value is not None), None)
        return (
            f"TemporaryTableCredentials(location={self.location!r}, "
            f"expiration_time={self.expiration_time}, credentials=<{kind} redacted>)"
        )
