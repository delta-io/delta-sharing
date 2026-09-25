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
import pytest

from delta_sharing import credentials
from delta_sharing.credentials import (
    DIR_ACCESS_MODE,
    GCS_BEARER_TOKEN_OPTION,
    URL_ACCESS_MODE,
    TemporaryTableCredentialsProvider,
    UnsupportedCloudError,
    choose_access_mode,
    is_dir_readable_scheme,
    to_object_store_options,
)
from delta_sharing.protocol import (
    Metadata,
    Table,
    TemporaryTableCredentials,
)

# ---------- TemporaryTableCredentials.from_json ----------


def test_temporary_credentials_from_json_aws_nested():
    creds = TemporaryTableCredentials.from_json(
        {
            "credentials": {
                "location": "s3://bucket/path/to/table",
                "expirationTime": 1234567890000,
                "awsTempCredentials": {
                    "accessKeyId": "AKIA",
                    "secretAccessKey": "secret",
                    "sessionToken": "token",
                },
            }
        }
    )
    assert creds.location == "s3://bucket/path/to/table"
    assert creds.expiration_time == 1234567890000
    assert creds.aws_temp_credentials.access_key_id == "AKIA"
    assert creds.azure_user_delegation_sas is None
    assert creds.gcp_oauth_token is None
    assert creds.r2_credentials is None


def test_temporary_credentials_from_json_unwrapped():
    creds = TemporaryTableCredentials.from_json(
        {
            "location": "abfss://c@acct.dfs.core.windows.net/p",
            "azureUserDelegationSas": {"sasToken": "sas"},
        }
    )
    assert creds.azure_user_delegation_sas.sas_token == "sas"
    assert creds.aws_temp_credentials is None


def test_temporary_credentials_from_json_gcp_and_r2():
    gcp = TemporaryTableCredentials.from_json(
        {"credentials": {"location": "gs://b/p", "gcpOauthToken": {"oauthToken": "tok"}}}
    )
    assert gcp.gcp_oauth_token.oauth_token == "tok"
    r2 = TemporaryTableCredentials.from_json(
        {
            "credentials": {
                "location": "s3://b/p",
                "r2Credentials": {
                    "accessKeyId": "id",
                    "secretAccessKey": "secret",
                    "sessionToken": "token",
                },
            }
        }
    )
    assert r2.r2_credentials.access_key_id == "id"


def test_temporary_credentials_repr_redacts_secrets():
    creds = TemporaryTableCredentials.from_json(
        {
            "credentials": {
                "location": "s3://bucket/p",
                "awsTempCredentials": {
                    "accessKeyId": "AKIA-SENSITIVE",
                    "secretAccessKey": "SUPER-SECRET",
                    "sessionToken": "SESSION-SECRET",
                },
            }
        }
    )
    text = repr(creds) + repr(creds.aws_temp_credentials)
    assert "SUPER-SECRET" not in text
    assert "SESSION-SECRET" not in text
    assert "AKIA-SENSITIVE" not in text
    assert "redacted" in repr(creds)


# ---------- Metadata access-mode fields ----------


def test_metadata_parses_access_modes_parquet():
    metadata = Metadata.from_json(
        {
            "id": "id",
            "format": {"provider": "parquet"},
            "schemaString": "{}",
            "partitionColumns": [],
            "location": "s3://bucket/tables/customer",
            "auxiliaryLocations": ["s3://secondary/tables/customer"],
            "accessModes": ["url", "dir"],
        }
    )
    assert metadata.location == "s3://bucket/tables/customer"
    assert metadata.auxiliary_locations == ["s3://secondary/tables/customer"]
    assert metadata.access_modes == ["url", "dir"]


def test_metadata_parses_access_modes_delta_wrapper():
    metadata = Metadata.from_json(
        {
            "deltaMetadata": {
                "id": "id",
                "format": {"provider": "parquet"},
                "schemaString": "{}",
                "partitionColumns": [],
            },
            "location": "s3://bucket/tables/customer",
            "accessModes": ["dir"],
        }
    )
    assert metadata.location == "s3://bucket/tables/customer"
    assert metadata.access_modes == ["dir"]


def test_metadata_access_modes_absent_defaults_none():
    metadata = Metadata.from_json(
        {"id": "id", "format": {}, "schemaString": "{}", "partitionColumns": []}
    )
    assert metadata.location is None
    assert metadata.access_modes is None
    assert metadata.auxiliary_locations is None


# ---------- to_object_store_options ----------


def _aws_creds(location="s3://bucket/p"):
    return TemporaryTableCredentials.from_json(
        {
            "credentials": {
                "location": location,
                "awsTempCredentials": {
                    "accessKeyId": "id",
                    "secretAccessKey": "secret",
                    "sessionToken": "token",
                },
            }
        }
    )


def test_to_object_store_options_aws(monkeypatch):
    monkeypatch.setattr(credentials, "_resolve_s3_region", lambda bucket: "us-west-2")
    options = to_object_store_options(_aws_creds())
    assert options == {
        "access_key_id": "id",
        "secret_access_key": "secret",
        "token": "token",
        "region": "us-west-2",
    }


def test_to_object_store_options_aws_region_unresolved(monkeypatch):
    monkeypatch.setattr(credentials, "_resolve_s3_region", lambda bucket: None)
    options = to_object_store_options(_aws_creds())
    assert "region" not in options and options["access_key_id"] == "id"


def test_to_object_store_options_r2_by_credentials():
    creds = TemporaryTableCredentials.from_json(
        {
            "credentials": {
                "location": "s3://bucket/p",
                "r2Credentials": {
                    "accessKeyId": "id",
                    "secretAccessKey": "secret",
                    "sessionToken": "token",
                },
            }
        }
    )
    options = to_object_store_options(creds)
    assert options["access_key_id"] == "id"
    assert options["region"] == "auto"
    assert "endpoint" in options


def test_to_object_store_options_r2_by_host():
    creds = _aws_creds(location="s3://bucket/p")
    options = to_object_store_options(
        creds, location="https://acct.r2.cloudflarestorage.com/bucket/p"
    )
    assert options["endpoint"] == "https://acct.r2.cloudflarestorage.com"
    assert options["region"] == "auto"


def test_to_object_store_options_azure():
    creds = TemporaryTableCredentials.from_json(
        {
            "credentials": {
                "location": "abfss://container@myacct.dfs.core.windows.net/p",
                "azureUserDelegationSas": {"sasToken": "sas"},
            }
        }
    )
    options = to_object_store_options(creds)
    assert options["azure_storage_sas_token"] == "sas"
    assert options["account_name"] == "myacct"


def test_to_object_store_options_gcs_bearer_token():
    creds = TemporaryTableCredentials.from_json(
        {"credentials": {"location": "gs://b/p", "gcpOauthToken": {"oauthToken": "tok"}}}
    )
    assert to_object_store_options(creds) == {GCS_BEARER_TOKEN_OPTION: "tok"}


def test_to_object_store_options_unknown_scheme():
    creds = _aws_creds(location="ftp://host/p")
    with pytest.raises(UnsupportedCloudError):
        to_object_store_options(creds)


def test_to_object_store_options_requires_location():
    creds = TemporaryTableCredentials.from_json(
        {
            "credentials": {
                "awsTempCredentials": {
                    "accessKeyId": "i",
                    "secretAccessKey": "s",
                    "sessionToken": "t",
                }
            }
        }
    )
    with pytest.raises(ValueError):
        to_object_store_options(creds)


# ---------- is_dir_readable_scheme ----------


@pytest.mark.parametrize(
    "location,expected",
    [
        ("s3://b/p", True),
        ("s3a://b/p", True),
        ("abfss://c@a.dfs.core.windows.net/p", True),
        ("wasbs://c@a.blob.core.windows.net/p", True),
        ("gs://b/p", True),
        ("file:///tmp/p", False),
        (None, False),
        ("", False),
    ],
)
def test_is_dir_readable_scheme(location, expected):
    assert is_dir_readable_scheme(location) is expected


# ---------- choose_access_mode ----------

S3 = "s3://bucket/table"


def test_choose_auto_url_only_picks_url():
    assert (
        choose_access_mode(None, ["url"], S3, version_requested=False, location_readable=True)
        == URL_ACCESS_MODE
    )


def test_choose_auto_omitted_picks_url():
    assert (
        choose_access_mode(None, None, S3, version_requested=False, location_readable=True)
        == URL_ACCESS_MODE
    )


def test_choose_auto_both_prefers_dir_when_readable():
    assert (
        choose_access_mode(
            None, ["url", "dir"], S3, version_requested=False, location_readable=True
        )
        == DIR_ACCESS_MODE
    )


def test_choose_auto_both_falls_back_to_url_when_unreadable():
    assert (
        choose_access_mode(
            None, ["url", "dir"], "file:///t", version_requested=False, location_readable=False
        )
        == URL_ACCESS_MODE
    )


def test_choose_auto_both_falls_back_to_url_for_time_travel():
    assert (
        choose_access_mode(None, ["url", "dir"], S3, version_requested=True, location_readable=True)
        == URL_ACCESS_MODE
    )


def test_choose_auto_dir_only_uses_dir():
    assert (
        choose_access_mode(None, ["dir"], S3, version_requested=False, location_readable=True)
        == DIR_ACCESS_MODE
    )


def test_choose_auto_dir_only_unreadable_raises():
    with pytest.raises(UnsupportedCloudError):
        choose_access_mode(
            None, ["dir"], "file:///t", version_requested=False, location_readable=False
        )


def test_choose_auto_dir_only_time_travel_raises():
    with pytest.raises(NotImplementedError):
        choose_access_mode(None, ["dir"], S3, version_requested=True, location_readable=True)


def test_choose_explicit_dir_when_offered():
    assert (
        choose_access_mode(
            DIR_ACCESS_MODE, ["url", "dir"], S3, version_requested=False, location_readable=True
        )
        == DIR_ACCESS_MODE
    )


def test_choose_explicit_dir_when_not_offered_raises():
    with pytest.raises(ValueError):
        choose_access_mode(
            DIR_ACCESS_MODE, ["url"], S3, version_requested=False, location_readable=True
        )


def test_choose_explicit_dir_without_location_raises():
    with pytest.raises(ValueError):
        choose_access_mode(
            DIR_ACCESS_MODE, ["dir"], None, version_requested=False, location_readable=False
        )


def test_choose_explicit_url_when_dir_only_raises():
    with pytest.raises(ValueError):
        choose_access_mode(
            URL_ACCESS_MODE, ["dir"], S3, version_requested=False, location_readable=True
        )


# ---------- TemporaryTableCredentialsProvider ----------


class _FakeRestClient:
    def __init__(self, expiration_time):
        self.calls = 0
        self._expiration_time = expiration_time

    def get_temporary_table_credentials(self, table, location=None):
        self.calls += 1
        return TemporaryTableCredentials.from_json(
            {
                "credentials": {
                    "location": location or "s3://b/t",
                    "expirationTime": self._expiration_time,
                    "awsTempCredentials": {
                        "accessKeyId": f"id-{self.calls}",
                        "secretAccessKey": "s",
                        "sessionToken": "t",
                    },
                }
            }
        )


_TABLE = Table(name="t", share="s", schema="sc")


def test_provider_caches_until_near_expiry(monkeypatch):
    monkeypatch.setattr(credentials.time, "time", lambda: 1000.0)
    rest = _FakeRestClient(expiration_time=10_000_000)
    provider = TemporaryTableCredentialsProvider(rest, _TABLE, refresh_skew_seconds=60.0)
    first = provider.get("s3://b/t")
    second = provider.get("s3://b/t")
    assert rest.calls == 1
    assert first is second


def test_provider_refreshes_when_within_skew(monkeypatch):
    now_ms = 1_000_000
    monkeypatch.setattr(credentials.time, "time", lambda: now_ms / 1000.0)
    rest = _FakeRestClient(expiration_time=now_ms + 30_000)
    provider = TemporaryTableCredentialsProvider(rest, _TABLE, refresh_skew_seconds=60.0)
    provider.get("s3://b/t")
    provider.get("s3://b/t")
    assert rest.calls == 2


def test_provider_force_refresh(monkeypatch):
    monkeypatch.setattr(credentials.time, "time", lambda: 1000.0)
    rest = _FakeRestClient(expiration_time=10_000_000)
    provider = TemporaryTableCredentialsProvider(rest, _TABLE)
    provider.get("s3://b/t")
    provider.get("s3://b/t", force_refresh=True)
    assert rest.calls == 2


def test_provider_no_expiration_is_cached(monkeypatch):
    monkeypatch.setattr(credentials.time, "time", lambda: 1000.0)
    rest = _FakeRestClient(expiration_time=None)
    provider = TemporaryTableCredentialsProvider(rest, _TABLE)
    provider.get("s3://b/t")
    provider.get("s3://b/t")
    assert rest.calls == 1


def test_provider_separate_cache_per_location(monkeypatch):
    monkeypatch.setattr(credentials.time, "time", lambda: 1000.0)
    rest = _FakeRestClient(expiration_time=10_000_000)
    provider = TemporaryTableCredentialsProvider(rest, _TABLE)
    provider.get("s3://b/t1")
    provider.get("s3://b/t2")
    assert rest.calls == 2
