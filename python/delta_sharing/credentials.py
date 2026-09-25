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
"""Helpers for Delta Sharing directory-based ("dir") access.

This module turns the short-lived credentials returned by the ``temporary-table-credentials``
endpoint into the configuration delta-kernel needs to read a table directly from object storage,
implements the access-mode negotiation described in PROTOCOL.md, and caches credentials so they are
refreshed before they expire.
"""

import time
from typing import Dict, Optional, Sequence
from urllib.parse import urlparse

from delta_sharing.protocol import TemporaryTableCredentials

# Access modes from the Delta Sharing protocol (table metadata `accessModes`).
URL_ACCESS_MODE = "url"
DIR_ACCESS_MODE = "dir"

# Location URL schemes for which we can build a credentialed `object_store` for the kernel reader.
_AWS_SCHEMES = frozenset({"s3", "s3a", "s3n"})
_AZURE_SCHEMES = frozenset({"abfs", "abfss", "az", "wasb", "wasbs"})
_GCP_SCHEMES = frozenset({"gs", "gcs"})

# Synthetic storage option carrying a raw GCS OAuth bearer token. `object_store` has no string config
# key for it, so the kernel wrapper recognizes this key and builds the GCS store with a credential
# provider. Must match GCS_BEARER_TOKEN_OPTION in delta-kernel-rust-sharing-wrapper/src/lib.rs.
GCS_BEARER_TOKEN_OPTION = "google_bearer_token"


class UnsupportedCloudError(Exception):
    """Raised when directory-based access is requested for a cloud/credential combination that the
    kernel object-store backend cannot read directly."""


def _host(location: str) -> str:
    return (urlparse(location).hostname or "").lower()


def _scheme(location: str) -> str:
    return (urlparse(location).scheme or "").lower()


def _is_r2(location: str, creds: TemporaryTableCredentials) -> bool:
    return creds.r2_credentials is not None or _host(location).endswith("r2.cloudflarestorage.com")


def _azure_account_name(location: str) -> Optional[str]:
    # abfss://container@account.dfs.core.windows.net/path -> account
    host = _host(location)
    return host.split(".")[0] if host else None


_s3_region_cache: Dict[str, Optional[str]] = {}


def _resolve_s3_region(bucket: str) -> Optional[str]:
    """Resolve an S3 bucket's region. The temporary-table-credentials response does not include
    the region, but `object_store` requires it (it defaults to us-east-1 and fails for buckets
    elsewhere). An unauthenticated HEAD to the bucket's virtual-hosted endpoint returns the region
    in the `x-amz-bucket-region` header. Cached per bucket."""
    if bucket in _s3_region_cache:
        return _s3_region_cache[bucket]
    region = None
    try:
        import requests

        resp = requests.head(
            f"https://{bucket}.s3.amazonaws.com", timeout=10, allow_redirects=False
        )
        region = resp.headers.get("x-amz-bucket-region")
    except Exception:
        region = None
    _s3_region_cache[bucket] = region
    return region


def is_dir_readable_scheme(location: Optional[str]) -> bool:
    """Whether the connector can build a credentialed object-store for `location`'s cloud.

    This is a cheap, credential-free check used during access-mode negotiation, before any
    credentials are fetched.
    """
    if not location:
        return False
    scheme = _scheme(location)
    return scheme in _AWS_SCHEMES or scheme in _AZURE_SCHEMES or scheme in _GCP_SCHEMES


def to_object_store_options(
    creds: TemporaryTableCredentials, location: Optional[str] = None
) -> Dict[str, str]:
    """Map temporary credentials to ``object_store`` configuration keys for delta-kernel.

    The returned dict is passed to
    ``delta_kernel_rust_sharing_wrapper.PythonInterface(location, storage_options)`` and consumed by
    ``object_store::parse_url_opts`` inside the wrapper.
    """
    location = location or creds.location
    if location is None:
        raise ValueError("A storage location is required to build object-store options")
    scheme = _scheme(location)

    # R2 is S3-compatible: same keys, plus a custom endpoint. Check this before the generic S3 path
    # so R2 buckets exposed under an `s3://` scheme still get the endpoint override.
    if _is_r2(location, creds):
        r2 = creds.r2_credentials or creds.aws_temp_credentials
        if r2 is None:
            raise UnsupportedCloudError(
                "R2 location returned without R2 or AWS-compatible credentials."
            )
        return {
            "access_key_id": r2.access_key_id,
            "secret_access_key": r2.secret_access_key,
            "token": r2.session_token,
            "endpoint": f"https://{_host(location)}",
            "region": "auto",
            "virtual_hosted_style_request": "false",
        }

    if scheme in _AWS_SCHEMES and creds.aws_temp_credentials is not None:
        aws = creds.aws_temp_credentials
        options = {
            "access_key_id": aws.access_key_id,
            "secret_access_key": aws.secret_access_key,
            "token": aws.session_token,
        }
        region = _resolve_s3_region(_host(location))
        if region:
            options["region"] = region
        return options

    if scheme in _AZURE_SCHEMES and creds.azure_user_delegation_sas is not None:
        options = {"azure_storage_sas_token": creds.azure_user_delegation_sas.sas_token}
        account = _azure_account_name(location)
        if account is not None:
            options["account_name"] = account
        return options

    if scheme in _GCP_SCHEMES and creds.gcp_oauth_token is not None:
        return {GCS_BEARER_TOKEN_OPTION: creds.gcp_oauth_token.oauth_token}

    raise UnsupportedCloudError(
        f"Cannot build object-store credentials for location scheme '{scheme}' from the returned "
        f"temporary credentials."
    )


def choose_access_mode(
    requested: Optional[str],
    access_modes: Optional[Sequence[str]],
    location: Optional[str],
    *,
    version_requested: bool,
    location_readable: bool,
) -> str:
    """Decide ``url`` vs ``dir`` per the protocol's Access Modes compatibility table.

    :param requested: the caller's explicit preference (``"url"``, ``"dir"``) or ``None`` for auto.
    :param access_modes: the ``accessModes`` array from table metadata (``None``/empty => URL-only).
    :param location: the table ``location`` from metadata.
    :param version_requested: whether a version/timestamp was requested (dir mode cannot time-travel
        yet, since the kernel wrapper does not expose snapshot-at-version).
    :param location_readable: whether the connector can build a credentialed store for ``location``.
    :returns: ``DIR_ACCESS_MODE`` or ``URL_ACCESS_MODE``.
    :raises ValueError / NotImplementedError / UnsupportedCloudError: when the request cannot be met.
    """
    modes = [m.lower() for m in access_modes] if access_modes else []
    server_dir = DIR_ACCESS_MODE in modes
    server_url = (URL_ACCESS_MODE in modes) or (not modes)  # omitted => URL-only

    def _require_dir_usable() -> None:
        if location is None:
            raise ValueError(
                "Server offers directory-based access but returned no table 'location'."
            )
        if version_requested:
            raise NotImplementedError(
                "Directory-based access does not yet support version/timestamp time travel in the "
                "Python connector; use access_mode='url'."
            )
        if not location_readable:
            raise UnsupportedCloudError(
                f"Directory-based access is required for location '{location}', but the connector "
                f"cannot read that cloud directly yet; use access_mode='url' if the server offers it."
            )

    if requested == DIR_ACCESS_MODE:
        if not server_dir:
            raise ValueError(
                "access_mode='dir' was requested but the server does not offer directory-based "
                "access for this table."
            )
        _require_dir_usable()
        return DIR_ACCESS_MODE

    if requested == URL_ACCESS_MODE:
        if not server_url:
            raise ValueError(
                "access_mode='url' was requested but the server only offers directory-based access "
                "for this table."
            )
        return URL_ACCESS_MODE

    # Auto (requested is None).
    if server_dir and not server_url:
        # Directory-only table: the client must use dir or fail.
        _require_dir_usable()
        return DIR_ACCESS_MODE
    if server_dir and server_url and not version_requested and location_readable:
        return DIR_ACCESS_MODE
    return URL_ACCESS_MODE


class TemporaryTableCredentialsProvider:
    """Fetches and caches short-lived directory-access credentials for a single table.

    Credentials are cached per requested location and refreshed once the cached value is within
    ``refresh_skew_seconds`` of its ``expiration_time``. The same provider can be shared across reads
    of one table (e.g. a long-running scan) so credentials are reused until they near expiry.
    """

    def __init__(self, rest_client, table, refresh_skew_seconds: float = 60.0):
        self._rest_client = rest_client
        self._table = table
        self._refresh_skew_ms = int(refresh_skew_seconds * 1000)
        self._cache: Dict[Optional[str], TemporaryTableCredentials] = {}

    def get(
        self, location: Optional[str] = None, force_refresh: bool = False
    ) -> TemporaryTableCredentials:
        cached = self._cache.get(location)
        if not force_refresh and cached is not None and not self._is_expiring(cached):
            return cached
        creds = self._rest_client.get_temporary_table_credentials(self._table, location)
        self._cache[location] = creds
        return creds

    def _is_expiring(self, creds: TemporaryTableCredentials) -> bool:
        if creds.expiration_time is None:
            return False
        now_ms = int(time.time() * 1000)
        return now_ms >= (creds.expiration_time - self._refresh_skew_ms)
