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

from unittest.mock import MagicMock
from datetime import datetime, timedelta
from delta_sharing._internal_auth import (
    ClientSecretOAuthClient,
    BasicAuthProvider,
    AuthCredentialProviderFactory,
    OAuthClientCredentialsAuthProvider,
    OAuthClientCredentials,
)
from requests import Session
import requests
from delta_sharing._internal_auth import BearerTokenAuthProvider
from delta_sharing.protocol import DeltaSharingProfile


def test_bearer_token_auth_provider_initialization():
    token = "test-token"
    expiration_time = "2021-11-12T00:12:29.0Z"
    provider = BearerTokenAuthProvider(token, expiration_time)
    assert provider.bearer_token == token
    assert provider.expiration_time == expiration_time


def test_bearer_token_auth_provider_add_auth_header():
    token = "test-token"
    provider = BearerTokenAuthProvider(token, None)
    session = requests.Session()
    provider.add_auth_header(session)
    assert session.headers["Authorization"] == f"Bearer {token}"


def test_bearer_token_auth_provider_is_expired():
    expired_token = "expired-token"
    expiration_time = (datetime.now() - timedelta(days=1)).isoformat()
    provider = BearerTokenAuthProvider(expired_token, expiration_time)
    assert provider.is_expired()

    valid_token = "valid-token"
    expiration_time = (datetime.now() + timedelta(days=1)).isoformat()
    provider = BearerTokenAuthProvider(valid_token, expiration_time)
    assert not provider.is_expired()


def test_bearer_token_auth_provider_get_expiration_time():
    token = "test-token"
    expiration_time = "2021-11-12T00:12:29.0Z"
    provider = BearerTokenAuthProvider(token, expiration_time)
    assert provider.get_expiration_time() == expiration_time

    provider = BearerTokenAuthProvider(token, None)
    assert provider.get_expiration_time() is None


def test_oauth_client_credentials_auth_provider_exchange_token():
    oauth_client = MagicMock(spec=ClientSecretOAuthClient)
    profile = MagicMock()
    profile.token_endpoint = "http://example.com/token"
    profile.client_id = "client-id"
    profile.client_secret = "client-secret"
    profile.scope = None

    provider = OAuthClientCredentialsAuthProvider(oauth_client)
    mock_session = MagicMock(spec=Session)
    mock_session.headers = MagicMock()

    token = OAuthClientCredentials("access-token", 3600, int(datetime.now().timestamp()))
    oauth_client.client_credentials.return_value = token

    provider.add_auth_header(mock_session)

    mock_session.headers.update.assert_called_once_with(
        {"Authorization": f"Bearer {token.access_token}"}
    )
    oauth_client.client_credentials.assert_called_once()


def test_oauth_client_credentials_auth_provider_reuse_token():
    oauth_client = MagicMock(spec=ClientSecretOAuthClient)
    profile = MagicMock()
    profile.token_endpoint = "http://example.com/token"
    profile.client_id = "client-id"
    profile.client_secret = "client-secret"
    profile.scope = None

    provider = OAuthClientCredentialsAuthProvider(oauth_client)
    mock_session = MagicMock(spec=Session)
    mock_session.headers = MagicMock()

    valid_token = OAuthClientCredentials("valid-token", 3600, int(datetime.now().timestamp()))
    provider.current_token = valid_token

    provider.add_auth_header(mock_session)

    mock_session.headers.update.assert_called_once_with(
        {"Authorization": f"Bearer {valid_token.access_token}"}
    )
    oauth_client.client_credentials.assert_not_called()


def test_oauth_client_credentials_auth_provider_refresh_token():
    oauth_client = MagicMock(spec=ClientSecretOAuthClient)
    profile = MagicMock()
    profile.token_endpoint = "http://example.com/token"
    profile.client_id = "client-id"
    profile.client_secret = "client-secret"
    profile.scope = None

    provider = OAuthClientCredentialsAuthProvider(oauth_client)
    mock_session = MagicMock(spec=Session)
    mock_session.headers = MagicMock()

    expired_token = OAuthClientCredentials(
        "expired-token", 1, int(datetime.now().timestamp()) - 3600
    )
    new_token = OAuthClientCredentials("new-token", 3600, int(datetime.now().timestamp()))
    provider.current_token = expired_token
    oauth_client.client_credentials.return_value = new_token

    provider.add_auth_header(mock_session)

    mock_session.headers.update.assert_called_once_with(
        {"Authorization": f"Bearer {new_token.access_token}"}
    )
    oauth_client.client_credentials.assert_called_once()


def test_oauth_client_credentials_auth_provider_needs_refresh():
    oauth_client = MagicMock(spec=ClientSecretOAuthClient)
    profile = MagicMock()
    profile.token_endpoint = "http://example.com/token"
    profile.client_id = "client-id"
    profile.client_secret = "client-secret"
    profile.scope = None

    provider = OAuthClientCredentialsAuthProvider(oauth_client)

    expired_token = OAuthClientCredentials(
        "expired-token", 1, int(datetime.now().timestamp()) - 3600
    )
    assert provider.needs_refresh(expired_token)

    token_expiring_soon = OAuthClientCredentials(
        "expiring-soon-token", 600 - 5, int(datetime.now().timestamp())
    )
    assert provider.needs_refresh(token_expiring_soon)

    valid_token = OAuthClientCredentials("valid-token", 600 + 10, int(datetime.now().timestamp()))
    assert not provider.needs_refresh(valid_token)


def test_oauth_client_credentials_auth_provider_is_expired():
    oauth_client = MagicMock(spec=ClientSecretOAuthClient)
    profile = MagicMock()
    profile.token_endpoint = "http://example.com/token"
    profile.client_id = "client-id"
    profile.client_secret = "client-secret"
    profile.scope = None

    provider = OAuthClientCredentialsAuthProvider(oauth_client)
    assert not provider.is_expired()


def test_oauth_client_credentials_auth_provider_get_expiration_time():
    oauth_client = MagicMock(spec=ClientSecretOAuthClient)
    profile = MagicMock()
    profile.token_endpoint = "http://example.com/token"
    profile.client_id = "client-id"
    profile.client_secret = "client-secret"
    profile.scope = None

    provider = OAuthClientCredentialsAuthProvider(oauth_client)
    assert provider.get_expiration_time() is None


def test_basic_auth_provider_initialization():
    provider = BasicAuthProvider("https://localhost", "username", "password")
    assert provider.username == "username"
    assert provider.password == "password"


def test_basic_auth_provider_add_auth_header():
    provider = BasicAuthProvider("https://localhost", "username", "password")
    session = MagicMock(spec=requests.Session)
    session.headers = MagicMock()
    session.auth = MagicMock()
    provider.add_auth_header(session)
    session.post("https://localhost/delta-sharing/", data={"grant_type": "client_credentials"})
    assert session.auth == ("username", "password")


def test_basic_auth_provider_is_expired():
    provider = BasicAuthProvider("https://localhost", "username", "password")
    assert not provider.is_expired()


def test_basic_auth_provider_get_expiration_time():
    provider = BasicAuthProvider("https://localhost", "username", "password")
    assert provider.get_expiration_time() is None


def test_factory_creation():
    profile_basic = DeltaSharingProfile(
        share_credentials_version=2,
        type="basic",
        endpoint="https://localhost/delta-sharing/",
        username="username",
        password="password",
    )
    provider = AuthCredentialProviderFactory.create_auth_credential_provider(profile_basic)
    assert isinstance(provider, BasicAuthProvider)

    profile_bearer = DeltaSharingProfile(
        share_credentials_version=1,
        type="bearer_token",
        endpoint="https://localhost/delta-sharing/",
        bearer_token="token",
        expiration_time=(datetime.now() + timedelta(hours=1)).isoformat(),
    )
    provider = AuthCredentialProviderFactory.create_auth_credential_provider(profile_bearer)
    assert isinstance(provider, BearerTokenAuthProvider)

    profile_oauth = DeltaSharingProfile(
        share_credentials_version=2,
        type="oauth_client_credentials",
        endpoint="https://localhost/delta-sharing/",
        token_endpoint="https://localhost/token",
        client_id="clientId",
        client_secret="clientSecret",
    )
    provider = AuthCredentialProviderFactory.create_auth_credential_provider(profile_oauth)
    assert isinstance(provider, OAuthClientCredentialsAuthProvider)

    profile_pk = DeltaSharingProfile(
        share_credentials_version=2,
        type="oauth_client_private_key",
        endpoint="https://localhost/delta-sharing/",
        token_endpoint="https://localhost/token",
        client_id="clientId",
        private_key="privateKey",
        key_id="keyId",
        issuer="issuer",
        scope="scope",
        audience="audience",
    )
    provider = AuthCredentialProviderFactory.create_auth_credential_provider(profile_pk)
    assert isinstance(provider, OAuthClientCredentialsAuthProvider)


def test_oauth_auth_provider_reused():
    profile_oauth1 = DeltaSharingProfile(
        share_credentials_version=2,
        type="oauth_client_credentials",
        endpoint="https://localhost/delta-sharing/",
        token_endpoint="https://localhost/token",
        client_id="clientId",
        client_secret="clientSecret",
    )
    provider1 = AuthCredentialProviderFactory.create_auth_credential_provider(profile_oauth1)
    assert isinstance(provider1, OAuthClientCredentialsAuthProvider)

    profile_oauth2 = DeltaSharingProfile(
        share_credentials_version=2,
        type="oauth_client_credentials",
        endpoint="https://localhost/delta-sharing/",
        token_endpoint="https://localhost/token",
        client_id="clientId",
        client_secret="clientSecret",
    )

    provider2 = AuthCredentialProviderFactory.create_auth_credential_provider(profile_oauth2)

    assert provider1 == provider2


def test_oauth_auth_provider_with_different_profiles():
    profile_oauth1 = DeltaSharingProfile(
        share_credentials_version=2,
        type="oauth_client_credentials",
        endpoint="https://localhost/delta-sharing/",
        token_endpoint="https://localhost/1/token",
        client_id="clientId",
        client_secret="clientSecret",
    )
    provider1 = AuthCredentialProviderFactory.create_auth_credential_provider(profile_oauth1)
    assert isinstance(provider1, OAuthClientCredentialsAuthProvider)

    profile_oauth2 = DeltaSharingProfile(
        share_credentials_version=2,
        type="oauth_client_credentials",
        endpoint="https://localhost/delta-sharing/",
        token_endpoint="https://localhost/2/token",
        client_id="clientId",
        client_secret="clientSecret",
    )

    provider2 = AuthCredentialProviderFactory.create_auth_credential_provider(profile_oauth2)

    assert provider1 != provider2


def test_oauth_private_key_auth_provider_reused():
    profile_pk1 = DeltaSharingProfile(
        share_credentials_version=2,
        type="oauth_client_private_key",
        endpoint="https://localhost/delta-sharing/",
        token_endpoint="https://localhost/token",
        client_id="clientId",
        private_key="privateKey",
        key_id="keyId",
        issuer="issuer",
        scope="scope",
        audience="audience",
    )
    provider1 = AuthCredentialProviderFactory.create_auth_credential_provider(profile_pk1)
    assert isinstance(provider1, OAuthClientCredentialsAuthProvider)

    profile_pk2 = DeltaSharingProfile(
        share_credentials_version=2,
        type="oauth_client_private_key",
        endpoint="https://localhost/delta-sharing/",
        token_endpoint="https://localhost/token",
        client_id="clientId",
        private_key="privateKey",
        key_id="keyId",
        issuer="issuer",
        scope="scope",
        audience="audience",
    )
    provider2 = AuthCredentialProviderFactory.create_auth_credential_provider(profile_pk2)
    assert provider1 == provider2


def test_oauth_private_key_auth_provider_with_different_profiles():
    profile_pk1 = DeltaSharingProfile(
        share_credentials_version=2,
        type="oauth_client_private_key",
        endpoint="https://localhost/delta-sharing/",
        token_endpoint="https://localhost/1/token",
        client_id="clientId",
        private_key="privateKey",
        key_id="keyId",
        issuer="issuer",
        scope="scope",
        audience="audience",
    )
    provider1 = AuthCredentialProviderFactory.create_auth_credential_provider(profile_pk1)
    assert isinstance(provider1, OAuthClientCredentialsAuthProvider)

    profile_pk2 = DeltaSharingProfile(
        share_credentials_version=2,
        type="oauth_client_private_key",
        endpoint="https://localhost/delta-sharing/",
        token_endpoint="https://localhost/2/token",
        client_id="clientId",
        private_key="privateKey",
        key_id="keyId",
        issuer="issuer",
        scope="scope",
        audience="audience",
    )
    provider2 = AuthCredentialProviderFactory.create_auth_credential_provider(profile_pk2)
    assert provider1 != provider2
