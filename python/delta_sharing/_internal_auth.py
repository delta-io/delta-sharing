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

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional
import requests
import base64
import json
import threading
import requests.sessions
import time
from typing import Dict

from delta_sharing.protocol import (
    DeltaSharingProfile,
)

# This module contains internal implementation classes.
# These classes are not part of the public API and should not be used directly by users.
# Internal classes may change or be removed at any time without notice.


class AuthConfig:
    def __init__(self, token_exchange_max_retries=5,
                 token_exchange_max_retry_duration_in_seconds=60,
                 token_renewal_threshold_in_seconds=600):
        self.token_exchange_max_retries = token_exchange_max_retries
        self.token_exchange_max_retry_duration_in_seconds = (
            token_exchange_max_retry_duration_in_seconds)
        self.token_renewal_threshold_in_seconds = token_renewal_threshold_in_seconds


class AuthCredentialProvider(ABC):
    @abstractmethod
    def add_auth_header(self, session: requests.Session) -> None:
        pass

    def is_expired(self) -> bool:
        return False

    @abstractmethod
    def get_expiration_time(self) -> Optional[str]:
        return None


class BearerTokenAuthProvider(AuthCredentialProvider):
    def __init__(self, bearer_token: str, expiration_time: Optional[str]):
        self.bearer_token = bearer_token
        self.expiration_time = expiration_time

    def add_auth_header(self, session: requests.Session) -> None:
        session.headers.update(
            {
                "Authorization": f"Bearer {self.bearer_token}",
            }
        )

    def is_expired(self) -> bool:
        if self.expiration_time is None:
            return False
        try:
            expiration_time_as_timestamp = datetime.fromisoformat(self.expiration_time)
            return expiration_time_as_timestamp < datetime.now()
        except ValueError:
            return False

    def get_expiration_time(self) -> Optional[str]:
        return self.expiration_time


class BasicAuthProvider(AuthCredentialProvider):
    def __init__(self, endpoint: str, username: str, password: str):
        self.username = username
        self.password = password
        self.endpoint = endpoint

    def add_auth_header(self, session: requests.Session) -> None:
        session.auth = (self.username, self.password)
        session.post(self.endpoint, data={"grant_type": "client_credentials"},)

    def is_expired(self) -> bool:
        return False

    def get_expiration_time(self) -> Optional[str]:
        return None


class OAuthClientCredentials:
    def __init__(self, access_token: str, expires_in: int, creation_timestamp: int):
        self.access_token = access_token
        self.expires_in = expires_in
        self.creation_timestamp = creation_timestamp


class OAuthClient:
    def __init__(self,
                 token_endpoint: str,
                 client_id: str,
                 client_secret: str,
                 scope: Optional[str] = None):
        self.token_endpoint = token_endpoint
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope

    def client_credentials(self) -> OAuthClientCredentials:
        credentials = base64.b64encode(
            f"{self.client_id}:{self.client_secret}".encode('utf-8')).decode('utf-8')
        headers = {
            'accept': 'application/json',
            'authorization': f'Basic {credentials}',
            'content-type': 'application/x-www-form-urlencoded'
        }
        body = f"grant_type=client_credentials{f'&scope={self.scope}' if self.scope else ''}"
        response = requests.post(self.token_endpoint, headers=headers, data=body)
        response.raise_for_status()
        return self.parse_oauth_token_response(response.text)

    def parse_oauth_token_response(self, response: str) -> OAuthClientCredentials:
        if not response:
            raise RuntimeError("Empty response from OAuth token endpoint")
        json_node = json.loads(response)
        if 'access_token' not in json_node or not isinstance(json_node['access_token'], str):
            raise RuntimeError("Missing 'access_token' field in OAuth token response")
        if 'expires_in' not in json_node or not isinstance(json_node['expires_in'], int):
            raise RuntimeError("Missing 'expires_in' field in OAuth token response")
        return OAuthClientCredentials(
            json_node['access_token'],
            json_node['expires_in'],
            int(datetime.now().timestamp())
        )


class OAuthClientCredentialsAuthProvider(AuthCredentialProvider):
    def __init__(self, oauth_client: OAuthClient, auth_config: AuthConfig = AuthConfig()):
        self.auth_config = auth_config
        self.oauth_client = oauth_client
        self.current_token: Optional[OAuthClientCredentials] = None
        self.lock = threading.RLock()

    def add_auth_header(self,session: requests.Session) -> None:
        token = self.maybe_refresh_token()
        with self.lock:
            session.headers.update(
                {
                    "Authorization": f"Bearer {token.access_token}",
                }
            )

    def maybe_refresh_token(self) -> OAuthClientCredentials:
        with self.lock:
            if self.current_token and not self.needs_refresh(self.current_token):
                return self.current_token
            new_token = self.oauth_client.client_credentials()
            self.current_token = new_token
            return new_token

    def needs_refresh(self, token: OAuthClientCredentials) -> bool:
        now = int(time.time())
        expiration_time = token.creation_timestamp + token.expires_in
        return expiration_time - now < self.auth_config.token_renewal_threshold_in_seconds

    def get_expiration_time(self) -> Optional[str]:
        return None


class AuthCredentialProviderFactory:
    __oauth_auth_provider_cache : Dict[
        DeltaSharingProfile,
        OAuthClientCredentialsAuthProvider] = {}

    @staticmethod
    def create_auth_credential_provider(profile: DeltaSharingProfile):
        if profile.share_credentials_version == 2:
            if profile.type == "oauth_client_credentials":
                return AuthCredentialProviderFactory.__oauth_client_credentials(profile)
            elif profile.type == "basic":
                return AuthCredentialProviderFactory.__auth_basic(profile)
        elif (profile.share_credentials_version == 1 and
              (profile.type is None or profile.type == "bearer_token")):
            return AuthCredentialProviderFactory.__auth_bearer_token(profile)

        # any other scenario is unsupported
        raise RuntimeError(f"unsupported profile.type: {profile.type}"
                           f" profile.share_credentials_version"
                           f" {profile.share_credentials_version}")

    @staticmethod
    def __oauth_client_credentials(profile):
        # Once a clientId/clientSecret is exchanged for an accessToken,
        # the accessToken can be reused until it expires.
        # The Python client re-creates DeltaSharingClient for different requests.
        # To ensure the OAuth access_token is reused,
        # we keep a mapping from profile -> OAuthClientCredentialsAuthProvider.
        # This prevents re-initializing OAuthClientCredentialsAuthProvider for the same profile,
        # ensuring the access_token can be reused.
        if profile in AuthCredentialProviderFactory.__oauth_auth_provider_cache:
            return AuthCredentialProviderFactory.__oauth_auth_provider_cache[profile]

        oauth_client = OAuthClient(
            token_endpoint=profile.token_endpoint,
            client_id=profile.client_id,
            client_secret=profile.client_secret,
            scope=profile.scope
        )
        provider = OAuthClientCredentialsAuthProvider(
            oauth_client=oauth_client,
            auth_config=AuthConfig()
        )
        AuthCredentialProviderFactory.__oauth_auth_provider_cache[profile] = provider
        return provider

    @staticmethod
    def __auth_bearer_token(profile):
        return BearerTokenAuthProvider(profile.bearer_token, profile.expiration_time)

    @staticmethod
    def __auth_basic(profile):
        return BasicAuthProvider(profile.endpoint, profile.username, profile.password)
