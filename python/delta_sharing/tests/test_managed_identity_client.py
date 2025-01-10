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
import requests
from requests.models import Response
from unittest.mock import patch
from datetime import datetime
from delta_sharing._internal_auth import OAuthClient

from datetime import datetime
from unittest.mock import patch

import pytest
from requests import Response
from delta_sharing._internal_auth import AzureManagedIdentityClient

class MockServer:
    def __init__(self):
        self.url = "http://169.254.169.254/metadata/identity/oauth2/token"
        self.responses = []

    def add_response(self, status_code, json_data):
        response = Response()
        response.status_code = status_code
        response._content = json_data.encode('utf-8')
        self.responses.append(response)

    def get_response(self):
        return self.responses.pop(0)


@pytest.fixture
def mock_server():
    server = MockServer()
    yield server


@pytest.mark.parametrize("response_data, expected_expires_in, expected_access_token", [
    (
            '{"access_token": "test-access-token", "expires_in": 3600, "token_type": "bearer"}',
            3600,
            "test-access-token"
    ),
    (
            '{"access_token": "test-access-token", "expires_in": "3600", "token_type": "bearer"}',
            3600,
            "test-access-token"
    )
])
def test_managed_identity_client_should_parse_token_response_correctly(mock_server,
                                                                       response_data,
                                                                       expected_expires_in,
                                                                       expected_access_token):
    mock_server.add_response(200, response_data)

    with patch('requests.get') as mock_get:
        mock_get.side_effect = lambda *args, **kwargs: mock_server.get_response()
        client = AzureManagedIdentityClient()

        start = datetime.now().timestamp()
        token = client.managed_identity_token()
        end = datetime.now().timestamp()

        assert token.access_token == expected_access_token
        assert token.expires_in == expected_expires_in
        assert int(start) <= token.creation_timestamp
        assert token.creation_timestamp <= int(end)


def test_managed_identity_client_should_handle_500_internal_server_error(mock_server):
    mock_server.add_response(500, 'Internal Server Error')

    with patch('requests.get') as mock_get:
        mock_get.side_effect = lambda *args, **kwargs: mock_server.get_response()
        client = AzureManagedIdentityClient()
        try:
            client.managed_identity_token()
        except Exception as e:
            assert e.response.status_code == 500