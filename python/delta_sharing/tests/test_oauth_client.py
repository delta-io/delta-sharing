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


class MockServer:
    def __init__(self):
        self.url = "http://localhost:1080/token"
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


def test_oauth_client_should_parse_token_response_correctly(mock_server):
    mock_server.add_response(
        200,
        '{"access_token": "test-access-token", "expires_in": 3600, "token_type": "bearer"}')

    with patch('requests.post') as mock_post:
        mock_post.side_effect = lambda *args, **kwargs: mock_server.get_response()
        oauth_client = OAuthClient(
            token_endpoint=mock_server.url,
            client_id="client-id",
            client_secret="client-secret"
        )

        start = datetime.now().timestamp()
        token = oauth_client.client_credentials()
        end = datetime.now().timestamp()

        assert token.access_token == "test-access-token"
        assert token.expires_in == 3600
        assert int(start) <= token.creation_timestamp
        assert token.creation_timestamp <= int(end)


def test_oauth_client_should_handle_401_unauthorized_response(mock_server):
    mock_server.add_response(401, 'Unauthorized')

    with patch('requests.post') as mock_post:
        mock_post.side_effect = lambda *args, **kwargs: mock_server.get_response()
        oauth_client = OAuthClient(
            token_endpoint=mock_server.url,
            client_id="client-id",
            client_secret="client-secret"
        )
        try:
            oauth_client.client_credentials()
        except requests.HTTPError as e:
            assert e.response.status_code == 401
