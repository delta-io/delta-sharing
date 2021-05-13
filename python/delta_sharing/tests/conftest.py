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
import os

import pytest

from delta_sharing.delta_sharing import SharingClient
from delta_sharing.protocol import ShareProfile
from delta_sharing.rest_client import DataSharingRestClient


# Make it `True` if the server is running.
ENABLE_INTEGRATION = False
SKIP_MESSAGE = "The integration tests are disabled."


@pytest.fixture
def profile_path() -> str:
    return os.path.join(os.path.dirname(__file__), "test_profile.json")


@pytest.fixture
def profile(profile_path) -> ShareProfile:
    return ShareProfile.read_from_file(profile_path)


@pytest.fixture
def rest_client(profile) -> DataSharingRestClient:
    return DataSharingRestClient(profile)


@pytest.fixture
def sharing_client(profile) -> SharingClient:
    return SharingClient(profile)
