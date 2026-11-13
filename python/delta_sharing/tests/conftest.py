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
from pathlib import Path
import subprocess
import threading
from typing import Iterator, Optional

import pytest
from pytest import TempPathFactory

from delta_sharing.delta_sharing import SharingClient
from delta_sharing.protocol import DeltaSharingProfile
from delta_sharing.rest_client import DataSharingRestClient

ENABLE_INTEGRATION = len(os.environ.get("AWS_ACCESS_KEY_ID", "")) > 0
SKIP_MESSAGE = "The integration tests are disabled."

# Must match opensharing-test-server TestDeltaSharingServer.SERVER_READY_MARKER
SERVER_READY_MARKER = b"DELTA_SHARING_TEST_SERVER_READY"

_integration_profile_path: Optional[Path] = None


@pytest.fixture
def profile_path() -> str:
    if ENABLE_INTEGRATION and _integration_profile_path is not None:
        return str(_integration_profile_path)
    return os.path.join(os.path.dirname(__file__), "test_profile.json")


@pytest.fixture
def profile(profile_path) -> DeltaSharingProfile:
    return DeltaSharingProfile.read_from_file(profile_path)


@pytest.fixture
def rest_client(profile) -> DataSharingRestClient:
    return DataSharingRestClient(profile)


@pytest.fixture
def sharing_client(profile) -> SharingClient:
    return SharingClient(profile)


@pytest.fixture(scope="session", autouse=ENABLE_INTEGRATION)
def test_server(tmp_path_factory: TempPathFactory) -> Iterator[None]:
    global _integration_profile_path
    pid_file: Optional[Path] = None
    proc: Optional[subprocess.Popen] = None
    try:
        if ENABLE_INTEGRATION:
            basetemp = tmp_path_factory.getbasetemp()
            pid_file = basetemp / "delta-sharing-server.pid"
            profile_file = basetemp / "opensharing.share"
            env = os.environ.copy()
            env.setdefault("AWS_REGION", "us-west-2")
            env.setdefault("AWS_DEFAULT_REGION", "us-west-2")
            proc = subprocess.Popen(
                [
                    "./build/sbt",
                    (
                        "opensharingTestServer/runMain "
                        "io.delta.sharing.server.TestDeltaSharingServer "
                        + str(pid_file)
                        + " "
                        + str(profile_file)
                    ),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd="..",
                env=env,
            )

            ready = threading.Event()

            def wait_for_server() -> None:
                assert proc is not None and proc.stdout is not None
                for line in proc.stdout:
                    print(line.decode("utf-8").strip())
                    if SERVER_READY_MARKER in line:
                        ready.set()

            threading.Thread(target=wait_for_server, daemon=True).start()

            if not ready.wait(timeout=180):
                raise TimeoutError("the server didn't start in 180 seconds")
            if not profile_file.exists() or profile_file.stat().st_size == 0:
                raise RuntimeError(
                    f"OpenSharing did not write a recipient profile to {profile_file}"
                )
            _integration_profile_path = profile_file
        yield
    finally:
        if ENABLE_INTEGRATION:
            if pid_file is not None and pid_file.exists():
                pid = pid_file.read_text().strip()
                if pid.isdigit():
                    subprocess.run(["kill", "-9", pid], check=False)
            if proc is not None and proc.poll() is None:
                proc.kill()
