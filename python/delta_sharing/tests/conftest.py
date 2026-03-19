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
import socket
import time
from pathlib import Path
import subprocess
import threading
from typing import Iterator, Optional

import pytest
from pytest import TempPathFactory

from delta_sharing.delta_sharing import SharingClient
from delta_sharing.protocol import DeltaSharingProfile
from delta_sharing.rest_client import DataSharingRestClient


# Port the test server listens on (must match TestResource.TEST_PORT).
TEST_SERVER_PORT = 12345
ENABLE_INTEGRATION = len(os.environ.get("AWS_ACCESS_KEY_ID", "")) > 0
SKIP_MESSAGE = "The integration tests are disabled."


def _repo_root() -> Path:
    """Return the repository root (directory containing build.sbt)."""
    path = Path(__file__).resolve().parent
    for _ in range(5):
        if (path / "build.sbt").exists():
            return path
        path = path.parent
    return Path.cwd()


def _wait_for_port(port: int, timeout_seconds: float = 30.0) -> None:
    """Wait until the server accepts TCP connections on the given port (IPv4 and/or localhost)."""
    deadline = time.monotonic() + timeout_seconds
    hosts = ("127.0.0.1", "localhost")
    while time.monotonic() < deadline:
        for host in hosts:
            try:
                with socket.create_connection((host, port), timeout=2):
                    return
            except (socket.error, OSError):
                pass
        time.sleep(0.5)
    raise TimeoutError(
        f"Server did not accept connections on port {port} within {timeout_seconds}s "
        f"(tried {hosts})"
    )


@pytest.fixture
def profile_path() -> str:
    return os.path.join(os.path.dirname(__file__), "test_profile.json")


@pytest.fixture
def profile(profile_path) -> DeltaSharingProfile:
    return DeltaSharingProfile.read_from_file(profile_path)


@pytest.fixture
def rest_client(profile, test_server) -> DataSharingRestClient:
    return DataSharingRestClient(profile)


@pytest.fixture
def sharing_client(profile, test_server) -> SharingClient:
    return SharingClient(profile)


@pytest.fixture(scope="session", autouse=True)
def test_server(tmp_path_factory: TempPathFactory) -> Iterator[None]:
    pid_file: Optional[Path] = None
    proc: Optional[subprocess.Popen] = None
    try:
        if ENABLE_INTEGRATION:
            repo_root = _repo_root()
            pid_file = tmp_path_factory.getbasetemp() / "delta-sharing-server.pid"
            proc = subprocess.Popen(
                [
                    "./build/sbt",
                    (
                        "server/test:runMain io.delta.sharing.server.TestDeltaSharingServer "
                        + str(pid_file)
                    ),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=str(repo_root),
            )

            def drain_server_output() -> None:
                assert proc.stdout is not None
                for line in proc.stdout:
                    print(line.decode("utf-8", errors="replace").strip())

            threading.Thread(target=drain_server_output, daemon=True).start()

            # Do not wait for a specific log line: JVM/sbt output can be buffered differently
            # across Python/OS versions, causing flaky timeouts. Wait until the port accepts
            # connections (with a generous budget for cold CI: compile + server start).
            start_timeout = float(os.environ.get("DELTA_SHARING_SERVER_START_TIMEOUT", "240"))
            _wait_for_port(TEST_SERVER_PORT, timeout_seconds=start_timeout)
            if proc.poll() is not None:
                raise RuntimeError(
                    f"Delta Sharing server process exited early with code {proc.poll()}"
                )
        yield
    finally:
        if ENABLE_INTEGRATION:
            if pid_file is not None and pid_file.exists():
                pid = pid_file.read_text()
                subprocess.run(["kill", "-9", pid])
            if proc is not None and proc.poll() is None:
                proc.kill()
