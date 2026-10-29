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
import json
import os
from pathlib import Path
import subprocess
import threading
import time
from typing import Iterator, Optional

import pytest
from pytest import TempPathFactory

from delta_sharing.delta_sharing import SharingClient
from delta_sharing.protocol import DeltaSharingProfile
from delta_sharing.rest_client import DataSharingRestClient

ENABLE_INTEGRATION = len(os.environ.get("AWS_ACCESS_KEY_ID", "")) > 0
SKIP_MESSAGE = "The integration tests are disabled."


@pytest.fixture
def profile_path() -> str:
    return os.path.join(os.path.dirname(__file__), "test_profile.json")


@pytest.fixture(scope="session")
def view_server_endpoint(tmp_path_factory: TempPathFactory) -> Iterator[str]:
    server_dir = tmp_path_factory.mktemp("view-sharing-server")
    ready_file = server_dir / "ready"
    stop_file = server_dir / "stop"
    log_file = server_dir / "server.log"
    repo_root = Path(__file__).resolve().parents[3]
    with log_file.open("w") as server_log:
        proc = subprocess.Popen(
            [
                "./build/sbt",
                "-batch",
                "server / Test / runMain io.delta.sharing.server.TestViewSharingServer "
                f"{ready_file} {stop_file}",
            ],
            stdout=server_log,
            stderr=subprocess.STDOUT,
            cwd=repo_root,
        )
        try:
            deadline = time.monotonic() + 180
            while not ready_file.exists():
                if proc.poll() is not None or time.monotonic() >= deadline:
                    pytest.fail(f"View sharing server failed to start:\n{log_file.read_text()}")
                time.sleep(0.1)
            yield ready_file.read_text()
        finally:
            stop_file.touch()
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=10)


@pytest.fixture
def view_profile_path(tmp_path: Path, view_server_endpoint: str) -> str:
    profile = tmp_path / "view.share"
    profile.write_text(
        json.dumps(
            {
                "shareCredentialsVersion": 1,
                "endpoint": view_server_endpoint,
                "bearerToken": "token",
            }
        )
    )
    return str(profile)


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
    pid_file: Optional[Path] = None
    proc: Optional[subprocess.Popen] = None
    try:
        if ENABLE_INTEGRATION:
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
                stderr=subprocess.PIPE,
                cwd="..",
            )

            ready = threading.Event()

            def wait_for_server() -> None:
                for line in proc.stdout:
                    print(line.decode("utf-8").strip())
                    if b"https://127.0.0.1:12345/" in line:
                        ready.set()

            threading.Thread(target=wait_for_server, daemon=True).start()

            if not ready.wait(timeout=120):
                raise TimeoutError("the server didn't start in 120 seconds")
        yield
    finally:
        if ENABLE_INTEGRATION:
            if pid_file is not None and pid_file.exists():
                pid = pid_file.read_text()
                subprocess.run(["kill", "-9", pid])
            if proc is not None and proc.poll() is None:
                proc.kill()
