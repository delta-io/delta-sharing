#!/usr/bin/env python3
"""Stage the cloud test secrets for the trusted ``bot-test`` launcher.

Run only from the default-branch control checkout, in the single workflow step
that receives these secrets. ``bot-test`` forwards only the names in
``FORWARDED`` to the integration tests it starts.
"""
from __future__ import annotations

import os
from pathlib import Path
import sys

SECRETS = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AZURE_TEST_ACCOUNT_KEY",
           "GOOGLE_SERVICE_ACCOUNT_KEY")
# Everything bot-test may forward from the file. Keep in sync with bot-test.sh.
FORWARDED = SECRETS + ("GOOGLE_APPLICATION_CREDENTIALS", "AWS_REGION", "AWS_DEFAULT_REGION")
REGION = "us-west-2"  # matches build-and-test.yml; required by the OpenSharing test server


def records(env: dict[str, str], key_file: Path) -> dict[str, str]:
    values = {name: env[name] for name in SECRETS if env.get(name)}
    if not values:
        return {}
    # Always set, as build-and-test.yml does: the client/Spark integration gates need it.
    # The test fixtures write the key to this path themselves (TestResource).
    values["GOOGLE_APPLICATION_CREDENTIALS"] = str(key_file)
    values["AWS_REGION"] = values["AWS_DEFAULT_REGION"] = REGION
    return values


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        raise SystemExit("usage: bot-e2e-env.py OUTPUT_FILE")
    out = Path(argv[1])
    values = records(dict(os.environ), out.parent / "google_service_account_key.json")
    if not values:
        print("::notice::No cloud test secrets are configured; bot integration tests will skip.")
        return 0
    if any("\0" in v for v in values.values()):
        raise SystemExit("A cloud test secret contains a NUL byte; refusing to stage it")
    data = b"".join(f"{name}={value}".encode() + b"\0" for name, value in values.items())
    fd = os.open(out, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(data)
    print(f"Staged {len([n for n in values if n in SECRETS])} cloud test secret(s) for bot-test.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
