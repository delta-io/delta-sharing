#!/usr/bin/env bash
# Separate step from fetch-bot-engine.sh: no engine or consumer write token here.
set +x
set -euo pipefail
: "${ENGINE_REF:?ENGINE_REF is required}"
: "${ENGINE_SOURCE:?ENGINE_SOURCE is required}"
: "${BOT_RUNTIME:?BOT_RUNTIME is required}"
: "${RUNNER_TEMP:?RUNNER_TEMP is required}"
if [ -n "${ENGINE_TOKEN:-}" ] || [ -n "${GH_TOKEN:-}" ]; then
  printf '%s\n' 'Do not pass engine or consumer credentials to package installation' >&2
  exit 1
fi
case "$ENGINE_REF" in
  *[!0-9a-f]* | "") printf '%s\n' 'ENGINE_REF must be a lowercase 40-character commit SHA' >&2; exit 1 ;;
  *) [ "${#ENGINE_REF}" -eq 40 ] || exit 1 ;;
esac
RUNNER_ROOT=$(cd "$RUNNER_TEMP" && pwd -P)
SOURCE_ROOT=$(cd "$ENGINE_SOURCE" && pwd -P)
case "$SOURCE_ROOT" in
  "$RUNNER_ROOT"/bot-engine-source.*) ;;
  *) printf '%s\n' 'ENGINE_SOURCE must be the job-local fetch output' >&2; exit 1 ;;
esac
export GIT_CONFIG_NOSYSTEM=1 GIT_TERMINAL_PROMPT=0
unset GIT_TRACE GIT_TRACE_CURL GIT_CURL_VERBOSE GIT_TRACE_PACKET
git -C "$SOURCE_ROOT" -c core.hooksPath=/dev/null checkout -q --detach "$ENGINE_REF"
[ "$(git -C "$SOURCE_ROOT" rev-parse HEAD)" = "$ENGINE_REF" ] || {
  printf '%s\n' 'Fetched engine commit does not match ENGINE_REF' >&2; exit 1;
}
# Upgrade only the job-local pip, pinned/hashed like the dependency locks.
PYTHON="$BOT_RUNTIME/venv/bin/python"
"$PYTHON" -I -m pip install --require-hashes --only-binary :all: \
  -r "$(dirname "${BASH_SOURCE[0]}")/../requirements/bot-bootstrap.txt"
"$PYTHON" -I -m pip install --require-hashes --only-binary :all: \
  -r "$SOURCE_ROOT/requirements/build.lock" -r "$SOURCE_ROOT/requirements/runtime.lock"
"$PYTHON" -I -m pip install --no-deps --no-build-isolation "$SOURCE_ROOT"
# The wheel carries this engine revision's runtime/SDK/npm locks; one engine pin.
"$PYTHON" -I -m databricks_bot_engine.install_runtime --cli-prefix "$BOT_RUNTIME/cli"
"$PYTHON" -I -m pip check
