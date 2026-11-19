#!/usr/bin/env bash
# Installed as `bot-test` in the bot venv from the DEFAULT-BRANCH control checkout.
# This restricts invocation shapes; tests themselves execute arbitrary code.
set -euo pipefail
: "${REPO_ROOT:?REPO_ROOT is required}"
cd "$REPO_ROOT"
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_PROFILE \
  AZURE_TEST_ACCOUNT_KEY GOOGLE_SERVICE_ACCOUNT_KEY GOOGLE_APPLICATION_CREDENTIALS
export SPARK_LOCAL_IP=127.0.0.1
export UV_CACHE_DIR="${BOT_RUNTIME:?BOT_RUNTIME is required}/uv-cache"
mode=${1:?Select python-test, python-lint, or scala-<module>-test}
shift
case "$mode" in
  python-test)
    # No all-tests default: select a test file and optional -k expression.
    [ "$#" -ge 1 ] && [ "$#" -le 3 ] || { printf '%s\n' 'Select one Python test file, optionally -k EXPRESSION' >&2; exit 2; }
    case "$1" in delta_sharing/tests/test_*.py) ;; *) exit 2 ;; esac
    [[ "$1" != *..* ]] || exit 2
    if [ "$#" -ne 1 ]; then [ "$#" -eq 3 ] && [ "$2" = '-k' ] || exit 2; fi
    cd python
    exec uv run --frozen --python 3.11 dev/pytest "$@"
    ;;
  python-lint)
    [ "$#" -eq 0 ] || exit 2
    cd python
    exec uv run --frozen --python 3.11 dev/lint-python
    ;;
  scala-client-test|scala-client212-test|scala-spark-test|scala-server-test)
    [ "$#" -eq 1 ] && [[ "$1" =~ ^[A-Za-z0-9_.*]+$ ]] && [[ "$1" =~ [A-Za-z0-9_] ]] || { printf '%s\n' 'Select a targeted test suite pattern' >&2; exit 2; }
    module=${mode#scala-}; module=${module%-test}
    if [ "$module" = server ]; then
      export JAVA_HOME="${JAVA_HOME_8_X64:?Java 8 must be provisioned for server tests}"
    else
      export JAVA_HOME="${JAVA_HOME_17_X64:?Java 17 must be provisioned for client/Spark tests}"
    fi
    export PATH="$JAVA_HOME/bin:$PATH"
    if [ "$module" = client212 ]; then
      exec ./build/sbt ++2.12.18 "client/testOnly $1"
    fi
    exec ./build/sbt "$module/testOnly $1"
    ;;
  *) printf 'Unsupported bot-test mode: %s\n' "$mode" >&2; exit 2 ;;
esac
