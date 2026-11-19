#!/usr/bin/env bash
# Source: bot-engine/docs/design/examples/fetch-consumer-engine.sh.
# Fetch only: do not execute engine code while the engine token is in scope.
set +x
set -euo pipefail
: "${ENGINE_REF:?ENGINE_REF is required}"
: "${ENGINE_TOKEN:?ENGINE_TOKEN is required}"
: "${RUNNER_TEMP:?RUNNER_TEMP is required}"
: "${GITHUB_OUTPUT:?GITHUB_OUTPUT is required}"
case "$ENGINE_REF" in
  *[!0-9a-f]* | "") printf '%s\n' 'ENGINE_REF must be a lowercase 40-character commit SHA' >&2; exit 1 ;;
  *) [ "${#ENGINE_REF}" -eq 40 ] || { printf '%s\n' 'ENGINE_REF must be a lowercase 40-character commit SHA' >&2; exit 1; } ;;
esac
unset GIT_TRACE GIT_TRACE_CURL GIT_CURL_VERBOSE GIT_TRACE_PACKET
umask 077
GIT_CONFIG_GLOBAL=$(mktemp "${RUNNER_TEMP%/}/bot-engine-gitconfig.XXXXXXXX")
export GIT_CONFIG_GLOBAL GIT_CONFIG_NOSYSTEM=1 GIT_TERMINAL_PROMPT=0
trap 'rm -f -- "$GIT_CONFIG_GLOBAL"' EXIT
BASIC=$(printf 'x-access-token:%s' "$ENGINE_TOKEN" | base64 | tr -d '\n')
if [ "${GITHUB_ACTIONS:-}" = true ]; then
  printf '::add-mask::%s\n' "$ENGINE_TOKEN"
  printf '::add-mask::%s\n' "$BASIC"
fi
printf '[http "https://github.com/databricks/databricks-bot-engine"]\n\textraheader = Authorization: Basic %s\n' "$BASIC" > "$GIT_CONFIG_GLOBAL"
ENGINE_SOURCE=$(mktemp -d "${RUNNER_TEMP%/}/bot-engine-source.XXXXXXXX")
git init -q "$ENGINE_SOURCE"
git -C "$ENGINE_SOURCE" remote add origin https://github.com/databricks/databricks-bot-engine
git -C "$ENGINE_SOURCE" -c core.hooksPath=/dev/null fetch --no-tags --depth=1 origin "$ENGINE_REF"
printf 'path=%s\n' "$ENGINE_SOURCE" >> "$GITHUB_OUTPUT"
