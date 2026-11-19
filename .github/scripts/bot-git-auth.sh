#!/usr/bin/env bash
# Source only in the exact step that must push. Never store a token in a remote URL.
# A per-process helper also overrides any credential cached by the persistent host.
set +x
: "${GH_TOKEN:?A consumer-scoped GitHub App token is required}"
export GIT_CONFIG_COUNT=2
export GIT_CONFIG_KEY_0='credential.https://github.com.helper'
export GIT_CONFIG_VALUE_0=''
export GIT_CONFIG_KEY_1='credential.https://github.com.helper'
export GIT_CONFIG_VALUE_1='!gh auth git-credential'
export GIT_TERMINAL_PROMPT=0 GH_PROMPT_DISABLED=1
unset GIT_TRACE GIT_TRACE_CURL GIT_CURL_VERBOSE GIT_TRACE_PACKET
