# Lessons

<!-- distilled-through: 2026-11-28 -->

## Scala client [lang=scala]
- L-0006 (1×, #962): Never place a retry/backoff sleep inside a synchronized method or block: the thread holds the object monitor for the whole sleep (up to the max retry duration) and starves every other caller.
- L-0007 (1×, #866): When exposing a profile field as a Spark option, name the option key to match the profile JSON deserializer's field name, or the supplied value is silently dropped.

## Scala tests [phase=cover; lang=scala]
- L-0008 (1×, #836 #837): Reserve the integrationTest tag for tests that genuinely need cloud credentials or real tables; the tag makes external contributors skip the test in pre-commit.
- L-0013 (1×, #1043): Assert on parsed JSON nodes, not exact substrings; JSON field order is insignificant and varies across server implementations.
- L-0014 (1×, #1044): Read endpoint and bearer token from the test run's profile rather than hardcoding them; some servers mint fresh recipient credentials per run.
- L-0015 (1×, #1044): In cross-implementation integration tests assert on stable signals (HTTP status, error presence), not an exact errorCode string or message text, which differ per server.
- L-0017 (1×, #1045): Compare list-endpoint responses (shares, tables) by sorting both sides on a stable key; the protocol leaves element order unspecified and it varies per server.
- L-0018 (1×, #1040): Define the integration-test share/table catalog once in a shared data file consumed by every server under test, rather than duplicating the fixture definitions in each server's code.

## Python connector [lang=python]
- L-0009 (1×, #1030): Under pandas 3, pd.Timestamp columns default to microsecond resolution (pandas 2 used nanosecond); normalize timestamp dtype in tests instead of asserting nanosecond.
- L-0016 (1×, #1045): Assert a mid-stream streaming (CDF) failure by its exception type plus the server's stream-failure marker in the unparseable payload, and assert up-front rejections as HTTP errors; one broad message regex also matches unrelated pre-stream errors.
- L-0020 (1×, #1035): For a streamed response that can fail after HTTP 200, opt in to a trailing end-of-stream action, surface its error and status as a typed exception, retry only on 429/5xx, and treat a promised-but-missing trailing action as a truncated, retryable response.
- L-0021 (1×, #1035): Scope a capability only some endpoints honor to a per-request header merged over the session value, never to session-global headers, so it neither leaks to other endpoints nor drops the session's auth headers.

## Build
- L-0010 [lang=python] (1×, #913): Generate committed public lockfiles against public indexes so no internal-only mirror URLs leak in or break external dependency resolution.
- L-0011 [lang=scala] (1×, #883): Remove resolver/repository URLs for shut-down services (e.g. dl.bintray.com) from build config: an abandoned dependency domain can be re-registered to serve malicious artifacts.

## CI / bot
- L-0012 (1×, #1048): Before executing agent- or issue-authored code, unset every sensitive token (model, git/GitHub, and cloud) or sandbox it credential-less; scrubbing only cloud credentials still allows token exfiltration via prompt injection.
- L-0019 (1×, #1064): When renaming a bot identity used in self-reply guards or allow/deny login sets, accept both the old and new logins during the migration window; drop the old one only after the App/secrets cutover is confirmed, or old-bot comments stop being filtered and can trigger bot loops.

## Python tests [phase=cover; lang=python]
- L-0022 (1×, #1035): Assert against full literal expected strings and the actual outgoing request, never a value reconstructed by the production code path or read back from the same state the code copies from, since such assertions are tautologically true.
