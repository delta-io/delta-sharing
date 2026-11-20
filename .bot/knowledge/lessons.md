# Lessons

<!-- distilled-through: 2026-11-20-2 -->

## Scala client [lang=scala]
- L-0006 (1×, #962): Never place a retry/backoff sleep inside a synchronized method or block: the thread holds the object monitor for the whole sleep (up to the max retry duration) and starves every other caller.
- L-0007 (1×, #866): When exposing a profile field as a Spark option, name the option key to match the profile JSON deserializer's field name, or the supplied value is silently dropped.

## Scala tests [phase=cover; lang=scala]
- L-0008 (1×, #836 #837): Reserve the integrationTest tag for tests that genuinely need cloud credentials or real tables; the tag makes external contributors skip the test in pre-commit.
- L-0013 (1×, #1043): Assert on parsed JSON nodes, not exact substrings; JSON field order is insignificant and varies across server implementations.
- L-0014 (1×, #1044): Read endpoint and bearer token from the test run's profile rather than hardcoding them; some servers mint fresh recipient credentials per run.
- L-0015 (1×, #1044): In cross-implementation integration tests assert on stable signals (HTTP status, error presence), not an exact errorCode string or message text, which differ per server.

## Python connector [lang=python]
- L-0009 (1×, #1030): Under pandas 3, pd.Timestamp columns default to microsecond resolution (pandas 2 used nanosecond); normalize timestamp dtype in tests instead of asserting nanosecond.

## Build
- L-0010 [lang=python] (1×, #913): Generate committed public lockfiles against public indexes so no internal-only mirror URLs leak in or break external dependency resolution.
- L-0011 [lang=scala] (1×, #883): Remove resolver/repository URLs for shut-down services (e.g. dl.bintray.com) from build config: an abandoned dependency domain can be re-registered to serve malicious artifacts.

## CI / bot
- L-0012 (1×, #1048): Before executing agent- or issue-authored code, unset every sensitive token (model, git/GitHub, and cloud) or sandbox it credential-less; scrubbing only cloud credentials still allows token exfiltration via prompt injection.
