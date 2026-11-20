# Lessons

<!-- distilled-through: 2026-11-20 -->

## CI bot security
- L-0001 (1×, #1048): When a CI job runs test/build code an agent generated from untrusted input, execute it with an allowlisted env (env -i plus only PATH/HOME/JAVA_HOME/cache vars), not by stripping a subset of secrets.
- L-0002 (1×, #1048): Keep the agent step on a read-only token; mint a fresh write token only for the publish/push step so agent test execution never runs with write (tag/release) scope.

## Scala client concurrency [lang=scala]
- L-0003 (1×, #962): Don't run blocking retry/backoff sleeps inside a synchronized method whose monitor close()/cancellation needs (e.g. RandomAccessHttpInputStream), and don't nest a retry loop inside another retry loop.

## CDF correctness [lang=python]
- L-0004 (1×, #992): For CDF materialization, derive _change_type for removeFile/addFile from the action type itself; a CDF table's raw non-CDC parquet can have a legitimately null _change_type column.

## Delta Sharing protocol [lang=scala]
- L-0005 (1×, #1007 #1006 #1005): Treat sharing-response version as optional/nullable on the versionless-CDF path (handled as a snapshot query), and only emit/expect versionless-CDF when the client advertised the versionlesscdf capability.
