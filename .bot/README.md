# Delta Sharing bot engine onboarding

## Status and scope

This integration prepares reviewer + reviewer follow-up, engineer author + engineer
follow-up, and daily journal/distilled learning. Every bot job targets the existing
`[self-hosted, Linux, X64, delta-sharing-linux]` runner. The integration is staged
OFF: no privileged job runs until a maintainer sets the activation variables below.
Learning auto-merge is explicitly disabled.

The engine repository is internal and IP-restricted. A consumer-owned composite
fetches a reviewed immutable engine commit using a read-only, engine-scoped App
token, then installs it in a separate step without that token. It never calls an
internal cross-repo action/reusable workflow. The single engine pin is in
`.github/actions/setup-bot-engine/action.yml`; its built wheel supplies matching
runtime/SDK/npm locks. All third-party Actions are pinned to full commits.

These are newly authored consumer workflows, not an assertion that the engine's
historical templates work unchanged. Until the checks below run with approved
credentials on this exact worker, the integration is NOT end-to-end verified.

## Maintainer prerequisites (do not paste secret values into chat or PRs)

1. Install the existing `peco-review-bot` and `peco-engineer-bot` GitHub Apps on
   `delta-io/delta-sharing`. An org owner may need to approve the installations.
   Give reviewer contents read + pull-requests write; engineer contents, issues
   and pull-requests write + Actions read (learning downloads author-run logs).
   Each App used to download the engine must also be installed on
   `databricks/databricks-bot-engine` with contents read.
   The engine currently recognizes a fixed reviewer login, so a differently named
   reviewer App is NOT a drop-in replacement. No App needs workflow-edit rights.
2. Add these repository secrets, using the values through GitHub Settings or your
   approved secret-management process, never through an agent conversation:

   | Secret | Purpose |
   | --- | --- |
   | `REVIEW_BOT_APP_ID` | Existing reviewer App ID |
   | `REVIEW_BOT_APP_PRIVATE_KEY` | Reviewer App private key |
   | `ENGINEER_BOT_APP_ID` | Existing engineer App ID |
   | `ENGINEER_BOT_APP_PRIVATE_KEY` | Engineer App private key |
   | `BOT_DATABRICKS_TOKEN` | Least-privilege model token for the selected workspace |

   Add repository variable `BOT_DATABRICKS_HOST`: the bare, direct workspace host,
   no scheme or path. The workflow validates it and builds a full Model Serving
   endpoint. It must pair with the token and serve the pinned engine's configured
   review/second-review/judge models. Check actual cross-review completion, not
   just the workflow's green status. No live cloud/storage test secrets are mapped.
3. Review the shared worker's trust boundary. It uses the same VM and Linux user
   as ADBC: separate runner folders and job-local HOME/venv/npm/Git configuration
   prevent accidental clashes, NOT cross-job credential access. Tests/native
   builds execute arbitrary code. An ephemeral isolated worker is preferred for
   engineer jobs. Explicitly approve the runner before enabling engineer; keep
   public fork jobs off this VM. Check runner-group/repo policy and require
   approval for untrusted public workflows as appropriate.
4. Worker prerequisites: Python 3.11 with venv, Git, `gh`, curl, tar and native
   C/C++ build prerequisites, plus approved access to the engine, GitHub Actions
   downloads, model endpoints and dependency registries. The workflows provision
   Node 22; engineer jobs also provision Java 8/17, uv and a job-local Rust
   toolchain for the native Python wrapper. Set optional variable
   `BOT_RUST_TOOLCHAIN` to a reviewed exact Rust version to pin it; otherwise the
   setup action installs stable. No sudo installs are performed by bot jobs.
   This worker follows ADBC's public-registry install pattern; if egress requires
   a mirror, have maintainers adapt the job-local registry configuration using
   the approved policy, without putting mirror credentials in agent-visible env.
5. Create the maintainer-controlled `engineer-bot` issue/PR label. Protect workflow,
   bot policy and release paths through code-owner review. Do not grant bots the
   ability to edit their own workflows. Maintainer authorization is checked via
   the live collaborator permission API (write/maintain/admin), not the broad
   `CONTRIBUTOR` association. Only the two exact existing App identities get the
   bot exception for review/follow-up events.

## Activation and supervised verification

Issues, schedules and manual-dispatch workflows register from the default branch.
Review and merge this onboarding before attempting privileged runs. A checkout of
main protects the control files; it does not protect against an already-trusted
writer changing the workflow definition itself. Keep branch/runner protections.

1. Verify the offline **Bot configuration checks** job. It runs on a standard
   hosted runner with read-only permissions, no secrets or private-engine access,
   and is safe to run on fork PRs. Existing product CI is unchanged.
2. Set `BOT_ENGINE_ENABLED=true` to enable reviewer + follow-up. Dispatch
   **Reviewer Bot** from main against a trusted, non-draft, same-repo PR with
   `dry_run=true`. This does not post, but generated review text can appear in
   public Actions logs. Verify the engine revision, SDK/CLI, model endpoint and
   the completion of both review passes/judge when findings exist. Then perform
   a supervised posting run and a maintainer reply to verify follow-up separately.
3. Only after approving test execution/isolation, set `BOT_ENGINEER_ENABLED=true`.
   A maintainer labels a small non-sensitive issue `engineer-bot`, or dispatches
   **Engineer Bot** from main for an already-labelled issue. Label-event runs use
   the issue body snapshot from the label event, not later author edits. Issue
   Types map Bug to bug-fix and Task/Feature to task; absent type defaults bug-fix.
   Check exact red/green evidence, DCO sign-off and a proposed PR. Nothing auto-merges.
4. Set `BOT_LEARNING_ENABLED=true`. First dispatch **Engineer Bot - Learning**
   with the default `dry_run=true`: this checks selection and Actions-log access
   without model calls or git/PR writes. Then supervise a real extraction with
   `dry_run=false`. The daily schedule is 08:37 UTC. Review the public learning
   PR for sensitive content; no auto-merge is enabled. For a bounded recovery,
   supply the optional ISO `since` input. `.bot/knowledge/journal` is created on
   the first real learning; lessons are injected automatically by the engine.
5. Disable any feature immediately by setting its variable to `false` (master
   `BOT_ENGINE_ENABLED=false` disables all five). Existing running jobs must be
   cancelled separately if necessary; a variable change doesn't stop them.

## Testing and limits

The engineer invokes a default-branch copy of `bot-test` installed into its venv.
It selects Java 8 for server and Java 17 for client/Spark, unsets cloud integration
credentials, and accepts only targeted test/lint command shapes. Python uses
`uv run --frozen`; the repository lock is not changed. Scala client work needs
both 2.12 and 2.13 verification. Some server suites still require cloud resources
and must be reported blocked. Dependency downloads/native builds need network.
Do not claim the full cloud integration matrix ran in a bot job.

The reviewer runs the installed engine using Python isolated imports from a
trusted control checkout, while reading a separately checked-out PR SHA through
`REVIEW_CONTENT_ROOT`. Engineer configuration/prompts also come from control,
not the PR head. `.github`, `.bot`, `.claude`, licensing, release/version files
and the uv lock are write-denied to engineer tools. These controls and credential
scrubbing are defense in depth, not OS isolation of arbitrary test code.

Consumer read/write tokens are scoped to Delta Sharing and minted after bootstrap
installation. The author uses a read-only token; after it finishes, a fresh token
can report the issue outcome. That token gets contents/PR write only when the
author succeeded and publishing is attempted. Follow-up/learning need write access during their respective
operations. Git push auth is per-process via `gh`, never a token in a remote URL.
No workflow enables `secrets: inherit`, a privileged fork trigger, learning
PR auto-merge, or automatic issue closure.

Local checks (no network/model credentials):

```bash
python -I -m unittest discover -s .github/tests -v
for script in .github/scripts/*bot*.sh; do bash -n "$script"; done
```

The test runner needs PyYAML 6.0.3. Report static checks, product tests, runner
bootstrap, model review, follow-up and learning verification separately.
