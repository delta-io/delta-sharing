"""Offline checks for the consumer-owned bot integration. No model or GitHub calls."""
from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import yaml

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / ".github/scripts"
spec = importlib.util.spec_from_file_location("bot_context", SCRIPTS / "bot-context.py")
context = importlib.util.module_from_spec(spec)
spec.loader.exec_module(context)
REPO = "example-owner/example-repo"
PR = {"state": "open", "draft": False,
      "head": {"sha": "a" * 40, "ref": "bot/test", "repo": {"full_name": REPO}},
      "base": {"sha": "b" * 40}, "labels": [{"name": "engineer-bot"}, {"name": "review-bot"}]}


class EventTests(unittest.TestCase):
    def setUp(self):
        scope = patch.dict(os.environ, {"GITHUB_REPOSITORY": REPO})
        scope.start()
        self.addCleanup(scope.stop)

    def event(self):
        return {"repository": {"full_name": REPO}, "sender": {"login": "maintainer", "type": "User"},
                "pull_request": {"number": 42}, "comment": {"id": 123}}

    def call(self, mode="review", event=None, actor="maintainer", pr=None, permission="write"):
        event = event or self.event()
        def api(route):
            if route.endswith("/permission"):
                return {"permission": permission}
            if "/pulls/" in route:
                return pr or PR
            raise AssertionError(route)
        with patch.object(context, "api", side_effect=api):
            return context.prepare(mode, "pull_request", event, actor)

    def test_maintainer_review_has_live_validated_shas(self):
        out, env = self.call()
        self.assertEqual(out["eligible"], "true")
        self.assertEqual(out["head"], "a" * 40)
        self.assertEqual(env["PR_NUMBER"], 42)

    def test_contributor_without_write_is_not_authorized(self):
        out, env = self.call(permission="read")
        self.assertEqual(out, {"eligible": "false"})
        self.assertFalse(env)

    def test_private_org_membership_does_not_require_association_member(self):
        event = self.event()
        event["comment"]["author_association"] = "CONTRIBUTOR"
        self.assertEqual(self.call(event=event)[0]["eligible"], "true")

    def test_same_repository_is_checked_live_not_just_in_event(self):
        pr = {**PR, "head": {**PR["head"], "repo": {"full_name": "outside/fork"}}}
        self.assertEqual(self.call(pr=pr)[0]["eligible"], "false")

    def test_draft_and_closed_prs_skip(self):
        for update in ({"draft": True}, {"state": "closed"}):
            with self.subTest(update=update):
                self.assertEqual(self.call(pr={**PR, **update})[0]["eligible"], "false")

    def test_deleted_head_repo_skips(self):
        pr = {**PR, "head": {**PR["head"], "repo": None}}
        self.assertEqual(self.call(pr=pr)[0]["eligible"], "false")

    def test_manual_dispatch_cannot_bypass_fork_floor(self):
        pr = {**PR, "head": {**PR["head"], "repo": {"full_name": "outside/fork"}}}
        def api(route):
            return {"permission": "admin"} if route.endswith("/permission") else pr
        with patch.object(context, "api", side_effect=api):
            out, env = context.prepare("review", "workflow_dispatch", self.event(), "maintainer", "42")
        self.assertEqual(out["eligible"], "false")
        self.assertFalse(env)

    def test_review_and_followup_require_live_opt_in_label(self):
        for mode in ("review", "review-followup"):
            for labels in ([], [{"name": "engineer-bot"}]):
                with self.subTest(mode=mode, labels=labels):
                    out, env = self.call(mode, pr={**PR, "labels": labels})
                    self.assertEqual(out, {"eligible": "false"})
                    self.assertFalse(env)
            with self.subTest(mode=mode):
                out, _ = self.call(mode, pr={**PR, "labels": [{"name": "review-bot"}]})
                self.assertEqual(out["eligible"], "true")

    def test_manual_review_cannot_bypass_live_opt_in_label(self):
        def api(route):
            return {"permission": "admin"} if route.endswith("/permission") else {**PR, "labels": []}
        with patch.object(context, "api", side_effect=api):
            out, env = context.prepare("review", "workflow_dispatch", self.event(), "maintainer", "42")
        self.assertEqual(out, {"eligible": "false"})
        self.assertFalse(env)

    def test_engineer_followup_requires_label(self):
        self.assertEqual(self.call("engineer-followup", pr={**PR, "labels": []})[0]["eligible"], "false")

    def test_bot_exception_requires_real_bot_sender(self):
        event = self.event()
        actor = "peco-review-bot[bot]"
        with patch.object(context, "api", return_value={"permission": "read"}):
            self.assertFalse(context.is_maintainer(actor, event, allow_bot=True))
            event["sender"] = {"login": actor, "type": "Bot"}
            self.assertTrue(context.is_maintainer(actor, event, allow_bot=True))
            self.assertFalse(context.is_maintainer(actor, event, allow_bot=False))

    def test_wrong_repo_and_invalid_numbers_fail(self):
        event = self.event()
        event["repository"]["full_name"] = "outside/fork"
        with self.assertRaises(ValueError):
            self.call(event=event)
        for value in ("0", "1; id", "-1", "42\nBAD=1"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                context.number(value)

    def test_api_and_head_checks_use_current_repository_without_a_fixed_name(self):
        repo = "another-owner/another-project"
        event = {**self.event(), "repository": {"full_name": repo}}
        pr = {**PR, "head": {**PR["head"], "repo": {"full_name": repo}}}
        routes = []
        def api(route):
            routes.append(route)
            return {"permission": "write"} if route.endswith("/permission") else pr
        with patch.dict(os.environ, {"GITHUB_REPOSITORY": repo}), patch.object(context, "api", side_effect=api):
            out, _ = context.prepare("review", "pull_request", event, "maintainer")
        self.assertEqual(out["eligible"], "true")
        self.assertEqual(routes, [f"repos/{repo}/collaborators/maintainer/permission", f"repos/{repo}/pulls/42"])

    def test_missing_repository_context_fails_before_any_api(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(context, "api") as api:
            with self.assertRaises(KeyError):
                context.prepare("review", "pull_request", self.event(), "maintainer")
            api.assert_not_called()

    def test_invalid_head_sha_fails(self):
        with self.assertRaises(ValueError):
            self.call(pr={**PR, "head": {**PR["head"], "sha": "main"}})

    def test_issue_body_comes_from_label_event_not_later_edit(self):
        event = self.event()
        event["issue"] = {"number": 7, "title": "Approved title\nUNTRUSTED=1", "body": "Approved snapshot", "type": {"name": "Bug"}}
        current = {"state": "open", "body": "Later edit", "html_url": f"https://github.com/{REPO}/issues/7", "labels": [{"name": "engineer-bot"}]}
        def api(route):
            return {"permission": "write"} if route.endswith("/permission") else current
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {"RUNNER_TEMP": tmp}), patch.object(context, "api", side_effect=api):
            out, env = context.prepare("engineer", "issues", event, "maintainer")
            self.assertEqual(out["flow"], "bug-fix")
            self.assertEqual(Path(tmp, "issue_body.txt").read_text(), "Approved snapshot")
            context.write_values(str(Path(tmp, "env")), env)
            text = Path(tmp, "env").read_text()
            self.assertIn("ISSUE_TITLE<<BOT_", text)
            self.assertIn("Approved title\nUNTRUSTED=1", text)


    def test_engineer_rejects_closed_pr_and_unlabelled_issues_without_writing_body(self):
        event = self.event()
        event["issue"] = {"number": 7, "body": "Approved snapshot"}
        for change in ({"state": "closed"}, {"pull_request": {"url": "a-pr"}}, {"labels": []}):
            current = {"state": "open", "labels": [{"name": "engineer-bot"}], **change}
            def api(route):
                return {"permission": "write"} if route.endswith("/permission") else current
            with self.subTest(change=change), tempfile.TemporaryDirectory() as tmp, \
                    patch.dict(os.environ, {"RUNNER_TEMP": tmp}), patch.object(context, "api", side_effect=api):
                out, env = context.prepare("engineer", "issues", event, "maintainer")
                self.assertEqual(out, {"eligible": "false"})
                self.assertFalse(env)
                self.assertFalse(Path(tmp, "issue_body.txt").exists())

    def test_engineer_dispatch_uses_current_issue_and_maps_issue_types(self):
        for issue_type, expected in (("Bug", "bug-fix"), ("Task", "task"), ("Feature", "task"), ("Other", "bug-fix")):
            current = {"state": "open", "labels": [{"name": "engineer-bot"}],
                       "body": "Current dispatch body", "title": "Current title",
                       "html_url": f"https://github.com/{REPO}/issues/7",
                       "type": {"name": issue_type}}
            def api(route):
                return {"permission": "write"} if route.endswith("/permission") else current
            with self.subTest(issue_type=issue_type), tempfile.TemporaryDirectory() as tmp, \
                    patch.dict(os.environ, {"RUNNER_TEMP": tmp}), patch.object(context, "api", side_effect=api):
                out, env = context.prepare("engineer", "workflow_dispatch", self.event(), "maintainer", "7")
                self.assertEqual(out, {"eligible": "true", "flow": expected})
                self.assertEqual(env["ISSUE_TITLE"], "Current title")
                self.assertEqual(Path(tmp, "issue_body.txt").read_text(), "Current dispatch body")


class MainTests(unittest.TestCase):
    def invoke(self, mode, event_name, event, repo=REPO, permission="read"):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload, output = root / "event.json", root / "output"
            payload.write_text(json.dumps(event))
            env = {"GITHUB_EVENT_PATH": str(payload), "GITHUB_EVENT_NAME": event_name,
                   "GITHUB_REPOSITORY": repo, "GITHUB_ACTOR": "maintainer",
                   "GITHUB_OUTPUT": str(output), "GITHUB_ENV": str(root / "env")}
            with patch.dict(os.environ, env), patch.object(sys, "argv", ["bot-context.py", mode]), \
                    patch.object(context, "api", return_value={"permission": permission}) as api:
                rc = context.main()
                return rc, output.read_text(), api.call_args_list

    def test_scheduled_learning_accepts_minimal_payload_without_api_calls(self):
        rc, output, calls = self.invoke("learning", "schedule", {"schedule": "37 8 * * *"})
        self.assertEqual(rc, 0)
        self.assertRegex(output, r"eligible<<BOT_[0-9a-f]+\ntrue\n")
        self.assertFalse(calls)

    def test_scheduled_learning_uses_current_repository_for_app_token_scope(self):
        rc, output, calls = self.invoke("learning", "schedule", {"schedule": "37 8 * * *"},
                                        repo="another-owner/another-project")
        self.assertEqual(rc, 0)
        self.assertRegex(output, r"repository_name<<BOT_[0-9a-f]+\nanother-project\n")
        self.assertFalse(calls)

    def test_schedule_exception_does_not_apply_to_other_modes(self):
        event = {"repository": {"full_name": REPO}, "schedule": "37 8 * * *"}
        rc, output, calls = self.invoke("review", "schedule", event)
        self.assertEqual(rc, 0)
        self.assertRegex(output, r"eligible<<BOT_[0-9a-f]+\nfalse\n")
        self.assertEqual(len(calls), 1)
        self.assertTrue(calls[0].args[0].endswith("/permission"))

    def test_scheduled_learning_rejects_wrong_runner_repository(self):
        # A claimed payload repository must not override the runner context.
        with self.assertRaisesRegex(ValueError, "Wrong repository"):
            self.invoke("learning", "schedule", {"repository": {"full_name": REPO}}, repo="outside/fork")

    def test_unsupported_trigger_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "Unsupported trigger"):
            self.invoke("learning", "issue_comment", {})

    def test_manual_learning_still_requires_maintainer(self):
        rc, output, calls = self.invoke("learning", "workflow_dispatch", {"repository": {"full_name": REPO}})
        self.assertEqual(rc, 0)
        self.assertRegex(output, r"eligible<<BOT_[0-9a-f]+\nfalse\n")
        self.assertEqual(len(calls), 1)


class WorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # BaseLoader preserves the Actions `on` key instead of YAML 1.1's boolean.
        cls.workflows = {p.name: yaml.load(p.read_text(), Loader=yaml.BaseLoader)
                         for p in (ROOT / ".github/workflows").glob("*bot*.yml")
                         if p.name != "bot-config-checks.yml"}

    def test_bot_policy_has_the_same_codeowners_as_github_configuration(self):
        rules = {}
        for line in (ROOT / ".github/CODEOWNERS").read_text().splitlines():
            fields = line.split("#", 1)[0].split()
            if len(fields) > 1:
                rules[fields[0]] = fields[1:]
        self.assertTrue(rules["/.bot/"])
        self.assertEqual(rules["/.bot/"], rules["/.github/"])

    def test_reviewer_followup_does_not_duplicate_push_reviews(self):
        triggers = self.workflows["reviewer-bot-followup.yml"]["on"]
        self.assertEqual(triggers, {"pull_request_review_comment": {"types": ["created"]}})

    def test_all_new_workflows_use_one_checkout_revision(self):
        workflows = list(self.workflows.values()) + [yaml.load(
            (ROOT / ".github/workflows/bot-config-checks.yml").read_text(), Loader=yaml.BaseLoader)]
        uses = {step["uses"] for workflow in workflows for job in workflow["jobs"].values()
                for step in job["steps"] if step.get("uses", "").startswith("actions/checkout@")}
        self.assertEqual(uses, {"actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1"})

    def test_model_host_guard_accepts_only_bare_databricks_hosts(self):
        action = yaml.load((ROOT / ".github/actions/setup-bot-engine/action.yml").read_text(), Loader=yaml.BaseLoader)
        command = action["runs"]["steps"][0]["run"]
        script = re.search(r"python3\.11 -I - <<'PY'\n(.*?)\nPY", command, re.S).group(1)
        valid = ("adb-1.2.azuredatabricks.net", "foo.cloud.databricks.com")
        invalid = ("evil.com", "x.databricks.com.evil.com", "https://foo.databricks.com",
                   "foo.databricks.com/path", "", "foo.databricks.com\nBAD=1")
        for host in valid + invalid:
            with self.subTest(host=host):
                result = subprocess.run([sys.executable, "-I", "-"], input=script,
                                        env={"BOT_DATABRICKS_HOST": host}, capture_output=True, text=True)
                self.assertEqual(result.returncode == 0, host in valid, result.stderr)

    def test_composite_actions_do_not_read_unsupported_workflow_contexts(self):
        # Regression for the live runner's "Unrecognized named-value: vars".
        # Contexts must be resolved by the caller, not inside a composite.
        for path in (ROOT / ".github/actions").glob("setup-bot-*/action.yml"):
            document = yaml.load(path.read_text(), Loader=yaml.BaseLoader)
            content = json.dumps(document["runs"])
            with self.subTest(action=path.parent.name):
                self.assertNotRegex(content, r"\b(?:vars|secrets)\.")
                for key in re.findall(r"\binputs\.([A-Za-z0-9_-]+)", content):
                    self.assertIn(key, document["inputs"])
        action = yaml.load((ROOT / ".github/actions/setup-bot-engine/action.yml").read_text(), Loader=yaml.BaseLoader)
        self.assertEqual(action["inputs"]["model-host"]["required"], "true")
        self.assertEqual(action["runs"]["steps"][0]["env"]["BOT_DATABRICKS_HOST"], "${{ inputs.model-host }}")
        tests = yaml.load((ROOT / ".github/actions/setup-bot-tests/action.yml").read_text(), Loader=yaml.BaseLoader)
        self.assertEqual(tests["inputs"]["rust-toolchain"]["default"], "stable")
        rust = next(s for s in tests["runs"]["steps"] if s.get("uses", "").startswith("dtolnay/rust-toolchain@"))
        self.assertEqual(rust["with"]["toolchain"], "${{ inputs.rust-toolchain }}")

    def test_workflows_pass_model_and_toolchain_settings_through_inputs(self):
        host = "${{ vars.BOT_DATABRICKS_HOST }}"
        token = "${{ secrets.BOT_DATABRICKS_TOKEN }}"
        for name, workflow in self.workflows.items():
            for step in workflow["jobs"]["bot"]["steps"]:
                with self.subTest(workflow=name, step=step.get("name")):
                    if step.get("uses", "").endswith("/setup-bot-engine"):
                        self.assertEqual(step["with"]["model-host"], host)
                    if step.get("uses", "").endswith("/setup-bot-tests"):
                        self.assertEqual(step["with"]["rust-toolchain"], "${{ vars.BOT_RUST_TOOLCHAIN || 'stable' }}")
                    if "DATABRICKS_TOKEN" in step.get("env", {}):
                        self.assertEqual(step["env"]["DATABRICKS_TOKEN"], token)

    def test_reviewer_uses_master_flag_and_review_label_without_an_extra_flag(self):
        workflow = self.workflows["reviewer-bot.yml"]
        self.assertIn("labeled", workflow["on"]["pull_request"]["types"])
        condition = workflow["jobs"]["bot"]["if"]
        self.assertIn("github.event.action != 'labeled' || github.event.label.name == 'review-bot'", condition)
        for name in ("reviewer-bot.yml", "reviewer-bot-followup.yml"):
            condition = self.workflows[name]["jobs"]["bot"]["if"]
            self.assertIn("vars.BOT_ENGINE_ENABLED == 'true'", condition)
            self.assertNotIn("BOT_REVIEWER_ENABLED", json.dumps(self.workflows[name]))
            self.assertIn("contains(github.event.pull_request.labels.*.name, 'review-bot')", condition)

    def test_configuration_check_is_hosted_and_unprivileged(self):
        workflow = yaml.load((ROOT / ".github/workflows/bot-config-checks.yml").read_text(), Loader=yaml.BaseLoader)
        self.assertEqual(workflow["permissions"], {"contents": "read"})
        self.assertEqual(workflow["jobs"]["configuration"]["runs-on"], "ubuntu-24.04")
        self.assertNotIn("secrets.", json.dumps(workflow))
        self.assertNotIn("setup-bot-engine", json.dumps(workflow))

    def test_five_bot_workflows_target_only_registered_runner_and_start_disabled(self):
        self.assertEqual(len(self.workflows), 5)
        for name, workflow in self.workflows.items():
            with self.subTest(name=name):
                job = workflow["jobs"]["bot"]
                self.assertEqual(job["runs-on"], ["self-hosted", "Linux", "X64", "delta-sharing-linux"])
                self.assertIn("vars.BOT_ENGINE_ENABLED == 'true'", job["if"])
                self.assertNotIn("github.repository ==", job["if"])
                self.assertNotIn("id-token", job["permissions"])
                self.assertNotIn("write", job["permissions"].values())

    def test_control_checkout_is_default_branch_and_actor_check_precedes_secrets(self):
        for name, workflow in self.workflows.items():
            with self.subTest(name=name):
                steps = workflow["jobs"]["bot"]["steps"]
                self.assertEqual(steps[0]["with"]["ref"], "${{ github.event.repository.default_branch || 'main' }}")
                self.assertEqual(steps[0]["with"]["persist-credentials"], "false")
                self.assertEqual(steps[1]["id"], "context")
                self.assertIn("bot-control/.github/scripts/bot-context.py", steps[1]["run"])
                self.assertNotIn("secrets.", json.dumps(steps[:2]))
                for step in steps[2:]:
                    self.assertIn("if", step)

    def test_pr_workflows_have_fork_floor(self):
        for workflow in self.workflows.values():
            if any(k in workflow["on"] for k in ("pull_request", "pull_request_review_comment")):
                self.assertIn("github.event.pull_request.head.repo.fork == false", workflow["jobs"]["bot"]["if"])

    def test_external_actions_are_pinned_and_no_private_cross_repo_uses(self):
        documents = list(self.workflows.values())
        documents += [yaml.load(p.read_text(), Loader=yaml.BaseLoader) for p in (ROOT / ".github/actions").glob("*/action.yml")]
        for document in documents:
            lists = [job["steps"] for job in document.get("jobs", {}).values()]
            if "runs" in document:
                lists.append(document["runs"]["steps"])
            for steps in lists:
                for step in steps:
                    uses = step.get("uses", "")
                    if uses and not uses.startswith("./"):
                        self.assertRegex(uses, r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_./-]+@[0-9a-f]{40}$")
                    self.assertNotIn("databricks/databricks-bot-engine/", uses)

    def test_consumer_app_tokens_never_default_to_all_installation_repos(self):
        for workflow in self.workflows.values():
            for step in workflow["jobs"]["bot"]["steps"]:
                if step.get("uses", "").startswith("actions/create-github-app-token@"):
                    self.assertEqual(step["with"]["repositories"], "${{ steps.context.outputs.repository_name }}")
                    self.assertEqual(step["with"]["owner"], "${{ github.repository_owner }}")

    def test_setup_separates_engine_fetch_from_build_and_pins_one_revision(self):
        action = yaml.load((ROOT / ".github/actions/setup-bot-engine/action.yml").read_text(), Loader=yaml.BaseLoader)
        steps = action["runs"]["steps"]
        self.assertRegex(steps[0]["env"]["ENGINE_REF"], r"^[0-9a-f]{40}$")
        fetch = next(s for s in steps if s.get("id") == "engine-source")
        install = steps[-1]
        self.assertIn("ENGINE_TOKEN", fetch["env"])
        self.assertNotIn("ENGINE_TOKEN", install["env"])
        self.assertNotIn("GH_TOKEN", install["env"])
        self.assertIn("databricks_bot_engine.install_runtime", (SCRIPTS / "install-bot-engine.sh").read_text())

    def test_engineer_and_learning_have_separate_activation_gates(self):
        for name in ("engineer-bot.yml", "engineer-bot-followup.yml"):
            self.assertIn("BOT_ENGINEER_ENABLED", self.workflows[name]["jobs"]["bot"]["if"])
        self.assertIn("BOT_LEARNING_ENABLED", self.workflows["engineer-bot-learning.yml"]["jobs"]["bot"]["if"])
        self.assertFalse(yaml.safe_load((ROOT / ".bot/config.yaml").read_text())["retrospective"]["auto_merge_learning_pr"])

    def test_cloud_test_secrets_reach_only_the_engineer_staging_step(self):
        secrets = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AZURE_TEST_ACCOUNT_KEY", "GOOGLE_SERVICE_ACCOUNT_KEY")
        stage = "Stage E2E test credentials for bot-test"
        for name, workflow in self.workflows.items():
            steps = workflow["jobs"]["bot"]["steps"] if "bot" in workflow.get("jobs", {}) else []
            with self.subTest(workflow=name):
                if name not in ("engineer-bot.yml", "engineer-bot-followup.yml"):
                    for secret in secrets:
                        self.assertNotIn("secrets." + secret, json.dumps(workflow))
                    continue
                names = [s.get("name") for s in steps]
                i = names.index(stage)
                for j, step in enumerate(steps):
                    text = json.dumps(step)
                    for secret in secrets:
                        if j == i:
                            self.assertEqual(step["env"][secret], "${{ secrets.%s }}" % secret)
                        else:
                            self.assertNotIn("secrets." + secret, text)
                # Staged after the engine install (which refuses ambient credentials) and
                # before any GitHub App token or agent step.
                self.assertLess(names.index("Install reviewed private engine"), i)
                first_token = min(k for k, s in enumerate(steps) if "create-github-app-token" in s.get("uses", ""))
                self.assertLess(i, first_token)
                # The only value the staging step exports is the file's path, never a secret.
                exports = [line for line in steps[i]["run"].splitlines() if "GITHUB_ENV" in line]
                self.assertEqual(len(exports), 1)
                self.assertIn("BOT_E2E_ENV_FILE=%s", exports[0])
                self.assertEqual(steps[-1]["name"], "Remove E2E test credentials")
                self.assertEqual(steps[-1]["if"], "always()")

    def test_all_bot_python_entrypoints_use_isolated_imports(self):
        for workflow in self.workflows.values():
            for step in workflow["jobs"]["bot"]["steps"]:
                for line in step.get("run", "").splitlines():
                    if "-m databricks_bot_engine." in line:
                        self.assertIn("python -I -m ", line)


class InstallBoundaryTests(unittest.TestCase):
    def test_fetch_rejects_multiline_ref_before_auth_or_git(self):
        with tempfile.TemporaryDirectory() as tmp:
            env = {"PATH": os.defpath, "ENGINE_REF": "a" * 40 + "\nmain", "ENGINE_TOKEN": "dummy",
                   "RUNNER_TEMP": tmp, "GITHUB_OUTPUT": str(Path(tmp, "output"))}
            result = subprocess.run(["bash", str(SCRIPTS / "fetch-bot-engine.sh")], env=env, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("40-character", result.stderr)
            self.assertEqual(list(Path(tmp).iterdir()), [])

    def test_install_refuses_ambient_credentials(self):
        with tempfile.TemporaryDirectory() as tmp:
            for key in ("ENGINE_TOKEN", "GH_TOKEN"):
                env = {"PATH": os.defpath, "ENGINE_REF": "a" * 40, "ENGINE_SOURCE": tmp,
                       "RUNNER_TEMP": tmp, "BOT_RUNTIME": tmp, key: "dummy"}
                with self.subTest(key=key):
                    result = subprocess.run(["bash", str(SCRIPTS / "install-bot-engine.sh")], env=env, capture_output=True, text=True)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("Do not pass", result.stderr)


class TestLauncherTests(unittest.TestCase):
    def test_every_launcher_path_uses_an_allowlisted_environment(self):
        cases = [
            (["python-test", "delta_sharing/tests/test_protocol.py", "-k", "test_name"], "bin/uv", "python",
             "run --frozen --python 3.11 dev/pytest delta_sharing/tests/test_protocol.py -k test_name", "17"),
            (["python-lint"], "bin/uv", "python", "run --frozen --python 3.11 dev/lint-python", "17"),
            (["scala-client-test", "SomeSuite"], "build/sbt", "", "client/testOnly SomeSuite", "17"),
            (["scala-client212-test", "SomeSuite"], "build/sbt", "", "++2.12.18 client/testOnly SomeSuite", "17"),
            (["scala-spark-test", "SomeSuite"], "build/sbt", "", "spark/testOnly SomeSuite", "17"),
            (["scala-server-test", "SomeSuite"], "build/sbt", "", "server/testOnly SomeSuite", "8"),
        ]
        excluded = ["DATABRICKS_TOKEN", "BOT_MODEL_TOKEN", "GH_TOKEN", "GITHUB_TOKEN",
                    "MODEL_ENDPOINT", "BOT_MODEL_HOST", "BOT_DATABRICKS_HOST", "ACTIONS_ID_TOKEN_REQUEST_TOKEN",
                    "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
                    "AZURE_TEST_ACCOUNT_KEY", "GOOGLE_SERVICE_ACCOUNT_KEY", "GOOGLE_APPLICATION_CREDENTIALS",
                    "PIP_INDEX_URL", "UNEXPECTED_CALLER_VARIABLE", "GIT_CONFIG_COUNT", "GIT_CONFIG_VALUE_1"]
        for args, executable, cwd, expected_args, java in cases:
            with self.subTest(mode=args[0]), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                (root / "python").mkdir()
                (root / "bin").mkdir()
                fake = root / executable
                fake.parent.mkdir(exist_ok=True)
                fake.write_text('#!/bin/bash\nprintf "%s\\n" "$PWD" "$*" > '
                                + shlex.quote(str(root / "args")) + '\n/usr/bin/env > '
                                + shlex.quote(str(root / "env")) + '\n')
                fake.chmod(0o700)
                env = {"PATH": str(root / "bin") + ":" + os.defpath, "HOME": str(root / "home"),
                       "REPO_ROOT": tmp, "BOT_RUNTIME": tmp, "JAVA_HOME": str(root / "jdk17"),
                       "JAVA_HOME_8_X64": str(root / "jdk8"), "JAVA_HOME_17_X64": str(root / "jdk17"),
                       "CARGO_HOME": str(root / "cargo"), "RUSTUP_HOME": str(root / "rustup"),
                       "RUSTUP_TOOLCHAIN": "stable", **{name: "not-forwarded" for name in excluded},
                       "GIT_CONFIG_GLOBAL": str(root / "untrusted-config"), "GIT_CONFIG_NOSYSTEM": "0"}
                result = subprocess.run(["bash", str(SCRIPTS / "bot-test.sh"), *args], env=env, capture_output=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual((root / "args").read_text().splitlines(), [str(root / cwd), expected_args])
                child = dict(line.split("=", 1) for line in (root / "env").read_text().splitlines())
                for name in excluded:
                    self.assertNotIn(name, child)
                for name in ("HOME", "REPO_ROOT", "BOT_RUNTIME", "JAVA_HOME_8_X64", "JAVA_HOME_17_X64",
                             "CARGO_HOME", "RUSTUP_HOME", "RUSTUP_TOOLCHAIN"):
                    self.assertEqual(child[name], env[name])
                self.assertEqual(child["JAVA_HOME"], str(root / ("jdk" + java)))
                self.assertEqual(child["UV_CACHE_DIR"], str(root / "uv-cache"))
                self.assertEqual(child["SPARK_LOCAL_IP"], "127.0.0.1")
                self.assertEqual(child["GIT_CONFIG_GLOBAL"], "/dev/null")
                self.assertEqual(child["GIT_CONFIG_NOSYSTEM"], "1")
                self.assertEqual(child["GIT_TERMINAL_PROMPT"], "0")

    def test_staged_e2e_credentials_reach_tests_but_nothing_else_does(self):
        forwarded = {"AWS_ACCESS_KEY_ID": "AKIAEXAMPLE", "AWS_SECRET_ACCESS_KEY": "secret value",
                     "AZURE_TEST_ACCOUNT_KEY": "azure-key", "GOOGLE_SERVICE_ACCOUNT_KEY": '{\n "type": "service_account"\n}',
                     "GOOGLE_APPLICATION_CREDENTIALS": "/runtime/e2e/key.json", "AWS_REGION": "us-west-2",
                     "AWS_DEFAULT_REGION": "us-west-2"}
        injected = {"LD_PRELOAD": "/tmp/evil.so", "DATABRICKS_TOKEN": "model", "PATH": "/evil"}
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "python").mkdir()
            (root / "bin").mkdir()
            fake = root / "bin" / "uv"
            fake.write_text('#!/bin/bash\n/usr/bin/env -0 > ' + shlex.quote(str(root / "env")) + '\n')
            fake.chmod(0o700)
            staged = root / "e2e-env"
            staged.write_bytes(b"".join(f"{k}={v}".encode() + b"\0" for k, v in {**forwarded, **injected}.items()))
            env = {"PATH": str(root / "bin") + ":" + os.defpath, "HOME": str(root / "home"), "REPO_ROOT": tmp,
                   "BOT_RUNTIME": tmp, "JAVA_HOME": str(root / "jdk17"), "BOT_E2E_ENV_FILE": str(staged),
                   # Caller-environment copies are still never forwarded.
                   "AZURE_TEST_ACCOUNT_KEY": "from-caller"}
            result = subprocess.run(["bash", str(SCRIPTS / "bot-test.sh"), "python-test",
                                     "delta_sharing/tests/test_protocol.py"], env=env, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            child = dict(r.split("=", 1) for r in (root / "env").read_text().split("\0") if r)
            for name, value in forwarded.items():
                self.assertEqual(child[name], value)
            for name in ("LD_PRELOAD", "DATABRICKS_TOKEN"):
                self.assertNotIn(name, child)
            self.assertEqual(child["PATH"], env["PATH"])
            self.assertNotIn("BOT_E2E_ENV_FILE", child)

    def test_stage_script_writes_a_private_allowlisted_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp, "env")
            env = {"PATH": os.defpath, "AWS_ACCESS_KEY_ID": "AKIAEXAMPLE", "AWS_SECRET_ACCESS_KEY": "s",
                   "AZURE_TEST_ACCOUNT_KEY": "a", "GOOGLE_SERVICE_ACCOUNT_KEY": "{}", "UNRELATED_SECRET": "x"}
            result = subprocess.run([sys.executable, "-I", str(SCRIPTS / "bot-e2e-env.py"), str(out)],
                                    env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(out.stat().st_mode & 0o777, 0o600)
            records = dict(r.split("=", 1) for r in out.read_text().split("\0") if r)
            self.assertEqual(records["GOOGLE_APPLICATION_CREDENTIALS"], str(Path(tmp, "google_service_account_key.json")))
            self.assertEqual(records["AWS_REGION"], "us-west-2")
            self.assertNotIn("UNRELATED_SECRET", records)
            self.assertNotIn("AKIAEXAMPLE", result.stdout + result.stderr)
            # No secrets configured: no file, integration tests skip.
            empty = Path(tmp, "none")
            result = subprocess.run([sys.executable, "-I", str(SCRIPTS / "bot-e2e-env.py"), str(empty)],
                                    env={"PATH": os.defpath}, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0)
            self.assertFalse(empty.exists())

    def test_rejects_unrecognized_commands_and_path_escape(self):
        with tempfile.TemporaryDirectory() as tmp:
            env = {**os.environ, "REPO_ROOT": tmp, "BOT_RUNTIME": tmp}
            for args in (["shell"], ["python-test", "../../outside.py"], ["scala-client-test", "Suite;id"]):
                with self.subTest(args=args):
                    result = subprocess.run(["bash", str(SCRIPTS / "bot-test.sh"), *args], env=env, capture_output=True)
                    self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
