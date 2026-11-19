"""Offline checks for the consumer-owned bot integration. No model or GitHub calls."""
from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import yaml

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / ".github/scripts"
spec = importlib.util.spec_from_file_location("bot_context", SCRIPTS / "bot-context.py")
context = importlib.util.module_from_spec(spec)
spec.loader.exec_module(context)
REPO = "delta-io/delta-sharing"
PR = {"state": "open", "draft": False,
      "head": {"sha": "a" * 40, "ref": "bot/test", "repo": {"full_name": REPO}},
      "base": {"sha": "b" * 40}, "labels": [{"name": "engineer-bot"}]}


class EventTests(unittest.TestCase):
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

    def test_invalid_head_sha_fails(self):
        with self.assertRaises(ValueError):
            self.call(pr={**PR, "head": {**PR["head"], "sha": "main"}})

    def test_issue_body_comes_from_label_event_not_later_edit(self):
        event = self.event()
        event["issue"] = {"number": 7, "title": "Approved title\nUNTRUSTED=1", "body": "Approved snapshot", "type": {"name": "Bug"}}
        current = {"state": "open", "body": "Later edit", "html_url": "https://github.com/delta-io/delta-sharing/issues/7", "labels": [{"name": "engineer-bot"}]}
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


class WorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # BaseLoader preserves the Actions `on` key instead of YAML 1.1's boolean.
        cls.workflows = {p.name: yaml.load(p.read_text(), Loader=yaml.BaseLoader)
                         for p in (ROOT / ".github/workflows").glob("*bot*.yml")
                         if p.name != "bot-config-checks.yml"}

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
                self.assertIn("github.repository == 'delta-io/delta-sharing'", job["if"])
                self.assertNotIn("id-token", job["permissions"])
                self.assertNotIn("write", job["permissions"].values())

    def test_control_checkout_is_default_branch_and_actor_check_precedes_secrets(self):
        for name, workflow in self.workflows.items():
            with self.subTest(name=name):
                steps = workflow["jobs"]["bot"]["steps"]
                self.assertEqual(steps[0]["with"]["ref"], "${{ github.event.repository.default_branch }}")
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

    def test_no_live_cloud_credentials_are_mapped(self):
        for workflow in self.workflows.values():
            text = json.dumps(workflow)
            for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AZURE_TEST_ACCOUNT_KEY", "GOOGLE_SERVICE_ACCOUNT_KEY"):
                self.assertNotIn("secrets." + name, text)

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
    def test_python_test_is_targeted_and_cloud_credentials_are_removed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "python").mkdir()
            (root / "bin").mkdir()
            fake = root / "bin/uv"
            fake.write_text('#!/bin/bash\nprintf "%s\\n" "$PWD" "$*" "${AWS_ACCESS_KEY_ID-unset}" "${AZURE_TEST_ACCOUNT_KEY-unset}" "${GOOGLE_APPLICATION_CREDENTIALS-unset}" > "$CAPTURE"\n')
            fake.chmod(0o700)
            env = {"PATH": str(root / "bin") + ":" + os.defpath, "REPO_ROOT": tmp, "BOT_RUNTIME": tmp,
                   "CAPTURE": str(root / "capture"), "AWS_ACCESS_KEY_ID": "dummy",
                   "AZURE_TEST_ACCOUNT_KEY": "dummy", "GOOGLE_APPLICATION_CREDENTIALS": "dummy"}
            result = subprocess.run(["bash", str(SCRIPTS / "bot-test.sh"), "python-test",
                                     "delta_sharing/tests/test_protocol.py", "-k", "test_name"], env=env, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            lines = (root / "capture").read_text().splitlines()
            self.assertEqual(lines[0], str(root / "python"))
            self.assertIn("run --frozen --python 3.11 dev/pytest delta_sharing/tests/test_protocol.py -k test_name", lines[1])
            self.assertEqual(lines[2:], ["unset", "unset", "unset"])

    def test_rejects_unrecognized_commands_and_path_escape(self):
        with tempfile.TemporaryDirectory() as tmp:
            env = {**os.environ, "REPO_ROOT": tmp, "BOT_RUNTIME": tmp}
            for args in (["shell"], ["python-test", "../../outside.py"], ["scala-client-test", "Suite;id"]):
                with self.subTest(args=args):
                    result = subprocess.run(["bash", str(SCRIPTS / "bot-test.sh"), *args], env=env, capture_output=True)
                    self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
