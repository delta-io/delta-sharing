#!/usr/bin/env python3
"""Validate a trusted bot trigger before installing the private engine.

Run this ONLY from the default-branch control checkout, with GITHUB_TOKEN's
read-only permissions. No model call, package installation or writes to GitHub.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys

REPOSITORY = "delta-io/delta-sharing"
MAINTAINER_PERMISSIONS = {"admin", "maintain", "write"}
BOT_LOGINS = {"peco-review-bot[bot]", "peco-engineer-bot[bot]"}


def api(route: str) -> dict:
    return json.loads(subprocess.check_output(["gh", "api", route], text=True))


def number(value: object) -> int:
    value = str(value)
    if not re.fullmatch(r"[1-9][0-9]*", value):
        raise ValueError("A positive issue/PR number is required")
    return int(value)


def write_values(path: str, values: dict) -> None:
    with open(path, "a", encoding="utf-8") as stream:
        for key, value in values.items():
            text = str(value)
            delimiter = "BOT_" + secrets.token_hex(24)
            while delimiter in text:
                delimiter = "BOT_" + secrets.token_hex(24)
            stream.write(f"{key}<<{delimiter}\n{text}\n{delimiter}\n")


def is_maintainer(login: str, event: dict, *, allow_bot: bool) -> bool:
    if not re.fullmatch(r"[A-Za-z0-9_\[\]-]+", login):
        return False
    sender = event.get("sender", {})
    if allow_bot and login in BOT_LOGINS and sender.get("login") == login and sender.get("type") == "Bot":
        return True
    return api(f"repos/{REPOSITORY}/collaborators/{login}/permission").get("permission") in MAINTAINER_PERMISSIONS


def prepare(mode: str, event_name: str, event: dict, actor: str, requested: str = "") -> tuple[dict, dict]:
    """Return (step outputs, environment). Kept separate for offline tests."""
    if event.get("repository", {}).get("full_name") != REPOSITORY:
        raise ValueError("Bot workflows are restricted to delta-io/delta-sharing")
    if not is_maintainer(actor, event, allow_bot=mode in {"review", "review-followup", "engineer-followup"}):
        return {"eligible": "false"}, {}
    if mode == "learning":
        return {"eligible": "true"}, {}
    if mode == "engineer":
        item = event.get("issue", {})
        n = number(requested if event_name == "workflow_dispatch" else item.get("number", ""))
        current = api(f"repos/{REPOSITORY}/issues/{n}")
        if current.get("pull_request") or current.get("state") != "open":
            return {"eligible": "false"}, {}
        if "engineer-bot" not in {label["name"] for label in current.get("labels", [])}:
            return {"eligible": "false"}, {}
        # For a label event use the body the maintainer approved AT LABEL TIME,
        # not a later edit by the issue author while this job was queued.
        issue = item if event_name == "issues" else current
        body = issue.get("body") or ""
        Path(os.environ["RUNNER_TEMP"], "issue_body.txt").write_text(body, encoding="utf-8")
        flow = {"Bug": "bug-fix", "Task": "task", "Feature": "task"}.get((issue.get("type") or {}).get("name"), "bug-fix")
        return {"eligible": "true", "flow": flow}, {
            "ISSUE_NUMBER": n, "ISSUE_TITLE": issue.get("title") or "", "ISSUE_URL": current["html_url"],
        }
    n = number(requested if event_name == "workflow_dispatch" else event.get("pull_request", {}).get("number", ""))
    pr = api(f"repos/{REPOSITORY}/pulls/{n}")
    if pr.get("state") != "open" or ((pr.get("head") or {}).get("repo") or {}).get("full_name") != REPOSITORY:
        return {"eligible": "false"}, {}
    if mode == "review" and pr.get("draft"):
        return {"eligible": "false"}, {}
    if mode == "engineer-followup" and "engineer-bot" not in {label["name"] for label in pr.get("labels", [])}:
        return {"eligible": "false"}, {}
    head, base = pr["head"]["sha"], pr["base"]["sha"]
    if not all(re.fullmatch(r"[0-9a-f]{40}", sha) for sha in (head, base)):
        raise ValueError("GitHub returned an invalid commit SHA")
    branch = pr["head"]["ref"]
    subprocess.run(["git", "check-ref-format", "--branch", branch], check=True, capture_output=True)
    return {"eligible": "true", "head": head, "base": base, "branch": branch}, {
        "PR_NUMBER": n, "HEAD_SHA": head, "PR_BASE_SHA": base,
        "TRIGGER_COMMENT_ID": event.get("comment", {}).get("id", ""),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["review", "review-followup", "engineer", "engineer-followup", "learning"])
    args = parser.parse_args()
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    name = os.environ["GITHUB_EVENT_NAME"]
    if name not in {"pull_request", "pull_request_review_comment", "issues", "workflow_dispatch", "schedule"}:
        raise ValueError("Unsupported trigger")
    # The schedule is declared on the default branch, not a user-controlled ref.
    # Use the runner-provided repository context: schedule payloads need not
    # contain the repository object that PR/issue webhooks carry.
    if args.mode == "learning" and name == "schedule":
        if os.environ.get("GITHUB_REPOSITORY") != REPOSITORY:
            raise ValueError("Wrong repository")
        outputs, env = {"eligible": "true"}, {}
    else:
        outputs, env = prepare(args.mode, name, event, os.environ["GITHUB_ACTOR"], os.environ.get("REQUESTED_NUMBER", ""))
    write_values(os.environ["GITHUB_OUTPUT"], outputs)
    if env:
        write_values(os.environ["GITHUB_ENV"], env)
    print(f"Bot trigger: {args.mode}; eligible={outputs['eligible']}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, KeyError, subprocess.CalledProcessError) as exc:
        print(f"::error::Cannot validate bot trigger ({type(exc).__name__}); refusing to run", file=sys.stderr)
        sys.exit(1)
