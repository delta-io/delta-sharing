#!/usr/bin/env python3
"""
Delta Sharing CLI — interact with a Delta Sharing server from the command line.

Connection resolution (first match wins):
  1. Explicit --endpoint + --token flags
  2. --profile FILE  (path to a Delta Sharing profile JSON file)
  3. --profile-name NAME  (named section in ~/.delta-sharing.cfg)
  4. DS_SHARING_PROFILE env var (path to profile file)
  5. DS_SHARING_PROFILE_NAME env var (named section lookup)
  6. [DEFAULT] section in ~/.delta-sharing.cfg

Config file (~/.delta-sharing.cfg) uses INI format:
  [DEFAULT]
  endpoint = https://prod.example.com/delta-sharing/
  token    = <token>

  [staging]
  endpoint = https://staging.example.com/delta-sharing/
  token    = <token>

A profile file follows the Delta Sharing protocol spec (JSON):
  {
    "shareCredentialsVersion": 1,
    "endpoint": "https://sharing.delta.io/delta-sharing/",
    "bearerToken": "<token>",
    "expirationTime": "2021-11-12T00:12:29.0Z"   (optional)
  }

All output is pretty-printed JSON.
"""

from __future__ import annotations

import argparse
import getpass
import json
import os
import re
import socket
import ssl
import sys
from typing import Dict, Optional
import urllib.error
import urllib.parse
import urllib.request

__version__ = "0.1.0"

# Default HTTP timeout (seconds) for all requests. Override with env DELTA_SHARING_REQUEST_TIMEOUT.
_DEFAULT_REQUEST_TIMEOUT = 120.0

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

CONFIG_FILE = os.path.expanduser("~/.delta-sharing.cfg")
VERBOSE = False
INSECURE = False


def _request_timeout() -> float:
    raw = os.environ.get("DELTA_SHARING_REQUEST_TIMEOUT", "").strip()
    if not raw:
        return _DEFAULT_REQUEST_TIMEOUT
    try:
        t = float(raw)
        if t <= 0:
            return _DEFAULT_REQUEST_TIMEOUT
        return t
    except ValueError:
        return _DEFAULT_REQUEST_TIMEOUT


def _user_agent() -> str:
    return f"delta-sharing-cli/{__version__} (Python {sys.version_info.major}.{sys.version_info.minor})"


def _extra_headers_from_args(args) -> Optional[Dict[str, str]]:
    """Parse repeatable -H/--header 'Name: value' into a dict."""
    raw_list = getattr(args, "header", None) or []
    if not raw_list:
        return None
    out = {}
    for raw in raw_list:
        if ":" not in raw:
            _die(f"Invalid --header (expected 'Name: value'): {raw!r}")
        name, value = raw.split(":", 1)
        name, value = name.strip(), value.strip()
        if not name:
            _die(f"Invalid --header (empty name): {raw!r}")
        out[name] = value
    return out


def _debug(msg):
    if VERBOSE:
        print(msg, file=sys.stderr)


def _mask_token(token):
    if len(token) <= 8:
        return "***"
    return token[:4] + "..." + token[-4:]

_SECTION_RE = re.compile(r"^\[([^\]]+)\]\s*$")
_KV_RE = re.compile(r"^(\w+)\s*=\s*(.*)$")


def _read_cfg():
    """Read .cfg into an ordered dict of {section_name: {key: value}}."""
    sections = {}
    if not os.path.isfile(CONFIG_FILE):
        return sections
    current = None
    with open(CONFIG_FILE) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            m = _SECTION_RE.match(line)
            if m:
                current = m.group(1)
                sections.setdefault(current, {})
                continue
            m = _KV_RE.match(line)
            if m and current is not None:
                sections[current][m.group(1)] = m.group(2).strip()
    return sections


def _write_cfg(sections):
    """Write sections dict to .cfg."""
    lines = []
    for name, kvs in sections.items():
        lines.append(f"[{name}]")
        for k, v in kvs.items():
            lines.append(f"{k} = {v}")
        lines.append("")
    with open(CONFIG_FILE, "w") as f:
        f.write("\n".join(lines))
    # Restrict read access to credentials (Unix-like systems).
    if os.name != "nt":
        try:
            os.chmod(CONFIG_FILE, 0o600)
        except OSError:
            pass


def _load_profile_file(path):
    """Load and validate a Delta Sharing profile JSON file."""
    try:
        with open(path) as f:
            profile = json.load(f)
    except FileNotFoundError:
        _die(f"Profile file not found: {path}")
    except json.JSONDecodeError as e:
        _die(f"Invalid JSON in profile file {path}: {e}")

    if not profile.get("endpoint"):
        _die(f"Profile file missing 'endpoint': {path}")
    if not profile.get("bearerToken"):
        _die(f"Profile file missing 'bearerToken': {path}")
    return profile["endpoint"], profile["bearerToken"]


def _load_cfg_section(name):
    """Load a section from ~/.delta-sharing.cfg as a dict."""
    sections = _read_cfg()
    if not sections:
        _die(f"Config file not found: {CONFIG_FILE}\n"
             f"Run 'delta-sharing configure' to create one.")

    if name not in sections:
        available = ", ".join(sorted(sections.keys())) or "(none)"
        _die(f"Profile '{name}' not found in {CONFIG_FILE}.\n"
             f"Available: {available}")

    section = sections[name]
    if not section.get("endpoint"):
        _die(f"Profile [{name}] missing 'endpoint' in {CONFIG_FILE}")
    if not section.get("token"):
        _die(f"Profile [{name}] missing 'token' in {CONFIG_FILE}")
    return section


def _apply_cfg_settings(section):
    """Apply per-profile settings (like insecure) from a cfg section."""
    global INSECURE
    if section.get("insecure", "").lower() in ("true", "1", "yes"):
        INSECURE = True


def resolve_connection(args):
    """Resolve endpoint + token. Returns (endpoint, token)."""
    section = None

    # 1. Explicit flags take highest priority
    if args.endpoint and args.token:
        return args.endpoint, args.token

    # 2. --profile (JSON file path)
    profile_path = getattr(args, "profile", None)
    if profile_path:
        ep, tk = _load_profile_file(profile_path)
        return args.endpoint or ep, args.token or tk

    # 3. --profile-name (named section in .cfg)
    profile_name = getattr(args, "profile_name", None)
    if profile_name:
        section = _load_cfg_section(profile_name)

    # 4. DS_SHARING_PROFILE env (JSON file path)
    if not section:
        env_path = os.environ.get("DS_SHARING_PROFILE")
        if env_path:
            ep, tk = _load_profile_file(env_path)
            return args.endpoint or ep, args.token or tk

    # 5. DS_SHARING_PROFILE_NAME env (named section)
    if not section:
        env_name = os.environ.get("DS_SHARING_PROFILE_NAME")
        if env_name:
            section = _load_cfg_section(env_name)

    # 6. Fall back to [default] in .cfg
    if not section:
        sections = _read_cfg()
        if "default" in sections:
            s = sections["default"]
            if s.get("endpoint") and s.get("token"):
                section = s

    if section:
        _apply_cfg_settings(section)
        return args.endpoint or section["endpoint"], args.token or section["token"]

    _die("No connection configured. Use one of:\n"
         "  delta-sharing configure   Set up ~/.delta-sharing.cfg\n"
         "  --profile <file>          Profile JSON file\n"
         "  --profile-name <name>     Named profile in ~/.delta-sharing.cfg\n"
         "  --endpoint + --token      Explicit flags\n"
         "  DS_SHARING_PROFILE        Env var (file path)\n"
         "  DS_SHARING_PROFILE_NAME   Env var (named profile)")


def _die(msg):
    print(json.dumps({"error": msg}, indent=2), file=sys.stderr)
    sys.exit(1)


def _request(method, url, token, body=None, headers=None, extra_headers=None):
    """Make an HTTP request and return (status, headers, body_bytes).

    extra_headers: from -H/--header (applied before command-specific headers).
    Authorization, Accept, and User-Agent always use CLI defaults (token wins).
    """
    hdrs = {}
    if extra_headers:
        hdrs.update(extra_headers)
    if headers:
        hdrs.update(headers)
    hdrs["Accept"] = "application/json, application/x-ndjson"
    hdrs["User-Agent"] = _user_agent()
    hdrs["Authorization"] = f"Bearer {token}"

    data = None
    if body is not None:
        data = json.dumps(body).encode()
        hdrs["Content-Type"] = "application/json; charset=utf-8"

    # --- verbose: request ---
    _debug(f"> {method} {url}")
    if VERBOSE:
        for k, v in hdrs.items():
            val = _mask_token(v) if k.lower() == "authorization" else v
            _debug(f"> {k}: {val}")
        if data:
            _debug(f"> Body: {data.decode()}")
        _debug("")

    req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
    ssl_ctx = None
    if INSECURE:
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE
    timeout = _request_timeout()
    try:
        resp = urllib.request.urlopen(req, timeout=timeout, context=ssl_ctx)
        status, resp_hdrs, resp_body = resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        _debug(f"< {e.code} {e.reason}")
        body_bytes = e.read()
        if VERBOSE:
            for k, v in e.headers.items():
                _debug(f"< {k}: {v}")
            _debug(f"< Body: {body_bytes.decode(errors='replace')[:2000]}")
        try:
            err = json.loads(body_bytes)
        except Exception:
            err = {"raw": body_bytes.decode(errors="replace")}
        err["httpStatus"] = e.code
        print(json.dumps(err, indent=2), file=sys.stderr)
        sys.exit(1)
    except urllib.error.URLError as e:
        reason = getattr(e, "reason", e)
        if isinstance(reason, (socket.timeout, TimeoutError)) or (
            isinstance(reason, str) and "timed out" in reason.lower()
        ):
            _die(
                f"Request timed out after {timeout}s. "
                f"Set DELTA_SHARING_REQUEST_TIMEOUT (seconds) or check network/server."
            )
        _die(f"Connection failed: {e.reason}")

    # --- verbose: response ---
    _debug(f"< {status}")
    if VERBOSE:
        for k, v in resp_hdrs.items():
            _debug(f"< {k}: {v}")
        preview = resp_body.decode(errors="replace")[:2000]
        _debug(f"< Body: {preview}")
        if len(resp_body) > 2000:
            _debug(f"< ... ({len(resp_body)} bytes total)")
        _debug("")

    return status, resp_hdrs, resp_body


def _url(base, *parts, **query):
    path = "/".join(urllib.parse.quote(p, safe="") for p in parts)
    url = f"{base.rstrip('/')}/{path}"
    qs = {k: str(v) for k, v in query.items() if v is not None}
    if qs:
        url += "?" + urllib.parse.urlencode(qs)
    return url


def _print_json(obj):
    print(json.dumps(obj, indent=2))


def _parse_ndjson(raw_bytes):
    """Parse newline-delimited JSON into a list of objects."""
    lines = raw_bytes.decode().strip().splitlines()
    return [json.loads(line) for line in lines if line.strip()]


def _collect_pages(base_url, token, max_results=None, extra_headers=None):
    """Auto-paginate a GET list endpoint, yielding all items."""
    all_items = []
    page_token = None
    while True:
        url = base_url
        qs = {}
        if max_results is not None:
            qs["maxResults"] = max_results
        if page_token:
            qs["pageToken"] = page_token
        if qs:
            url += ("&" if "?" in url else "?") + urllib.parse.urlencode(qs)

        _, _, body = _request("GET", url, token, extra_headers=extra_headers)
        data = json.loads(body)
        all_items.extend(data.get("items", []))
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return all_items


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_list_shares(args):
    endpoint, token = resolve_connection(args)
    xh = _extra_headers_from_args(args)
    url = _url(endpoint, "shares")
    if args.all:
        items = _collect_pages(url, token, max_results=args.max_results, extra_headers=xh)
        _print_json({"items": items})
    else:
        url = _url(endpoint, "shares",
                   maxResults=args.max_results, pageToken=args.page_token)
        _, _, body = _request("GET", url, token, extra_headers=xh)
        _print_json(json.loads(body))


def cmd_get_share(args):
    endpoint, token = resolve_connection(args)
    xh = _extra_headers_from_args(args)
    url = _url(endpoint, "shares", args.share)
    _, _, body = _request("GET", url, token, extra_headers=xh)
    _print_json(json.loads(body))


def cmd_list_schemas(args):
    endpoint, token = resolve_connection(args)
    xh = _extra_headers_from_args(args)
    url = _url(endpoint, "shares", args.share, "schemas")
    if args.all:
        items = _collect_pages(url, token, max_results=args.max_results, extra_headers=xh)
        _print_json({"items": items})
    else:
        url = _url(endpoint, "shares", args.share, "schemas",
                   maxResults=args.max_results, pageToken=args.page_token)
        _, _, body = _request("GET", url, token, extra_headers=xh)
        _print_json(json.loads(body))


def cmd_list_tables(args):
    endpoint, token = resolve_connection(args)
    xh = _extra_headers_from_args(args)
    url = _url(endpoint, "shares", args.share, "schemas", args.schema, "tables")
    if args.all:
        items = _collect_pages(url, token, max_results=args.max_results, extra_headers=xh)
        _print_json({"items": items})
    else:
        url = _url(endpoint, "shares", args.share, "schemas", args.schema, "tables",
                   maxResults=args.max_results, pageToken=args.page_token)
        _, _, body = _request("GET", url, token, extra_headers=xh)
        _print_json(json.loads(body))


def cmd_list_all_tables(args):
    endpoint, token = resolve_connection(args)
    xh = _extra_headers_from_args(args)
    url = _url(endpoint, "shares", args.share, "all-tables")
    if args.all:
        items = _collect_pages(url, token, max_results=args.max_results, extra_headers=xh)
        _print_json({"items": items})
    else:
        url = _url(endpoint, "shares", args.share, "all-tables",
                   maxResults=args.max_results, pageToken=args.page_token)
        _, _, body = _request("GET", url, token, extra_headers=xh)
        _print_json(json.loads(body))


def cmd_get_table_version(args):
    endpoint, token = resolve_connection(args)
    xh = _extra_headers_from_args(args)
    url = _url(endpoint, "shares", args.share, "schemas", args.schema,
               "tables", args.table, "version",
               startingTimestamp=args.starting_timestamp)
    _, hdrs, _ = _request("GET", url, token, extra_headers=xh)
    version = hdrs.get("Delta-Table-Version") or hdrs.get("delta-table-version")
    _print_json({"version": int(version) if version else None})


def cmd_get_table_metadata(args):
    endpoint, token = resolve_connection(args)
    xh = _extra_headers_from_args(args)
    url = _url(endpoint, "shares", args.share, "schemas", args.schema,
               "tables", args.table, "metadata")
    hdrs = {}
    if args.response_format or args.reader_features:
        caps = []
        if args.response_format:
            caps.append(f"responseformat={args.response_format}")
        if args.reader_features:
            caps.append(f"readerfeatures={args.reader_features}")
        hdrs["delta-sharing-capabilities"] = ";".join(caps)
    _, resp_hdrs, body = _request("GET", url, token, headers=hdrs, extra_headers=xh)
    version = resp_hdrs.get("Delta-Table-Version") or resp_hdrs.get("delta-table-version")
    records = _parse_ndjson(body)
    _print_json({"version": int(version) if version else None, "lines": records})


def cmd_query_table(args):
    endpoint, token = resolve_connection(args)
    xh = _extra_headers_from_args(args)
    url = _url(endpoint, "shares", args.share, "schemas", args.schema,
               "tables", args.table, "query")

    body = {}
    if args.predicate_hints:
        body["predicateHints"] = args.predicate_hints
    if args.json_predicate_hints:
        body["jsonPredicateHints"] = args.json_predicate_hints
    if args.limit_hint is not None:
        body["limitHint"] = args.limit_hint
    if args.version is not None:
        body["version"] = args.version
    if args.timestamp:
        body["timestamp"] = args.timestamp
    if args.starting_version is not None:
        body["startingVersion"] = args.starting_version
    if args.ending_version is not None:
        body["endingVersion"] = args.ending_version

    hdrs = {}
    if args.response_format or args.reader_features:
        caps = []
        if args.response_format:
            caps.append(f"responseformat={args.response_format}")
        if args.reader_features:
            caps.append(f"readerfeatures={args.reader_features}")
        hdrs["delta-sharing-capabilities"] = ";".join(caps)

    _, resp_hdrs, raw = _request("POST", url, token, body=body, headers=hdrs, extra_headers=xh)
    version = resp_hdrs.get("Delta-Table-Version") or resp_hdrs.get("delta-table-version")
    records = _parse_ndjson(raw)
    _print_json({"version": int(version) if version else None, "lines": records})


def cmd_query_changes(args):
    endpoint, token = resolve_connection(args)
    xh = _extra_headers_from_args(args)
    url = _url(endpoint, "shares", args.share, "schemas", args.schema,
               "tables", args.table, "changes",
               startingVersion=args.starting_version,
               startingTimestamp=args.starting_timestamp,
               endingVersion=args.ending_version,
               endingTimestamp=args.ending_timestamp,
               includeHistoricalMetadata=args.include_historical_metadata)

    hdrs = {}
    if args.response_format or args.reader_features:
        caps = []
        if args.response_format:
            caps.append(f"responseformat={args.response_format}")
        if args.reader_features:
            caps.append(f"readerfeatures={args.reader_features}")
        hdrs["delta-sharing-capabilities"] = ";".join(caps)

    _, resp_hdrs, raw = _request("GET", url, token, headers=hdrs, extra_headers=xh)
    version = resp_hdrs.get("Delta-Table-Version") or resp_hdrs.get("delta-table-version")
    records = _parse_ndjson(raw)
    _print_json({"version": int(version) if version else None, "lines": records})


def cmd_get_temp_creds(args):
    endpoint, token = resolve_connection(args)
    xh = _extra_headers_from_args(args)
    url = _url(endpoint, "shares", args.share, "schemas", args.schema,
               "tables", args.table, "temporary-table-credentials")
    body = {}
    if args.location:
        body["location"] = args.location
    _, _, raw = _request("POST", url, token, body=body or None, extra_headers=xh)
    _print_json(json.loads(raw))


def cmd_configure(args):
    name = args.name
    if args.from_profile:
        endpoint, token = _load_profile_file(args.from_profile)
    else:
        endpoint = input("Endpoint: ").strip()
        if sys.stdin.isatty():
            token = getpass.getpass("Token: ").strip()
        else:
            token = input("Token: ").strip()
        if not endpoint or not token:
            _die("Endpoint and token are required.")

    profile = {"endpoint": endpoint, "token": token}
    if args.insecure:
        profile["insecure"] = "true"
    else:
        insecure = input("Skip SSL verification (y/N): ").strip().lower()
        if insecure in ("y", "yes"):
            profile["insecure"] = "true"

    sections = _read_cfg()
    sections[name] = profile
    _write_cfg(sections)
    print(f"Profile '{name}' saved to {CONFIG_FILE}")


def cmd_profiles(args):
    sections = _read_cfg()
    if not sections:
        _die(f"No config file found at {CONFIG_FILE}. Run 'delta-sharing configure' first.")

    summary = {}
    for name, kvs in sections.items():
        summary[name] = {"endpoint": kvs.get("endpoint", "")}
    _print_json(summary)


def cmd_version(args):
    _print_json(
        {
            "version": __version__,
            "python": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        }
    )


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def _add_common(parser):
    """Add flags shared by all subcommands."""
    parser.add_argument("-p", "--profile",
                        help="Path to Delta Sharing profile JSON file")
    parser.add_argument("-P", "--profile-name",
                        help="Named profile from ~/.delta-sharing.cfg")
    parser.add_argument("-e", "--endpoint",
                        help="Server URL (overrides profile)")
    parser.add_argument("-t", "--token",
                        help="Bearer token (overrides profile)")
    parser.add_argument("-V", "--verbose", action="store_true",
                        help="Print request/response details to stderr")
    parser.add_argument("-k", "--insecure", action="store_true",
                        help="Skip SSL certificate verification")
    parser.add_argument(
        "-H",
        "--header",
        action="append",
        default=None,
        metavar="NAME:VALUE",
        help="Extra HTTP header (repeatable). Example: -H 'X-Request-Id: abc'. "
        "Cannot override Authorization; Accept/User-Agent are always set by the CLI.",
    )


def _add_pagination(parser):
    parser.add_argument("-n", "--max-results", type=int, default=None,
                        help="Max items per page")
    parser.add_argument("--page-token", default=None,
                        help="Pagination token from a previous response")
    parser.add_argument("-a", "--all", action="store_true",
                        help="Auto-paginate and return all results")


def _add_table_path(parser):
    parser.add_argument("share", help="Share name")
    parser.add_argument("schema", help="Schema name")
    parser.add_argument("table", help="Table name")


def _add_capabilities(parser):
    parser.add_argument("--response-format", default=None,
                        help="Response format capability (e.g. delta, parquet)")
    parser.add_argument("--reader-features", default=None,
                        help="Reader features capability (e.g. deletionvectors)")


def build_parser():
    parser = argparse.ArgumentParser(
        prog="delta-sharing",
        description="Delta Sharing CLI — interact with a Delta Sharing server.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
        help="Print program version and exit",
    )
    sub = parser.add_subparsers(dest="group")
    sub.required = True

    # ===================== shares =====================
    shares_parser = sub.add_parser("shares", help="Share commands")
    shares_sub = shares_parser.add_subparsers(dest="command")
    shares_sub.required = True

    p = shares_sub.add_parser("list", help="List shares")
    _add_common(p)
    _add_pagination(p)
    p.set_defaults(func=cmd_list_shares)

    p = shares_sub.add_parser("get", help="Get a share")
    _add_common(p)
    p.add_argument("share", help="Share name")
    p.set_defaults(func=cmd_get_share)

    # ===================== schemas =====================
    schemas_parser = sub.add_parser("schemas", help="Schema commands")
    schemas_sub = schemas_parser.add_subparsers(dest="command")
    schemas_sub.required = True

    p = schemas_sub.add_parser("list", help="List schemas in a share")
    _add_common(p)
    p.add_argument("share", help="Share name")
    _add_pagination(p)
    p.set_defaults(func=cmd_list_schemas)

    # ===================== tables =====================
    tables_parser = sub.add_parser("tables", help="Table commands")
    tables_sub = tables_parser.add_subparsers(dest="command")
    tables_sub.required = True

    p = tables_sub.add_parser("list", help="List tables in a schema")
    _add_common(p)
    p.add_argument("share", help="Share name")
    p.add_argument("schema", help="Schema name")
    _add_pagination(p)
    p.set_defaults(func=cmd_list_tables)

    p = tables_sub.add_parser("list-all", help="List all tables in a share")
    _add_common(p)
    p.add_argument("share", help="Share name")
    _add_pagination(p)
    p.set_defaults(func=cmd_list_all_tables)

    p = tables_sub.add_parser("version", help="Get table version")
    _add_common(p)
    _add_table_path(p)
    p.add_argument("--starting-timestamp", default=None,
                   help="ISO 8601 timestamp to resolve version at")
    p.set_defaults(func=cmd_get_table_version)

    p = tables_sub.add_parser("metadata", help="Get table metadata")
    _add_common(p)
    _add_table_path(p)
    _add_capabilities(p)
    p.set_defaults(func=cmd_get_table_metadata)

    p = tables_sub.add_parser("query", help="Read data from a table")
    _add_common(p)
    _add_table_path(p)
    _add_capabilities(p)
    p.add_argument("--predicate-hints", nargs="+", default=None,
                   help="SQL-like predicate hints for partition pruning")
    p.add_argument("--json-predicate-hints", default=None,
                   help="JSON-encoded predicate for advanced filtering")
    p.add_argument("-l", "--limit-hint", type=int, default=None,
                   help="Hint for max rows to return")
    p.add_argument("-v", "--version", type=int, default=None,
                   help="Specific table version to query")
    p.add_argument("--timestamp", default=None,
                   help="ISO 8601 timestamp for time-travel query")
    p.add_argument("--starting-version", type=int, default=None,
                   help="Starting version for version range query")
    p.add_argument("--ending-version", type=int, default=None,
                   help="Ending version for version range query")
    p.set_defaults(func=cmd_query_table)

    p = tables_sub.add_parser("changes", help="Read Change Data Feed")
    _add_common(p)
    _add_table_path(p)
    _add_capabilities(p)
    p.add_argument("--starting-version", type=int, default=None,
                   help="CDF starting version")
    p.add_argument("--starting-timestamp", default=None,
                   help="CDF starting timestamp (ISO 8601)")
    p.add_argument("--ending-version", type=int, default=None,
                   help="CDF ending version")
    p.add_argument("--ending-timestamp", default=None,
                   help="CDF ending timestamp (ISO 8601)")
    p.add_argument("--include-historical-metadata", default=None,
                   help="Include historical metadata (true/false)")
    p.set_defaults(func=cmd_query_changes)

    p = tables_sub.add_parser("credentials",
                              help="Generate temporary table credentials")
    _add_common(p)
    _add_table_path(p)
    p.add_argument("-l", "--location", default=None,
                   help="Cloud storage location (optional, e.g. s3://bucket/path)")
    p.set_defaults(func=cmd_get_temp_creds)

    # ===================== configure =====================
    p = sub.add_parser("configure",
                       help="Create or update a named profile in ~/.delta-sharing.cfg")
    p.add_argument("name", nargs="?", default="default",
                   help="Profile name (default: 'default')")
    p.add_argument("-f", "--from-profile",
                   help="Import from a Delta Sharing profile JSON file")
    p.add_argument("-k", "--insecure", action="store_true",
                   help="Mark this profile as insecure (skip SSL verification)")
    p.set_defaults(func=cmd_configure)

    # ===================== profiles =====================
    p = sub.add_parser("profiles",
                       help="List all configured named profiles")
    p.set_defaults(func=cmd_profiles)

    p = sub.add_parser("version", help="Print version as JSON (see also --version)")
    p.set_defaults(func=cmd_version)

    return parser


def main():
    global VERBOSE, INSECURE
    parser = build_parser()
    try:
        args = parser.parse_args()
    except SystemExit:
        raise
    VERBOSE = getattr(args, "verbose", False)
    INSECURE = getattr(args, "insecure", False)
    try:
        args.func(args)
    except KeyboardInterrupt:
        print(json.dumps({"error": "interrupted"}), file=sys.stderr)
        sys.exit(130)
    except BrokenPipeError:
        # e.g. stdout closed early: `delta-sharing shares list | head`
        try:
            sys.stdout.close()
        except Exception:
            pass
        sys.exit(0)
    except Exception as e:
        if VERBOSE:
            raise
        _die(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()