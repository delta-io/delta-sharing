#!/usr/bin/env python3
"""Unit tests for delta-sharing CLI."""

import argparse
import json
import os
import tempfile
import unittest

import cli


class TestUrl(unittest.TestCase):
    def test_simple(self):
        self.assertEqual(
            cli._url("https://host/prefix", "shares"),
            "https://host/prefix/shares",
        )

    def test_trailing_slash(self):
        self.assertEqual(
            cli._url("https://host/prefix/", "shares"),
            "https://host/prefix/shares",
        )

    def test_multiple_parts(self):
        self.assertEqual(
            cli._url("https://h", "shares", "s1", "schemas", "sc1", "tables"),
            "https://h/shares/s1/schemas/sc1/tables",
        )

    def test_query_params(self):
        url = cli._url("https://h", "shares", maxResults=10, pageToken="abc")
        self.assertIn("maxResults=10", url)
        self.assertIn("pageToken=abc", url)

    def test_none_query_params_excluded(self):
        url = cli._url("https://h", "shares", maxResults=None, pageToken="abc")
        self.assertNotIn("maxResults", url)
        self.assertIn("pageToken=abc", url)

    def test_url_encoding(self):
        url = cli._url("https://h", "shares", "name with spaces")
        self.assertIn("name%20with%20spaces", url)

    def test_no_query_params(self):
        url = cli._url("https://h", "shares")
        self.assertNotIn("?", url)


class TestMaskToken(unittest.TestCase):
    def test_long_token(self):
        self.assertEqual(cli._mask_token("abcdefghijklmnop"), "abcd...mnop")

    def test_short_token(self):
        self.assertEqual(cli._mask_token("abc"), "***")

    def test_exactly_8(self):
        self.assertEqual(cli._mask_token("12345678"), "***")

    def test_9_chars(self):
        self.assertEqual(cli._mask_token("123456789"), "1234...6789")


class TestParseNdjson(unittest.TestCase):
    def test_basic(self):
        data = b'{"a":1}\n{"b":2}\n'
        result = cli._parse_ndjson(data)
        self.assertEqual(result, [{"a": 1}, {"b": 2}])

    def test_empty_lines_skipped(self):
        data = b'{"a":1}\n\n{"b":2}\n\n'
        result = cli._parse_ndjson(data)
        self.assertEqual(result, [{"a": 1}, {"b": 2}])

    def test_empty_input(self):
        self.assertEqual(cli._parse_ndjson(b""), [])

    def test_single_line(self):
        data = b'{"protocol":{"minReaderVersion":1}}'
        result = cli._parse_ndjson(data)
        self.assertEqual(result, [{"protocol": {"minReaderVersion": 1}}])


class TestCfgReadWrite(unittest.TestCase):
    def setUp(self):
        self.tmpfile = tempfile.NamedTemporaryFile(
            mode="w", suffix=".cfg", delete=False
        )
        self.tmpfile.close()
        self._orig = cli.CONFIG_FILE
        cli.CONFIG_FILE = self.tmpfile.name

    def tearDown(self):
        cli.CONFIG_FILE = self._orig
        os.unlink(self.tmpfile.name)

    def test_roundtrip(self):
        sections = {
            "default": {"endpoint": "https://prod.example.com", "token": "tok1"},
            "staging": {"endpoint": "https://staging.example.com", "token": "tok2"},
        }
        cli._write_cfg(sections)
        result = cli._read_cfg()
        self.assertEqual(result, sections)

    def test_config_file_mode_restricted_on_unix(self):
        if os.name == "nt":
            self.skipTest("POSIX file modes")
        sections = {"default": {"endpoint": "https://h", "token": "t"}}
        cli._write_cfg(sections)
        mode = os.stat(self.tmpfile.name).st_mode & 0o777
        self.assertEqual(mode, 0o600)

    def test_empty_file(self):
        with open(self.tmpfile.name, "w") as f:
            f.write("")
        self.assertEqual(cli._read_cfg(), {})

    def test_comments_ignored(self):
        with open(self.tmpfile.name, "w") as f:
            f.write("# comment\n[default]\nendpoint = https://h\ntoken = t\n")
        result = cli._read_cfg()
        self.assertEqual(result["default"]["endpoint"], "https://h")

    def test_insecure_field(self):
        sections = {
            "staging": {
                "endpoint": "https://staging.example.com",
                "token": "tok",
                "insecure": "true",
            },
        }
        cli._write_cfg(sections)
        result = cli._read_cfg()
        self.assertEqual(result["staging"]["insecure"], "true")

    def test_missing_file(self):
        cli.CONFIG_FILE = "/nonexistent/path.cfg"
        self.assertEqual(cli._read_cfg(), {})

    def test_overwrite_existing_section(self):
        sections = {"default": {"endpoint": "https://old", "token": "old"}}
        cli._write_cfg(sections)
        sections["default"] = {"endpoint": "https://new", "token": "new"}
        cli._write_cfg(sections)
        result = cli._read_cfg()
        self.assertEqual(result["default"]["endpoint"], "https://new")


class TestLoadProfileFile(unittest.TestCase):
    def setUp(self):
        self.tmpfile = tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        )
        self.tmpfile.close()

    def tearDown(self):
        os.unlink(self.tmpfile.name)

    def test_valid_profile(self):
        profile = {
            "shareCredentialsVersion": 1,
            "endpoint": "https://host/delta-sharing",
            "bearerToken": "mytoken",
        }
        with open(self.tmpfile.name, "w") as f:
            json.dump(profile, f)
        ep, tk = cli._load_profile_file(self.tmpfile.name)
        self.assertEqual(ep, "https://host/delta-sharing")
        self.assertEqual(tk, "mytoken")

    def test_missing_endpoint(self):
        with open(self.tmpfile.name, "w") as f:
            json.dump({"shareCredentialsVersion": 1, "bearerToken": "t"}, f)
        with self.assertRaises(SystemExit):
            cli._load_profile_file(self.tmpfile.name)

    def test_missing_token(self):
        with open(self.tmpfile.name, "w") as f:
            json.dump({"shareCredentialsVersion": 1, "endpoint": "https://h"}, f)
        with self.assertRaises(SystemExit):
            cli._load_profile_file(self.tmpfile.name)

    def test_file_not_found(self):
        with self.assertRaises(SystemExit):
            cli._load_profile_file("/nonexistent/file.json")

    def test_invalid_json(self):
        with open(self.tmpfile.name, "w") as f:
            f.write("not json")
        with self.assertRaises(SystemExit):
            cli._load_profile_file(self.tmpfile.name)

    def test_optional_fields_ignored(self):
        profile = {
            "shareCredentialsVersion": 1,
            "endpoint": "https://h",
            "bearerToken": "t",
            "expirationTime": "2025-01-01T00:00:00Z",
        }
        with open(self.tmpfile.name, "w") as f:
            json.dump(profile, f)
        ep, tk = cli._load_profile_file(self.tmpfile.name)
        self.assertEqual(ep, "https://h")
        self.assertEqual(tk, "t")


class TestLoadCfgSection(unittest.TestCase):
    def setUp(self):
        self.tmpfile = tempfile.NamedTemporaryFile(
            mode="w", suffix=".cfg", delete=False
        )
        self.tmpfile.close()
        self._orig = cli.CONFIG_FILE
        cli.CONFIG_FILE = self.tmpfile.name

    def tearDown(self):
        cli.CONFIG_FILE = self._orig
        os.unlink(self.tmpfile.name)

    def test_valid_section(self):
        cli._write_cfg({"prod": {"endpoint": "https://h", "token": "t"}})
        section = cli._load_cfg_section("prod")
        self.assertEqual(section["endpoint"], "https://h")
        self.assertEqual(section["token"], "t")

    def test_missing_section(self):
        cli._write_cfg({"prod": {"endpoint": "https://h", "token": "t"}})
        with self.assertRaises(SystemExit):
            cli._load_cfg_section("staging")

    def test_missing_endpoint(self):
        cli._write_cfg({"prod": {"token": "t"}})
        with self.assertRaises(SystemExit):
            cli._load_cfg_section("prod")

    def test_missing_token(self):
        cli._write_cfg({"prod": {"endpoint": "https://h"}})
        with self.assertRaises(SystemExit):
            cli._load_cfg_section("prod")

    def test_empty_config(self):
        with self.assertRaises(SystemExit):
            cli._load_cfg_section("default")


class TestApplyCfgSettings(unittest.TestCase):
    def setUp(self):
        cli.INSECURE = False

    def tearDown(self):
        cli.INSECURE = False

    def test_insecure_true(self):
        cli._apply_cfg_settings({"insecure": "true"})
        self.assertTrue(cli.INSECURE)

    def test_insecure_yes(self):
        cli._apply_cfg_settings({"insecure": "yes"})
        self.assertTrue(cli.INSECURE)

    def test_insecure_1(self):
        cli._apply_cfg_settings({"insecure": "1"})
        self.assertTrue(cli.INSECURE)

    def test_insecure_false(self):
        cli._apply_cfg_settings({"insecure": "false"})
        self.assertFalse(cli.INSECURE)

    def test_no_insecure_key(self):
        cli._apply_cfg_settings({"endpoint": "https://h"})
        self.assertFalse(cli.INSECURE)


class TestResolveConnection(unittest.TestCase):
    def setUp(self):
        self.tmpfile = tempfile.NamedTemporaryFile(
            mode="w", suffix=".cfg", delete=False
        )
        self.tmpfile.close()
        self._orig_cfg = cli.CONFIG_FILE
        cli.CONFIG_FILE = self.tmpfile.name
        cli.INSECURE = False
        # Clear env vars
        self._env_backup = {}
        for k in ("DS_SHARING_PROFILE", "DS_SHARING_PROFILE_NAME"):
            self._env_backup[k] = os.environ.pop(k, None)

    def tearDown(self):
        cli.CONFIG_FILE = self._orig_cfg
        cli.INSECURE = False
        os.unlink(self.tmpfile.name)
        for k, v in self._env_backup.items():
            if v is not None:
                os.environ[k] = v

    def _make_args(self, **kwargs):
        defaults = {
            "endpoint": None, "token": None,
            "profile": None, "profile_name": None,
        }
        defaults.update(kwargs)
        return argparse.Namespace(**defaults)

    def test_explicit_flags(self):
        args = self._make_args(endpoint="https://h", token="t")
        ep, tk = cli.resolve_connection(args)
        self.assertEqual(ep, "https://h")
        self.assertEqual(tk, "t")

    def test_cfg_default_fallback(self):
        cli._write_cfg({"default": {"endpoint": "https://cfg", "token": "cfgtok"}})
        args = self._make_args()
        ep, tk = cli.resolve_connection(args)
        self.assertEqual(ep, "https://cfg")
        self.assertEqual(tk, "cfgtok")

    def test_profile_name_flag(self):
        cli._write_cfg({
            "default": {"endpoint": "https://d", "token": "dt"},
            "staging": {"endpoint": "https://s", "token": "st"},
        })
        args = self._make_args(profile_name="staging")
        ep, tk = cli.resolve_connection(args)
        self.assertEqual(ep, "https://s")
        self.assertEqual(tk, "st")

    def test_explicit_flags_override_cfg(self):
        cli._write_cfg({"default": {"endpoint": "https://cfg", "token": "cfgtok"}})
        args = self._make_args(endpoint="https://override", token="overtok")
        ep, tk = cli.resolve_connection(args)
        self.assertEqual(ep, "https://override")
        self.assertEqual(tk, "overtok")

    def test_partial_override_endpoint(self):
        cli._write_cfg({"default": {"endpoint": "https://cfg", "token": "cfgtok"}})
        args = self._make_args(endpoint="https://override")
        ep, tk = cli.resolve_connection(args)
        self.assertEqual(ep, "https://override")
        self.assertEqual(tk, "cfgtok")

    def test_partial_override_token(self):
        cli._write_cfg({"default": {"endpoint": "https://cfg", "token": "cfgtok"}})
        args = self._make_args(token="overtok")
        ep, tk = cli.resolve_connection(args)
        self.assertEqual(ep, "https://cfg")
        self.assertEqual(tk, "overtok")

    def test_insecure_applied_from_cfg(self):
        cli._write_cfg({"default": {
            "endpoint": "https://h", "token": "t", "insecure": "true",
        }})
        args = self._make_args()
        cli.resolve_connection(args)
        self.assertTrue(cli.INSECURE)

    def test_no_config_dies(self):
        # Empty config, no flags
        with self.assertRaises(SystemExit):
            cli.resolve_connection(self._make_args())

    def test_env_profile_name(self):
        cli._write_cfg({"myenv": {"endpoint": "https://env", "token": "envtok"}})
        os.environ["DS_SHARING_PROFILE_NAME"] = "myenv"
        args = self._make_args()
        ep, tk = cli.resolve_connection(args)
        self.assertEqual(ep, "https://env")
        self.assertEqual(tk, "envtok")

    def test_profile_file(self):
        profile_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        )
        json.dump({
            "shareCredentialsVersion": 1,
            "endpoint": "https://json",
            "bearerToken": "jsontok",
        }, profile_file)
        profile_file.close()
        try:
            args = self._make_args(profile=profile_file.name)
            ep, tk = cli.resolve_connection(args)
            self.assertEqual(ep, "https://json")
            self.assertEqual(tk, "jsontok")
        finally:
            os.unlink(profile_file.name)


class TestBuildParser(unittest.TestCase):
    """Verify subcommand structure parses correctly."""

    def setUp(self):
        self.parser = cli.build_parser()

    def test_shares_list(self):
        args = self.parser.parse_args(["shares", "list", "-e", "h", "-t", "t"])
        self.assertEqual(args.group, "shares")
        self.assertEqual(args.command, "list")

    def test_shares_get(self):
        args = self.parser.parse_args(
            ["shares", "get", "myshare", "-e", "h", "-t", "t"]
        )
        self.assertEqual(args.share, "myshare")

    def test_schemas_list(self):
        args = self.parser.parse_args(
            ["schemas", "list", "myshare", "-e", "h", "-t", "t"]
        )
        self.assertEqual(args.share, "myshare")

    def test_tables_list(self):
        args = self.parser.parse_args(
            ["tables", "list", "s", "sc", "-e", "h", "-t", "t"]
        )
        self.assertEqual(args.share, "s")
        self.assertEqual(args.schema, "sc")

    def test_tables_query(self):
        args = self.parser.parse_args([
            "tables", "query", "s", "sc", "tbl",
            "-e", "h", "-t", "t", "-l", "100", "-v", "5",
        ])
        self.assertEqual(args.share, "s")
        self.assertEqual(args.schema, "sc")
        self.assertEqual(args.table, "tbl")
        self.assertEqual(args.limit_hint, 100)
        self.assertEqual(args.version, 5)

    def test_tables_changes(self):
        args = self.parser.parse_args([
            "tables", "changes", "s", "sc", "tbl",
            "-e", "h", "-t", "t",
            "--starting-version", "3", "--ending-version", "10",
        ])
        self.assertEqual(args.starting_version, 3)
        self.assertEqual(args.ending_version, 10)

    def test_tables_credentials_no_location(self):
        args = self.parser.parse_args([
            "tables", "credentials", "s", "sc", "tbl",
            "-e", "h", "-t", "t",
        ])
        self.assertIsNone(args.location)

    def test_tables_credentials_with_location(self):
        args = self.parser.parse_args([
            "tables", "credentials", "s", "sc", "tbl",
            "-e", "h", "-t", "t", "-l", "s3://bucket/path",
        ])
        self.assertEqual(args.location, "s3://bucket/path")

    def test_configure_default_name(self):
        args = self.parser.parse_args(["configure"])
        self.assertEqual(args.name, "default")

    def test_configure_named(self):
        args = self.parser.parse_args(["configure", "staging", "-k"])
        self.assertEqual(args.name, "staging")
        self.assertTrue(args.insecure)

    def test_configure_from_profile(self):
        args = self.parser.parse_args(["configure", "-f", "file.json"])
        self.assertEqual(args.from_profile, "file.json")

    def test_verbose_and_insecure(self):
        args = self.parser.parse_args(
            ["shares", "list", "-e", "h", "-t", "t", "-V", "-k"]
        )
        self.assertTrue(args.verbose)
        self.assertTrue(args.insecure)

    def test_pagination_flags(self):
        args = self.parser.parse_args([
            "shares", "list", "-e", "h", "-t", "t",
            "-n", "25", "--page-token", "abc", "-a",
        ])
        self.assertEqual(args.max_results, 25)
        self.assertEqual(args.page_token, "abc")
        self.assertTrue(args.all)

    def test_tables_list_all(self):
        args = self.parser.parse_args([
            "tables", "list-all", "myshare", "-e", "h", "-t", "t", "-a",
        ])
        self.assertEqual(args.share, "myshare")
        self.assertTrue(args.all)
        self.assertEqual(args.command, "list-all")

    def test_tables_version(self):
        args = self.parser.parse_args([
            "tables", "version", "s", "sc", "tbl", "-e", "h", "-t", "t",
        ])
        self.assertEqual(args.share, "s")
        self.assertEqual(args.schema, "sc")
        self.assertEqual(args.table, "tbl")
        self.assertIsNone(args.starting_timestamp)

    def test_tables_version_with_timestamp(self):
        args = self.parser.parse_args([
            "tables", "version", "s", "sc", "tbl", "-e", "h", "-t", "t",
            "--starting-timestamp", "2025-01-01T00:00:00Z",
        ])
        self.assertEqual(args.starting_timestamp, "2025-01-01T00:00:00Z")

    def test_tables_metadata(self):
        args = self.parser.parse_args([
            "tables", "metadata", "s", "sc", "tbl", "-e", "h", "-t", "t",
            "--response-format", "delta", "--reader-features", "deletionvectors",
        ])
        self.assertEqual(args.response_format, "delta")
        self.assertEqual(args.reader_features, "deletionvectors")

    def test_tables_metadata_defaults(self):
        args = self.parser.parse_args([
            "tables", "metadata", "s", "sc", "tbl", "-e", "h", "-t", "t",
        ])
        self.assertIsNone(args.response_format)
        self.assertIsNone(args.reader_features)

    def test_tables_query_all_flags(self):
        args = self.parser.parse_args([
            "tables", "query", "s", "sc", "tbl", "-e", "h", "-t", "t",
            "--predicate-hints", "x > 1", "y = 'a'",
            "--json-predicate-hints", '{"op":"equal"}',
            "-l", "50", "-v", "3",
            "--timestamp", "2025-01-01T00:00:00Z",
            "--starting-version", "1", "--ending-version", "10",
            "--response-format", "parquet", "--reader-features", "dv",
        ])
        self.assertEqual(args.predicate_hints, ["x > 1", "y = 'a'"])
        self.assertEqual(args.json_predicate_hints, '{"op":"equal"}')
        self.assertEqual(args.limit_hint, 50)
        self.assertEqual(args.version, 3)
        self.assertEqual(args.timestamp, "2025-01-01T00:00:00Z")
        self.assertEqual(args.starting_version, 1)
        self.assertEqual(args.ending_version, 10)
        self.assertEqual(args.response_format, "parquet")
        self.assertEqual(args.reader_features, "dv")

    def test_tables_query_defaults(self):
        args = self.parser.parse_args([
            "tables", "query", "s", "sc", "tbl", "-e", "h", "-t", "t",
        ])
        self.assertIsNone(args.predicate_hints)
        self.assertIsNone(args.json_predicate_hints)
        self.assertIsNone(args.limit_hint)
        self.assertIsNone(args.version)
        self.assertIsNone(args.timestamp)
        self.assertIsNone(args.starting_version)
        self.assertIsNone(args.ending_version)
        self.assertIsNone(args.response_format)
        self.assertIsNone(args.reader_features)

    def test_tables_changes_all_flags(self):
        args = self.parser.parse_args([
            "tables", "changes", "s", "sc", "tbl", "-e", "h", "-t", "t",
            "--starting-version", "5",
            "--starting-timestamp", "2025-01-01T00:00:00Z",
            "--ending-version", "20",
            "--ending-timestamp", "2025-06-01T00:00:00Z",
            "--include-historical-metadata", "true",
            "--response-format", "delta", "--reader-features", "dv",
        ])
        self.assertEqual(args.starting_version, 5)
        self.assertEqual(args.starting_timestamp, "2025-01-01T00:00:00Z")
        self.assertEqual(args.ending_version, 20)
        self.assertEqual(args.ending_timestamp, "2025-06-01T00:00:00Z")
        self.assertEqual(args.include_historical_metadata, "true")
        self.assertEqual(args.response_format, "delta")
        self.assertEqual(args.reader_features, "dv")

    def test_tables_changes_defaults(self):
        args = self.parser.parse_args([
            "tables", "changes", "s", "sc", "tbl", "-e", "h", "-t", "t",
        ])
        self.assertIsNone(args.starting_version)
        self.assertIsNone(args.starting_timestamp)
        self.assertIsNone(args.ending_version)
        self.assertIsNone(args.ending_timestamp)
        self.assertIsNone(args.include_historical_metadata)

    def test_profiles(self):
        args = self.parser.parse_args(["profiles"])
        self.assertEqual(args.group, "profiles")

    def test_version_subcommand(self):
        args = self.parser.parse_args(["version"])
        self.assertEqual(args.group, "version")

    def test_shares_list_with_headers(self):
        args = self.parser.parse_args([
            "shares", "list",
            "-e", "https://h/", "-t", "tok",
            "-H", "X-Custom: one",
            "--header", "X-Other: two",
        ])
        self.assertEqual(args.header, ["X-Custom: one", "X-Other: two"])

    def test_short_flag_profile(self):
        args = self.parser.parse_args([
            "shares", "list", "-p", "file.json",
        ])
        self.assertEqual(args.profile, "file.json")

    def test_short_flag_profile_name(self):
        args = self.parser.parse_args([
            "shares", "list", "-P", "staging",
        ])
        self.assertEqual(args.profile_name, "staging")

    def test_schemas_list_pagination(self):
        args = self.parser.parse_args([
            "schemas", "list", "myshare", "-e", "h", "-t", "t",
            "-n", "10", "-a",
        ])
        self.assertEqual(args.max_results, 10)
        self.assertTrue(args.all)

    def test_tables_list_pagination(self):
        args = self.parser.parse_args([
            "tables", "list", "s", "sc", "-e", "h", "-t", "t",
            "-n", "5", "--page-token", "xyz",
        ])
        self.assertEqual(args.max_results, 5)
        self.assertEqual(args.page_token, "xyz")

    def test_tables_list_all_pagination(self):
        args = self.parser.parse_args([
            "tables", "list-all", "s", "-e", "h", "-t", "t",
            "-n", "20",
        ])
        self.assertEqual(args.max_results, 20)


class TestExtraHeaders(unittest.TestCase):
    def test_none(self):
        self.assertIsNone(cli._extra_headers_from_args(argparse.Namespace(header=None)))

    def test_two_headers(self):
        h = cli._extra_headers_from_args(
            argparse.Namespace(header=["X-Request-Id: abc-123", "X-Other: value:with:colons"])
        )
        self.assertEqual(h["X-Request-Id"], "abc-123")
        self.assertEqual(h["X-Other"], "value:with:colons")

    def test_strips_whitespace(self):
        h = cli._extra_headers_from_args(argparse.Namespace(header=["  Name  :  val  "]))
        self.assertEqual(h, {"Name": "val"})


class TestRequestTimeout(unittest.TestCase):
    def tearDown(self):
        os.environ.pop("DELTA_SHARING_REQUEST_TIMEOUT", None)

    def test_default(self):
        os.environ.pop("DELTA_SHARING_REQUEST_TIMEOUT", None)
        self.assertEqual(cli._request_timeout(), 120.0)

    def test_from_env(self):
        os.environ["DELTA_SHARING_REQUEST_TIMEOUT"] = "60"
        self.assertEqual(cli._request_timeout(), 60.0)

    def test_invalid_env_falls_back(self):
        os.environ["DELTA_SHARING_REQUEST_TIMEOUT"] = "not-a-number"
        self.assertEqual(cli._request_timeout(), 120.0)


if __name__ == "__main__":
    unittest.main()