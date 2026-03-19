# delta-sharing CLI

A command-line tool for interacting with [Delta Sharing](https://github.com/delta-io/delta-sharing) servers. Covers all endpoints in the [protocol spec](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md). Zero external dependencies — Python 3.6+ stdlib only.

## Install

Run installs from the **`cli`** directory (the one that contains `setup.py`):

```bash
cd /path/to/delta-sharing/cli
python3 -m pip install -e .
```

Using `python3 -m pip` ensures the `delta-sharing` script is installed next to **that** Python (same as `pip3` only when `pip3` points to the same interpreter).

This registers the `delta-sharing` console script. Pip puts it in the **scripts** directory for your environment. **Always confirm the path for your Python:**

```bash
python3 -c "import sysconfig; print(sysconfig.get_path('scripts'))"
```

Typical locations:

- **Linux / Homebrew / many Pythons (user install, no venv):** often `~/.local/bin`
- **macOS — python.org installer ("Framework" build):** next to that Python, e.g.
  `/Library/Frameworks/Python.framework/Versions/3.13/bin`
  (version matches your `python3`). Adding only `~/.local/bin` will **not** find `delta-sharing` in that setup.
- **Virtualenv:** `.venv/bin` (after `activate`)

If the shell still says `command not found: delta-sharing`:

1. **Put the `scripts` directory on your `PATH`** — use the directory printed by `sysconfig.get_path('scripts')`, not assumed `~/.local/bin`:

   ```bash
   # zsh — example: Framework Python 3.13 on macOS (adjust version to match your python3)
   echo 'export PATH="/Library/Frameworks/Python.framework/Versions/3.13/bin:$PATH"' >> ~/.zshrc
   source ~/.zshrc
   rehash   # zsh: refresh command cache so new PATH is used
   ```

   Or for a typical user-local install on Linux:

   ```bash
   echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.zshrc
   source ~/.zshrc
   rehash
   ```

2. **Verify the script exists:** `ls "$(python3 -c "import sysconfig; print(sysconfig.get_path('scripts'))")"/delta-sharing`

3. **Run without PATH** (from the `cli` repo): `python3 cli.py --help`

Alternatively, install in a virtualenv:

```bash
cd /path/to/delta-sharing/cli
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -e .
# delta-sharing is now on PATH while the venv is active
```

## Quick start

```bash
# Configure a default profile
delta-sharing configure
# Endpoint: https://sharing.example.com/delta-sharing/
# Token: <paste token>
# Skip SSL verification (y/N): n

# Or import from a profile file
delta-sharing configure -f ~/downloads/share-profile.json

# List shares
delta-sharing shares list

# Drill down
delta-sharing schemas list my_share
delta-sharing tables list my_share my_schema
delta-sharing tables query my_share my_schema my_table -l 10
```

## Authentication

The CLI resolves connection info in this order:

1. `--endpoint` + `--token` flags (highest priority, override everything)
2. `--profile` / `-p` — path to a [profile file](#profile-file-json) (JSON, per protocol spec)
3. `--profile-name` / `-P` — named section in `~/.delta-sharing.cfg`
4. `DS_SHARING_PROFILE` env var — path to a profile file
5. `DS_SHARING_PROFILE_NAME` env var — named section lookup
6. `[default]` section in `~/.delta-sharing.cfg` (fallback)

### Named profiles (~/.delta-sharing.cfg)

Profiles are stored in `~/.delta-sharing.cfg` using INI format (like `~/.databrickscfg`):

```ini
[default]
endpoint = https://prod.example.com/delta-sharing/
token    = dapi...

[staging]
endpoint = https://staging.example.com/delta-sharing/
token    = dapi...
insecure = true
```

Create profiles interactively:

```bash
delta-sharing configure           # creates/updates [default]
delta-sharing configure staging   # creates/updates [staging]
```

Import from a profile file:

```bash
delta-sharing configure -f ~/share-profile.json              # import as [default]
delta-sharing configure staging -f ~/share-profile.json      # import as [staging]
delta-sharing configure staging -f ~/share-profile.json -k   # import + skip SSL
```

List configured profiles:

```bash
delta-sharing profiles
```

Switch between profiles:

```bash
delta-sharing shares list -P staging
# or
export DS_SHARING_PROFILE_NAME=staging
delta-sharing shares list
```

### Profile file (JSON)

You can also point to a standalone JSON file following the [protocol spec](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#profile-file-format):

```json
{
  "shareCredentialsVersion": 1,
  "endpoint": "https://sharing.example.com/delta-sharing/",
  "bearerToken": "<token>",
  "expirationTime": "2025-12-01T00:00:00.0Z"
}
```

Usage: `delta-sharing shares list -p ./my-profile.json`

## Commands

### shares

```bash
delta-sharing shares list                       # list shares (one page)
delta-sharing shares list -a                    # list all shares (auto-paginate)
delta-sharing shares list -n 10                 # limit to 10 per page
delta-sharing shares get my_share               # get a single share
```

### schemas

```bash
delta-sharing schemas list my_share             # list schemas in a share
delta-sharing schemas list my_share -a          # auto-paginate
```

### tables

```bash
delta-sharing tables list my_share my_schema    # list tables in a schema
delta-sharing tables list-all my_share          # list all tables across all schemas
```

### tables version

```bash
delta-sharing tables version my_share my_schema my_table
delta-sharing tables version my_share my_schema my_table \
  --starting-timestamp "2025-01-01T00:00:00Z"
```

### tables metadata

```bash
delta-sharing tables metadata my_share my_schema my_table
delta-sharing tables metadata my_share my_schema my_table \
  --response-format delta --reader-features deletionvectors
```

### tables query

```bash
delta-sharing tables query my_share my_schema my_table
delta-sharing tables query my_share my_schema my_table -l 100
delta-sharing tables query my_share my_schema my_table -v 5
delta-sharing tables query my_share my_schema my_table \
  --timestamp "2025-01-01T00:00:00Z"
delta-sharing tables query my_share my_schema my_table \
  --predicate-hints "date > '2025-01-01'" "region = 'us-east'"
delta-sharing tables query my_share my_schema my_table \
  --starting-version 3 --ending-version 10
```

### tables changes

```bash
delta-sharing tables changes my_share my_schema my_table \
  --starting-version 5
delta-sharing tables changes my_share my_schema my_table \
  --starting-timestamp "2025-01-01T00:00:00Z" \
  --ending-timestamp "2025-06-01T00:00:00Z"
delta-sharing tables changes my_share my_schema my_table \
  --starting-version 5 --ending-version 20 \
  --include-historical-metadata true
```

### tables credentials

```bash
delta-sharing tables credentials my_share my_schema my_table \
  -l s3://bucket/path/to/table
```

## Flag reference

### Common flags (all commands)

| Short | Long | Description |
|-------|------|-------------|
| `-p` | `--profile` | Path to profile JSON file |
| `-P` | `--profile-name` | Named profile in `~/.delta-sharing.cfg` |
| `-e` | `--endpoint` | Server URL (overrides profile) |
| `-t` | `--token` | Bearer token (overrides profile) |
| `-V` | `--verbose` | Print request/response details to stderr |
| `-k` | `--insecure` | Skip SSL certificate verification |

### Pagination flags (list commands)

| Short | Long | Description |
|-------|------|-------------|
| `-n` | `--max-results` | Max items per page |
| | `--page-token` | Pagination token from previous response |
| `-a` | `--all` | Auto-paginate and return all results |

### Query flags

| Short | Long | Command | Description |
|-------|------|---------|-------------|
| `-l` | `--limit-hint` | tables query | Hint for max rows |
| `-v` | `--version` | tables query | Specific table version |
| | `--timestamp` | tables query | Time-travel timestamp (ISO 8601) |
| | `--predicate-hints` | tables query | SQL-like predicates for pruning |
| | `--json-predicate-hints` | tables query | JSON predicate for filtering |
| | `--starting-version` | tables query, tables changes | Start of version range |
| | `--ending-version` | tables query, tables changes | End of version range |
| | `--starting-timestamp` | tables changes | CDF start timestamp |
| | `--ending-timestamp` | tables changes | CDF end timestamp |
| | `--response-format` | tables metadata/query/changes | e.g. `delta`, `parquet` |
| | `--reader-features` | tables metadata/query/changes | e.g. `deletionvectors` |

## Debugging

Use `-V` / `--verbose` to print request and response details to stderr:

```bash
delta-sharing shares list -V
# > GET https://prod.example.com/delta-sharing/shares
# > Authorization: Bear...n123
# > Accept: application/json, application/x-ndjson
#
# < 200
# < Content-Type: application/json
# < Body: {"items":[...]}
```

Token is masked in verbose output. Debug info goes to stderr so JSON output can still be piped.

## Output

All output is pretty-printed JSON to stdout. Errors go to stderr as JSON with an `error` or `httpStatus` field.

```bash
# Pipe to jq for further processing
delta-sharing shares list -a | jq '.items[].name'

# Save to file
delta-sharing tables query my_share my_schema my_table > data.json
```
# tom.zhu at ip-10-90-33-20 in ~/universe (git:stack/ds-cli) [21:12:15]
$ cat delta-sharing/docs/README.md
# delta-sharing CLI

A command-line tool for interacting with [Delta Sharing](https://github.com/delta-io/delta-sharing) servers. Covers all endpoints in the [protocol spec](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md). Zero external dependencies — Python 3.6+ stdlib only.

## Install

Run installs from the **`cli`** directory (the one that contains `setup.py`):

```bash
cd /path/to/delta-sharing/cli
python3 -m pip install -e .
```

Using `python3 -m pip` ensures the `delta-sharing` script is installed next to **that** Python (same as `pip3` only when `pip3` points to the same interpreter).

This registers the `delta-sharing` console script. Pip puts it in the **scripts** directory for your environment. **Always confirm the path for your Python:**

```bash
python3 -c "import sysconfig; print(sysconfig.get_path('scripts'))"
```

Typical locations:

- **Linux / Homebrew / many Pythons (user install, no venv):** often `~/.local/bin`
- **macOS — python.org installer ("Framework" build):** next to that Python, e.g.
  `/Library/Frameworks/Python.framework/Versions/3.13/bin`
  (version matches your `python3`). Adding only `~/.local/bin` will **not** find `delta-sharing` in that setup.
- **Virtualenv:** `.venv/bin` (after `activate`)

If the shell still says `command not found: delta-sharing`:

1. **Put the `scripts` directory on your `PATH`** — use the directory printed by `sysconfig.get_path('scripts')`, not assumed `~/.local/bin`:

   ```bash
   # zsh — example: Framework Python 3.13 on macOS (adjust version to match your python3)
   echo 'export PATH="/Library/Frameworks/Python.framework/Versions/3.13/bin:$PATH"' >> ~/.zshrc
   source ~/.zshrc
   rehash   # zsh: refresh command cache so new PATH is used
   ```

   Or for a typical user-local install on Linux:

   ```bash
   echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.zshrc
   source ~/.zshrc
   rehash
   ```

2. **Verify the script exists:** `ls "$(python3 -c "import sysconfig; print(sysconfig.get_path('scripts'))")"/delta-sharing`

3. **Run without PATH** (from the `cli` repo): `python3 cli.py --help`

Alternatively, install in a virtualenv:

```bash
cd /path/to/delta-sharing/cli
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -e .
# delta-sharing is now on PATH while the venv is active
```

## Quick start

```bash
# Configure a default profile
delta-sharing configure
# Endpoint: https://sharing.example.com/delta-sharing/
# Token: <paste token>
# Skip SSL verification (y/N): n

# Or import from a profile file
delta-sharing configure -f ~/downloads/share-profile.json

# List shares
delta-sharing shares list

# Drill down
delta-sharing schemas list my_share
delta-sharing tables list my_share my_schema
delta-sharing tables query my_share my_schema my_table -l 10
```

## Authentication

The CLI resolves connection info in this order:

1. `--endpoint` + `--token` flags (highest priority, override everything)
2. `--profile` / `-p` — path to a [profile file](#profile-file-json) (JSON, per protocol spec)
3. `--profile-name` / `-P` — named section in `~/.delta-sharing.cfg`
4. `DS_SHARING_PROFILE` env var — path to a profile file
5. `DS_SHARING_PROFILE_NAME` env var — named section lookup
6. `[default]` section in `~/.delta-sharing.cfg` (fallback)

### Named profiles (~/.delta-sharing.cfg)

Profiles are stored in `~/.delta-sharing.cfg` using INI format (like `~/.databrickscfg`):

```ini
[default]
endpoint = https://prod.example.com/delta-sharing/
token    = dapi...

[staging]
endpoint = https://staging.example.com/delta-sharing/
token    = dapi...
insecure = true
```

Create profiles interactively:

```bash
delta-sharing configure           # creates/updates [default]
delta-sharing configure staging   # creates/updates [staging]
```

Import from a profile file:

```bash
delta-sharing configure -f ~/share-profile.json              # import as [default]
delta-sharing configure staging -f ~/share-profile.json      # import as [staging]
delta-sharing configure staging -f ~/share-profile.json -k   # import + skip SSL
```

List configured profiles:

```bash
delta-sharing profiles
```

Switch between profiles:

```bash
delta-sharing shares list -P staging
# or
export DS_SHARING_PROFILE_NAME=staging
delta-sharing shares list
```

### Profile file (JSON)

You can also point to a standalone JSON file following the [protocol spec](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#profile-file-format):

```json
{
  "shareCredentialsVersion": 1,
  "endpoint": "https://sharing.example.com/delta-sharing/",
  "bearerToken": "<token>",
  "expirationTime": "2025-12-01T00:00:00.0Z"
}
```

Usage: `delta-sharing shares list -p ./my-profile.json`

## Commands

### shares

```bash
delta-sharing shares list                       # list shares (one page)
delta-sharing shares list -a                    # list all shares (auto-paginate)
delta-sharing shares list -n 10                 # limit to 10 per page
delta-sharing shares get my_share               # get a single share
```

### schemas

```bash
delta-sharing schemas list my_share             # list schemas in a share
delta-sharing schemas list my_share -a          # auto-paginate
```

### tables

```bash
delta-sharing tables list my_share my_schema    # list tables in a schema
delta-sharing tables list-all my_share          # list all tables across all schemas
```

### tables version

```bash
delta-sharing tables version my_share my_schema my_table
delta-sharing tables version my_share my_schema my_table \
  --starting-timestamp "2025-01-01T00:00:00Z"
```

### tables metadata

```bash
delta-sharing tables metadata my_share my_schema my_table
delta-sharing tables metadata my_share my_schema my_table \
  --response-format delta --reader-features deletionvectors
```

### tables query

```bash
delta-sharing tables query my_share my_schema my_table
delta-sharing tables query my_share my_schema my_table -l 100
delta-sharing tables query my_share my_schema my_table -v 5
delta-sharing tables query my_share my_schema my_table \
  --timestamp "2025-01-01T00:00:00Z"
delta-sharing tables query my_share my_schema my_table \
  --predicate-hints "date > '2025-01-01'" "region = 'us-east'"
delta-sharing tables query my_share my_schema my_table \
  --starting-version 3 --ending-version 10
```

### tables changes

```bash
delta-sharing tables changes my_share my_schema my_table \
  --starting-version 5
delta-sharing tables changes my_share my_schema my_table \
  --starting-timestamp "2025-01-01T00:00:00Z" \
  --ending-timestamp "2025-06-01T00:00:00Z"
delta-sharing tables changes my_share my_schema my_table \
  --starting-version 5 --ending-version 20 \
  --include-historical-metadata true
```

### tables credentials

```bash
delta-sharing tables credentials my_share my_schema my_table
delta-sharing tables credentials my_share my_schema my_table \
  -l s3://bucket/path/to/table
```

## Flag reference

### Common flags (all commands)

| Short | Long | Description |
|-------|------|-------------|
| `-p` | `--profile` | Path to profile JSON file |
| `-P` | `--profile-name` | Named profile in `~/.delta-sharing.cfg` |
| `-e` | `--endpoint` | Server URL (overrides profile) |
| `-t` | `--token` | Bearer token (overrides profile) |
| `-V` | `--verbose` | Print request/response details to stderr |
| `-k` | `--insecure` | Skip SSL certificate verification |

### Pagination flags (list commands)

| Short | Long | Description |
|-------|------|-------------|
| `-n` | `--max-results` | Max items per page |
| | `--page-token` | Pagination token from previous response |
| `-a` | `--all` | Auto-paginate and return all results |

### Query flags

| Short | Long | Command | Description |
|-------|------|---------|-------------|
| `-l` | `--limit-hint` | tables query | Hint for max rows |
| `-v` | `--version` | tables query | Specific table version |
| | `--timestamp` | tables query | Time-travel timestamp (ISO 8601) |
| | `--predicate-hints` | tables query | SQL-like predicates for pruning |
| | `--json-predicate-hints` | tables query | JSON predicate for filtering |
| | `--starting-version` | tables query, tables changes | Start of version range |
| | `--ending-version` | tables query, tables changes | End of version range |
| | `--starting-timestamp` | tables changes | CDF start timestamp |
| | `--ending-timestamp` | tables changes | CDF end timestamp |
| | `--response-format` | tables metadata/query/changes | e.g. `delta`, `parquet` |
| | `--reader-features` | tables metadata/query/changes | e.g. `deletionvectors` |

## Debugging

Use `-V` / `--verbose` to print request and response details to stderr:

```bash
delta-sharing shares list -V
# > GET https://prod.example.com/delta-sharing/shares
# > Authorization: Bear...n123
# > Accept: application/json, application/x-ndjson
#
# < 200
# < Content-Type: application/json
# < Body: {"items":[...]}
```

Token is masked in verbose output. Debug info goes to stderr so JSON output can still be piped.

## Output

All output is pretty-printed JSON to stdout. Errors go to stderr as JSON with an `error` or `httpStatus` field.

```bash
# Pipe to jq for further processing
delta-sharing shares list -a | jq '.items[].name'

# Save to file
delta-sharing tables query my_share my_schema my_table > data.json
```