# Ops Knowledge

Workflow-first CLI for collecting, processing, and maintaining a local knowledge root.

Primary entrypoint: `./ops-knowledge`

Compatibility shim: `./knowledge`

## Root

Default root:

```text
components/ops-knowledge/runtime/Knowledge
```

Override:

```bash
./ops-knowledge --root /path/to/Knowledge ...
KNOWLEDGE_ROOT=/path/to/Knowledge ./ops-knowledge ...
```

`KNOWLEDGE_ROOT` wins over `--root`.

## Basic Help

Normal help shows only workflows:

```bash
./ops-knowledge --help
```

Advanced help reveals primitives and workflow composition:

```bash
./ops-knowledge --advanced-help
```

Primitive commands still work directly for agents and power users.

## Workflows

`from-tabs [SOURCE]`

- Import tab JSON into inbox.
- Fetch only the new batch.
- Generate summary notes.
- Generate grounding extraction notes.
- Refresh index.

`from-url URL`

- Capture one URL or local file.
- Fetch only that batch.
- Generate summary note.
- Generate grounding extraction note.
- Refresh index.

`from-library [SOURCE]`

- Import current library source into `exports/library/`.
- Normalize text files.
- Refresh index.
- Source must be passed explicitly or configured in `config.json`.

`maintain`

- Read status.
- Run doctor checks.
- Refresh index.
- Run backup.

All workflows:

- preview first
- ask for confirmation by default
- support `-y` / `--yes` to run immediately
- create a real-time run trace on disk

## Run Trace

Each workflow run gets:

```text
logs/runs/<run_id>/
  state.json
  events.jsonl
  stages/
    01-*.json
    02-*.json
    ...
```

Meaning:

- `state.json`: current live run state
- `events.jsonl`: append-only stage and run events
- `stages/*.json`: accepted result payload of each stage

Failed runs keep partial trace data.

## Stage Contracts

Typed acceptance models live in:

- [workflow_models.py](./workflow_models.py)

This file defines request and result models for:

- workflow run state and events
- capture and import
- fetch
- summarize
- extract
- normalize
- index
- status
- doctor
- backup

## Data Layout

`./ops-knowledge init` creates:

- `inbox/urls.jsonl`
- `articles/raw/`
- `articles/markdown/`
- `notes/articles/`
- `notes/extractions/`
- `exports/`
- `index/knowledge.db`
- `logs/`
- `config.json`

Workflow runs add:

- `logs/runs/<run_id>/`

Notes include run metadata:

- summary notes stay under `notes/articles/`
- extraction notes go under `notes/extractions/<date>/<batch_id>/`

## Primitive Commands

Primitive commands remain available, but are hidden from normal help. Examples:

- `capture-url`
- `import-tabs`
- `fetch-new`
- `summarize`
- `extract`
- `index update`
- `backup`
- `status`
- `doctor`

Use `--advanced-help` for the full mapping from workflow steps to primitives.

## Remote Config

Remote access lives in `config.json`:

```json
{
  "remote_access": {
    "transport": "tailscale-ssh",
    "ssh_command": ["tailscale", "ssh"],
    "executors": {
      "remote": {
        "ssh_target": "user@example-host",
        "backup_destination": "/var/tmp/ops-knowledge-backup"
      }
    }
  }
}
```

Replace the placeholder `ssh_target` before using remote backup or `remote ...`.
