from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import itertools
import json
import os
import re
import shlex
import shutil
import sqlite3
import subprocess
import textwrap
import urllib.parse
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Iterable

import requests

from workflow_models import (
    BackupRequest,
    BackupResult,
    BatchRef,
    CaptureUrlRequest,
    CaptureUrlResult,
    DoctorRequest,
    DoctorResult,
    ExtractBatchRequest,
    ExtractBatchResult,
    FetchBatchRequest,
    FetchBatchResult,
    ImportLibraryRequest,
    ImportLibraryResult,
    ImportTabsRequest,
    ImportTabsResult,
    IndexUpdateRequest,
    IndexUpdateResult,
    NormalizeExportRequest,
    NormalizeExportResult,
    StageItemResult,
    StatusRequest,
    StatusResult,
    SummarizeBatchRequest,
    SummarizeBatchResult,
    WorkflowEvent,
    WorkflowRunState,
    WorkflowStageSnapshot,
    WorkflowStageStatus,
    model_to_dict,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_ROOT = PROJECT_ROOT
DEFAULT_STATE_DIR_NAME = ".ops-knowledge"
DEFAULT_PAGES_DIR_NAME = "Pages"
DEFAULT_TABS_SOURCE_NAME = "browser-tabs"
DEFAULT_OPERATOR_NAME = "knowledge-worker"
DEFAULT_LOCAL_EXECUTOR = "local"
DEFAULT_REMOTE_EXECUTOR = "remote"
DEFAULT_CLOUD_EXECUTOR = "cloud"
DEFAULT_LIBRARY_APP = "library"
DEFAULT_REMOTE_TARGET_PLACEHOLDER = "user@example-host"
EXECUTOR_NAME_ALIASES = {
    "laptop": DEFAULT_LOCAL_EXECUTOR,
    "home-server": DEFAULT_REMOTE_EXECUTOR,
    "cloud-agent": DEFAULT_CLOUD_EXECUTOR,
}
REMOTE_TARGET_PLACEHOLDERS = {
    DEFAULT_REMOTE_TARGET_PLACEHOLDER,
    "user@remote-magic-address",
}


def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).astimezone().isoformat(timespec="seconds")


def today_iso() -> str:
    return dt.date.today().isoformat()


def relpath_str(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def dump_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(model_to_dict(payload), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(model_to_dict(record), ensure_ascii=False) + "\n")


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for lineno, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSONL at {path}:{lineno}: {exc}") from exc
    return rows


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(model_to_dict(row), ensure_ascii=False) + "\n")


def parse_tags(values: list[str] | None) -> list[str]:
    if not values:
        return []
    tags: list[str] = []
    for value in values:
        for tag in value.split(","):
            clean = tag.strip()
            if clean and clean not in tags:
                tags.append(clean)
    return tags


def safe_filename(value: str, fallback: str = "document") -> str:
    value = re.sub(r"[^\w.-]+", "_", value.strip(), flags=re.UNICODE)
    value = value.strip("._")
    return value or fallback


def now_compact() -> str:
    return dt.datetime.now(dt.timezone.utc).astimezone().strftime("%Y%m%d_%H%M%S")


def build_run_id(workflow: str) -> str:
    return f"run_{safe_filename(workflow, fallback='workflow')}_{now_compact()}_{sha256_text(now_iso())[:6]}"


def build_batch_id(source_kind: str, source_ref: str) -> str:
    digest = sha256_text(f"{source_kind}:{source_ref}:{now_iso()}")[:8]
    return f"batch_{safe_filename(source_kind, fallback='source')}_{now_compact()}_{digest}"


def detect_default_root() -> Path:
    for start in (Path.cwd(), PROJECT_ROOT):
        current = start.resolve()
        for candidate in (current, *current.parents):
            if (candidate / ".obsidian").exists():
                return candidate
    return DEFAULT_ROOT.resolve()


def load_existing_root_config(root: Path) -> tuple[Path, dict[str, Any]]:
    candidates = [
        root / DEFAULT_STATE_DIR_NAME / "config.json",
        root / "config.json",
    ]
    for path in candidates:
        if path.exists():
            return path, load_json(path, {})
    return candidates[0], {}


def resolve_layout_path(root: Path, value: Any) -> Path | None:
    if not isinstance(value, str):
        return None
    clean = value.strip()
    if not clean:
        return None
    path = Path(clean).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (root / path).resolve()


def resolve_layout_paths(root: Path, value: Any) -> list[Path]:
    raw_values = [value] if isinstance(value, str) else value if isinstance(value, list) else []
    resolved: list[Path] = []
    for raw in raw_values:
        path = resolve_layout_path(root, raw)
        if path is not None and path not in resolved:
            resolved.append(path)
    return resolved


def choose_layout_path(root: Path, configured: Any, candidates: Iterable[Path], default: Path) -> Path:
    configured_path = resolve_layout_path(root, configured)
    if configured_path is not None:
        return configured_path
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    return default.resolve()


def detect_obsidian_cli() -> str:
    discovered = shutil.which("obsidian")
    if discovered:
        return discovered
    local_bin = Path.home() / ".local" / "bin" / "obsidian"
    if local_bin.exists():
        return str(local_bin)
    return "obsidian"


def read_source_content(source: str) -> tuple[str, bytes, str]:
    parsed = urllib.parse.urlparse(source)
    if parsed.scheme in {"http", "https"}:
        response = requests.get(
            source,
            timeout=30,
            headers={"User-Agent": "ops-knowledge/0.1 (+local-first pipeline)"},
        )
        response.raise_for_status()
        content_type = response.headers.get("Content-Type", "application/octet-stream")
        return source, response.content, content_type
    if parsed.scheme == "file":
        path = Path(urllib.parse.unquote(parsed.path)).expanduser()
    else:
        path = Path(source).expanduser()
    payload = path.read_bytes()
    mime = "text/html" if path.suffix.lower() in {".html", ".htm"} else "text/plain"
    return str(path.resolve()), payload, mime


def resolve_obsidian_settings(config: dict[str, Any]) -> tuple[str, str]:
    obsidian = config.get("obsidian", {})
    if not isinstance(obsidian, dict):
        obsidian = {}
    cli_path = str(obsidian.get("cli_path") or "").strip() or detect_obsidian_cli()
    vault_name = str(obsidian.get("vault_name") or "").strip() or Path(config.get("knowledge_root", "")).name
    return cli_path, vault_name


def run_obsidian_eval(code: str, config: dict[str, Any]) -> str:
    cli_path, vault_name = resolve_obsidian_settings(config)
    result = subprocess.run(
        [
            cli_path,
            f"vault={vault_name}",
            "eval",
            f"code={code}",
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        message = (result.stderr or result.stdout or "obsidian eval failed").strip()
        raise RuntimeError(message)
    output = (result.stdout or "").strip()
    if output.startswith("=> "):
        return output[3:].strip()
    return output


def html_to_markdown_via_obsidian(html_path: Path, source_url: str, config: dict[str, Any]) -> tuple[str, str]:
    code = textwrap.dedent(
        f"""
        (() => {{
          const fs = require("fs");
          const html = fs.readFileSync({json.dumps(str(html_path))}, "utf8");
          const sourceUrl = {json.dumps(source_url)};
          const doc = new DOMParser().parseFromString(html, "text/html");
          const lines = [];
          const seen = new Set();
          const clean = (value) => (value || "").replace(/\\s+/g, " ").trim();
          const add = (value) => {{
            const line = clean(value);
            if (!line || seen.has(line)) {{
              return;
            }}
            seen.add(line);
            lines.push(line);
          }};
          const title =
            clean(doc.querySelector("sr-rd-title")?.textContent) ||
            clean(doc.querySelector("meta[property='og:title']")?.getAttribute("content")) ||
            clean(doc.querySelector("title")?.textContent) ||
            sourceUrl;
          const desc = clean(doc.querySelector("sr-rd-desc")?.textContent);
          if (desc) {{
            add(`> ${{desc}}`);
          }}
          const root = doc.querySelector("sr-rd-content, article, main, body") || doc.body;
          const nodes = root.querySelectorAll("h1,h2,h3,h4,h5,h6,p,blockquote,pre,li,figcaption,td,th");
          const pushNode = (node) => {{
            const tag = (node.tagName || "").toLowerCase();
            const text = tag === "pre" ? (node.textContent || "").trim() : clean(node.textContent);
            if (!text) {{
              return;
            }}
            if (/^h[1-6]$/.test(tag)) {{
              add(`${{Array(Number(tag.slice(1))).fill("#").join("")}} ${{text}}`);
              return;
            }}
            if (tag === "li") {{
              add(`- ${{text}}`);
              return;
            }}
            if (tag === "blockquote") {{
              add(`> ${{text}}`);
              return;
            }}
            if (tag === "pre") {{
              add(`\\`\\`\\`\\n${{text}}\\n\\`\\`\\``);
              return;
            }}
            add(text);
          }};
          if (nodes.length === 0) {{
            add(root.textContent || "");
          }} else {{
            nodes.forEach(pushNode);
          }}
          return JSON.stringify({{
            title,
            markdown: `# ${{title}}\\n\\nSource: ${{sourceUrl}}\\n\\n${{lines.join("\\n\\n")}}\\n`,
          }});
        }})()
        """
    ).strip()
    output = run_obsidian_eval(code, config)
    payload = json.loads(output)
    title = str(payload.get("title") or source_url).strip() or source_url
    markdown = str(payload.get("markdown") or f"# {title}\n\nSource: {source_url}\n").rstrip() + "\n"
    return title, markdown


def extract_title_from_markdown(text: str, fallback: str) -> str:
    for line in text.splitlines():
        if line.startswith("# "):
            return line[2:].strip()
    return fallback


def extract_markdown_outline(text: str) -> list[str]:
    headings: list[str] = []
    for line in text.splitlines():
        if re.match(r"^#{1,6}\s+", line):
            headings.append(re.sub(r"^#{1,6}\s+", "", line).strip())
    return headings[:10]


def extract_summary_paragraphs(text: str, limit: int = 3) -> list[str]:
    paragraphs = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
    cleaned = [p for p in paragraphs if not p.startswith("# ") and not p.lower().startswith("source:")]
    return cleaned[:limit]


def copy_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def normalize_command_list(value: Any, fallback: list[str]) -> list[str]:
    if isinstance(value, list) and all(isinstance(item, str) and item for item in value):
        return value
    if isinstance(value, str) and value.strip():
        return shlex.split(value.strip())
    return fallback


def merge_defaults(current: Any, defaults: Any) -> Any:
    if isinstance(current, dict) and isinstance(defaults, dict):
        merged = dict(current)
        for key, value in defaults.items():
            if key not in merged:
                merged[key] = value
            else:
                merged[key] = merge_defaults(merged[key], value)
        return merged
    return current


def canonicalize_executor_name(value: str | None) -> str | None:
    if value is None:
        return None
    return EXECUTOR_NAME_ALIASES.get(value, value)


def executor_name_candidates(value: str) -> list[str]:
    canonical = canonicalize_executor_name(value) or value
    candidates = [canonical]
    for alias, target in EXECUTOR_NAME_ALIASES.items():
        if target == canonical and alias not in candidates:
            candidates.append(alias)
    if value not in candidates:
        candidates.append(value)
    return candidates


def get_named_mapping_entry(mapping: Any, name: str) -> tuple[str, dict[str, Any]]:
    if not isinstance(mapping, dict):
        return canonicalize_executor_name(name) or name, {}
    for candidate in executor_name_candidates(name):
        value = mapping.get(candidate)
        if isinstance(value, dict):
            return candidate, value
    return canonicalize_executor_name(name) or name, {}


def get_import_default(config: dict[str, Any], primary_key: str, *legacy_keys: str) -> str | None:
    imports = config.get("imports", {})
    if not isinstance(imports, dict):
        return None
    for key in (primary_key, *legacy_keys):
        value = imports.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def resolve_import_source(
    explicit_source: str | None,
    config: dict[str, Any],
    *,
    primary_key: str,
    legacy_keys: tuple[str, ...] = (),
    label: str,
) -> str:
    if isinstance(explicit_source, str) and explicit_source.strip():
        return explicit_source.strip()
    configured = get_import_default(config, primary_key, *legacy_keys)
    if configured:
        return configured
    raise SystemExit(f"{label} is not configured; pass SOURCE or set config.json imports.{primary_key}")


@dataclass
class KnowledgePaths:
    root: Path
    state_dir: Path
    inbox_dir: Path
    inbox_urls: Path
    articles_raw: Path
    articles_markdown: Path
    articles_markdown_dirs: tuple[Path, ...]
    articles_assets: Path
    notes_articles: Path
    notes_extractions: Path
    exports_dir: Path
    index_dir: Path
    index_db: Path
    logs_dir: Path
    runs_dir: Path
    backups_dir: Path
    config_path: Path


class KnowledgeStore:
    def __init__(self, root: Path | None = None) -> None:
        root_path = Path(os.environ.get("KNOWLEDGE_ROOT") or root or detect_default_root()).expanduser().resolve()
        config_path, existing_config = load_existing_root_config(root_path)
        layout = existing_config.get("layout", {}) if isinstance(existing_config.get("layout"), dict) else {}

        if config_path.parent == root_path:
            detected_state_dir = root_path
        elif (root_path / DEFAULT_STATE_DIR_NAME).exists():
            detected_state_dir = root_path / DEFAULT_STATE_DIR_NAME
        elif any((root_path / name).exists() for name in ("inbox", "index", "logs", "backups")):
            detected_state_dir = root_path
        else:
            detected_state_dir = root_path / DEFAULT_STATE_DIR_NAME
        state_dir = choose_layout_path(root_path, layout.get("state_dir"), [detected_state_dir], detected_state_dir)

        legacy_notes_dir = choose_layout_path(
            root_path,
            layout.get("notes_dir"),
            [root_path / "notes", root_path / "Notes"],
            root_path / "notes",
        )
        notes_articles = choose_layout_path(
            root_path,
            layout.get("summary_dir"),
            [
                state_dir / "summary",
                legacy_notes_dir / "articles",
                legacy_notes_dir / "ops-knowledge" / "articles",
            ],
            state_dir / "summary",
        )
        notes_extractions = choose_layout_path(
            root_path,
            layout.get("extractions_dir"),
            [
                state_dir / "extractions",
                legacy_notes_dir / "extractions",
                legacy_notes_dir / "ops-knowledge" / "extractions",
            ],
            state_dir / "extractions",
        )
        articles_markdown = choose_layout_path(
            root_path,
            layout.get("managed_pages_dir"),
            [
                state_dir / "pages",
                state_dir / "articles" / "markdown",
            ],
            state_dir / "pages",
        )
        configured_pages_dirs = resolve_layout_paths(root_path, layout.get("pages_dir"))
        default_pages_dirs = [
            articles_markdown,
            root_path / "Pages",
            root_path / "pages",
            root_path / "articles" / "markdown",
            root_path / "SimpRead",
        ]
        articles_markdown_dirs = tuple(
            dict.fromkeys(
                configured_pages_dirs
                or [path.resolve() for path in default_pages_dirs if path == articles_markdown or path.exists()]
                or [articles_markdown]
            )
        )
        articles_raw = choose_layout_path(
            root_path,
            layout.get("raw_articles_dir"),
            [
                root_path / "articles" / "raw",
                state_dir / "articles" / "raw",
            ],
            state_dir / "articles" / "raw",
        )
        articles_assets = choose_layout_path(
            root_path,
            layout.get("assets_dir"),
            [
                root_path / "articles" / "assets",
                state_dir / "articles" / "assets",
            ],
            state_dir / "articles" / "assets",
        )
        exports_dir = choose_layout_path(
            root_path,
            layout.get("exports_dir"),
            [
                root_path / "exports",
                state_dir / "exports",
            ],
            state_dir / "exports",
        )
        self.paths = KnowledgePaths(
            root=root_path,
            state_dir=state_dir,
            inbox_dir=state_dir / "inbox",
            inbox_urls=state_dir / "inbox" / "urls.jsonl",
            articles_raw=articles_raw,
            articles_markdown=articles_markdown,
            articles_markdown_dirs=articles_markdown_dirs,
            articles_assets=articles_assets,
            notes_articles=notes_articles,
            notes_extractions=notes_extractions,
            exports_dir=exports_dir,
            index_dir=state_dir / "index",
            index_db=state_dir / "index" / "knowledge.db",
            logs_dir=state_dir / "logs",
            runs_dir=state_dir / "logs" / "runs",
            backups_dir=state_dir / "backups",
            config_path=state_dir / "config.json",
        )

    def default_config(self) -> dict[str, Any]:
        return {
            "knowledge_root": str(self.paths.root),
            "executors": {
                DEFAULT_LOCAL_EXECUTOR: {
                    "enabled": True,
                    "ip_profile": "interactive",
                    "roles": ["interactive", "editing", "review"],
                },
                DEFAULT_REMOTE_EXECUTOR: {
                    "enabled": True,
                    "ip_profile": "safe-ip",
                    "roles": ["fetch", "backup", "scheduled-jobs", "indexing"],
                },
                "mobile": {
                    "enabled": True,
                    "ip_profile": "capture",
                    "roles": ["capture", "review"],
                },
                DEFAULT_CLOUD_EXECUTOR: {
                    "enabled": True,
                    "ip_profile": "public-cloud",
                    "roles": ["batch-processing", "research"],
                },
            },
            "routing_defaults": {
                "capture": DEFAULT_LOCAL_EXECUTOR,
                "fetch_public": DEFAULT_REMOTE_EXECUTOR,
                "fetch_safe_ip": DEFAULT_REMOTE_EXECUTOR,
                "summarize_private": DEFAULT_LOCAL_EXECUTOR,
                "public_batch": DEFAULT_CLOUD_EXECUTOR,
                "backup": DEFAULT_REMOTE_EXECUTOR,
            },
            "remote_access": {
                "transport": "tailscale-ssh",
                "ssh_command": ["tailscale", "ssh"],
                "executors": {
                    DEFAULT_REMOTE_EXECUTOR: {
                        "ssh_target": DEFAULT_REMOTE_TARGET_PLACEHOLDER,
                        "backup_destination": "/var/tmp/ops-knowledge-backup",
                    }
                },
            },
            "backups": {
                "destination": str((PROJECT_ROOT / "backup_mirror").resolve()),
                "last_run_at": None,
            },
            "imports": {
                "library_default_source": "",
                "tabs_default_source": "",
            },
            "obsidian": {
                "cli_path": detect_obsidian_cli(),
                "vault_name": self.paths.root.name,
            },
            "layout": {
                "state_dir": relpath_str(self.paths.state_dir, self.paths.root),
                "summary_dir": relpath_str(self.paths.notes_articles, self.paths.root),
                "extractions_dir": relpath_str(self.paths.notes_extractions, self.paths.root),
                "managed_pages_dir": relpath_str(self.paths.articles_markdown, self.paths.root),
                "pages_dir": [relpath_str(path, self.paths.root) for path in self.paths.articles_markdown_dirs],
                "raw_articles_dir": relpath_str(self.paths.articles_raw, self.paths.root),
                "assets_dir": relpath_str(self.paths.articles_assets, self.paths.root),
                "exports_dir": relpath_str(self.paths.exports_dir, self.paths.root),
            },
            "indexing": {
                "include_folders": [".", *[relpath_str(path, self.paths.root) for path in self.paths.articles_markdown_dirs if not is_relative_to(path, self.paths.root)]],
            },
        }

    def ensure_layout(self) -> None:
        for path in [
            self.paths.root,
            self.paths.state_dir,
            self.paths.inbox_dir,
            self.paths.articles_raw,
            self.paths.articles_markdown,
            self.paths.articles_assets,
            self.paths.notes_articles,
            self.paths.notes_extractions,
            self.paths.exports_dir,
            self.paths.index_dir,
            self.paths.logs_dir,
            self.paths.runs_dir,
            self.paths.backups_dir,
        ]:
            path.mkdir(parents=True, exist_ok=True)
        if not self.paths.inbox_urls.exists():
            self.paths.inbox_urls.write_text("", encoding="utf-8")
        if not self.paths.config_path.exists():
            dump_json(self.paths.config_path, self.default_config())

    def load_config(self) -> dict[str, Any]:
        self.ensure_layout()
        default_config = self.default_config()
        config = merge_defaults(load_json(self.paths.config_path, default_config), default_config)
        if "knowledge_root" not in config:
            config["knowledge_root"] = str(self.paths.root)
        layout = config.get("layout", {})
        if not isinstance(layout, dict):
            layout = {}
            config["layout"] = layout
        indexing = config.get("indexing", {})
        if not isinstance(indexing, dict):
            indexing = {}
            config["indexing"] = indexing
        if isinstance(layout.get("pages_dir"), str):
            layout["pages_dir"] = [layout["pages_dir"]]
        include_folders = indexing.get("include_folders")
        if isinstance(include_folders, list):
            legacy_roots: list[str] = []
            legacy_notes_dir = layout.get("notes_dir")
            if isinstance(legacy_notes_dir, str) and legacy_notes_dir.strip():
                legacy_roots.append(legacy_notes_dir.strip().split("/", 1)[0])
            for path in resolve_layout_paths(self.paths.root, layout.get("pages_dir")):
                relative = relpath_str(path, self.paths.root)
                if not Path(relative).is_absolute():
                    legacy_roots.append(relative.split("/", 1)[0])
            legacy_roots.extend(["SimpRead", "wiki"])
            if "." not in include_folders and include_folders == list(dict.fromkeys(legacy_roots)):
                indexing["include_folders"] = default_config["indexing"]["include_folders"]
        return config

    def load_urls(self) -> list[dict[str, Any]]:
        self.ensure_layout()
        return read_jsonl(self.paths.inbox_urls)

    def save_urls(self, rows: list[dict[str, Any]]) -> None:
        write_jsonl(self.paths.inbox_urls, rows)


@dataclass
class WorkflowStepRecipe:
    name: str
    label: str
    description: str
    primitive_command: str
    stage_kind: str


@dataclass
class WorkflowRecipe:
    name: str
    description: str
    source_kind: str
    steps: tuple[WorkflowStepRecipe, ...]


class WorkflowTrace:
    def __init__(self, store: KnowledgeStore, recipe: WorkflowRecipe, run_id: str, source_ref: str, batch_id: str | None) -> None:
        self.store = store
        self.recipe = recipe
        self.run_id = run_id
        self.source_ref = source_ref
        self.batch_id = batch_id
        self.run_dir = store.paths.runs_dir / run_id
        self.stages_dir = self.run_dir / "stages"
        self.events_path = self.run_dir / "events.jsonl"
        self.state_path = self.run_dir / "state.json"
        self.event_count = 0
        created_at = now_iso()
        self.state = WorkflowRunState(
            run_id=run_id,
            workflow=recipe.name,
            status=WorkflowStageStatus.PENDING,
            created_at=created_at,
            updated_at=created_at,
            source_kind=recipe.source_kind,
            source_ref=source_ref,
            batch_id=batch_id,
            current_stage=None,
            current_step_index=0,
            total_steps=len(recipe.steps),
            stages=[
                WorkflowStageSnapshot(name=step.name, label=step.label, status=WorkflowStageStatus.PENDING)
                for step in recipe.steps
            ],
        )
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.stages_dir.mkdir(parents=True, exist_ok=True)
        self.write_state()

    def write_state(self) -> None:
        self.state.updated_at = now_iso()
        dump_json(self.state_path, self.state)

    def append_event(
        self,
        *,
        event_type: str,
        status: WorkflowStageStatus,
        stage_name: str | None = None,
        message: str = "",
        payload: dict[str, Any] | None = None,
    ) -> None:
        self.event_count += 1
        event = WorkflowEvent(
            run_id=self.run_id,
            workflow=self.recipe.name,
            event_type=event_type,
            at=now_iso(),
            status=status,
            stage_name=stage_name,
            message=message,
            payload=payload,
        )
        append_jsonl(self.events_path, model_to_dict(event))

    def mark_confirmation(self, accepted: bool) -> None:
        self.state.status = WorkflowStageStatus.RUNNING if accepted else WorkflowStageStatus.CANCELED
        self.append_event(
            event_type="confirmation",
            status=self.state.status,
            message="workflow confirmed" if accepted else "workflow canceled before execution",
            payload={"batch_id": self.batch_id},
        )
        self.write_state()

    def start_stage(self, step_index: int, step: WorkflowStepRecipe) -> None:
        snapshot = self.state.stages[step_index]
        snapshot.status = WorkflowStageStatus.RUNNING
        snapshot.started_at = now_iso()
        self.state.current_stage = step.name
        self.state.current_step_index = step_index + 1
        self.state.status = WorkflowStageStatus.RUNNING
        self.append_event(
            event_type="stage_started",
            status=WorkflowStageStatus.RUNNING,
            stage_name=step.name,
            message=step.label,
            payload={"primitive_command": step.primitive_command, "stage_kind": step.stage_kind},
        )
        self.write_state()

    def finish_stage(self, step_index: int, step: WorkflowStepRecipe, result: Any) -> Path:
        stage_path = self.stages_dir / f"{step_index + 1:02d}-{step.name}.json"
        dump_json(stage_path, result)
        snapshot = self.state.stages[step_index]
        snapshot.result_path = relpath_str(stage_path, self.store.paths.root)
        snapshot.status = WorkflowStageStatus.SUCCEEDED
        snapshot.finished_at = now_iso()
        self.append_event(
            event_type="stage_succeeded",
            status=WorkflowStageStatus.SUCCEEDED,
            stage_name=step.name,
            message=step.label,
            payload={"result_path": snapshot.result_path},
        )
        self.write_state()
        return stage_path

    def record_failed_stage(self, step_index: int, step: WorkflowStepRecipe, result: Any, error: str) -> None:
        stage_path = self.stages_dir / f"{step_index + 1:02d}-{step.name}.json"
        dump_json(stage_path, result)
        snapshot = self.state.stages[step_index]
        snapshot.result_path = relpath_str(stage_path, self.store.paths.root)
        self.fail_stage(step_index, step, error)

    def fail_stage(self, step_index: int, step: WorkflowStepRecipe, error: str) -> None:
        snapshot = self.state.stages[step_index]
        snapshot.status = WorkflowStageStatus.FAILED
        snapshot.finished_at = now_iso()
        snapshot.error = error
        self.state.status = WorkflowStageStatus.FAILED
        self.state.current_stage = step.name
        self.append_event(
            event_type="stage_failed",
            status=WorkflowStageStatus.FAILED,
            stage_name=step.name,
            message=error,
            payload={"primitive_command": step.primitive_command},
        )
        self.write_state()

    def finish_run(self, status: WorkflowStageStatus, message: str = "") -> None:
        self.state.status = status
        self.state.current_stage = None
        self.append_event(event_type="run_finished", status=status, message=message, payload={"batch_id": self.batch_id})
        self.write_state()


def build_url_record(args: argparse.Namespace) -> dict[str, Any]:
    return build_url_record_from_values(
        url=args.url,
        title=args.title or "",
        captured_from=args.source,
        priority=args.priority,
        tags=parse_tags(args.tag),
        notes=args.notes or "",
        risk={
            "requires_safe_ip": args.safe_ip,
            "requires_login": args.requires_login,
            "private": args.private,
        },
    )


def canonicalize_source(source: str) -> str:
    parsed = urllib.parse.urlparse(source)
    if parsed.scheme in {"http", "https"}:
        normalized = parsed._replace(
            scheme=parsed.scheme.lower(),
            netloc=parsed.netloc.lower(),
            fragment="",
        )
        return urllib.parse.urlunparse(normalized)
    if parsed.scheme == "file":
        path = Path(urllib.parse.unquote(parsed.path)).expanduser()
        return str(path.resolve())
    return str(Path(source).expanduser().resolve())


def infer_domain(source: str) -> str:
    parsed = urllib.parse.urlparse(source)
    if parsed.scheme in {"http", "https"}:
        return parsed.netloc.lower()
    return "local-file"


def build_url_record_from_values(
    *,
    url: str,
    title: str,
    captured_from: str,
    priority: str,
    tags: list[str],
    notes: str,
    risk: dict[str, Any],
    pool: str = "saved-page-urls",
    item_type: str = "page",
    source_context: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    record_id = f"url_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}_{sha256_text(url)[:8]}"
    record = {
        "id": record_id,
        "url": url,
        "canonical_url": canonicalize_source(url),
        "title": title,
        "captured_at": now_iso(),
        "captured_from": captured_from,
        "status": "new",
        "priority": priority,
        "tags": tags,
        "notes": notes,
        "pool": pool,
        "item_type": item_type,
        "domain": infer_domain(url),
        "risk": risk,
    }
    if source_context:
        record["source_context"] = source_context
    if extra:
        record.update(extra)
    return record


def index_existing_urls(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(row.get("canonical_url", "")): row for row in rows if row.get("canonical_url")}


def set_workflow_metadata(row: dict[str, Any], *, run_id: str, batch_id: str, workflow: str) -> None:
    row["workflow_run_id"] = run_id
    row["batch_id"] = batch_id
    row["workflow"] = workflow
    row["workflow_updated_at"] = now_iso()


def records_for_batch(rows: list[dict[str, Any]], batch_id: str | None) -> list[dict[str, Any]]:
    if not batch_id:
        return rows
    return [row for row in rows if row.get("batch_id") == batch_id]


def resolve_article_path(store: KnowledgeStore, markdown_path: str) -> Path:
    article_path = Path(markdown_path).expanduser()
    if not article_path.is_absolute():
        article_path = (store.paths.root / markdown_path).resolve()
    return article_path


def build_article_markdown_path(store: KnowledgeStore, article_id: str) -> Path:
    return store.paths.articles_markdown / article_id / f"{article_id}.md"


def summary_note_path(store: KnowledgeStore, article_id: str) -> Path:
    return store.paths.notes_articles / f"{article_id}.summary.md"


def extraction_note_path(store: KnowledgeStore, article_id: str, template: str, batch_id: str | None) -> Path:
    date_dir = today_iso()
    batch_dir = batch_id or "manual"
    return store.paths.notes_extractions / date_dir / batch_dir / f"{article_id}.{safe_filename(template, fallback='grounding')}.md"


def command_init(store: KnowledgeStore, _args: argparse.Namespace) -> int:
    store.ensure_layout()
    print(
        json.dumps(
            {
                "knowledge_root": str(store.paths.root),
                "status": "initialized",
                "layout": {
                    "state_dir": relpath_str(store.paths.state_dir, store.paths.root),
                    "summary_dir": relpath_str(store.paths.notes_articles, store.paths.root),
                    "extractions_dir": relpath_str(store.paths.notes_extractions, store.paths.root),
                    "managed_pages_dir": relpath_str(store.paths.articles_markdown, store.paths.root),
                    "pages_dir": [relpath_str(path, store.paths.root) for path in store.paths.articles_markdown_dirs],
                    "raw_articles_dir": relpath_str(store.paths.articles_raw, store.paths.root),
                    "assets_dir": relpath_str(store.paths.articles_assets, store.paths.root),
                    "exports_dir": relpath_str(store.paths.exports_dir, store.paths.root),
                },
            },
            indent=2,
        )
    )
    return 0


def stage_capture_url(store: KnowledgeStore, request: CaptureUrlRequest, *, workflow: str = "capture-url") -> CaptureUrlResult:
    store.ensure_layout()
    rows = store.load_urls()
    record = build_url_record_from_values(
        url=request.url,
        title=request.title,
        captured_from=request.source,
        priority=request.priority,
        tags=request.tags,
        notes=request.notes,
        risk={
            "requires_safe_ip": request.safe_ip,
            "requires_login": request.requires_login,
            "private": request.private,
        },
        extra={
            "workflow": workflow,
            "workflow_run_id": request.run_id,
            "batch_id": request.batch_id,
        },
    )
    existing = index_existing_urls(rows).get(record["canonical_url"])
    created_at = now_iso()
    if existing:
        if record.get("title") and not existing.get("title"):
            existing["title"] = record["title"]
        if record.get("notes"):
            existing["notes"] = record["notes"]
        existing["tags"] = list(dict.fromkeys([*existing.get("tags", []), *record.get("tags", [])]))
        existing["last_seen_at"] = record["captured_at"]
        existing["pool"] = existing.get("pool", record.get("pool", "saved-page-urls"))
        set_workflow_metadata(existing, run_id=request.run_id, batch_id=request.batch_id, workflow=workflow)
        store.save_urls(rows)
        return CaptureUrlResult(
            run_id=request.run_id,
            batch_id=request.batch_id,
            url=request.url,
            canonical_url=record["canonical_url"],
            record_id=str(existing.get("id", "")),
            duplicate=True,
            touched_paths=[relpath_str(store.paths.inbox_urls, store.paths.root)],
            items=[
                StageItemResult(
                    item_id=str(existing.get("id", "")),
                    status=WorkflowStageStatus.SUCCEEDED,
                    source_ref=request.url,
                )
            ],
            status=WorkflowStageStatus.SUCCEEDED,
            created_at=created_at,
        )
    rows.append(record)
    store.save_urls(rows)
    return CaptureUrlResult(
        run_id=request.run_id,
        batch_id=request.batch_id,
        url=request.url,
        canonical_url=record["canonical_url"],
        record_id=str(record["id"]),
        duplicate=False,
        touched_paths=[relpath_str(store.paths.inbox_urls, store.paths.root)],
        items=[
            StageItemResult(
                item_id=str(record["id"]),
                status=WorkflowStageStatus.SUCCEEDED,
                source_ref=request.url,
            )
        ],
        status=WorkflowStageStatus.SUCCEEDED,
        created_at=created_at,
    )


def command_capture_url(store: KnowledgeStore, args: argparse.Namespace) -> int:
    request = CaptureUrlRequest(
        run_id=f"legacy_capture_{now_compact()}",
        batch_id=f"legacy_capture_{now_compact()}",
        url=args.url,
        title=args.title or "",
        source=args.source,
        tags=parse_tags(args.tag),
        notes=args.notes or "",
        priority=args.priority,
        safe_ip=args.safe_ip,
        requires_login=args.requires_login,
        private=args.private,
    )
    result = stage_capture_url(store, request)
    print(json.dumps(model_to_dict(result), indent=2, ensure_ascii=False))
    return 0


def command_inbox(store: KnowledgeStore, args: argparse.Namespace) -> int:
    rows = store.load_urls()
    if args.status:
        rows = [row for row in rows if row.get("status") == args.status]
    if getattr(args, "pool", None):
        rows = [row for row in rows if row.get("pool") == args.pool]
    if getattr(args, "domain", None):
        rows = [row for row in rows if row.get("domain") == args.domain]
    if getattr(args, "captured_from", None):
        rows = [row for row in rows if row.get("captured_from") == args.captured_from]
    if args.limit:
        rows = rows[: args.limit]
    print(json.dumps(rows, indent=2, ensure_ascii=False))
    return 0


def stage_import_tabs(store: KnowledgeStore, request: ImportTabsRequest, *, workflow: str = "import-tabs") -> ImportTabsResult:
    config = store.load_config()
    source_value = resolve_import_source(
        request.source,
        config,
        primary_key="tabs_default_source",
        label="tabs source",
    )
    source = Path(source_value).expanduser().resolve()
    payload = load_json(source, [])
    if not isinstance(payload, list):
        raise SystemExit(f"tabs source must be a JSON array: {source}")

    rows = store.load_urls()
    existing = index_existing_urls(rows)
    imported = 0
    merged = 0
    skipped = 0
    created_record_ids: list[str] = []
    merged_record_ids: list[str] = []
    items: list[StageItemResult] = []

    for index, item in enumerate(payload, start=1):
        if request.limit and imported + merged + skipped >= request.limit:
            break
        if not isinstance(item, dict):
            skipped += 1
            continue
        url = str(item.get("url", "")).strip()
        if not url:
            skipped += 1
            continue
        canonical_url = canonicalize_source(url)
        title = str(item.get("title", "")).strip()
        item_type = str(item.get("type", "page")).strip() or "page"
        record = build_url_record_from_values(
            url=url,
            title=title,
            captured_from=request.source_name,
            priority=request.priority,
            tags=request.tags,
            notes=request.notes,
            risk={
                "requires_safe_ip": False,
                "requires_login": False,
                "private": False,
            },
            pool="saved-page-urls",
            item_type=item_type,
            source_context={
                "path": str(source),
                "item_index": index,
            },
            extra={
                "source_item": {"title": title, "type": item_type},
                "workflow": workflow,
                "workflow_run_id": request.run_id,
                "batch_id": request.batch_id,
            },
        )
        existing_row = existing.get(canonical_url)
        if existing_row:
            if title and not existing_row.get("title"):
                existing_row["title"] = title
            existing_row["last_seen_at"] = now_iso()
            existing_row["source_context"] = {"path": str(source), "item_index": index}
            existing_row["source_item"] = {"title": title, "type": item_type}
            existing_row["tags"] = list(dict.fromkeys([*existing_row.get("tags", []), *request.tags]))
            set_workflow_metadata(existing_row, run_id=request.run_id, batch_id=request.batch_id, workflow=workflow)
            merged += 1
            merged_record_ids.append(str(existing_row.get("id", "")))
            items.append(
                StageItemResult(
                    item_id=str(existing_row.get("id", "")),
                    status=WorkflowStageStatus.SUCCEEDED,
                    source_ref=url,
                )
            )
            continue
        rows.append(record)
        existing[canonical_url] = record
        imported += 1
        created_record_ids.append(str(record["id"]))
        items.append(
            StageItemResult(
                item_id=str(record["id"]),
                status=WorkflowStageStatus.SUCCEEDED,
                source_ref=url,
            )
        )

    store.save_urls(rows)
    return ImportTabsResult(
        run_id=request.run_id,
        batch_id=request.batch_id,
        source=str(source),
        captured_from=request.source_name,
        imported=imported,
        merged=merged,
        skipped=skipped,
        total_inbox=len(rows),
        created_record_ids=created_record_ids,
        merged_record_ids=merged_record_ids,
        touched_paths=[relpath_str(store.paths.inbox_urls, store.paths.root)],
        items=items,
        status=WorkflowStageStatus.SUCCEEDED,
        created_at=now_iso(),
    )


def command_import_tabs(store: KnowledgeStore, args: argparse.Namespace) -> int:
    config = store.load_config()
    source = resolve_import_source(
        args.source,
        config,
        primary_key="tabs_default_source",
        label="tabs source",
    )
    request = ImportTabsRequest(
        run_id=f"legacy_tabs_{now_compact()}",
        batch_id=f"legacy_tabs_{now_compact()}",
        source=source,
        source_name=args.source_name,
        tags=parse_tags(args.tag),
        notes=args.notes or "",
        priority=args.priority,
        limit=args.limit,
    )
    result = stage_import_tabs(store, request)
    print(json.dumps(model_to_dict(result), indent=2, ensure_ascii=False))
    return 0


def choose_executor(record: dict[str, Any], executor: str | None, config: dict[str, Any]) -> str:
    if executor:
        return canonicalize_executor_name(executor) or executor
    risk = record.get("risk", {})
    if risk.get("requires_safe_ip"):
        return canonicalize_executor_name(config.get("routing_defaults", {}).get("fetch_safe_ip", DEFAULT_REMOTE_EXECUTOR)) or DEFAULT_REMOTE_EXECUTOR
    return canonicalize_executor_name(config.get("routing_defaults", {}).get("fetch_public", DEFAULT_REMOTE_EXECUTOR)) or DEFAULT_REMOTE_EXECUTOR


def choose_backup_executor(executor: str | None, config: dict[str, Any]) -> str:
    if executor:
        return canonicalize_executor_name(executor) or executor
    return canonicalize_executor_name(config.get("routing_defaults", {}).get("backup", DEFAULT_REMOTE_EXECUTOR)) or DEFAULT_REMOTE_EXECUTOR


def get_remote_executor_config(config: dict[str, Any], executor: str) -> dict[str, Any]:
    remote_access = config.get("remote_access", {})
    executors = remote_access.get("executors", {})
    _, value = get_named_mapping_entry(executors, executor)
    return value


def build_remote_target(config: dict[str, Any], executor: str) -> str:
    remote = get_remote_executor_config(config, executor)
    target = remote.get("ssh_target")
    if isinstance(target, str) and target.strip():
        return target.strip()
    user = remote.get("user")
    magic_address = remote.get("magic_address")
    if isinstance(user, str) and user.strip() and isinstance(magic_address, str) and magic_address.strip():
        return f"{user.strip()}@{magic_address.strip()}"
    raise SystemExit(
        f"remote target for executor '{executor}' is not configured; set remote_access.executors.{executor}.ssh_target or .user and .magic_address"
    )


def build_remote_ssh_base_command(config: dict[str, Any], executor: str) -> list[str]:
    remote_access = config.get("remote_access", {})
    ssh_command = normalize_command_list(remote_access.get("ssh_command"), ["tailscale", "ssh"])
    return [*ssh_command, build_remote_target(config, executor)]


def run_remote_command(
    config: dict[str, Any],
    executor: str,
    remote_command: list[str],
    *,
    capture_output: bool = True,
) -> subprocess.CompletedProcess[str]:
    command = [*build_remote_ssh_base_command(config, executor), "--", *remote_command]
    try:
        return subprocess.run(command, text=True, capture_output=capture_output, check=False)
    except FileNotFoundError as exc:
        raise SystemExit(f"remote transport command not found: {exc.filename}") from exc


def validate_remote_backup_destination(destination: str) -> str:
    clean = destination.strip()
    if not clean:
        raise SystemExit("remote backup destination cannot be empty")
    if clean in {"/", "/home", "/root", "/var", "/tmp"}:
        raise SystemExit(f"unsafe remote backup destination: {clean}")
    return clean


def backup_to_remote_executor(store: KnowledgeStore, config: dict[str, Any], executor: str, destination: str) -> dict[str, Any]:
    remote_destination = validate_remote_backup_destination(destination)
    prepare_script = textwrap.dedent(
        """\
        from pathlib import Path
        import shutil
        import sys

        target = Path(sys.argv[1])
        if str(target) in {"/", "/home", "/root", "/var", "/tmp"}:
            raise SystemExit(f"unsafe remote backup destination: {target}")
        if target.exists():
            shutil.rmtree(target)
        target.mkdir(parents=True, exist_ok=True)
        """
    ).strip()
    prepared = run_remote_command(
        config,
        executor,
        ["python3", "-c", prepare_script, remote_destination],
    )
    if prepared.returncode != 0:
        raise SystemExit((prepared.stderr or prepared.stdout or "remote prepare failed").strip())

    tar_command = ["tar", "-cf", "-", "-C", str(store.paths.root), "."]
    remote_extract_command = [*build_remote_ssh_base_command(config, executor), "--", "tar", "-xf", "-", "-C", remote_destination]
    try:
        producer = subprocess.Popen(tar_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=False)
        consumer = subprocess.Popen(
            remote_extract_command,
            stdin=producer.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=False,
        )
    except FileNotFoundError as exc:
        raise SystemExit(f"backup command not found: {exc.filename}") from exc
    assert producer.stdout is not None
    producer.stdout.close()
    consumer_stdout, consumer_stderr = consumer.communicate()
    producer_stderr = b""
    if producer.stderr is not None:
        producer_stderr = producer.stderr.read()
        producer.stderr.close()
    producer_returncode = producer.wait()
    if producer_returncode != 0:
        raise SystemExit((producer_stderr.decode("utf-8", errors="replace") or "local tar failed").strip())
    if consumer.returncode != 0:
        message = consumer_stderr.decode("utf-8", errors="replace") or consumer_stdout.decode("utf-8", errors="replace")
        raise SystemExit((message or "remote tar extract failed").strip())

    return {
        "destination": remote_destination,
        "executor": executor,
        "transport": config.get("remote_access", {}).get("transport", "tailscale-ssh"),
        "target": build_remote_target(config, executor),
        "mode": "remote",
    }


def build_article_id(source: str, content_hash: str) -> str:
    stem = safe_filename(Path(urllib.parse.urlparse(source).path).stem or "article", fallback="article")
    return f"{stem}_{content_hash[:10]}"


def stage_fetch_batch(store: KnowledgeStore, request: FetchBatchRequest) -> FetchBatchResult:
    rows = store.load_urls()
    config = store.load_config()
    pending = [row for row in records_for_batch(rows, request.batch_id) if row.get("status") == "new"]
    if request.limit:
        pending = pending[: request.limit]
    results: list[StageItemResult] = []
    touched_paths: list[str] = [relpath_str(store.paths.inbox_urls, store.paths.root)]
    selected_records = [str(row.get("id", "")) for row in pending]
    fetched = 0
    failed = 0
    for row in pending:
        source = row["url"]
        executor = choose_executor(row, request.executor, config)
        try:
            resolved_source, payload, content_type = read_source_content(source)
            content_hash = sha256_bytes(payload)
            article_id = build_article_id(resolved_source, content_hash)
            raw_ext = ".html" if "html" in content_type.lower() else ".txt"
            raw_path = store.paths.articles_raw / f"{article_id}{raw_ext}"
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            raw_path.write_bytes(payload)

            text = payload.decode("utf-8", errors="replace")
            if "html" in content_type.lower():
                try:
                    title, markdown = html_to_markdown_via_obsidian(raw_path, resolved_source, config)
                except Exception:
                    title = resolved_source
                    body = re.sub(r"\s+", " ", text).strip()
                    markdown = f"# {title}\n\nSource: {resolved_source}\n\n{body}\n"
            else:
                title = extract_title_from_markdown(text, fallback=resolved_source)
                markdown = text

            md_path = build_article_markdown_path(store, article_id)
            md_path.parent.mkdir(parents=True, exist_ok=True)
            md_path.write_text(markdown, encoding="utf-8")

            article_record = {
                "id": article_id,
                "source_url": source,
                "resolved_source": resolved_source,
                "title": title,
                "fetched_at": now_iso(),
                "content_hash": content_hash,
                "raw_path": relpath_str(raw_path, store.paths.root),
                "markdown_path": relpath_str(md_path, store.paths.root),
                "status": "fetched",
                "executor": executor,
                "content_type": content_type,
                "workflow_run_id": request.run_id,
                "batch_id": request.batch_id,
            }
            append_jsonl(store.paths.logs_dir / "articles.jsonl", article_record)
            row["status"] = "fetched"
            row["article_id"] = article_id
            row["last_fetched_at"] = article_record["fetched_at"]
            row["executor"] = executor
            row["markdown_path"] = article_record["markdown_path"]
            row["workflow_run_id"] = request.run_id
            if request.batch_id:
                row["batch_id"] = request.batch_id
            if not row.get("title"):
                row["title"] = title
            fetched += 1
            touched_paths.extend([article_record["raw_path"], article_record["markdown_path"], relpath_str(store.paths.logs_dir / "articles.jsonl", store.paths.root)])
            results.append(
                StageItemResult(
                    item_id=article_id,
                    status=WorkflowStageStatus.SUCCEEDED,
                    source_ref=source,
                    output_path=article_record["markdown_path"],
                )
            )
        except Exception as exc:
            row["status"] = "fetch_error"
            row["last_error"] = str(exc)
            failed += 1
            results.append(
                StageItemResult(
                    item_id=str(row.get("id", "")),
                    status=WorkflowStageStatus.FAILED,
                    source_ref=source,
                    error=str(exc),
                )
            )
    store.save_urls(rows)
    status = WorkflowStageStatus.SUCCEEDED if failed == 0 else WorkflowStageStatus.FAILED
    return FetchBatchResult(
        run_id=request.run_id,
        batch_id=request.batch_id,
        selected_records=selected_records,
        fetched=fetched,
        failed=failed,
        touched_paths=list(dict.fromkeys(touched_paths)),
        items=results,
        status=status,
        created_at=now_iso(),
    )


def command_fetch_new(store: KnowledgeStore, args: argparse.Namespace) -> int:
    request = FetchBatchRequest(
        run_id=f"legacy_fetch_{now_compact()}",
        batch_id=getattr(args, "batch_id", None),
        executor=args.executor,
        limit=args.limit,
    )
    result = stage_fetch_batch(store, request)
    print(json.dumps(model_to_dict(result), indent=2, ensure_ascii=False))
    return 0


def load_article_context(article_path: Path) -> tuple[str, str, str]:
    text = article_path.read_text(encoding="utf-8")
    article_id = article_path.stem
    title = extract_title_from_markdown(text, fallback=article_id)
    return article_id, title, text


def build_extraction_questions(batch: str, title: str, assumptions: list[str], goal: str) -> list[str]:
    batch_templates = {
        "grounding": [
            f"What concrete, to-ground facts, processes, or observations does '{title}' provide?",
            "Which claims in the article are directly supported by examples, data, experiments, or implementation details?",
            "What parts are merely interpretive, speculative, or rhetorical rather than grounded?",
            "Which passages are worth indexing as high-signal reference points later?",
        ],
        "assumption-check": [
            f"Which parts of '{title}' support, weaken, or falsify my current assumptions?",
            "What counterexamples, caveats, or scope limits appear in the article?",
            "What does the article imply that would change how I act or prioritize?",
        ],
        "mental-model-update": [
            f"What mental model does '{title}' reinforce, refine, or overturn?",
            "What causal mechanism or structural pattern is described clearly enough to reuse elsewhere?",
            "What is the smallest durable model update worth preserving in the knowledge base?",
        ],
        "implementation-detail": [
            f"What implementation details, mechanisms, workflows, or constraints from '{title}' are operationally reusable?",
            "What inputs, outputs, and prerequisites are explicit enough to turn into a checklist or SOP?",
            "What is missing if I wanted to reproduce or apply the method described?",
        ],
    }
    questions = list(batch_templates.get(batch, batch_templates["grounding"]))
    if goal:
        questions.insert(0, f"Primary goal: {goal}")
    for assumption in assumptions:
        questions.append(f"Assumption check: Does the article support, refine, or disprove this assumption: {assumption}?")
    return questions


def build_extraction_note_markdown(
    *,
    article_id: str,
    title: str,
    source_url: str,
    body: str,
    batch: str,
    goal: str,
    assumptions: list[str],
    model: str,
    operator: str,
    executor: str,
    run_id: str | None = None,
    batch_id: str | None = None,
    workflow: str | None = None,
) -> str:
    outline = extract_markdown_outline(body)
    summary = "\n".join(f"- {section}" for section in outline[:12]) or "- Add article sections or landmarks here."
    assumption_lines = "\n".join(f"- {item}" for item in assumptions) or "- None supplied."
    questions = build_extraction_questions(batch, title, assumptions, goal)
    question_lines = "\n".join(f"{idx}. {question}" for idx, question in enumerate(questions, start=1))
    return (
        f"---\n"
        f"id: extract_{article_id}_{safe_filename(batch, fallback='batch')}\n"
        f"type: article_extraction_batch\n"
        f"source_article: {article_id}\n"
        f"source_url: {source_url}\n"
        f"created_at: {today_iso()}\n"
        f"batch: {batch}\n"
        f"goal: {json.dumps(goal, ensure_ascii=False)}\n"
        f"assumptions: {json.dumps(assumptions, ensure_ascii=False)}\n"
        f"model: {model}\n"
        f"operator: {operator}\n"
        f"executor: {executor}\n"
        f"run_id: {json.dumps(run_id or '')}\n"
        f"batch_id: {json.dumps(batch_id or '')}\n"
        f"workflow: {json.dumps(workflow or '')}\n"
        f"---\n\n"
        f"# Extraction Batch: {title}\n\n"
        f"## Goal\n\n"
        f"{goal or 'Clarify the target decision, question, or model update before querying.'}\n\n"
        f"## Assumptions Under Test\n\n"
        f"{assumption_lines}\n\n"
        f"## Article Landmarks\n\n"
        f"{summary}\n\n"
        f"## Batch Questions\n\n"
        f"{question_lines}\n\n"
        f"## Findings\n\n"
        f"### Finding 1\n"
        f"- question:\n"
        f"- judgment: grounding | supports | disproves | refines | unclear\n"
        f"- strength: high | medium | low\n"
        f"- section_or_anchor:\n"
        f"- quote:\n"
        f"- interpretation:\n"
        f"- model_impact:\n"
        f"- index_candidate: yes | no\n\n"
        f"### Finding 2\n"
        f"- question:\n"
        f"- judgment: grounding | supports | disproves | refines | unclear\n"
        f"- strength: high | medium | low\n"
        f"- section_or_anchor:\n"
        f"- quote:\n"
        f"- interpretation:\n"
        f"- model_impact:\n"
        f"- index_candidate: yes | no\n\n"
        f"### Finding 3\n"
        f"- question:\n"
        f"- judgment: grounding | supports | disproves | refines | unclear\n"
        f"- strength: high | medium | low\n"
        f"- section_or_anchor:\n"
        f"- quote:\n"
        f"- interpretation:\n"
        f"- model_impact:\n"
        f"- index_candidate: yes | no\n\n"
        f"## Index Candidates\n\n"
        f"- claim_or_pattern:\n"
        f"  evidence:\n"
        f"  why_preserve:\n\n"
        f"- claim_or_pattern:\n"
        f"  evidence:\n"
        f"  why_preserve:\n\n"
        f"## Decision Impact\n\n"
        f"- What should change in my assumptions, priorities, or next actions?\n"
    )


def build_note_markdown(
    *,
    article_id: str,
    title: str,
    source_url: str,
    body: str,
    model: str,
    operator: str,
    executor: str,
    run_id: str | None = None,
    batch_id: str | None = None,
    workflow: str | None = None,
) -> str:
    paragraphs = extract_summary_paragraphs(body, limit=3)
    outline = extract_markdown_outline(body)
    bullets = "\n".join(f"- {section}" for section in outline) or "- Review required"
    summary = "\n\n".join(paragraphs) or "Summary pending."
    return textwrap.dedent(
        f"""\
        ---
        id: note_{article_id}
        type: article_summary
        source_article: {article_id}
        source_url: {source_url}
        created_at: {today_iso()}
        model: {model}
        operator: {operator}
        executor: {executor}
        run_id: {json.dumps(run_id or "")}
        batch_id: {json.dumps(batch_id or "")}
        workflow: {json.dumps(workflow or "")}
        confidence: low
        tags: []
        ---

        # {title}

        ## Summary

        {summary}

        ## Key Sections

        {bullets}

        ## My Notes

        - Fill in interpretation, implications, and disagreements here.

        ## Open Questions

        - What is still unresolved?

        ## Related

        - Add links to adjacent notes or decisions.
        """
    )


def article_paths_for_batch(store: KnowledgeStore, batch_id: str | None) -> list[Path]:
    rows = records_for_batch(store.load_urls(), batch_id)
    paths: list[Path] = []
    seen: set[str] = set()
    for row in rows:
        markdown_path = str(row.get("markdown_path", "")).strip()
        if not markdown_path:
            article_id = str(row.get("article_id", "")).strip()
            if article_id:
                markdown_path = relpath_str(build_article_markdown_path(store, article_id), store.paths.root)
        if not markdown_path or markdown_path in seen:
            continue
        path = resolve_article_path(store, markdown_path)
        if path.exists():
            paths.append(path)
            seen.add(markdown_path)
    return paths


def stage_summarize_batch(store: KnowledgeStore, request: SummarizeBatchRequest, *, workflow: str = "summarize") -> SummarizeBatchResult:
    article_paths = [Path(path) for path in request.article_paths] if request.article_paths else article_paths_for_batch(store, request.batch_id)
    rows = store.load_urls()
    touched_paths: list[str] = []
    items: list[StageItemResult] = []
    summarized = 0
    for article_path in article_paths:
        article_path = article_path.expanduser().resolve()
        article_id, title, body = load_article_context(article_path)
        source_url = ""
        batch_id = request.batch_id
        for row in rows:
            if row.get("article_id") == article_id:
                source_url = row.get("url", "")
                row["status"] = "summarized"
                row["workflow_run_id"] = request.run_id
                if batch_id:
                    row["batch_id"] = batch_id
                break
        note_path = summary_note_path(store, article_id)
        note_path.parent.mkdir(parents=True, exist_ok=True)
        note = build_note_markdown(
            article_id=article_id,
            title=title,
            source_url=source_url,
            body=body,
            model=request.model,
            operator=request.operator,
            executor=request.executor,
            run_id=request.run_id,
            batch_id=batch_id,
            workflow=workflow,
        )
        note_path.write_text(note, encoding="utf-8")
        summarized += 1
        touched_paths.append(relpath_str(note_path, store.paths.root))
        items.append(
            StageItemResult(
                item_id=article_id,
                status=WorkflowStageStatus.SUCCEEDED,
                source_ref=str(article_path),
                note_path=relpath_str(note_path, store.paths.root),
            )
        )
    store.save_urls(rows)
    return SummarizeBatchResult(
        run_id=request.run_id,
        batch_id=request.batch_id,
        summarized=summarized,
        touched_paths=list(dict.fromkeys(touched_paths)),
        items=items,
        status=WorkflowStageStatus.SUCCEEDED,
        created_at=now_iso(),
    )


def command_summarize(store: KnowledgeStore, args: argparse.Namespace) -> int:
    article_path = Path(args.article).expanduser()
    if not article_path.is_absolute():
        article_path = (Path.cwd() / article_path).resolve()
    request = SummarizeBatchRequest(
        run_id=f"legacy_summarize_{now_compact()}",
        batch_id=getattr(args, "batch_id", None),
        article_paths=[str(article_path)],
        model=args.model,
        operator=args.operator,
        executor=args.executor,
    )
    result = stage_summarize_batch(store, request)
    print(json.dumps(model_to_dict(result), indent=2, ensure_ascii=False))
    return 0


def stage_extract_batch(store: KnowledgeStore, request: ExtractBatchRequest, *, workflow: str = "extract") -> ExtractBatchResult:
    article_paths = [Path(path) for path in request.article_paths] if request.article_paths else article_paths_for_batch(store, request.batch_id)
    rows = store.load_urls()
    touched_paths: list[str] = []
    items: list[StageItemResult] = []
    extracted = 0
    for article_path in article_paths:
        article_path = article_path.expanduser().resolve()
        article_id, title, body = load_article_context(article_path)
        source_url = ""
        for row in rows:
            if row.get("article_id") == article_id:
                source_url = row.get("url", "")
                row["workflow_run_id"] = request.run_id
                if request.batch_id:
                    row["batch_id"] = request.batch_id
                break
        note_path = extraction_note_path(store, article_id, request.template, request.batch_id)
        note = build_extraction_note_markdown(
            article_id=article_id,
            title=title,
            source_url=source_url,
            body=body,
            batch=request.template,
            goal=request.goal,
            assumptions=request.assumptions,
            model=request.model,
            operator=request.operator,
            executor=request.executor,
            run_id=request.run_id,
            batch_id=request.batch_id,
            workflow=workflow,
        )
        note_path.parent.mkdir(parents=True, exist_ok=True)
        note_path.write_text(note, encoding="utf-8")
        extracted += 1
        touched_paths.append(relpath_str(note_path, store.paths.root))
        items.append(
            StageItemResult(
                item_id=article_id,
                status=WorkflowStageStatus.SUCCEEDED,
                source_ref=str(article_path),
                note_path=relpath_str(note_path, store.paths.root),
            )
        )
    store.save_urls(rows)
    return ExtractBatchResult(
        run_id=request.run_id,
        batch_id=request.batch_id,
        template=request.template,
        extracted=extracted,
        touched_paths=list(dict.fromkeys(touched_paths)),
        items=items,
        status=WorkflowStageStatus.SUCCEEDED,
        created_at=now_iso(),
    )


def command_extract(store: KnowledgeStore, args: argparse.Namespace) -> int:
    article_path = Path(args.article).expanduser()
    if not article_path.is_absolute():
        article_path = (Path.cwd() / article_path).resolve()
    assumptions = [item.strip() for item in (args.assumption or []) if item.strip()]
    request = ExtractBatchRequest(
        run_id=f"legacy_extract_{now_compact()}",
        batch_id=getattr(args, "batch_id", None),
        article_paths=[str(article_path)],
        template=args.batch,
        goal=args.goal or "",
        assumptions=assumptions,
        model=args.model,
        operator=args.operator,
        executor=args.executor,
    )
    result = stage_extract_batch(store, request)
    print(json.dumps(model_to_dict(result), indent=2, ensure_ascii=False))
    return 0


def iter_source_files(source: Path) -> Iterable[Path]:
    for path in sorted(source.rglob("*")):
        if path.is_file():
            yield path


def normalize_text_document(source: Path, destination: Path, metadata: dict[str, Any]) -> None:
    text = source.read_text(encoding="utf-8", errors="replace")
    title = extract_title_from_markdown(text, fallback=source.stem)
    normalized = textwrap.dedent(
        f"""\
        ---
        title: {json.dumps(title, ensure_ascii=False)}
        origin_path: {json.dumps(str(source), ensure_ascii=False)}
        imported_at: {json.dumps(now_iso())}
        tags: []
        metadata: {json.dumps(metadata, ensure_ascii=False)}
        ---

        {text}
        """
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(normalized, encoding="utf-8")


def stage_import_library(store: KnowledgeStore, request: ImportLibraryRequest, *, normalize_files: bool = True) -> ImportLibraryResult:
    source = Path(request.source).expanduser().resolve()
    export_root = store.paths.exports_dir / request.app
    raw_root = export_root / "raw"
    normalized_root = export_root / "normalized"
    raw_root.mkdir(parents=True, exist_ok=True)
    normalized_root.mkdir(parents=True, exist_ok=True)

    manifest: list[dict[str, Any]] = []
    items: list[StageItemResult] = []
    touched_paths: list[str] = []
    count = 0
    normalized_count = 0
    for path in iter_source_files(source):
        rel = path.relative_to(source)
        raw_dst = raw_root / rel
        copy_file(path, raw_dst)
        metadata = {
            "app": request.app,
            "relative_path": str(rel),
            "kind": path.suffix.lower(),
            "run_id": request.run_id,
            "batch_id": request.batch_id,
            "workflow": "from-library",
        }
        if normalize_files and path.suffix.lower() in {".md", ".txt"}:
            normalized_dst = normalized_root / rel.with_suffix(".md")
            normalize_text_document(path, normalized_dst, metadata)
            normalized_path = relpath_str(normalized_dst, store.paths.root)
            normalized_count += 1
            touched_paths.append(normalized_path)
        else:
            normalized_path = None
        record = {
            "app": request.app,
            "source_path": str(path),
            "raw_path": relpath_str(raw_dst, store.paths.root),
            "normalized_path": normalized_path,
            "exported_at": now_iso(),
            "workflow_run_id": request.run_id,
            "batch_id": request.batch_id,
        }
        manifest.append(record)
        count += 1
        touched_paths.append(record["raw_path"])
        items.append(
            StageItemResult(
                item_id=str(rel),
                status=WorkflowStageStatus.SUCCEEDED,
                source_ref=str(path),
                output_path=normalized_path or record["raw_path"],
            )
        )
        if request.limit and count >= request.limit:
            break
    manifest_path = export_root / "metadata.jsonl"
    write_jsonl(manifest_path, manifest)
    touched_paths.append(relpath_str(manifest_path, store.paths.root))
    return ImportLibraryResult(
        run_id=request.run_id,
        batch_id=request.batch_id,
        app=request.app,
        source=str(source),
        export_root=relpath_str(export_root, store.paths.root),
        exported=len(manifest),
        normalized=normalized_count,
        manifest_path=relpath_str(manifest_path, store.paths.root),
        touched_paths=list(dict.fromkeys(touched_paths)),
        items=items,
        status=WorkflowStageStatus.SUCCEEDED,
        created_at=now_iso(),
    )


def command_export(store: KnowledgeStore, args: argparse.Namespace) -> int:
    request = ImportLibraryRequest(
        run_id=f"legacy_export_{now_compact()}",
        batch_id=f"legacy_export_{now_compact()}",
        app=args.app,
        source=args.source,
        limit=args.limit,
    )
    result = stage_import_library(store, request)
    print(json.dumps(model_to_dict(result), indent=2, ensure_ascii=False))
    return 0


def command_import_library(store: KnowledgeStore, args: argparse.Namespace) -> int:
    config = store.load_config()
    source_value = resolve_import_source(
        args.source,
        config,
        primary_key="library_default_source",
        legacy_keys=("simpread_default_source",),
        label="library source",
    )
    source = Path(source_value).expanduser().resolve()
    request = ImportLibraryRequest(
        run_id=f"legacy_library_{now_compact()}",
        batch_id=f"legacy_library_{now_compact()}",
        app=DEFAULT_LIBRARY_APP,
        source=str(source),
        limit=args.limit,
    )
    result = stage_import_library(store, request)
    print(json.dumps(model_to_dict(result), indent=2, ensure_ascii=False))
    return 0


def command_import_simpread(store: KnowledgeStore, args: argparse.Namespace) -> int:
    return command_import_library(store, args)


def stage_normalize_export(store: KnowledgeStore, request: NormalizeExportRequest) -> NormalizeExportResult:
    source = Path(request.path).expanduser()
    if not source.is_absolute():
        source = (store.paths.root / source).resolve()
    normalized_root = source / "normalized"
    raw_root = source / "raw"
    if not raw_root.exists():
        raise SystemExit(f"raw directory not found under {source}")
    count = 0
    items: list[StageItemResult] = []
    touched_paths: list[str] = []
    for path in iter_source_files(raw_root):
        if path.suffix.lower() not in {".md", ".txt"}:
            continue
        rel = path.relative_to(raw_root)
        dst = normalized_root / rel.with_suffix(".md")
        normalize_text_document(path, dst, {"source_root": str(source), "run_id": request.run_id, "batch_id": request.batch_id})
        count += 1
        touched_paths.append(relpath_str(dst, store.paths.root))
        items.append(
            StageItemResult(
                item_id=str(rel),
                status=WorkflowStageStatus.SUCCEEDED,
                source_ref=str(path),
                output_path=relpath_str(dst, store.paths.root),
            )
        )
    return NormalizeExportResult(
        run_id=request.run_id,
        path=str(source),
        normalized=count,
        touched_paths=touched_paths,
        items=items,
        status=WorkflowStageStatus.SUCCEEDED,
        created_at=now_iso(),
    )


def command_normalize(store: KnowledgeStore, args: argparse.Namespace) -> int:
    request = NormalizeExportRequest(run_id=f"legacy_normalize_{now_compact()}", path=args.path, batch_id=None)
    result = stage_normalize_export(store, request)
    print(json.dumps(model_to_dict(result), indent=2, ensure_ascii=False))
    return 0


def open_index(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS documents (
            path TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            kind TEXT NOT NULL,
            body TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    try:
        conn.execute(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts
            USING fts5(path, title, body, kind, tokenize='unicode61')
            """
        )
    except sqlite3.OperationalError:
        pass
    return conn


def collect_index_documents(store: KnowledgeStore) -> list[dict[str, str]]:
    config = store.load_config()
    include_roots: list[Path] = []
    indexing = config.get("indexing", {})
    configured_roots = indexing.get("include_folders", []) if isinstance(indexing, dict) else []
    if isinstance(configured_roots, list):
        for value in configured_roots:
            path = resolve_layout_path(store.paths.root, value)
            if path is not None and path.exists():
                include_roots.append(path)
    if not include_roots:
        include_roots = list(
            dict.fromkeys(
                [
                    store.paths.root,
                    *[path for path in store.paths.articles_markdown_dirs if not is_relative_to(path, store.paths.root)],
                ]
            )
        )
    excluded_roots = [
        store.paths.inbox_dir,
        store.paths.index_dir,
        store.paths.logs_dir,
        store.paths.backups_dir,
        store.paths.articles_raw,
        store.paths.articles_assets,
    ]
    candidates: list[Path] = []
    seen_candidates: set[Path] = set()
    for root in include_roots:
        if root.exists():
            for path in root.rglob("*"):
                if not path.is_file() or path.suffix.lower() not in {".md", ".txt"}:
                    continue
                resolved = path.resolve()
                if any(is_relative_to(resolved, excluded_root) for excluded_root in excluded_roots):
                    continue
                if resolved in seen_candidates:
                    continue
                candidates.append(resolved)
                seen_candidates.add(resolved)
    docs: list[dict[str, str]] = []
    for path in sorted(candidates):
        body = path.read_text(encoding="utf-8", errors="replace")
        title = extract_title_from_markdown(body, fallback=path.stem)
        if is_relative_to(path, store.paths.root):
            kind = path.relative_to(store.paths.root).parts[0]
        else:
            kind = safe_filename(path.parent.name, fallback="external")
        docs.append(
            {
                "path": relpath_str(path, store.paths.root),
                "title": title,
                "kind": kind,
                "body": body,
                "updated_at": now_iso(),
            }
        )
    return docs


def stage_index_update(store: KnowledgeStore, request: IndexUpdateRequest) -> IndexUpdateResult:
    docs = collect_index_documents(store)
    conn = open_index(store.paths.index_db)
    with conn:
        conn.execute("DELETE FROM documents")
        try:
            conn.execute("DELETE FROM documents_fts")
            fts_enabled = True
        except sqlite3.OperationalError:
            fts_enabled = False
        for doc in docs:
            conn.execute(
                "INSERT INTO documents(path, title, kind, body, updated_at) VALUES (?, ?, ?, ?, ?)",
                (doc["path"], doc["title"], doc["kind"], doc["body"], doc["updated_at"]),
            )
            if fts_enabled:
                conn.execute(
                    "INSERT INTO documents_fts(path, title, body, kind) VALUES (?, ?, ?, ?)",
                    (doc["path"], doc["title"], doc["body"], doc["kind"]),
                )
    conn.close()
    return IndexUpdateResult(
        run_id=request.run_id,
        batch_id=request.batch_id,
        indexed=len(docs),
        database_path=relpath_str(store.paths.index_db, store.paths.root),
        touched_paths=[relpath_str(store.paths.index_db, store.paths.root)],
        status=WorkflowStageStatus.SUCCEEDED,
        created_at=now_iso(),
    )


def command_index_update(store: KnowledgeStore, _args: argparse.Namespace) -> int:
    result = stage_index_update(store, IndexUpdateRequest(run_id=f"legacy_index_{now_compact()}", batch_id=None))
    print(json.dumps(model_to_dict(result), indent=2, ensure_ascii=False))
    return 0


def command_search(store: KnowledgeStore, args: argparse.Namespace) -> int:
    conn = open_index(store.paths.index_db)
    results: list[dict[str, Any]] = []
    try:
        rows = conn.execute(
            """
            SELECT path, title, kind, snippet(documents_fts, 2, '[', ']', ' ... ', 12) AS excerpt
            FROM documents_fts
            WHERE documents_fts MATCH ?
            LIMIT ?
            """,
            (args.query, args.limit),
        ).fetchall()
        for path, title, kind, excerpt in rows:
            results.append({"path": path, "title": title, "kind": kind, "excerpt": excerpt})
    except sqlite3.OperationalError:
        like = f"%{args.query}%"
        rows = conn.execute(
            """
            SELECT path, title, kind, substr(body, 1, 220)
            FROM documents
            WHERE title LIKE ? OR body LIKE ?
            LIMIT ?
            """,
            (like, like, args.limit),
        ).fetchall()
        for path, title, kind, excerpt in rows:
            results.append({"path": path, "title": title, "kind": kind, "excerpt": excerpt})
    finally:
        conn.close()
    print(json.dumps(results, indent=2, ensure_ascii=False))
    return 0


def copy_tree_filtered(source: Path, destination: Path, ignore_names: set[str]) -> None:
    if destination.exists():
        shutil.rmtree(destination)
    destination.mkdir(parents=True, exist_ok=True)
    for path in source.rglob("*"):
        if any(part in ignore_names for part in path.relative_to(source).parts):
            continue
        target = destination / path.relative_to(source)
        if path.is_dir():
            target.mkdir(parents=True, exist_ok=True)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)


def stage_backup(store: KnowledgeStore, request: BackupRequest) -> BackupResult:
    config = store.load_config()
    requested_executor = request.executor
    executor = choose_backup_executor(requested_executor, config)
    if executor == DEFAULT_REMOTE_EXECUTOR:
        remote = get_remote_executor_config(config, executor)
        remote_target = ""
        try:
            remote_target = build_remote_target(config, executor)
        except SystemExit:
            if requested_executor:
                raise
        destination_value = request.destination or remote.get("backup_destination")
        if remote_target and remote_target not in REMOTE_TARGET_PLACEHOLDERS and destination_value:
            report = backup_to_remote_executor(store, config, executor, str(destination_value))
        elif requested_executor and canonicalize_executor_name(requested_executor) == DEFAULT_REMOTE_EXECUTOR:
            raise SystemExit(
                f"remote backup for executor '{executor}' requires remote_access.executors.{executor}.ssh_target or .user and .magic_address, plus .backup_destination"
            )
        else:
            destination = Path(request.destination or config.get("backups", {}).get("destination")).expanduser().resolve()
            if destination == store.paths.root or store.paths.root in destination.parents:
                raise SystemExit("backup destination must be outside the knowledge root to avoid recursive copies")
            copy_tree_filtered(store.paths.root, destination, ignore_names={"mirror"})
            report = {
                "destination": str(destination),
                "executor": "local",
                "mode": "local",
            }
    else:
        destination = Path(request.destination or config.get("backups", {}).get("destination")).expanduser().resolve()
        if destination == store.paths.root or store.paths.root in destination.parents:
            raise SystemExit("backup destination must be outside the knowledge root to avoid recursive copies")
        copy_tree_filtered(store.paths.root, destination, ignore_names={"mirror"})
        report = {
            "destination": str(destination),
            "executor": executor,
            "mode": "local",
        }
    config.setdefault("backups", {})
    config["backups"]["destination"] = report["destination"]
    config["backups"]["executor"] = executor
    config["backups"]["last_run_at"] = now_iso()
    dump_json(store.paths.config_path, config)
    report["last_run_at"] = config["backups"]["last_run_at"]
    report["status"] = "ok"
    dump_json(store.paths.logs_dir / "last_backup.json", report)
    return BackupResult(
        run_id=request.run_id,
        destination=str(report["destination"]),
        executor=str(report["executor"]),
        mode=str(report["mode"]),
        transport=str(report.get("transport")) if report.get("transport") is not None else None,
        target=str(report.get("target")) if report.get("target") is not None else None,
        last_run_at=str(report["last_run_at"]),
        touched_paths=[
            relpath_str(store.paths.logs_dir / "last_backup.json", store.paths.root),
            relpath_str(store.paths.config_path, store.paths.root),
        ],
        status=WorkflowStageStatus.SUCCEEDED,
        created_at=now_iso(),
    )


def command_backup(store: KnowledgeStore, args: argparse.Namespace) -> int:
    request = BackupRequest(run_id=f"legacy_backup_{now_compact()}", destination=args.destination, executor=args.executor)
    result = stage_backup(store, request)
    print(json.dumps(model_to_dict(result), indent=2, ensure_ascii=False))
    return 0


def command_remote(store: KnowledgeStore, args: argparse.Namespace) -> int:
    config = store.load_config()
    executor = canonicalize_executor_name(args.executor) or args.executor
    transport = config.get("remote_access", {}).get("transport", "tailscale-ssh")
    ssh_base = build_remote_ssh_base_command(config, executor)
    target = build_remote_target(config, executor)

    if args.remote_command == "show-target":
        print(
            json.dumps(
                {
                    "executor": executor,
                    "transport": transport,
                    "target": target,
                    "command": ssh_base,
                },
                indent=2,
            )
        )
        return 0

    if args.remote_command == "check":
        result = run_remote_command(config, executor, ["hostname"])
        payload = {
            "executor": executor,
            "transport": transport,
            "target": target,
            "status": "ok" if result.returncode == 0 else "error",
            "returncode": result.returncode,
            "stdout": (result.stdout or "").strip(),
            "stderr": (result.stderr or "").strip(),
        }
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 0 if result.returncode == 0 else result.returncode

    if args.remote_command == "exec":
        command = list(args.remote_args or [])
        if command and command[0] == "--":
            command = command[1:]
        if not command:
            raise SystemExit("remote exec requires a command, for example: ops-knowledge remote exec -- uname -a")
        result = run_remote_command(config, executor, command)
        payload = {
            "executor": executor,
            "transport": transport,
            "target": target,
            "command": command,
            "returncode": result.returncode,
            "stdout": (result.stdout or "").rstrip(),
            "stderr": (result.stderr or "").rstrip(),
        }
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return result.returncode

    raise SystemExit(f"Unhandled remote command: {args.remote_command}")


def stage_status(store: KnowledgeStore, request: StatusRequest) -> StatusResult:
    rows = store.load_urls()
    counts: dict[str, int] = {}
    pools: dict[str, int] = {}
    captured_from: dict[str, int] = {}
    for row in rows:
        status = row.get("status", "unknown")
        counts[status] = counts.get(status, 0) + 1
        pool = row.get("pool", "unknown")
        pools[pool] = pools.get(pool, 0) + 1
        source_name = row.get("captured_from", "unknown")
        captured_from[source_name] = captured_from.get(source_name, 0) + 1
    config = store.load_config()
    indexed_count = 0
    if store.paths.index_db.exists():
        conn = sqlite3.connect(str(store.paths.index_db))
        try:
            indexed_count = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
        finally:
            conn.close()
    remote_targets: dict[str, dict[str, str | None]] = {}
    for name in sorted(config.get("remote_access", {}).get("executors", {}).keys()):
        canonical_name = canonicalize_executor_name(name) or name
        try:
            target: str | None = build_remote_target(config, name)
        except SystemExit:
            target = None
        remote_targets[canonical_name] = {"target": target}
    report = {
        "knowledge_root": str(store.paths.root),
        "inbox": counts,
        "pools": pools,
        "captured_from": captured_from,
        "index": {
            "documents_indexed": indexed_count,
            "database": relpath_str(store.paths.index_db, store.paths.root),
        },
        "backups": config.get("backups", {}),
        "routing_defaults": config.get("routing_defaults", {}),
        "obsidian": config.get("obsidian", {}),
        "remote_access": {
            "transport": config.get("remote_access", {}).get("transport", "tailscale-ssh"),
            "executors": remote_targets,
        },
    }
    return StatusResult(
        run_id=request.run_id,
        knowledge_root=str(store.paths.root),
        inbox=counts,
        pools=pools,
        captured_from=captured_from,
        index=report["index"],
        backups=report["backups"],
        routing_defaults=report["routing_defaults"],
        remote_access=report["remote_access"],
        touched_paths=[relpath_str(store.paths.index_db, store.paths.root), relpath_str(store.paths.config_path, store.paths.root)],
        status=WorkflowStageStatus.SUCCEEDED,
        created_at=now_iso(),
    )


def command_status(store: KnowledgeStore, _args: argparse.Namespace) -> int:
    result = stage_status(store, StatusRequest(run_id=f"legacy_status_{now_compact()}"))
    print(json.dumps(model_to_dict(result), indent=2, ensure_ascii=False))
    return 0


def stage_doctor(store: KnowledgeStore, request: DoctorRequest) -> DoctorResult:
    issues: list[str] = []
    store.ensure_layout()
    try:
        store.load_urls()
    except ValueError as exc:
        issues.append(str(exc))
    config = store.load_config()
    tabs_source_value = get_import_default(config, "tabs_default_source")
    if tabs_source_value and not Path(tabs_source_value).expanduser().exists():
        issues.append(f"Tabs source does not exist yet: {tabs_source_value}")
    library_source_value = get_import_default(config, "library_default_source", "simpread_default_source")
    if library_source_value and not Path(library_source_value).expanduser().exists():
        issues.append(f"Library source does not exist yet: {library_source_value}")
    backup_destination = Path(config.get("backups", {}).get("destination", ""))
    if backup_destination and not backup_destination.exists():
        issues.append(f"Backup destination does not exist yet: {backup_destination}")
    cli_path, vault_name = resolve_obsidian_settings(config)
    if shutil.which(cli_path) is None and not Path(cli_path).expanduser().exists():
        issues.append(f"Obsidian CLI not found: {cli_path}")
    if not vault_name:
        issues.append("Obsidian vault_name is not configured")
    try:
        conn = open_index(store.paths.index_db)
        conn.close()
    except sqlite3.Error as exc:
        issues.append(f"SQLite index unavailable: {exc}")
    _, remote_executor = get_named_mapping_entry(config.get("executors", {}), DEFAULT_REMOTE_EXECUTOR)
    if not remote_executor.get("enabled"):
        issues.append("remote executor disabled; safe-ip routing cannot be honored")
    remote_executors = config.get("remote_access", {}).get("executors", {})
    if remote_executors and shutil.which("tailscale") is None:
        issues.append("tailscale CLI not found in PATH; remote_access cannot use tailscale ssh")
    for name in sorted(remote_executors):
        remote = get_remote_executor_config(config, name)
        canonical_name = canonicalize_executor_name(name) or name
        try:
            target = build_remote_target(config, name)
        except SystemExit as exc:
            issues.append(str(exc))
            continue
        if target in REMOTE_TARGET_PLACEHOLDERS:
            issues.append(f"remote target for executor '{canonical_name}' still uses the default placeholder value")
        if canonical_name == DEFAULT_REMOTE_EXECUTOR and not remote.get("backup_destination"):
            issues.append(f"remote backup destination for executor '{canonical_name}' is not configured")
    return DoctorResult(
        run_id=request.run_id,
        knowledge_root=str(store.paths.root),
        issues=issues,
        touched_paths=[relpath_str(store.paths.config_path, store.paths.root), relpath_str(store.paths.inbox_urls, store.paths.root)],
        status=WorkflowStageStatus.SUCCEEDED,
        created_at=now_iso(),
    )


def command_doctor(store: KnowledgeStore, _args: argparse.Namespace) -> int:
    result = stage_doctor(store, DoctorRequest(run_id=f"legacy_doctor_{now_compact()}"))
    print(json.dumps(model_to_dict(result), indent=2, ensure_ascii=False))
    return 0


def workflow_run_import_tabs(store: KnowledgeStore, args: argparse.Namespace, context: dict[str, Any]) -> ImportTabsResult:
    config = store.load_config()
    source = resolve_import_source(
        args.source,
        config,
        primary_key="tabs_default_source",
        label="tabs source",
    )
    request = ImportTabsRequest(
        run_id=context["run_id"],
        batch_id=context["batch_id"],
        source=source,
        source_name=args.source_name,
        tags=parse_tags(args.tag),
        notes=args.notes or "",
        priority=args.priority,
        limit=args.limit,
    )
    return stage_import_tabs(store, request, workflow=context["workflow"])


def workflow_run_capture_url(store: KnowledgeStore, args: argparse.Namespace, context: dict[str, Any]) -> CaptureUrlResult:
    request = CaptureUrlRequest(
        run_id=context["run_id"],
        batch_id=context["batch_id"],
        url=args.url,
        title=args.title or "",
        source="manual",
        tags=parse_tags(args.tag),
        notes=args.notes or "",
        priority=args.priority,
        safe_ip=args.safe_ip,
        requires_login=args.requires_login,
        private=args.private,
    )
    return stage_capture_url(store, request, workflow=context["workflow"])


def workflow_run_import_library(store: KnowledgeStore, args: argparse.Namespace, context: dict[str, Any]) -> ImportLibraryResult:
    config = store.load_config()
    source = resolve_import_source(
        args.source,
        config,
        primary_key="library_default_source",
        legacy_keys=("simpread_default_source",),
        label="library source",
    )
    request = ImportLibraryRequest(
        run_id=context["run_id"],
        batch_id=context["batch_id"],
        app=DEFAULT_LIBRARY_APP,
        source=source,
        limit=args.limit,
    )
    return stage_import_library(store, request, normalize_files=False)


def workflow_run_normalize_library(store: KnowledgeStore, _args: argparse.Namespace, context: dict[str, Any]) -> NormalizeExportResult:
    return stage_normalize_export(
        store,
        NormalizeExportRequest(
            run_id=context["run_id"],
            path=f"exports/{DEFAULT_LIBRARY_APP}",
            batch_id=context["batch_id"],
        ),
    )


def workflow_run_fetch_batch(store: KnowledgeStore, args: argparse.Namespace, context: dict[str, Any]) -> FetchBatchResult:
    return stage_fetch_batch(
        store,
        FetchBatchRequest(
            run_id=context["run_id"],
            batch_id=context["batch_id"],
            executor=None,
            limit=None,
        ),
    )


def workflow_run_summarize_batch(store: KnowledgeStore, _args: argparse.Namespace, context: dict[str, Any]) -> SummarizeBatchResult:
    return stage_summarize_batch(
        store,
        SummarizeBatchRequest(
            run_id=context["run_id"],
            batch_id=context["batch_id"],
            article_paths=None,
            model="manual",
            operator=DEFAULT_OPERATOR_NAME,
            executor=DEFAULT_LOCAL_EXECUTOR,
        ),
        workflow=context["workflow"],
    )


def workflow_run_extract_batch(store: KnowledgeStore, _args: argparse.Namespace, context: dict[str, Any]) -> ExtractBatchResult:
    return stage_extract_batch(
        store,
        ExtractBatchRequest(
            run_id=context["run_id"],
            batch_id=context["batch_id"],
            article_paths=None,
            template="grounding",
            goal="",
            assumptions=[],
            model="manual",
            operator=DEFAULT_OPERATOR_NAME,
            executor=DEFAULT_LOCAL_EXECUTOR,
        ),
        workflow=context["workflow"],
    )


def workflow_run_index_update(store: KnowledgeStore, _args: argparse.Namespace, context: dict[str, Any]) -> IndexUpdateResult:
    return stage_index_update(store, IndexUpdateRequest(run_id=context["run_id"], batch_id=context.get("batch_id")))


def workflow_run_status(store: KnowledgeStore, _args: argparse.Namespace, context: dict[str, Any]) -> StatusResult:
    return stage_status(store, StatusRequest(run_id=context["run_id"]))


def workflow_run_doctor(store: KnowledgeStore, _args: argparse.Namespace, context: dict[str, Any]) -> DoctorResult:
    return stage_doctor(store, DoctorRequest(run_id=context["run_id"]))


def workflow_run_backup(store: KnowledgeStore, args: argparse.Namespace, context: dict[str, Any]) -> BackupResult:
    return stage_backup(
        store,
        BackupRequest(
            run_id=context["run_id"],
            destination=getattr(args, "destination", None),
            executor=getattr(args, "executor", None),
        ),
    )


WORKFLOW_STAGE_RUNNERS: dict[str, Callable[[KnowledgeStore, argparse.Namespace, dict[str, Any]], Any]] = {
    "import-tabs": workflow_run_import_tabs,
    "capture-url": workflow_run_capture_url,
    "import-library": workflow_run_import_library,
    "normalize-library": workflow_run_normalize_library,
    "fetch-batch": workflow_run_fetch_batch,
    "summarize-batch": workflow_run_summarize_batch,
    "extract-batch": workflow_run_extract_batch,
    "index-update": workflow_run_index_update,
    "status": workflow_run_status,
    "doctor": workflow_run_doctor,
    "backup": workflow_run_backup,
}


WORKFLOW_RECIPES: dict[str, WorkflowRecipe] = {
    "from-tabs": WorkflowRecipe(
        name="from-tabs",
        description="Import a tabs export, fetch the new batch, generate summary and extraction notes, then refresh the index.",
        source_kind="tabs",
        steps=(
            WorkflowStepRecipe("import-tabs", "Import tab batch", "Load tabs JSON into the inbox and mark this run batch.", "import-tabs", "import-tabs"),
            WorkflowStepRecipe("fetch", "Fetch imported pages", "Fetch only inbox items created or touched by this batch.", "fetch-new", "fetch-batch"),
            WorkflowStepRecipe("summarize", "Generate summary notes", "Create summary stubs for fetched articles in this batch.", "summarize", "summarize-batch"),
            WorkflowStepRecipe("extract", "Generate extraction notes", "Create grounding extraction notes for the same batch.", "extract", "extract-batch"),
            WorkflowStepRecipe("index", "Refresh search index", "Rebuild the SQLite search index after note generation.", "index update", "index-update"),
        ),
    ),
    "from-url": WorkflowRecipe(
        name="from-url",
        description="Capture a URL or local file, fetch it, generate notes, then refresh the index.",
        source_kind="url",
        steps=(
            WorkflowStepRecipe("capture-url", "Capture URL", "Write one URL or file path into the inbox as the current batch.", "capture-url", "capture-url"),
            WorkflowStepRecipe("fetch", "Fetch captured page", "Fetch only the captured batch item into raw and markdown storage.", "fetch-new", "fetch-batch"),
            WorkflowStepRecipe("summarize", "Generate summary note", "Create a summary stub for the fetched article.", "summarize", "summarize-batch"),
            WorkflowStepRecipe("extract", "Generate extraction note", "Create a grounding extraction note for the fetched article.", "extract", "extract-batch"),
            WorkflowStepRecipe("index", "Refresh search index", "Rebuild the SQLite search index after note generation.", "index update", "index-update"),
        ),
    ),
    "from-library": WorkflowRecipe(
        name="from-library",
        description="Import the current library source into exports, normalize text files, then refresh the index.",
        source_kind="library",
        steps=(
            WorkflowStepRecipe("import-library", "Import library files", f"Mirror the current library source into exports/{DEFAULT_LIBRARY_APP}/raw and write manifest data.", "import-library", "import-library"),
            WorkflowStepRecipe("normalize-library", "Normalize text files", "Build normalized markdown files for the imported library batch.", "normalize", "normalize-library"),
            WorkflowStepRecipe("index", "Refresh search index", "Rebuild the SQLite search index after library import.", "index update", "index-update"),
        ),
    ),
    "maintain": WorkflowRecipe(
        name="maintain",
        description="Run health checks, refresh the index, and back up the knowledge root.",
        source_kind="maintenance",
        steps=(
            WorkflowStepRecipe("status", "Read current status", "Collect inbox, index, backup, and remote-access status.", "status", "status"),
            WorkflowStepRecipe("doctor", "Run health checks", "Inspect the workspace for broken config or missing dependencies.", "doctor", "doctor"),
            WorkflowStepRecipe("index", "Refresh search index", "Rebuild the SQLite search index from current documents.", "index update", "index-update"),
            WorkflowStepRecipe("backup", "Run backup", "Copy or stream the knowledge root to the configured backup target.", "backup", "backup"),
        ),
    ),
}


PRIMITIVE_COMMANDS = [
    "init",
    "capture-url",
    "inbox",
    "fetch-new",
    "summarize",
    "extract",
    "export",
    "normalize",
    "import-library",
    "import-tabs",
    "index update",
    "search",
    "backup",
    "remote",
    "status",
    "doctor",
]


def workflow_source_ref(store: KnowledgeStore, args: argparse.Namespace) -> str:
    config = store.load_config()
    if args.command == "from-tabs":
        return resolve_import_source(
            args.source,
            config,
            primary_key="tabs_default_source",
            label="tabs source",
        )
    if args.command == "from-url":
        return args.url
    if args.command == "from-library":
        return resolve_import_source(
            args.source,
            config,
            primary_key="library_default_source",
            legacy_keys=("simpread_default_source",),
            label="library source",
        )
    return str(store.paths.root)


def print_workflow_preview(store: KnowledgeStore, recipe: WorkflowRecipe, run_id: str, batch_id: str | None, source_ref: str) -> None:
    print(f"Workflow: {recipe.name}")
    print(f"Run ID: {run_id}")
    if batch_id:
        print(f"Batch ID: {batch_id}")
    print(f"Source: {source_ref}")
    print(f"Trace: {relpath_str(store.paths.runs_dir / run_id, store.paths.root)}")
    print("Steps:")
    for index, step in enumerate(recipe.steps, start=1):
        print(f"  {index}. {step.label}")
        print(f"     {step.description}")


def render_advanced_help() -> str:
    lines = ["Advanced commands:", "  Primitive commands stay callable for automation and agents."]
    for command in PRIMITIVE_COMMANDS:
        lines.append(f"  - {command}")
    lines.append("")
    lines.append("Workflow to primitive mapping:")
    for recipe in WORKFLOW_RECIPES.values():
        lines.append(f"  {recipe.name}:")
        for step in recipe.steps:
            lines.append(f"    - {step.label} -> {step.primitive_command}")
    lines.append("")
    lines.append("Low-level docs: README.md")
    return "\n".join(lines)


def command_workflow(store: KnowledgeStore, args: argparse.Namespace) -> int:
    recipe = WORKFLOW_RECIPES[args.command]
    source_ref = workflow_source_ref(store, args)
    run_id = build_run_id(recipe.name)
    batch_id = None if recipe.name == "maintain" else build_batch_id(recipe.source_kind, source_ref)
    trace = WorkflowTrace(store, recipe, run_id, source_ref, batch_id)
    trace.append_event(
        event_type="run_created",
        status=WorkflowStageStatus.PENDING,
        message="workflow preview created",
        payload={"batch_id": batch_id, "source_ref": source_ref},
    )
    print_workflow_preview(store, recipe, run_id, batch_id, source_ref)
    if getattr(args, "yes", False):
        accepted = True
    else:
        try:
            answer = input("Run this workflow? [y/N]: ").strip().lower()
        except EOFError:
            answer = ""
        accepted = answer in {"y", "yes"}
    trace.mark_confirmation(accepted)
    if not accepted:
        trace.finish_run(WorkflowStageStatus.CANCELED, "workflow canceled by user")
        print(json.dumps({"run_id": run_id, "batch_id": batch_id, "status": "canceled"}, indent=2))
        return 0

    context = {"run_id": run_id, "batch_id": batch_id, "workflow": recipe.name}
    trace.append_event(event_type="run_started", status=WorkflowStageStatus.RUNNING, message="workflow execution started")
    for step_index, step in enumerate(recipe.steps):
        print(f"[{step_index + 1}/{len(recipe.steps)}] {step.label}")
        trace.start_stage(step_index, step)
        try:
            result = WORKFLOW_STAGE_RUNNERS[step.stage_kind](store, args, context)
            if getattr(result, "status", WorkflowStageStatus.SUCCEEDED) == WorkflowStageStatus.FAILED:
                trace.record_failed_stage(step_index, step, result, f"{step.label} reported failure")
                trace.finish_run(WorkflowStageStatus.FAILED, f"{step.label} failed")
                print(json.dumps({"run_id": run_id, "batch_id": batch_id, "status": "failed"}, indent=2))
                return 1
            trace.finish_stage(step_index, step, result)
        except Exception as exc:
            trace.fail_stage(step_index, step, str(exc))
            trace.finish_run(WorkflowStageStatus.FAILED, str(exc))
            print(json.dumps({"run_id": run_id, "batch_id": batch_id, "status": "failed", "error": str(exc)}, indent=2, ensure_ascii=False))
            return 1
    trace.finish_run(WorkflowStageStatus.SUCCEEDED, "workflow completed successfully")
    print(
        json.dumps(
            {
                "run_id": run_id,
                "batch_id": batch_id,
                "status": "ok",
                "trace_dir": relpath_str(trace.run_dir, store.paths.root),
            },
            indent=2,
        )
    )
    return 0
