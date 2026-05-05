from __future__ import annotations

import argparse
from pathlib import Path

from core import (
    DEFAULT_LIBRARY_APP,
    DEFAULT_LOCAL_EXECUTOR,
    DEFAULT_OPERATOR_NAME,
    DEFAULT_REMOTE_EXECUTOR,
    DEFAULT_TABS_SOURCE_NAME,
    KnowledgeStore,
    command_backup,
    command_capture_url,
    command_doctor,
    command_export,
    command_extract,
    command_fetch_new,
    command_import_library,
    command_import_tabs,
    command_inbox,
    command_index_update,
    command_init,
    command_normalize,
    command_remote,
    command_search,
    command_status,
    command_summarize,
    command_workflow,
    render_advanced_help,
)


class HelpFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter):
    pass


def add_workflow_parsers(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    from_tabs = subparsers.add_parser(
        "from-tabs",
        help="Import tabs, fetch the new batch, generate notes, and refresh the index.",
        description="Import tabs, fetch the new batch, generate notes, and refresh the index.",
    )
    from_tabs.add_argument("source", nargs="?", help="Tabs JSON source. Falls back to config.json imports.tabs_default_source.")
    from_tabs.add_argument("--limit", type=int, help="Maximum number of source items to process.")
    from_tabs.add_argument("--tag", action="append", help="Tag to attach to imported rows. Repeat or pass comma-separated values.")
    from_tabs.add_argument("--notes", help="Notes applied to each imported row.")
    from_tabs.add_argument("--priority", choices=["low", "medium", "high"], default="medium", help="Priority for imported rows.")
    from_tabs.add_argument("--source-name", default=DEFAULT_TABS_SOURCE_NAME, help="Capture source label stored on imported rows.")
    from_tabs.add_argument("-y", "--yes", action="store_true", help="Run immediately without confirmation.")

    from_url = subparsers.add_parser(
        "from-url",
        help="Capture one URL or file path, fetch it, generate notes, and refresh the index.",
        description="Capture one URL or file path, fetch it, generate notes, and refresh the index.",
    )
    from_url.add_argument("url", help="URL or local file path to capture.")
    from_url.add_argument("--title", help="Optional title stored with the inbox record.")
    from_url.add_argument("--priority", choices=["low", "medium", "high"], default="medium", help="Priority for the captured row.")
    from_url.add_argument("--tag", action="append", help="Tag to attach. Repeat or pass comma-separated values.")
    from_url.add_argument("--notes", help="Freeform notes stored on the inbox record.")
    from_url.add_argument("--safe-ip", action="store_true", help="Mark the source as requiring a safe IP for fetching.")
    from_url.add_argument("--requires-login", action="store_true", help="Mark the source as login-gated.")
    from_url.add_argument("--private", action="store_true", help="Mark the source as private.")
    from_url.add_argument("-y", "--yes", action="store_true", help="Run immediately without confirmation.")

    from_library = subparsers.add_parser(
        "from-library",
        help="Import the current library source, normalize text files, and refresh the index.",
        description="Import the current library source, normalize text files, and refresh the index.",
    )
    from_library.add_argument("source", nargs="?", help="Library source directory. Falls back to config.json imports.library_default_source.")
    from_library.add_argument("--limit", type=int, help="Maximum number of files to import.")
    from_library.add_argument("-y", "--yes", action="store_true", help="Run immediately without confirmation.")

    maintain = subparsers.add_parser(
        "maintain",
        help="Run status, doctor, index refresh, and backup as one maintenance workflow.",
        description="Run status, doctor, index refresh, and backup as one maintenance workflow.",
    )
    maintain.add_argument("--destination", help="Backup destination path override.")
    maintain.add_argument("--executor", help="Backup executor override.")
    maintain.add_argument("-y", "--yes", action="store_true", help="Run immediately without confirmation.")


def add_primitive_parsers(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
    *,
    show_primitives: bool,
) -> None:
    primitive_help = None if show_primitives else argparse.SUPPRESS

    subparsers.add_parser(
        "init",
        help=primitive_help,
        description="Discover the current vault layout, create only missing app-owned paths, and write config.json.",
    )

    capture = subparsers.add_parser(
        "capture-url",
        help=primitive_help,
        description="Append a URL to the inbox, or merge metadata into an existing canonical URL.",
    )
    capture.add_argument("url", help="URL or local path to capture.")
    capture.add_argument("--title", help="Optional human title stored with the inbox record.")
    capture.add_argument("--source", default="manual", help=f"Capture source label, such as `manual` or `{DEFAULT_TABS_SOURCE_NAME}`.")
    capture.add_argument("--priority", choices=["low", "medium", "high"], default="medium", help="Priority recorded on the inbox item.")
    capture.add_argument("--tag", action="append", help="Tag to attach. Repeat or pass comma-separated values.")
    capture.add_argument("--notes", help="Freeform notes stored on the inbox record.")
    capture.add_argument("--safe-ip", action="store_true", help="Mark the URL as requiring a safe IP profile for fetching.")
    capture.add_argument("--requires-login", action="store_true", help="Mark the URL as requiring an authenticated session.")
    capture.add_argument("--private", action="store_true", help="Mark the URL as private content.")

    inbox = subparsers.add_parser(
        "inbox",
        help=primitive_help,
        description="List inbox URL records, optionally filtered by status or provenance.",
    )
    inbox.add_argument("--status", help="Filter by inbox status, for example `new` or `fetched`.")
    inbox.add_argument("--pool", help="Filter by pool name, for example `saved-page-urls`.")
    inbox.add_argument("--domain", help="Filter by inferred domain, or `local-file` for paths.")
    inbox.add_argument("--captured-from", help="Filter by capture source label.")
    inbox.add_argument("--limit", type=int, default=20, help="Maximum number of records to print.")

    fetch = subparsers.add_parser(
        "fetch-new",
        help=primitive_help,
        description="Fetch inbox items with status `new` into the raw store and the configured vault page directory.",
    )
    fetch.add_argument("--limit", type=int, default=20, help="Maximum number of `new` inbox items to fetch.")
    fetch.add_argument("--executor", help="Executor label to record on fetched items. When omitted, routing defaults choose one from config.json.")

    summarize = subparsers.add_parser(
        "summarize",
        help=primitive_help,
        description="Create a summary-note stub for a fetched markdown article.",
    )
    summarize.add_argument("article", help="Path to the article markdown file. Relative paths are resolved from the current working directory.")
    summarize.add_argument("--model", default="manual", help="Model label written into the generated note metadata.")
    summarize.add_argument("--operator", default=DEFAULT_OPERATOR_NAME, help="Operator label written into the generated note metadata.")
    summarize.add_argument("--executor", default=DEFAULT_LOCAL_EXECUTOR, help="Executor label written into the generated note metadata.")

    extract = subparsers.add_parser(
        "extract",
        help=primitive_help,
        description="Create a structured extraction batch note for a fetched markdown article.",
    )
    extract.add_argument("article", help="Path to the article markdown file. Relative paths are resolved from the current working directory.")
    extract.add_argument("--batch", choices=["grounding", "assumption-check", "mental-model-update", "implementation-detail"], default="grounding", help="Extraction template to generate.")
    extract.add_argument("--goal", help="Primary decision or question the extraction should focus on.")
    extract.add_argument("--assumption", action="append", help="Assumption to test. Repeat to add multiple assumptions.")
    extract.add_argument("--model", default="manual", help="Model label written into the generated note metadata.")
    extract.add_argument("--operator", default=DEFAULT_OPERATOR_NAME, help="Operator label written into the generated note metadata.")
    extract.add_argument("--executor", default=DEFAULT_LOCAL_EXECUTOR, help="Executor label written into the generated note metadata.")

    export = subparsers.add_parser(
        "export",
        help=primitive_help,
        description="Copy an app-owned content tree into exports/, normalizing text files to Markdown.",
    )
    export.add_argument("app", help=f"App namespace under exports/, for example `{DEFAULT_LIBRARY_APP}`.")
    export.add_argument("--source", required=True, help="Source directory to export from.")
    export.add_argument("--limit", type=int, help="Maximum number of files to export.")

    normalize = subparsers.add_parser(
        "normalize",
        help=primitive_help,
        description="Rebuild normalized Markdown files under an export tree that already contains raw/.",
    )
    normalize.add_argument("path", help="Path to an export root containing raw/. Relative paths are resolved from the knowledge root.")

    import_library = subparsers.add_parser(
        "import-library",
        help=primitive_help,
        description=f"Import a library export into exports/{DEFAULT_LIBRARY_APP}/.",
    )
    import_library.add_argument("source", nargs="?", help="Library source directory. Falls back to imports.library_default_source in config.json.")
    import_library.add_argument("--limit", type=int, help="Maximum number of files to export.")

    import_simpread = subparsers.add_parser(
        "import-simpread",
        help=argparse.SUPPRESS,
        description="Deprecated alias for import-library.",
    )
    import_simpread.add_argument("source", nargs="?", help=argparse.SUPPRESS)
    import_simpread.add_argument("--limit", type=int, help=argparse.SUPPRESS)

    import_tabs = subparsers.add_parser(
        "import-tabs",
        help=primitive_help,
        description="Import saved browser tabs into the inbox with canonical-URL deduplication.",
    )
    import_tabs.add_argument("source", nargs="?", help="JSON array of tab objects. Falls back to imports.tabs_default_source in config.json.")
    import_tabs.add_argument("--limit", type=int, help="Maximum number of source items to process.")
    import_tabs.add_argument("--tag", action="append", help="Tag to attach to each imported record. Repeat or pass comma-separated values.")
    import_tabs.add_argument("--notes", help="Notes applied to each imported record.")
    import_tabs.add_argument("--priority", choices=["low", "medium", "high"], default="medium", help="Priority applied to each imported record.")
    import_tabs.add_argument("--source-name", default=DEFAULT_TABS_SOURCE_NAME, help="Capture source label stored on imported records.")

    index = subparsers.add_parser(
        "index",
        help=primitive_help,
        description="Index management commands.",
    )
    index_subparsers = index.add_subparsers(dest="index_command", required=True, metavar="index_command")
    index_subparsers.add_parser("update", help=primitive_help, description="Rebuild the SQLite document index from the configured vault content folders.")

    search = subparsers.add_parser(
        "search",
        help=primitive_help,
        description="Query the SQLite knowledge index.",
    )
    search.add_argument("query", help="FTS query string. Falls back to LIKE search if FTS5 is unavailable.")
    search.add_argument("--limit", type=int, default=10, help="Maximum number of results to return.")

    backup = subparsers.add_parser(
        "backup",
        help=primitive_help,
        description="Copy the knowledge root to a local mirror or a configured remote executor.",
    )
    backup.add_argument("--destination", help="Backup destination path. Uses config.json defaults when omitted.")
    backup.add_argument("--executor", help=f"Executor to use. `{DEFAULT_REMOTE_EXECUTOR}` attempts a remote SSH backup when fully configured.")

    remote = subparsers.add_parser(
        "remote",
        help=primitive_help,
        description="Inspect or use configured remote executor access.",
    )
    remote_subparsers = remote.add_subparsers(dest="remote_command", required=True, metavar="remote_command")

    remote_show_target = remote_subparsers.add_parser("show-target", help=primitive_help, description="Print the resolved SSH transport command and target for an executor.")
    remote_show_target.add_argument("--executor", default=DEFAULT_REMOTE_EXECUTOR, help="Executor name to inspect.")

    remote_check = remote_subparsers.add_parser("check", help=primitive_help, description="Run a remote hostname check using the configured SSH transport.")
    remote_check.add_argument("--executor", default=DEFAULT_REMOTE_EXECUTOR, help="Executor name to check.")

    remote_exec = remote_subparsers.add_parser("exec", help=primitive_help, description="Run an arbitrary command on a configured remote executor.")
    remote_exec.add_argument("--executor", default=DEFAULT_REMOTE_EXECUTOR, help="Executor name to use.")
    remote_exec.add_argument("remote_args", nargs=argparse.REMAINDER, help="Remote command to execute. Prefix with `--`, for example `ops-knowledge remote exec -- uname -a`.")

    subparsers.add_parser("status", help=primitive_help, description="Print inbox, index, backup, and remote-access status.")
    subparsers.add_parser("doctor", help=primitive_help, description="Run configuration and environment checks and report warnings.")


def build_parser(*, show_primitives: bool = False) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ops-knowledge",
        description="Workflow-first CLI for collecting, processing, and maintaining local knowledge assets.",
        epilog=(
            "`KNOWLEDGE_ROOT` takes precedence over `--root` when both are set.\n"
            "Use `--advanced-help` to reveal primitive commands and workflow-to-primitive mapping."
        ),
        formatter_class=HelpFormatter,
    )
    parser.add_argument("--root", type=Path, help="Knowledge root directory for this invocation. Ignored when KNOWLEDGE_ROOT is set.")
    subparsers = parser.add_subparsers(dest="command", required=True, metavar="workflow")
    add_workflow_parsers(subparsers)
    add_primitive_parsers(subparsers, show_primitives=show_primitives)
    if not show_primitives:
        subparsers._choices_actions = [action for action in subparsers._choices_actions if action.help != argparse.SUPPRESS]
    return parser


def main(argv: list[str] | None = None) -> int:
    raw_argv = list(argv or [])
    if argv is None:
        import sys

        raw_argv = sys.argv[1:]
    advanced_help = False
    if "--advanced-help" in raw_argv:
        advanced_help = True
        raw_argv = [item for item in raw_argv if item != "--advanced-help"]

    parser = build_parser(show_primitives=advanced_help)
    if advanced_help and not raw_argv:
        parser.print_help()
        print()
        print(render_advanced_help())
        return 0

    args = parser.parse_args(raw_argv)
    store = KnowledgeStore(root=args.root)

    if args.command in {"from-tabs", "from-url", "from-library", "maintain"}:
        return command_workflow(store, args)
    if args.command == "init":
        return command_init(store, args)
    if args.command == "capture-url":
        return command_capture_url(store, args)
    if args.command == "inbox":
        return command_inbox(store, args)
    if args.command == "fetch-new":
        return command_fetch_new(store, args)
    if args.command == "summarize":
        return command_summarize(store, args)
    if args.command == "extract":
        return command_extract(store, args)
    if args.command == "export":
        return command_export(store, args)
    if args.command == "normalize":
        return command_normalize(store, args)
    if args.command in {"import-library", "import-simpread"}:
        return command_import_library(store, args)
    if args.command == "import-tabs":
        return command_import_tabs(store, args)
    if args.command == "index" and args.index_command == "update":
        return command_index_update(store, args)
    if args.command == "search":
        return command_search(store, args)
    if args.command == "backup":
        return command_backup(store, args)
    if args.command == "remote":
        return command_remote(store, args)
    if args.command == "status":
        return command_status(store, args)
    if args.command == "doctor":
        return command_doctor(store, args)
    parser.error(f"Unhandled command: {args.command}")
    return 2
