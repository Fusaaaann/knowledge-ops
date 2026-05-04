from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from enum import Enum
from pathlib import Path
from typing import Any


class WorkflowStageStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"


@dataclass
class BatchRef:
    run_id: str
    batch_id: str
    workflow: str
    source_kind: str
    source_ref: str


@dataclass
class WorkflowEvent:
    run_id: str
    workflow: str
    event_type: str
    at: str
    status: WorkflowStageStatus
    stage_name: str | None = None
    message: str = ""
    payload: dict[str, Any] | None = None


@dataclass
class WorkflowStageSnapshot:
    name: str
    label: str
    status: WorkflowStageStatus
    started_at: str | None = None
    finished_at: str | None = None
    result_path: str | None = None
    error: str | None = None


@dataclass
class WorkflowRunState:
    run_id: str
    workflow: str
    status: WorkflowStageStatus
    created_at: str
    updated_at: str
    source_kind: str
    source_ref: str
    batch_id: str | None = None
    current_stage: str | None = None
    current_step_index: int = 0
    total_steps: int = 0
    stages: list[WorkflowStageSnapshot] | None = None


@dataclass
class StageItemResult:
    item_id: str
    status: WorkflowStageStatus
    source_ref: str
    output_path: str | None = None
    note_path: str | None = None
    error: str | None = None


@dataclass
class ImportTabsRequest:
    run_id: str
    batch_id: str
    source: str
    source_name: str
    tags: list[str]
    notes: str
    priority: str
    limit: int | None = None


@dataclass
class ImportTabsResult:
    run_id: str
    batch_id: str
    source: str
    captured_from: str
    imported: int
    merged: int
    skipped: int
    total_inbox: int
    created_record_ids: list[str]
    merged_record_ids: list[str]
    touched_paths: list[str]
    items: list[StageItemResult]
    status: WorkflowStageStatus
    created_at: str


@dataclass
class CaptureUrlRequest:
    run_id: str
    batch_id: str
    url: str
    title: str
    source: str
    tags: list[str]
    notes: str
    priority: str
    safe_ip: bool
    requires_login: bool
    private: bool


@dataclass
class CaptureUrlResult:
    run_id: str
    batch_id: str
    url: str
    canonical_url: str
    record_id: str
    duplicate: bool
    touched_paths: list[str]
    items: list[StageItemResult]
    status: WorkflowStageStatus
    created_at: str


@dataclass
class ImportLibraryRequest:
    run_id: str
    batch_id: str
    app: str
    source: str
    limit: int | None = None


@dataclass
class ImportLibraryResult:
    run_id: str
    batch_id: str
    app: str
    source: str
    export_root: str
    exported: int
    normalized: int
    manifest_path: str
    touched_paths: list[str]
    items: list[StageItemResult]
    status: WorkflowStageStatus
    created_at: str


@dataclass
class FetchBatchRequest:
    run_id: str
    batch_id: str | None
    executor: str | None = None
    limit: int | None = None


@dataclass
class FetchBatchResult:
    run_id: str
    batch_id: str | None
    selected_records: list[str]
    fetched: int
    failed: int
    touched_paths: list[str]
    items: list[StageItemResult]
    status: WorkflowStageStatus
    created_at: str


@dataclass
class SummarizeBatchRequest:
    run_id: str
    batch_id: str | None
    article_paths: list[str] | None
    model: str
    operator: str
    executor: str


@dataclass
class SummarizeBatchResult:
    run_id: str
    batch_id: str | None
    summarized: int
    touched_paths: list[str]
    items: list[StageItemResult]
    status: WorkflowStageStatus
    created_at: str


@dataclass
class ExtractBatchRequest:
    run_id: str
    batch_id: str | None
    article_paths: list[str] | None
    template: str
    goal: str
    assumptions: list[str]
    model: str
    operator: str
    executor: str


@dataclass
class ExtractBatchResult:
    run_id: str
    batch_id: str | None
    template: str
    extracted: int
    touched_paths: list[str]
    items: list[StageItemResult]
    status: WorkflowStageStatus
    created_at: str


@dataclass
class NormalizeExportRequest:
    run_id: str
    path: str
    batch_id: str | None = None


@dataclass
class NormalizeExportResult:
    run_id: str
    path: str
    normalized: int
    touched_paths: list[str]
    items: list[StageItemResult]
    status: WorkflowStageStatus
    created_at: str


@dataclass
class IndexUpdateRequest:
    run_id: str
    batch_id: str | None


@dataclass
class IndexUpdateResult:
    run_id: str
    batch_id: str | None
    indexed: int
    database_path: str
    touched_paths: list[str]
    status: WorkflowStageStatus
    created_at: str


@dataclass
class StatusRequest:
    run_id: str


@dataclass
class StatusResult:
    run_id: str
    knowledge_root: str
    inbox: dict[str, int]
    pools: dict[str, int]
    captured_from: dict[str, int]
    index: dict[str, Any]
    backups: dict[str, Any]
    routing_defaults: dict[str, Any]
    remote_access: dict[str, Any]
    touched_paths: list[str]
    status: WorkflowStageStatus
    created_at: str


@dataclass
class DoctorRequest:
    run_id: str


@dataclass
class DoctorResult:
    run_id: str
    knowledge_root: str
    issues: list[str]
    touched_paths: list[str]
    status: WorkflowStageStatus
    created_at: str


@dataclass
class BackupRequest:
    run_id: str
    destination: str | None
    executor: str | None


@dataclass
class BackupResult:
    run_id: str
    destination: str
    executor: str
    mode: str
    transport: str | None
    target: str | None
    last_run_at: str
    touched_paths: list[str]
    status: WorkflowStageStatus
    created_at: str


def model_to_dict(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if is_dataclass(value):
        return model_to_dict(asdict(value))
    if isinstance(value, dict):
        return {key: model_to_dict(item) for key, item in value.items()}
    if isinstance(value, list):
        return [model_to_dict(item) for item in value]
    return value
