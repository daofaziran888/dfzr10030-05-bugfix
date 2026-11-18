"""数据血缘追踪 DAG 拓扑接口：节点连线、增量台账血缘联动。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload
from app.services.lineage import LineageService

router = APIRouter(prefix="/api/lineage", tags=["数据血缘追踪"])
service = LineageService()
LIST_FIELDS = ["关系编号", "上游节点", "下游节点", "来源字段", "目标字段", "转换说明", "采集批次", "关系版本"]
STATUSES = ["草稿", "已记录", "已验证", "已归档"]


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


@router.get("")
def get_graph() -> dict[str, Any]:
    """返回节点与边组成的 DAG 图数据。"""
    return service.get_graph()


@router.get("/incremental-ledger")
def get_incremental_ledger() -> dict[str, Any]:
    """血缘视角读取增量游标处理台账与补采血缘绑定。"""
    return service.incremental_overview()


@router.post("/backfill/{order_id}/bind")
def bind_backfill(order_id: str, payload: EntryPayload) -> ActionResult:
    """把补采单的每个补采批次绑定到下游字段关系，并回写待核对台账。"""
    entry_or_none, message = service.bind_backfill(order_id, payload.values)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)


@router.post("/checks/refresh")
def refresh_checks() -> ActionResult:
    """跟随增量作业时间线 / 台账状态变化更新待核对内容。"""
    items = service.follow_status_changes()
    return ActionResult(ok=True, message=f"已刷新 {len(items)} 条待核对内容", entry={"items": items})


@router.post("/checks/{check_id}/resolve")
def resolve_check(check_id: int) -> ActionResult:
    entry_or_none, message = service.resolve_check(check_id)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)


@router.get("/orchestration")
def get_orchestration() -> dict[str, Any]:
    """按增量台账编排增量游标的后续处理。"""
    return {"items": service.orchestrate_next()}


@router.get("/export")
def export_entries() -> StreamingResponse:
    nodes, edges = service.raw_data()
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    writer.writerow(["type", "id", "name", "from", "to", "label", "status"])
    for node in nodes:
        writer.writerow(["node", node.get("id", ""), csv_cell(node.get("name", "")), "", "", "", node.get("status", "")])
    for edge in edges:
        writer.writerow(["edge", "", "", edge.get("from", ""), edge.get("to", ""), edge.get("label", ""), ""])
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=lineage.csv"})


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"血缘关系 {entry_id} 不存在")
    return entry


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="血缘关系已登记", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    entry_or_none, message = service.run_action(entry_id, payload.action)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)
