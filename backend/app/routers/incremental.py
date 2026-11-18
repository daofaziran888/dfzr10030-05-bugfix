"""增量采集 时间线接口：作业时间序列、游标处理台账、冲突与补采。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload
from app.services.incremental import IncrementalService

router = APIRouter(prefix="/api/incremental", tags=["增量采集"])
service = IncrementalService()
LIST_FIELDS = ["游标编号", "来源标识", "增量字段", "当前水位", "批次大小", "采集间隔", "上次采集", "下次采集"]
STATUSES = ["待启动", "运行中", "已完成", "失败"]


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


@router.get("")
def list_entries(cursor: str | None = Query(default=None, description="按游标编号过滤")) -> dict[str, Any]:
    """增量作业时间线：每个批次显示解析后的真实来源名称与独立状态。"""
    items = service.get_timeline(cursor)
    return {"items": items, "total": len(items)}


@router.get("/ledger")
def get_ledger() -> dict[str, Any]:
    """游标处理台账：条件 / 标准 / 适用范围 + 游标水位 + 批次核对 + 补采单 + 待核对。"""
    return service.get_ledger()


@router.get("/ledger/rules")
def get_ledger_rules() -> dict[str, Any]:
    """台账保存的条件、标准与适用范围。"""
    return service.get_rules()


@router.post("/ledger/validate")
def validate_ledger(payload: EntryPayload) -> ActionResult:
    """台账合规预检：不合规结果不允许保存，返回违规条件 / 标准编号。"""
    violations = service.validate_ledger(payload.values)
    if violations:
        detail = "；".join(f"{v['rule_code']} {v['message']}" for v in violations)
        return ActionResult(ok=False, message=f"台账校验未通过：{detail}", entry={"violations": violations})
    return ActionResult(ok=True, message="台账校验通过，允许保存")


@router.get("/export")
def export_entries(cursor: str | None = Query(default=None)) -> StreamingResponse:
    items = service.get_timeline(cursor)
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    all_keys = ["id", "name", "来源标识", "游标编号", "批次序号", "status", "水位区间",
                "起始水位", "结束水位", "下一次采集起点", "已提交水位", "start", "end"]
    writer.writerow(all_keys)
    for item in items:
        writer.writerow([csv_cell(item.get(k, "")) for k in all_keys])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=incremental.csv"})


@router.get("/batches/{batch_id}")
def get_batch(batch_id: int) -> dict[str, Any]:
    """批次详情：与列表入口读取同一版本记录，包含来源名称与补采范围。"""
    detail = service.get_batch_detail(batch_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"批次 {batch_id} 不存在或已归档")
    return detail


@router.post("/batches/{batch_id}/actions")
def run_batch_action(batch_id: int, payload: ActionPayload) -> ActionResult:
    """批次状态流转（标记失败 / 标记完成）；失败不会推进当前水位。"""
    entry_or_none, message = service.run_batch_action(batch_id, payload.action)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)


@router.get("/orders")
def list_orders() -> dict[str, Any]:
    """补采单列表。"""
    return {"items": service.list_orders()}


@router.post("/cursors/{cursor_no}/late-batch")
def register_late_batch(cursor_no: str, payload: EntryPayload) -> ActionResult:
    """登记迟到批次并按 rollback_first / backfill_order 处理同链冲突。"""
    result_or_none, message = service.register_late_batch(cursor_no, payload.values)
    if result_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=result_or_none)


@router.post("/orders/{order_id}/recover")
def recover_order(order_id: str) -> ActionResult:
    """从补采单恢复：按补采区间重新采集，完成后水位才继续向前。"""
    result_or_none, message = service.recover_order(order_id)
    if result_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=result_or_none)


@router.post("/confirmations")
def create_confirmation(payload: EntryPayload) -> ActionResult:
    """批次确认：重复提交仅保留首份确认（按确认键幂等）。"""
    result = service.confirm(payload.values)
    return ActionResult(ok=bool(result.get("接受")), message=result.get("说明", ""), entry=result)


@router.get("/checks")
def get_checks(status: str | None = Query(default=None)) -> dict[str, Any]:
    """待核对台账（血缘服务回写、跟随时间线状态变化更新）。"""
    return {"items": service.check_ledger(status)}


@router.post("/checks/{check_id}/resolve")
def resolve_check(check_id: int) -> ActionResult:
    entry_or_none, message = service.resolve_check(check_id)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"增量游标 {entry_id} 不存在")
    return entry


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="增量游标已登记", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    entry_or_none, message = service.run_action(entry_id, payload.action)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)
