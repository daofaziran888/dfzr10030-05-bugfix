"""增量采集 时间线服务。

修复点：
1. 名称解析改为读取真实来源标识并经来源目录解析为来源名称，不再把状态列当作业名；
2. 批次水位由台账引擎统一计算，失败批次守住未完成水位，后续数据不得越过未处理区间；
3. 输出增量作业时间线与游标处理台账，并提供迟到冲突、补采恢复、幂等确认能力。
"""
from __future__ import annotations

from typing import Any

from app.services.incremental_bootstrap import bootstrap
from app.services.incremental_ledger import (
    BATCH_DONE,
    BATCH_FAILED,
    CONFLICT_STRATEGIES,
    Cursor,
    IncrementalLedgerEngine,
    LedgerViolation,
    ledger_store,
)
from app.store import store

MODULE = "incremental"
REQUIRED_FIELDS = ["游标编号", "来源标识", "增量字段"]
STATUS_ORDER = ["待启动", "运行中", "已完成", "失败"]
ACTION_RULES = {"启动采集": "运行中", "暂停采集": "待启动", "标记失败": "失败"}
NEGATIVE_ACTIONS = ["标记失败"]
BATCH_ACTION_RULES = {"标记失败": BATCH_FAILED, "标记完成": BATCH_DONE}

# 首次使用时引导演示台账（幂等：已存在游标时不重建）。
if not ledger_store.cursors:
    bootstrap(ledger_store)


class IncrementalService:
    def __init__(self) -> None:
        self.engine = IncrementalLedgerEngine(ledger_store)

    # -- 时间线（修复名称 + 水位）-------------------------------------------

    def get_timeline(self, cursor_no: str | None = None) -> list[dict[str, Any]]:
        return self.engine.timeline(cursor_no)

    def get_batch_detail(self, batch_id: int) -> dict[str, Any] | None:
        # 详情与列表读取同一份台账记录和同一版本，避免详情页丢版本。
        return self.engine.batch_detail(batch_id)

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        """兼容旧契约：按游标自增 id 返回游标明细。"""
        for cursor in ledger_store.cursors.values():
            if cursor.id == entry_id:
                snapshot = self.engine.watermark_snapshot(cursor.游标编号)
                row = store.find(MODULE, entry_id)
                base = dict(row) if row else cursor.to_dict()
                base.update(cursor.to_dict())
                base["来源名称"] = ledger_store.sources.resolve(cursor.来源标识)["来源名称"]
                base["当前水位"] = snapshot["下一次采集起点"]
                base["下次采集起点"] = snapshot["下一次采集起点"]
                base["已提交水位"] = snapshot["已提交水位"]
                return base
        return store.find(MODULE, entry_id)

    # -- 台账 ----------------------------------------------------------------

    def get_ledger(self) -> dict[str, Any]:
        return {
            "rules": self.engine.rules(),
            "cursors": self.engine.cursor_ledger(),
            "batches": self.engine.batch_ledger(),
            "orders": self.engine.orders(),
            "checks": self.engine.check_ledger(),
            "bindings": self.engine.lineage_bindings(),
        }

    def get_rules(self) -> dict[str, list[dict[str, str]]]:
        return self.engine.rules()

    def validate_ledger(self, payload: dict[str, Any]) -> list[dict[str, str]]:
        return self.engine.validate_ledger_payload(payload)

    # -- 批次状态流转 --------------------------------------------------------

    def run_batch_action(self, batch_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action not in BATCH_ACTION_RULES:
            return None, f"动作「{action}」不属于批次处理可执行范围（支持：{'、'.join(BATCH_ACTION_RULES)}）"
        try:
            batch = self.engine.mark_batch(batch_id, BATCH_ACTION_RULES[action])
        except LedgerViolation as exc:
            return None, exc.message
        return batch.to_dict(), f"批次 {batch_id} 已{action}，下一次采集起点未越过未完成水位"

    # -- 迟到批次冲突 --------------------------------------------------------

    def register_late_batch(self, cursor_no: str, payload: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        strategy = str(payload.get("strategy", "")).strip()
        if strategy not in CONFLICT_STRATEGIES:
            return None, f"冲突策略必须是 {'、'.join(CONFLICT_STRATEGIES)} 之一"
        try:
            result = self.engine.register_late_batch(
                cursor_no,
                int(payload["批次序号"]),
                int(payload["起始水位"]),
                int(payload["结束水位"]),
                strategy,
                arrived_at=str(payload.get("到达时间") or "").strip() or None,
            )
        except LedgerViolation as exc:
            return None, exc.message
        except (KeyError, ValueError, TypeError):
            return None, "迟到批次参数不完整，需提供 批次序号、起始水位、结束水位"
        return result, "迟到批次已按业务边界策略处理"

    def list_orders(self) -> list[dict[str, Any]]:
        return self.engine.orders()

    def recover_order(self, order_id: str) -> tuple[dict[str, Any] | None, str]:
        try:
            result = self.engine.recover_backfill(order_id)
        except LedgerViolation as exc:
            return None, exc.message
        return result, f"补采单 {order_id} 已恢复，水位按补采区间重新提交"

    # -- 幂等确认 ------------------------------------------------------------

    def confirm(self, payload: dict[str, Any]) -> dict[str, Any]:
        key = str(payload.get("确认键", "")).strip()
        if not key:
            return {"接受": False, "说明": "确认键不允许为空"}
        return self.engine.confirm(key, payload.get("内容", payload))

    def check_ledger(self, status: str | None = None) -> list[dict[str, Any]]:
        return self.engine.check_ledger(status)

    def resolve_check(self, check_id: int) -> tuple[dict[str, Any] | None, str]:
        entry = self.engine.resolve_check(check_id)
        if entry is None:
            return None, f"待核对记录 {check_id} 不存在"
        return entry.to_dict(), f"待核对记录 {check_id} 已核对"

    # -- 旧契约：游标登记与动作（同时同步台账）------------------------------

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
        entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)

        # 同步登记到增量台账：来源标识进来源目录，游标可立即出现在时间线。
        cursor_no = str(values["游标编号"]).strip()
        source_id = str(values["来源标识"]).strip()
        if cursor_no not in ledger_store.cursors:
            ledger_store.sources.register(source_id, source_id)
            ledger_store.cursors[cursor_no] = Cursor(
                id=entry["id"], 游标编号=cursor_no, 来源标识=source_id,
                增量字段=str(values.get("增量字段", "")).strip(),
                批次大小=1000, 采集间隔="15分钟", 初始水位=0,
                上次采集="", 下次采集="", status=STATUS_ORDER[0],
            )
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"增量游标 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于增量采集可执行范围"
        target = ACTION_RULES[action]
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        cursor_no = str(entry.get("游标编号", "")).strip()
        if cursor_no in ledger_store.cursors:
            ledger_store.cursors[cursor_no].status = target
        return entry, f"增量游标已{action}"
