"""数据血缘追踪 DAG 图服务。

沿增量采集服务调用关系处理：
1. 读取增量游标处理台账，把每个补采批次绑定到下游字段关系；
2. 同步写入「增量作业时间线及游标处理台账」的血缘绑定视图，并回写待核对台账；
3. 跟随台账状态变化更新待核对内容；
4. 按台账编排增量游标的后续处理（补采恢复 / 继续采集建议）。
"""
from __future__ import annotations

from typing import Any

from app.services.incremental_ledger import (
    CHECK_DONE,
    IncrementalLedgerEngine,
    LedgerViolation,
    ledger_store,
)
from app.store import store

MODULE = "lineage"
REQUIRED_FIELDS = ["关系编号", "上游节点", "下游节点"]
STATUS_ORDER = ["草稿", "已记录", "已验证", "已归档"]
ACTION_RULES = {"记录关系": "已记录", "验证关系": "已验证", "归档关系": "已归档"}
NEGATIVE_ACTIONS = []


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


class LineageService:
    def __init__(self) -> None:
        self.engine = IncrementalLedgerEngine(ledger_store)

    def get_graph(self) -> dict[str, Any]:
        rows = store.rows(MODULE)
        nodes: list[dict] = []
        edges: list[dict] = []
        for i, row in enumerate(rows):
            # 名称解析使用明确业务字段（关系编号），不再按键位取值。
            name = str(row.get("关系编号") or row.get("id") or f"节点{i + 1}")
            nodes.append({
                "id": int(row.get("id", i + 1)),
                "name": name,
                "status": str(row.get("status", STATUS_ORDER[0])),
                **{k: row[k] for k in row if k not in ("id", "status", "pending", "abnormal")},
            })
        for i in range(len(nodes) - 1):
            edges.append({"from": nodes[i]["id"], "to": nodes[i + 1]["id"], "label": "依赖"})
        return {"nodes": nodes, "edges": edges}

    # -- 沿增量调用关系：读取游标处理台账 -----------------------------------

    def incremental_overview(self) -> dict[str, Any]:
        """血缘视角的增量台账：游标水位、批次链、补采单、待核对。"""
        return {
            "cursors": self.engine.cursor_ledger(),
            "batches": self.engine.batch_ledger(),
            "orders": self.engine.orders(),
            "checks": self.engine.check_ledger(),
            "bindings": self.engine.lineage_bindings(),
        }

    def bind_backfill(self, order_id: str, payload: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """把补采单上的每个补采批次绑定到下游字段关系，并回写待核对台账。"""
        try:
            binding = self.engine.bind_lineage(
                order_id,
                upstream=str(payload.get("上游节点", "增量采集.补采批次")),
                downstream=str(payload.get("下游节点", "")),
                source_field=str(payload.get("来源字段", "")),
                target_field=str(payload.get("目标字段", "")),
                version=str(payload.get("关系版本", "v1")),
            )
        except LedgerViolation as exc:
            return None, exc.message
        # 同步回写待核对台账：血缘绑定后该补采链进入待核对。
        order = self.engine.get_order(order_id)
        if order is not None:
            for batch_id in order.关联批次:
                batch = self.engine.get_batch(batch_id)
                if batch is not None:
                    self.engine._touch_check(batch, reason=f"血缘绑定 {order_id}")
        return binding.to_dict(), f"补采单 {order_id} 的补采批次已绑定下游字段关系"

    def follow_status_changes(self) -> list[dict[str, Any]]:
        """跟随增量台账状态变化，刷新待核对内容（已核对项保留，新状态生成新待核对）。"""
        refreshed: list[dict[str, Any]] = []
        for item in self.engine.batch_ledger():
            if item["批次状态"] in ("失败", "迟到"):
                batch = self.engine.get_batch(item["台账id"])
                if batch is not None:
                    entry = self.engine._touch_check(batch, reason="血缘服务跟随状态变化刷新待核对")
                    refreshed.append(entry.to_dict())
        return refreshed

    def resolve_check(self, check_id: int) -> tuple[dict[str, Any] | None, str]:
        entry = self.engine.resolve_check(check_id)
        if entry is None:
            return None, f"待核对记录 {check_id} 不存在"
        return entry.to_dict(), f"待核对记录 {check_id} 已核对"

    def orchestrate_next(self) -> list[dict[str, Any]]:
        """按增量作业时间线及游标处理台账编排增量游标的后续处理。"""
        plan: list[dict[str, Any]] = []
        for cursor in self.engine.cursor_ledger():
            cursor_no = cursor["游标编号"]
            open_orders = [o for o in self.engine.orders()
                           if o["游标编号"] == cursor_no and o["status"] != "补采完成"]
            if open_orders:
                plan.append({
                    "游标编号": cursor_no,
                    "来源名称": cursor["来源名称"],
                    "下一次采集起点": cursor["下一次采集起点"],
                    "编排动作": "从补采单恢复",
                    "补采单": [o["id"] for o in open_orders],
                    "补采范围": [o["补采范围"] for o in open_orders],
                    "说明": "先恢复未完成补采区间，再继续常规增量，不得跳过未完成批次",
                })
            elif cursor.get("最早未完成批次") is not None:
                plan.append({
                    "游标编号": cursor_no,
                    "来源名称": cursor["来源名称"],
                    "下一次采集起点": cursor["下一次采集起点"],
                    "编排动作": "从未完成水位继续采集",
                    "补采单": [],
                    "补采范围": [],
                    "说明": f"从批次 {cursor['最早未完成批次']} 起点 {cursor['下一次采集起点']} 续采",
                })
            else:
                plan.append({
                    "游标编号": cursor_no,
                    "来源名称": cursor["来源名称"],
                    "下一次采集起点": cursor["下一次采集起点"],
                    "编排动作": "常规增量采集",
                    "补采单": [],
                    "补采范围": [],
                    "说明": "无未完成批次，按采集间隔调度下一轮",
                })
        return plan

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

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
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"血缘关系 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于数据血缘追踪可执行范围"
        target = ACTION_RULES[action]
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        # 血缘关系验证后，联动核对增量台账中的待核对项。
        if target == "已验证":
            self.follow_status_changes()
        return entry, f"血缘关系已{action}"

    def raw_data(self) -> tuple[list[dict], list[dict]]:
        g = self.get_graph()
        return g["nodes"], g["edges"]
