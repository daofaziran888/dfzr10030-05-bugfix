"""增量作业时间线与游标处理台账领域引擎。

输入：增量游标、来源标识、历史批次结果。
输出：增量作业时间线、游标处理台账、补采单、待核对台账、血缘绑定。

核心规则（按业务边界取舍，而不是按处理先后）：

1. 名称解析：时间线名称必须来自真实来源标识，经来源目录解析为来源名称；
   来源目录缺失时回退到来源标识本身，再回退到游标编号；任何情况下都不允许
   用批次状态充当作业名称。
2. 批次水位：只有「已完成」批次才能按顺序提交水位；存在失败/未完成批次时，
   已提交水位（下一次采集起点）停在最早未完成批次的起点，失败批次之后到达的
   新数据也不能把水位推过未处理区间。
3. 迟到批次与失败批次落在同一来源标识 + 同一水位区间时属于同一数据链冲突，
   支持两种业务取舍：
   - rollback_first：先按失败批次回退水位，再按原失败批次顺序继续采集，迟到
     批次并入补采链；
   - backfill_order：水位可前移，但失败区间另存为独立补采单，后续从补采单恢复。
   两种策略都必须守住「下一次采集起点不得跳过任何未完成批次」。
4. 重复确认按确认键幂等，只保留首份确认。
5. 台账保存前执行条件 / 标准 / 适用范围校验，不合规结果不允许保存。
6. 同一时间线内批次顺序与来源名称保持一致，按当前业务边界判定，旧结果保留。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

# ---------------------------------------------------------------------------
# 常量：状态、条件、标准、适用范围
# ---------------------------------------------------------------------------

BATCH_RUNNING = "运行中"
BATCH_DONE = "已完成"
BATCH_FAILED = "失败"
BATCH_LATE = "迟到"
BATCH_BOUNDARY_STATUSES = (BATCH_RUNNING, BATCH_DONE, BATCH_FAILED, BATCH_LATE)

# 可提交水位的批次状态：只有已完成批次可以推进水位。
COMMITTABLE_STATUSES = (BATCH_DONE,)
# 未完成状态：这些状态存在时水位必须停在其起点之前。
UNFINISHED_STATUSES = (BATCH_RUNNING, BATCH_FAILED, BATCH_LATE)

ORDER_PENDING = "待补采"
ORDER_COLLECTING = "补采中"
ORDER_DONE = "补采完成"
ORDER_STATUSES = (ORDER_PENDING, ORDER_COLLECTING, ORDER_DONE)

CHECK_PENDING = "待核对"
CHECK_DONE = "已核对"
CHECK_STATUSES = (CHECK_PENDING, CHECK_DONE)

CONFLICT_STRATEGIES = ("rollback_first", "backfill_order")

# 台账保存的条件 / 标准 / 适用范围（不合规结果不允许保存）。
LEDGER_RULES: dict[str, list[dict[str, str]]] = {
    "条件": [
        {"code": "C1", "rule": "批次必须绑定有效游标编号与真实来源标识，来源标识不允许为空或使用状态值占位"},
        {"code": "C2", "rule": "批次水位区间必须为 [起始水位, 结束水位]，且起始水位 < 结束水位"},
        {"code": "C3", "rule": "同一游标内批次序号在同一时间线内唯一且按顺序排列"},
        {"code": "C4", "rule": "冲突处理策略必须是 rollback_first 或 backfill_order"},
    ],
    "标准": [
        {"code": "S1", "rule": "时间线名称必须由来源标识解析为来源名称，禁止用批次状态充当作业名称"},
        {"code": "S2", "rule": "已提交水位只能由已完成批次按批次顺序推进，失败批次不得推进当前水位"},
        {"code": "S3", "rule": "存在未完成批次时，下一次采集起点等于最早未完成批次的起始水位"},
        {"code": "S4", "rule": "同一来源标识 + 水位区间的迟到批次按冲突策略处理，且不得跳过任何未完成批次"},
        {"code": "S5", "rule": "重复确认按确认键幂等，仅保留首份确认内容"},
    ],
    "适用范围": [
        {"code": "A1", "rule": "适用于增量游标下全部历史批次、迟到批次与补采批次"},
        {"code": "A2", "rule": "适用于按来源标识划分的业务边界；跨来源标识的批次不构成同一数据链冲突"},
        {"code": "A3", "rule": "已归档的历史批次结果保留原值，不因当前边界规则变化而重写"},
    ],
}


class LedgerViolation(ValueError):
    """台账不合规：条件 / 标准 / 适用范围校验未通过，结果不允许保存。"""

    def __init__(self, rule_code: str, message: str) -> None:
        super().__init__(f"[{rule_code}] {message}")
        self.rule_code = rule_code
        self.message = message


# ---------------------------------------------------------------------------
# 数据模型
# ---------------------------------------------------------------------------

@dataclass
class SourceDirectory:
    """来源标识 -> 来源名称 的解析目录（真实来源标识登记处）。"""

    mapping: dict[str, dict[str, str]] = field(default_factory=dict)

    def register(self, source_id: str, source_name: str, boundary: str = "默认边界") -> None:
        self.mapping[source_id] = {"来源名称": source_name, "业务边界": boundary}

    def resolve(self, source_id: str | None) -> dict[str, str]:
        """解析来源标识。解析不到时回退到标识本身，绝不回退到状态字段。"""
        sid = (source_id or "").strip()
        hit = self.mapping.get(sid)
        if hit:
            return {
                "来源标识": sid,
                "来源名称": hit["来源名称"],
                "业务边界": hit.get("业务边界", "默认边界"),
                "解析方式": "目录匹配",
            }
        return {
            "来源标识": sid,
            "来源名称": sid or "未登记来源",
            "业务边界": "默认边界",
            "解析方式": "标识回退",
        }


@dataclass
class Cursor:
    """增量游标：每个来源标识一条，记录原始水位与台账已提交水位。"""

    id: int
    游标编号: str
    来源标识: str
    增量字段: str
    批次大小: int
    采集间隔: str
    初始水位: int
    上次采集: str
    下次采集: str
    status: str = "待启动"

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "游标编号": self.游标编号,
            "来源标识": self.来源标识,
            "增量字段": self.增量字段,
            "批次大小": self.批次大小,
            "采集间隔": self.采集间隔,
            "初始水位": self.初始水位,
            "上次采集": self.上次采集,
            "下次采集": self.下次采集,
            "status": self.status,
        }


@dataclass
class Batch:
    """历史批次结果（增量游标处理台账的最小记录单位）。"""

    id: int
    游标编号: str
    来源标识: str
    批次序号: int
    起始水位: int
    结束水位: int
    status: str
    到达时间: str
    完成时间: str | None = None
    是否迟到: bool = False
    关联补采单: str | None = None
    版本: int = 1
    归档: bool = False
    备注: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "游标编号": self.游标编号,
            "来源标识": self.来源标识,
            "批次序号": self.批次序号,
            "起始水位": self.起始水位,
            "结束水位": self.结束水位,
            "status": self.status,
            "到达时间": self.到达时间,
            "完成时间": self.完成时间,
            "是否迟到": self.是否迟到,
            "关联补采单": self.关联补采单,
            "版本": self.版本,
            "归档": self.归档,
            "备注": self.备注,
        }


@dataclass
class BackfillOrder:
    """补采单：失败 / 迟到区间的独立补采恢复凭证。"""

    id: str
    游标编号: str
    来源标识: str
    起始水位: int
    结束水位: int
    原因批次序号: int
    策略: str
    status: str = ORDER_PENDING
    关联批次: list[int] = field(default_factory=list)
    创建时间: str = ""
    恢复时间: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "游标编号": self.游标编号,
            "来源标识": self.来源标识,
            "起始水位": self.起始水位,
            "结束水位": self.结束水位,
            "补采范围": f"[{self.起始水位}, {self.结束水位})",
            "原因批次序号": self.原因批次序号,
            "策略": self.策略,
            "status": self.status,
            "关联批次": list(self.关联批次),
            "创建时间": self.创建时间,
            "恢复时间": self.恢复时间,
        }


@dataclass
class CheckEntry:
    """待核对台账：血缘服务按时间线 / 台账状态变化回写。"""

    id: int
    来源标识: str
    游标编号: str
    批次序号: int | None
    水位区间: str
    触发状态: str
    status: str = CHECK_PENDING
    版本: int = 1
    更新时间: str = ""
    说明: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "来源标识": self.来源标识,
            "游标编号": self.游标编号,
            "批次序号": self.批次序号,
            "水位区间": self.水位区间,
            "触发状态": self.触发状态,
            "status": self.status,
            "版本": self.版本,
            "更新时间": self.更新时间,
            "说明": self.说明,
        }


@dataclass
class LineageBinding:
    """血缘绑定：把每个补采批次绑定到下游字段关系。"""

    id: int
    补采单: str
    批次id: int
    来源标识: str
    上游节点: str
    下游节点: str
    来源字段: str
    目标字段: str
    关系版本: str
    创建时间: str

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


# ---------------------------------------------------------------------------
# 台账仓库
# ---------------------------------------------------------------------------

class LedgerStore:
    """内存台账仓库：游标、批次、补采单、确认、待核对、血缘绑定统一存放。"""

    def __init__(self) -> None:
        self.sources = SourceDirectory()
        self.cursors: dict[str, Cursor] = {}
        self.batches: list[Batch] = []
        self.orders: list[BackfillOrder] = []
        self.checks: list[CheckEntry] = []
        self.bindings: list[LineageBinding] = []
        # 确认键 -> 首份确认内容（重复提交仅保留首份）。
        self.confirmations: dict[str, dict[str, Any]] = {}
        self._seq: dict[str, int] = {}

    def next_id(self, kind: str) -> int:
        self._seq[kind] = self._seq.get(kind, 0) + 1
        return self._seq[kind]

    def reset(self) -> None:
        self.__init__()


# 模块级单例，服务层与血缘模块共用同一份台账。
ledger_store = LedgerStore()


# ---------------------------------------------------------------------------
# 领域引擎
# ---------------------------------------------------------------------------

def _today() -> str:
    return date.today().isoformat()


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


class IncrementalLedgerEngine:
    """增量作业时间线 + 游标处理台账规则引擎。"""

    def __init__(self, store: LedgerStore | None = None) -> None:
        self.store = store or ledger_store

    # -- 校验 ---------------------------------------------------------------

    def _validate_batch(self, cursor_no: str, source_id: str, seq: int,
                        low: int, high: int, status: str) -> None:
        """保存前台账校验：条件 / 标准 / 适用范围，不合规直接拒绝保存。"""
        if not cursor_no or not source_id or source_id in BATCH_BOUNDARY_STATUSES:
            raise LedgerViolation("C1", "批次缺少有效游标编号或真实来源标识（不允许用状态值占位）")
        if not isinstance(low, int) or not isinstance(high, int) or low >= high:
            raise LedgerViolation("C2", f"批次水位区间非法：[{low}, {high})，要求起始水位 < 结束水位")
        if status not in BATCH_BOUNDARY_STATUSES:
            raise LedgerViolation("C3", f"批次状态「{status}」不在允许范围 {BATCH_BOUNDARY_STATUSES}")
        existing = [b for b in self.store.batches
                    if b.游标编号 == cursor_no and not b.归档 and b.批次序号 == seq]
        if existing:
            raise LedgerViolation("C3", f"游标 {cursor_no} 批次序号 {seq} 已存在，同一时间线内序号必须唯一")
        # 同一时间线内来源名称必须一致（按当前业务边界判定）。
        same_cursor = [b for b in self.store.batches if b.游标编号 == cursor_no and not b.归档]
        if same_cursor:
            bound_sid = same_cursor[0].来源标识
            if bound_sid != source_id:
                raise LedgerViolation(
                    "A2",
                    f"游标 {cursor_no} 已绑定来源标识 {bound_sid}，同一时间线不允许混入来源 {source_id}",
                )

    def validate_ledger_payload(self, payload: dict[str, Any]) -> list[dict[str, str]]:
        """对外的台账合规预检：返回违规清单；空清单代表允许保存。"""
        violations: list[dict[str, str]] = []
        try:
            self._validate_batch(
                str(payload.get("游标编号", "")),
                str(payload.get("来源标识", "")),
                int(payload.get("批次序号", 0)),
                int(payload.get("起始水位", -1)),
                int(payload.get("结束水位", -1)),
                str(payload.get("status", "")),
            )
        except (LedgerViolation, ValueError, TypeError) as exc:
            code = getattr(exc, "rule_code", "C2")
            violations.append({"rule_code": code, "message": str(exc)})
        return violations

    def rules(self) -> dict[str, list[dict[str, str]]]:
        return LEDGER_RULES

    # -- 写入 ---------------------------------------------------------------

    def add_batch(self, cursor_no: str, seq: int, low: int, high: int,
                  status: str, *, is_late: bool = False, arrived_at: str | None = None,
                  finished_at: str | None = None, remark: str = "",
                  archive: bool = False) -> Batch:
        cursor = self._require_cursor(cursor_no)
        source_id = cursor.来源标识
        self._validate_batch(cursor_no, source_id, seq, low, high, status)
        batch = Batch(
            id=self.store.next_id("batch"),
            游标编号=cursor_no,
            来源标识=source_id,
            批次序号=seq,
            起始水位=low,
            结束水位=high,
            status=BATCH_LATE if is_late else status,
            到达时间=arrived_at or _today(),
            完成时间=finished_at,
            是否迟到=is_late,
            归档=archive,
            备注=remark,
        )
        self.store.batches.append(batch)
        self._touch_check(batch, reason="批次登记")
        return batch

    def mark_batch(self, batch_id: int, status: str) -> Batch:
        """批次状态流转：标记失败 / 完成；失败不会推进水位。"""
        if status not in BATCH_BOUNDARY_STATUSES:
            raise LedgerViolation("C3", f"批次状态「{status}」不在允许范围")
        batch = self.get_batch(batch_id)
        if batch is None:
            raise LedgerViolation("C1", f"批次 {batch_id} 不存在")
        if batch.归档:
            raise LedgerViolation("A3", f"批次 {batch_id} 已归档，旧结果保留，不允许改写")
        batch.status = status
        batch.版本 = batch.版本 + 1
        if status == BATCH_DONE:
            batch.完成时间 = _today()
        self._touch_check(batch, reason=f"状态变更为{status}")
        return batch

    # -- 水位规则 -----------------------------------------------------------

    def _cursor_batches(self, cursor_no: str) -> list[Batch]:
        rows = [b for b in self.store.batches if b.游标编号 == cursor_no and not b.归档]
        return sorted(rows, key=lambda b: (b.批次序号, b.id))

    def watermark_snapshot(self, cursor_no: str) -> dict[str, Any]:
        """计算游标水位：失败批次守住未完成水位，后续数据不得越过未处理区间。

        - 已提交水位：只由已完成批次沿连续 / 重叠区间向前推进，失败批次不推进；
          backfill_order 下迟到批次已完成时，已提交水位可前移，但失败区间仍由补采单兜底。
        - 下一次采集起点：存在未完成批次时恒等于最早未完成批次的起始水位，
          任何策略都不允许跳过未完成批次。
        """
        cursor = self._require_cursor(cursor_no)
        rows = self._cursor_batches(cursor_no)
        committed = cursor.初始水位
        first_unfinished: Batch | None = None
        for batch in rows:
            if batch.status in UNFINISHED_STATUSES:
                if first_unfinished is None:
                    first_unfinished = batch
                continue
            # 已完成批次：与当前水位接壤或重叠时才能向前推进。
            if batch.起始水位 <= committed < batch.结束水位:
                committed = batch.结束水位
            elif batch.起始水位 > committed:
                # 中间存在未覆盖缺口，不能跳到后面的区间。
                if first_unfinished is None:
                    first_unfinished = batch
                break
            # 结束水位 <= 已提交水位：重复 / 被覆盖区间（如恢复后的迟到副本），跳过。
        if first_unfinished is None:
            next_start = committed
        else:
            # 下一次采集起点 = 最早未完成批次的起始水位（失败批次守住的水位）。
            next_start = first_unfinished.起始水位
        high_water_seen = max((b.结束水位 for b in rows), default=cursor.初始水位)
        return {
            "游标编号": cursor_no,
            "来源标识": cursor.来源标识,
            "初始水位": cursor.初始水位,
            "已提交水位": committed,
            "下一次采集起点": next_start,
            "最早未完成批次": first_unfinished.批次序号 if first_unfinished else None,
            "最早未完成状态": first_unfinished.status if first_unfinished else None,
            "已观测最高水位": high_water_seen,
            "水位是否被失败批次守住": first_unfinished is not None,
            # 已提交水位越过未完成区间，只可能发生在 backfill_order：
            # 数据已由迟到批次送达，失败区间另存补采单，编排时必须先恢复补采单。
            "已提交水位越过未完成区间": first_unfinished is not None
            and committed > first_unfinished.起始水位,
            # 不变量：下一次采集起点永远不会跳过未完成批次。
            "跳过未完成批次": first_unfinished is not None
            and next_start > first_unfinished.起始水位,
        }

    # -- 时间线 -------------------------------------------------------------

    def timeline(self, cursor_no: str | None = None) -> list[dict[str, Any]]:
        """生成增量作业时间线：每条批次一个时间线条目，名称解析自真实来源标识。"""
        cursors = [self._require_cursor(cursor_no)] if cursor_no else list(self.store.cursors.values())
        items: list[dict[str, Any]] = []
        for cursor in cursors:
            source = self.store.sources.resolve(cursor.来源标识)
            snapshot = self.watermark_snapshot(cursor.游标编号)
            rows = self._cursor_batches(cursor.游标编号)
            for index, batch in enumerate(rows):
                # 同一时间线内名称保持一致：统一采用游标边界解析出的来源名称。
                items.append({
                    "id": batch.id,
                    "版本": batch.版本,
                    "name": source["来源名称"],          # 修复点：真实来源名称，绝不是状态
                    "label": source["来源名称"],
                    "来源标识": source["来源标识"],
                    "来源名称": source["来源名称"],
                    "业务边界": source["业务边界"],
                    "名称解析方式": source["解析方式"],
                    "游标编号": cursor.游标编号,
                    "批次序号": batch.批次序号,
                    "start": batch.到达时间,
                    "end": batch.完成时间 or batch.到达时间,
                    "status": batch.status,             # 状态与名称彻底分开
                    "起始水位": batch.起始水位,
                    "结束水位": batch.结束水位,
                    "水位区间": f"[{batch.起始水位}, {batch.结束水位})",
                    "是否迟到": batch.是否迟到,
                    "关联补采单": batch.关联补采单,
                    "归档": batch.归档,
                    "备注": batch.备注,
                    "同线顺序": index + 1,
                    "下一次采集起点": snapshot["下一次采集起点"],
                    "已提交水位": snapshot["已提交水位"],
                    "logs": self._batch_logs(batch, source["来源名称"], snapshot),
                })
        return items

    def _batch_logs(self, batch: Batch, source_name: str,
                    snapshot: dict[str, Any]) -> str:
        lines = [
            f"[{batch.到达时间}] 来源「{source_name}」批次 {batch.批次序号} 到达，区间 [{batch.起始水位}, {batch.结束水位})",
        ]
        if batch.是否迟到:
            lines.append(f"[{batch.到达时间}] 判定为迟到批次，来源标识 {batch.来源标识}")
        if batch.status == BATCH_DONE:
            lines.append(f"[{batch.完成时间 or batch.到达时间}] 批次完成，水位可顺序提交至 {batch.结束水位}")
        elif batch.status == BATCH_FAILED:
            lines.append(
                f"[{batch.到达时间}] 批次失败，当前水位停在 {snapshot['下一次采集起点']}，"
                f"失败区间 [{batch.起始水位}, {batch.结束水位}) 不允许越过"
            )
        else:
            lines.append(f"[{batch.到达时间}] 批次状态 {batch.status}，等待处理")
        lines.append(f"下一次采集起点：{snapshot['下一次采集起点']}（不得跳过未完成批次）")
        if batch.关联补采单:
            lines.append(f"已挂接补采单 {batch.关联补采单}，恢复时按补采单区间采集")
        return "\n".join(lines)

    # -- 游标处理台账 -------------------------------------------------------

    def cursor_ledger(self) -> list[dict[str, Any]]:
        """游标处理台账：游标维度的水位核对结果。"""
        ledger: list[dict[str, Any]] = []
        for cursor in self.store.cursors.values():
            snapshot = self.watermark_snapshot(cursor.游标编号)
            source = self.store.sources.resolve(cursor.来源标识)
            rows = self._cursor_batches(cursor.游标编号)
            ledger.append({
                **cursor.to_dict(),
                "来源名称": source["来源名称"],
                "业务边界": source["业务边界"],
                "批次总数": len(rows),
                "失败批次数": sum(1 for b in rows if b.status == BATCH_FAILED),
                "迟到批次数": sum(1 for b in rows if b.是否迟到),
                "未完成批次数": sum(1 for b in rows if b.status in UNFINISHED_STATUSES),
                **{k: v for k, v in snapshot.items() if k != "游标编号"},
            })
        return ledger

    def batch_ledger(self, cursor_no: str | None = None) -> list[dict[str, Any]]:
        """批次处理台账：批次维度的水位推进核对。"""
        items = self.timeline(cursor_no)
        out: list[dict[str, Any]] = []
        for item in items:
            out.append({
                "台账id": item["id"],
                "游标编号": item["游标编号"],
                "来源标识": item["来源标识"],
                "来源名称": item["来源名称"],
                "批次序号": item["批次序号"],
                "水位区间": item["水位区间"],
                "批次状态": item["status"],
                "是否迟到": item["是否迟到"],
                "版本": item["版本"],
                "关联补采单": item["关联补采单"],
                "下一次采集起点": item["下一次采集起点"],
                "已提交水位": item["已提交水位"],
                "合规": True,
            })
        return out

    # -- 迟到批次冲突处理 ---------------------------------------------------

    def register_late_batch(self, cursor_no: str, seq: int, low: int, high: int,
                            strategy: str, *, arrived_at: str | None = None) -> dict[str, Any]:
        """登记迟到批次并按业务边界策略处理与失败批次的同链冲突。"""
        if strategy not in CONFLICT_STRATEGIES:
            raise LedgerViolation("C4", f"冲突策略必须是 {CONFLICT_STRATEGIES} 之一")
        cursor = self._require_cursor(cursor_no)
        conflict = self._find_conflict(cursor_no, low, high)
        if conflict is None:
            # 无冲突：迟到批次按普通批次入账，仍受同一套水位规则约束。
            batch = self.add_batch(cursor_no, seq, low, high, BATCH_DONE,
                                   is_late=True, arrived_at=arrived_at,
                                   finished_at=arrived_at or _today(),
                                   remark="迟到批次（无同链冲突）")
            return {"策略": strategy, "冲突": False, "批次": batch.to_dict(), "补采单": None,
                    "水位快照": self.watermark_snapshot(cursor_no)}

        order_id = f"BO-{cursor_no}-{conflict.批次序号:03d}"
        if strategy == "rollback_first":
            # 先按失败批次回退水位，再按原失败批次顺序继续采集；
            # 迟到批次并入补采链，状态保持「迟到」等待顺序采集。
            late = self.add_batch(cursor_no, seq, low, high, BATCH_LATE,
                                  is_late=True, arrived_at=arrived_at,
                                  remark=f"迟到批次，按 rollback_first 回退至批次 {conflict.批次序号} 顺序")
            order = self._ensure_order(conflict, strategy, order_id, extra_batch=late.id)
            late.关联补采单 = order.id
            snapshot = self.watermark_snapshot(cursor_no)
            # 回退后起点必须等于失败批次起点，不能跳过。
            return {"策略": strategy, "冲突": True, "批次": late.to_dict(),
                    "补采单": order.to_dict(), "水位快照": snapshot,
                    "说明": f"水位回退至 {snapshot['下一次采集起点']}，按原失败批次顺序继续采集"}

        # backfill_order：水位可前移，失败区间另存独立补采单。
        late = self.add_batch(cursor_no, seq, low, high, BATCH_DONE,
                              is_late=True, arrived_at=arrived_at,
                              finished_at=arrived_at or _today(),
                              remark="迟到批次，按 backfill_order 入账，失败区间独立补采")
        order = self._ensure_order(conflict, strategy, order_id, extra_batch=late.id)
        late.关联补采单 = order.id
        snapshot = self.watermark_snapshot(cursor_no)
        return {"策略": strategy, "冲突": True, "批次": late.to_dict(),
                "补采单": order.to_dict(), "水位快照": snapshot,
                "说明": "水位前移但失败区间另存补采单，后续从补采单恢复，不跳过未完成批次"}

    def _find_conflict(self, cursor_no: str, low: int, high: int) -> Batch | None:
        """同一来源标识 + 同一水位区间上的失败批次构成同链冲突。"""
        for batch in self._cursor_batches(cursor_no):
            if batch.status == BATCH_FAILED and batch.起始水位 == low and batch.结束水位 == high:
                return batch
        return None

    def _ensure_order(self, conflict: Batch, strategy: str, order_id: str,
                      *, extra_batch: int | None = None) -> BackfillOrder:
        for order in self.store.orders:
            if order.id == order_id:
                if extra_batch is not None and extra_batch not in order.关联批次:
                    order.关联批次.append(extra_batch)
                return order
        order = BackfillOrder(
            id=order_id,
            游标编号=conflict.游标编号,
            来源标识=conflict.来源标识,
            起始水位=conflict.起始水位,
            结束水位=conflict.结束水位,
            原因批次序号=conflict.批次序号,
            策略=strategy,
            关联批次=[conflict.id] + ([extra_batch] if extra_batch is not None else []),
            创建时间=_now(),
        )
        self.store.orders.append(order)
        if not conflict.关联补采单:
            conflict.关联补采单 = order.id
        return order

    def recover_backfill(self, order_id: str) -> dict[str, Any]:
        """从补采单恢复：按补采区间重新采集，完成后水位才能继续向前。"""
        order = self.get_order(order_id)
        if order is None:
            raise LedgerViolation("C1", f"补采单 {order_id} 不存在")
        order.status = ORDER_COLLECTING
        order.恢复时间 = _now()
        # 补采链上的未完成批次按原顺序恢复为已完成。
        recovered: list[dict[str, Any]] = []
        for batch in self._cursor_batches(order.游标编号):
            if batch.id in order.关联批次 and batch.status in (BATCH_FAILED, BATCH_LATE, BATCH_RUNNING):
                batch.status = BATCH_DONE
                batch.完成时间 = _today()
                batch.版本 += 1
                recovered.append(batch.to_dict())
                self._touch_check(batch, reason="补采恢复完成")
        order.status = ORDER_DONE
        snapshot = self.watermark_snapshot(order.游标编号)
        return {"补采单": order.to_dict(), "恢复批次": recovered, "水位快照": snapshot}

    # -- 确认（幂等：重复提交仅保留首份）------------------------------------

    def confirm(self, key: str, content: dict[str, Any]) -> dict[str, Any]:
        if key in self.store.confirmations:
            first = self.store.confirmations[key]
            return {"接受": False, "确认键": key, "首份确认": first,
                    "说明": "重复提交，仅保留首份确认"}
        payload = {"确认键": key, "内容": content, "确认时间": _now()}
        self.store.confirmations[key] = payload
        return {"接受": True, "确认键": key, "首份确认": payload, "说明": "首份确认已保存"}

    # -- 待核对台账（供血缘服务回写 / 跟随状态变化）--------------------------

    def _touch_check(self, batch: Batch, *, reason: str) -> CheckEntry:
        entry = CheckEntry(
            id=self.store.next_id("check"),
            来源标识=batch.来源标识,
            游标编号=batch.游标编号,
            批次序号=batch.批次序号,
            水位区间=f"[{batch.起始水位}, {batch.结束水位})",
            触发状态=batch.status,
            status=CHECK_PENDING,
            版本=batch.版本,
            更新时间=_now(),
            说明=reason,
        )
        self.store.checks.append(entry)
        return entry

    def check_ledger(self, status: str | None = None) -> list[dict[str, Any]]:
        rows = self.store.checks if status is None else [c for c in self.store.checks if c.status == status]
        return [c.to_dict() for c in rows]

    def resolve_check(self, check_id: int) -> CheckEntry | None:
        for entry in self.store.checks:
            if entry.id == check_id:
                entry.status = CHECK_DONE
                entry.更新时间 = _now()
                return entry
        return None

    # -- 血缘绑定 -----------------------------------------------------------

    def bind_lineage(self, order_id: str, *, upstream: str, downstream: str,
                     source_field: str, target_field: str, version: str) -> LineageBinding:
        """把补采单上的每个补采批次绑定到下游字段关系。"""
        order = self.get_order(order_id)
        if order is None:
            raise LedgerViolation("C1", f"补采单 {order_id} 不存在，无法绑定血缘")
        for batch_id in order.关联批次:
            # 同一补采批次 + 同一关系版本只绑定一次。
            if any(b.批次id == batch_id and b.关系版本 == version for b in self.store.bindings):
                continue
            self.store.bindings.append(LineageBinding(
                id=self.store.next_id("binding"),
                补采单=order_id,
                批次id=batch_id,
                来源标识=order.来源标识,
                上游节点=upstream,
                下游节点=downstream,
                来源字段=source_field,
                目标字段=target_field,
                关系版本=version,
                创建时间=_now(),
            ))
        return self.store.bindings[-1]

    def lineage_bindings(self) -> list[dict[str, Any]]:
        return [b.to_dict() for b in self.store.bindings]

    # -- 查询辅助 -----------------------------------------------------------

    def get_batch(self, batch_id: int) -> Batch | None:
        return next((b for b in self.store.batches if b.id == batch_id), None)

    def get_order(self, order_id: str) -> BackfillOrder | None:
        return next((o for o in self.store.orders if o.id == order_id), None)

    def orders(self) -> list[dict[str, Any]]:
        return [o.to_dict() for o in self.store.orders]

    def batch_detail(self, batch_id: int) -> dict[str, Any] | None:
        """详情页：与列表入口读取同一份版本记录，避免详情丢失同一版本。"""
        batch = self.get_batch(batch_id)
        if batch is None:
            return None
        source = self.store.sources.resolve(batch.来源标识)
        snapshot = self.watermark_snapshot(batch.游标编号)
        order = self.get_order(batch.关联补采单) if batch.关联补采单 else None
        order_dict = order.to_dict() if order else None
        return {
            **batch.to_dict(),
            "版本": batch.版本,
            "来源名称": source["来源名称"],
            "业务边界": source["业务边界"],
            "名称解析方式": source["解析方式"],
            "水位区间": f"[{batch.起始水位}, {batch.结束水位})",
            "补采范围": order_dict["补采范围"] if order_dict else None,
            "下一次采集起点": snapshot["下一次采集起点"],
            "已提交水位": snapshot["已提交水位"],
            "水位快照": snapshot,
            "补采单": order_dict,
            "logs": self._batch_logs(batch, source["来源名称"], snapshot),
        }

    def _require_cursor(self, cursor_no: str) -> Cursor:
        cursor = self.store.cursors.get(cursor_no)
        if cursor is None:
            raise LedgerViolation("C1", f"增量游标 {cursor_no} 不存在")
        return cursor
