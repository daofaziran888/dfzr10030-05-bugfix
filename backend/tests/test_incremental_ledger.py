"""增量作业时间线与游标处理台账领域规则测试。

仅依赖标准库，可直接运行：.venv/bin/python tests/test_incremental_ledger.py
也兼容 pytest 收集（函数名以 test_ 开头）。
"""
from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.incremental_ledger import (  # noqa: E402
    BATCH_DONE,
    BATCH_FAILED,
    BATCH_LATE,
    BATCH_RUNNING,
    Cursor,
    IncrementalLedgerEngine,
    LedgerStore,
    LedgerViolation,
)
from app.services.incremental_bootstrap import bootstrap  # noqa: E402


def fresh_engine() -> IncrementalLedgerEngine:
    store = LedgerStore()
    bootstrap(store)
    return IncrementalLedgerEngine(store)


def test_timeline_name_resolves_from_source_not_status() -> None:
    """时间线名称必须来自真实来源标识解析，绝不使用批次状态。"""
    engine = fresh_engine()
    items = engine.timeline()
    assert items, "时间线不应为空"
    for item in items:
        assert item["name"] == item["来源名称"]
        assert item["name"] in ("订单中心库", "会员中心库", "支付网关")
        assert item["name"] != item["status"], "名称与状态撞车：把状态列当成了作业名"
        assert item["来源标识"].startswith("SRC-")
    # 同一时间线内来源名称保持一致。
    order = [i for i in items if i["游标编号"] == "INCR-0001"]
    assert {i["来源名称"] for i in order} == {"订单中心库"}


def test_failed_batch_holds_watermark() -> None:
    """失败批次不得推进当前水位；失败后到达的新数据也不能越过未处理区间。"""
    engine = fresh_engine()
    snap = engine.watermark_snapshot("INCR-0001")
    # B1 [0,1000) 完成；B2 [1000,2000) 失败；B3/B4 是失败后到达的新数据。
    assert snap["已提交水位"] == 1000
    assert snap["下一次采集起点"] == 1000
    assert snap["最早未完成批次"] == 2
    assert snap["最早未完成状态"] == BATCH_FAILED
    assert snap["跳过未完成批次"] is False
    assert snap["已观测最高水位"] == 4000  # 新数据已到 4000，但水位仍守在 1000


def test_marking_failed_never_advances_watermark() -> None:
    """把批次标记失败后，下一次采集起点停在该批次起点。"""
    engine = fresh_engine()
    batch = next(b for b in engine.store.batches
                 if b.游标编号 == "INCR-0001" and b.批次序号 == 3)
    engine.mark_batch(batch.id, BATCH_FAILED)
    snap = engine.watermark_snapshot("INCR-0001")
    assert snap["已提交水位"] == 1000
    assert snap["下一次采集起点"] == 1000
    assert snap["最早未完成批次"] == 2


def test_rollback_first_strategy() -> None:
    """先按失败批次回退水位再接收迟到批次：按原失败批次顺序继续采集。"""
    engine = fresh_engine()
    result = engine.register_late_batch(
        "INCR-0002", 3, 500, 1000, "rollback_first", arrived_at="2026-11-17 10:00")
    assert result["冲突"] is True
    assert result["批次"]["status"] == BATCH_LATE
    order = result["补采单"]
    assert order["补采范围"] == "[500, 1000)"
    # 回退后水位停在失败批次起点，没有跳过未完成批次。
    assert result["水位快照"]["下一次采集起点"] == 500
    recovered = engine.recover_backfill(order["id"])
    assert recovered["补采单"]["status"] == "补采完成"
    assert {b["批次序号"] for b in recovered["恢复批次"]} == {2, 3}
    assert recovered["水位快照"]["已提交水位"] == 1000
    assert recovered["水位快照"]["最早未完成批次"] is None


def test_backfill_order_strategy() -> None:
    """先接收迟到批次：水位前移但失败区间另存补采单，后续从补采单恢复。"""
    engine = fresh_engine()
    # 引导数据里 INCR-0003 已按 backfill_order 建单。
    order = engine.get_order("BO-INCR-0003-002")
    assert order is not None and order.策略 == "backfill_order"
    snap_before = engine.watermark_snapshot("INCR-0003")
    assert snap_before["下一次采集起点"] == 2000  # 失败区间起点仍被守住
    result = engine.recover_backfill(order.id)
    assert result["水位快照"]["已提交水位"] == 4000
    assert result["水位快照"]["下一次采集起点"] == 4000


def test_next_start_never_skips_unfinished_batch() -> None:
    """任何策略下，下一次采集起点都不得跳过未完成批次。"""
    engine = fresh_engine()
    for cursor_no in ("INCR-0001", "INCR-0002", "INCR-0003"):
        snap = engine.watermark_snapshot(cursor_no)
        assert snap["跳过未完成批次"] is False


def test_confirmation_keeps_first_only() -> None:
    """重复提交仅保留首份确认。"""
    engine = fresh_engine()
    first = engine.confirm("K-1", {"opinion": "first"})
    second = engine.confirm("K-1", {"opinion": "second"})
    assert first["接受"] is True
    assert second["接受"] is False
    assert engine.store.confirmations["K-1"]["内容"] == {"opinion": "first"}


def test_non_compliant_payload_rejected() -> None:
    """条件 / 标准 / 适用范围不合规的结果不允许保存。"""
    engine = fresh_engine()

    def expect_violation(payload: dict[str, object]) -> str:
        violations = engine.validate_ledger_payload(payload)
        assert violations, f"应拒绝保存：{payload}"
        return violations[0]["rule_code"]

    # C1 状态值冒充来源标识
    assert expect_violation({
        "游标编号": "INCR-0001", "来源标识": BATCH_FAILED,
        "批次序号": 9, "起始水位": 0, "结束水位": 100, "status": BATCH_DONE,
    }) == "C1"
    # C2 非法水位区间
    assert expect_violation({
        "游标编号": "INCR-0001", "来源标识": "SRC-ORDER",
        "批次序号": 9, "起始水位": 500, "结束水位": 500, "status": BATCH_DONE,
    }) == "C2"
    # C3 同时间线批次序号重复
    assert expect_violation({
        "游标编号": "INCR-0001", "来源标识": "SRC-ORDER",
        "批次序号": 1, "起始水位": 0, "结束水位": 100, "status": BATCH_DONE,
    }) == "C3"
    # A2 同一时间线混入其他来源
    assert expect_violation({
        "游标编号": "INCR-0002", "来源标识": "SRC-ORDER",
        "批次序号": 9, "起始水位": 0, "结束水位": 100, "status": BATCH_DONE,
    }) == "A2"
    # 合规载荷放行
    assert engine.validate_ledger_payload({
        "游标编号": "INCR-0001", "来源标识": "SRC-ORDER",
        "批次序号": 9, "起始水位": 4000, "结束水位": 5000, "status": BATCH_DONE,
    }) == []


def test_archived_batch_keeps_old_result() -> None:
    """已归档旧结果保留，不允许改写。"""
    engine = fresh_engine()
    batch = engine.add_batch("INCR-0001", 9, 4000, 5000, BATCH_DONE,
                             archive=True, arrived_at="2026-11-17 11:00",
                             finished_at="2026-11-17 11:05")
    try:
        engine.mark_batch(batch.id, BATCH_FAILED)
    except LedgerViolation as exc:
        assert exc.rule_code == "A3"
    else:  # pragma: no cover
        raise AssertionError("归档批次必须拒绝改写")
    assert engine.get_batch(batch.id).status == BATCH_DONE


def test_detail_shares_same_version_as_list() -> None:
    """列表入口与详情页读取同一版本记录，详情含来源名称与补采范围。"""
    engine = fresh_engine()
    item = engine.timeline("INCR-0003")[0]
    detail = engine.batch_detail(item["id"])
    assert detail is not None
    assert detail["版本"] == item["版本"]
    assert detail["来源名称"] == "支付网关"
    failed = next(i for i in engine.timeline("INCR-0003") if i["status"] == BATCH_FAILED)
    failed_detail = engine.batch_detail(failed["id"])
    assert failed_detail["补采范围"] == "[2000, 4000)"
    assert failed_detail["下一次采集起点"] == 2000

def test_lineage_binds_every_backfill_batch() -> None:
    """血缘服务把每个补采批次绑定到下游字段关系。"""
    engine = fresh_engine()
    order_id = "BO-INCR-0003-002"
    engine.bind_lineage(order_id, upstream="u", downstream="d",
                        source_field="f1", target_field="f2", version="v9")
    order = engine.get_order(order_id)
    bindings = [b for b in engine.lineage_bindings()
                if b["补采单"] == order_id and b["关系版本"] == "v9"]
    assert {b["批次id"] for b in bindings} == set(order.关联批次)
    # 同版本重复绑定幂等。
    engine.bind_lineage(order_id, upstream="u", downstream="d",
                        source_field="f1", target_field="f2", version="v9")
    again = [b for b in engine.lineage_bindings()
             if b["补采单"] == order_id and b["关系版本"] == "v9"]
    assert len(again) == len(bindings)


def test_check_ledger_follows_status_changes() -> None:
    """批次状态变化回写待核对台账，核对后可闭环。"""
    engine = fresh_engine()
    before = len(engine.check_ledger())
    batch = next(b for b in engine.store.batches
                 if b.游标编号 == "INCR-0001" and b.批次序号 == 4)
    engine.mark_batch(batch.id, BATCH_FAILED)
    after = len(engine.check_ledger())
    assert after == before + 1
    latest = engine.check_ledger()[-1]
    assert latest["触发状态"] == BATCH_FAILED and latest["status"] == "待核对"
    resolved = engine.resolve_check(latest["id"])
    assert resolved is not None and resolved.status == "已核对"


def test_cross_source_batches_are_not_conflict() -> None:
    """跨来源标识不构成同一数据链冲突（按业务边界取舍）。"""
    store = LedgerStore()
    store.sources.register("SRC-A", "来源甲")
    store.sources.register("SRC-B", "来源乙")
    store.cursors["CA"] = Cursor(1, "CA", "SRC-A", "t", 100, "5分钟", 0, "", "")
    store.cursors["CB"] = Cursor(2, "CB", "SRC-B", "t", 100, "5分钟", 0, "", "")
    engine = IncrementalLedgerEngine(store)
    engine.add_batch("CA", 1, 0, 100, BATCH_FAILED, arrived_at="2026-11-18 08:00")
    engine.add_batch("CB", 1, 0, 100, BATCH_DONE, arrived_at="2026-11-18 08:05",
                     finished_at="2026-11-18 08:06")
    result = engine.register_late_batch(
        "CB", 2, 0, 100, "rollback_first", arrived_at="2026-11-18 08:10")
    assert result["冲突"] is False  # 同区间但来源不同，不算同链冲突
    assert engine.watermark_snapshot("CB")["已提交水位"] == 100


def _run_all() -> int:
    tests = [(name, obj) for name, obj in sorted(globals().items())
             if name.startswith("test_") and callable(obj)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"PASS {name}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL {name}: {type(exc).__name__}: {exc}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(_run_all())
