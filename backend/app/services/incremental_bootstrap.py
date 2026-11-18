"""增量台账引导数据：构造失败批次守住水位、迟到批次冲突、补采恢复的演示链。

场景设计（按来源标识划分业务边界）：

- SRC-ORDER（订单中心库，边界：交易域）
  游标 INCR-0001，批次大小 1000：
    B1 [0,1000)    已完成
    B2 [1000,2000) 失败        <- 失败批次，水位被守在 1000
    B3 [2000,3000) 已完成       <- 失败之后新数据已到达，也不能越过未处理区间
    B4 [3000,4000) 运行中
- SRC-MEMBER（会员中心库，边界：会员域）
  游标 INCR-0002：
    B1 [0,500) 已完成
    B2 [500,1000) 失败
    迟到批次 [500,1000) 与 B2 同来源同区间 -> 同链冲突（等待页面选择策略）
- SRC-PAY（支付网关，边界：资金域）
  游标 INCR-0003：
    B1 [0,2000) 已完成
    B2 [2000,4000) 失败 -> backfill_order 已生成补采单（旧结果保留）
"""
from __future__ import annotations

from app.services.incremental_ledger import (
    BATCH_DONE,
    BATCH_FAILED,
    BATCH_RUNNING,
    Cursor,
    IncrementalLedgerEngine,
    LedgerStore,
)


def bootstrap(store: LedgerStore) -> LedgerStore:
    store.reset()
    store.sources.register("SRC-ORDER", "订单中心库", boundary="交易域")
    store.sources.register("SRC-MEMBER", "会员中心库", boundary="会员域")
    store.sources.register("SRC-PAY", "支付网关", boundary="资金域")

    store.cursors["INCR-0001"] = Cursor(
        id=1, 游标编号="INCR-0001", 来源标识="SRC-ORDER", 增量字段="updated_at",
        批次大小=1000, 采集间隔="15分钟", 初始水位=0,
        上次采集="2026-11-17 08:00", 下次采集="2026-11-18 08:15", status="运行中",
    )
    store.cursors["INCR-0002"] = Cursor(
        id=2, 游标编号="INCR-0002", 来源标识="SRC-MEMBER", 增量字段="modify_time",
        批次大小=500, 采集间隔="30分钟", 初始水位=0,
        上次采集="2026-11-17 09:00", 下次采集="2026-11-18 09:30", status="运行中",
    )
    store.cursors["INCR-0003"] = Cursor(
        id=3, 游标编号="INCR-0003", 来源标识="SRC-PAY", 增量字段="event_time",
        批次大小=2000, 采集间隔="10分钟", 初始水位=0,
        上次采集="2026-11-17 10:00", 下次采集="2026-11-18 10:10", status="失败",
    )

    engine = IncrementalLedgerEngine(store)

    # 订单中心：失败批次 B2 之后新数据 B3/B4 已到达，但水位必须守在 1000。
    engine.add_batch("INCR-0001", 1, 0, 1000, BATCH_DONE,
                     arrived_at="2026-11-17 08:00", finished_at="2026-11-17 08:12",
                     remark="首批次正常完成")
    engine.add_batch("INCR-0001", 2, 1000, 2000, BATCH_FAILED,
                     arrived_at="2026-11-17 08:15",
                     remark="来源端超时，批次失败，水位停在 1000 等待补采")
    engine.add_batch("INCR-0001", 3, 2000, 3000, BATCH_DONE,
                     arrived_at="2026-11-17 08:30", finished_at="2026-11-17 08:40",
                     remark="失败后到达的新数据，已完成但不能越过未处理区间")
    engine.add_batch("INCR-0001", 4, 3000, 4000, BATCH_RUNNING,
                     arrived_at="2026-11-17 08:45",
                     remark="运行中批次")

    # 会员中心：失败 B2，迟到批次同区间冲突留给值班人员在页面决策。
    engine.add_batch("INCR-0002", 1, 0, 500, BATCH_DONE,
                     arrived_at="2026-11-17 09:00", finished_at="2026-11-17 09:10")
    engine.add_batch("INCR-0002", 2, 500, 1000, BATCH_FAILED,
                     arrived_at="2026-11-17 09:30",
                     remark="校验不通过，批次失败")

    # 支付网关：失败区间按 backfill_order 已另存补采单（旧结果保留）。
    engine.add_batch("INCR-0003", 1, 0, 2000, BATCH_DONE,
                     arrived_at="2026-11-17 10:00", finished_at="2026-11-17 10:08")
    pay_failed = engine.add_batch("INCR-0003", 2, 2000, 4000, BATCH_FAILED,
                                  arrived_at="2026-11-17 10:10",
                                  remark="网关限流，失败区间独立补采")
    result = engine.register_late_batch(
        "INCR-0003", 3, 2000, 4000, "backfill_order", arrived_at="2026-11-17 10:40",
    )
    # 绑定一条血缘关系，演示补采批次 -> 下游字段关系。
    engine.bind_lineage(
        result["补采单"]["id"],
        upstream="支付网关.pay_event",
        downstream="资金域.dwd_pay_detail",
        source_field="event_time",
        target_field="pay_time",
        version="v1",
    )
    assert pay_failed.关联补采单 is not None
    return store
