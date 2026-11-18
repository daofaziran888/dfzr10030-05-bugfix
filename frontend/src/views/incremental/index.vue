<template>
  <section class="page" data-module="incremental">
    <header class="page-head">
      <div>
        <h2>增量采集 · 作业时间线与游标处理台账</h2>
        <p class="page-desc">
          来源名称由真实来源标识解析；失败批次守住未完成水位，下一次采集只从未完成水位继续。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记增量游标</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出台账清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="card in stats" :key="card.label" class="stat-card">
        <span class="stat-label">{{ card.label }}</span>
        <strong class="stat-value">{{ card.value }}</strong>
      </article>
    </div>

    <div class="filter-bar">
      <label class="filter-item"><span>开始日期</span><input type="date" v-model="dateFrom" /></label>
      <label class="filter-item"><span>结束日期</span><input type="date" v-model="dateTo" /></label>
      <label class="filter-item"><span>游标</span>
        <select v-model="cursorFilter" data-testid="filter-cursor">
          <option value="">全部游标</option>
          <option v-for="c in ledger.cursors" :key="c.游标编号" :value="c.游标编号">{{ c.游标编号 }} · {{ c.来源名称 }}</option>
        </select>
      </label>
      <button class="btn" type="button" data-testid="query" @click="reload">查询</button>
      <button class="btn ghost" type="button" data-testid="reset" @click="resetFilters">重置条件</button>
    </div>

    <nav class="tab-bar" data-testid="tab-bar">
      <button v-for="tab in tabs" :key="tab.key" class="tab-btn" :class="{ active: activeTab === tab.key }"
              :data-testid="`tab-${tab.key}`" @click="activeTab = tab.key">{{ tab.label }}</button>
    </nav>

    <!-- 作业时间线 -->
    <div v-show="activeTab === 'timeline'">
      <div class="timeline-wrap">
        <div v-for="item in filteredTimeline" :key="item.id" class="timeline-row">
          <span class="timeline-label" :title="`来源标识：${item.来源标识}`">{{ item.来源名称 }}</span>
          <div class="timeline-track">
            <div class="timeline-bar" :class="`status-${item.status}`"
                 :style="{ left: barLeft(item) + '%', width: barWidth(item) + '%' }"
                 :data-testid="`bar-${item.id}`" :title="`${item.批次序号} · ${item.水位区间} · ${item.status}`"
                 @click="openBatch(item)">&nbsp;</div>
          </div>
          <span class="timeline-meta">
            {{ item.游标编号 }} #{{ item.批次序号 }}
            <em :class="['status-pill', `pill-${item.status}`]">{{ item.status }}</em>
            {{ item.水位区间 }}
          </span>
        </div>
        <p v-if="!filteredTimeline.length" class="empty-state">暂无作业记录</p>
      </div>

      <div class="conflict-panel" data-testid="conflict-panel">
        <h3>迟到批次冲突处理（同来源标识 + 同水位区间）</h3>
        <p class="page-desc">
          两种业务取舍都会守住下一次采集起点：rollback_first 回退水位按原顺序续采；
          backfill_order 水位前移但失败区间另存补采单。
        </p>
        <div class="conflict-form">
          <label><span>游标</span><select v-model="lateForm.cursor">
            <option v-for="c in ledger.cursors" :key="c.游标编号" :value="c.游标编号">{{ c.游标编号 }}</option>
          </select></label>
          <label><span>批次序号</span><input type="number" v-model.number="lateForm.seq" data-testid="late-seq" /></label>
          <label><span>起始水位</span><input type="number" v-model.number="lateForm.low" data-testid="late-low" /></label>
          <label><span>结束水位</span><input type="number" v-model.number="lateForm.high" data-testid="late-high" /></label>
          <label><span>冲突策略</span><select v-model="lateForm.strategy" data-testid="late-strategy">
            <option value="rollback_first">rollback_first（回退续采）</option>
            <option value="backfill_order">backfill_order（另存补采单）</option>
          </select></label>
          <button class="btn primary" type="button" data-testid="submit-late" @click="submitLate">登记迟到批次</button>
        </div>
        <p v-if="lateMessage" class="error-text" data-testid="late-message">{{ lateMessage }}</p>
      </div>
    </div>

    <!-- 游标处理台账 -->
    <div v-show="activeTab === 'ledger'">
      <section class="rules-card" data-testid="rules-card">
        <h3>台账保存条件、标准与适用范围（不合规结果不允许保存）</h3>
        <div v-for="(group, name) in ledger.rules" :key="name" class="rule-group">
          <h4>{{ name }}</h4>
          <ul><li v-for="r in group" :key="r.code"><b>{{ r.code }}</b> {{ r.rule }}</li></ul>
        </div>
      </section>
      <table class="data-table" data-testid="cursor-ledger">
        <thead><tr>
          <th>游标编号</th><th>来源名称</th><th>来源标识</th><th>批次总数</th><th>失败/迟到/未完成</th>
          <th>已提交水位</th><th>下一次采集起点</th><th>最早未完成批次</th><th>状态</th>
        </tr></thead>
        <tbody>
          <tr v-for="c in ledger.cursors" :key="c.游标编号" :data-testid="`ledger-row-${c.游标编号}`">
            <td>{{ c.游标编号 }}</td><td>{{ c.来源名称 }}</td><td>{{ c.来源标识 }}</td>
            <td>{{ c.批次总数 }}</td>
            <td>{{ c.失败批次数 }} / {{ c.迟到批次数 }} / {{ c.未完成批次数 }}</td>
            <td>{{ c.已提交水位 }}</td>
            <td><b :data-testid="`next-water-${c.游标编号}`">{{ c.下一次采集起点 }}</b></td>
            <td>{{ c.最早未完成批次 ? `#${c.最早未完成批次}（${c.最早未完成状态}）` : '—' }}</td>
            <td><em :class="['status-pill', c.status === '失败' ? 'pill-失败' : 'pill-运行中']">{{ c.status }}</em></td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- 补采单 -->
    <div v-show="activeTab === 'orders'">
      <table class="data-table" data-testid="orders-table">
        <thead><tr>
          <th>补采单</th><th>游标/来源</th><th>补采范围</th><th>原因批次</th><th>冲突策略</th>
          <th>关联批次</th><th>状态</th><th>操作</th>
        </tr></thead>
        <tbody>
          <tr v-for="o in ledger.orders" :key="o.id" :data-testid="`order-row-${o.id}`">
            <td>{{ o.id }}</td><td>{{ o.游标编号 }} · {{ sourceName(o.来源标识) }}</td>
            <td>{{ o.补采范围 }}</td><td>#{{ o.原因批次序号 }}</td><td>{{ o.策略 }}</td>
            <td>{{ o.关联批次.join(', ') }}</td>
            <td><em class="status-pill">{{ o.status }}</em></td>
            <td>
              <button v-if="o.status !== '补采完成'" class="link" type="button"
                      :data-testid="`recover-${o.id}`" @click="recover(o.id)">从补采单恢复</button>
              <span v-else class="page-desc">已恢复</span>
            </td>
          </tr>
        </tbody>
      </table>
      <section class="rules-card" data-testid="lineage-bindings">
        <h3>补采批次血缘绑定（血缘服务读取游标处理台账）</h3>
        <table class="data-table">
          <thead><tr><th>补采单</th><th>批次id</th><th>上游节点</th><th>下游节点</th><th>来源字段 → 目标字段</th><th>版本</th></tr></thead>
          <tbody>
            <tr v-for="b in ledger.bindings" :key="b.id">
              <td>{{ b.补采单 }}</td><td>{{ b.批次id }}</td><td>{{ b.上游节点 }}</td><td>{{ b.下游节点 }}</td>
              <td>{{ b.来源字段 }} → {{ b.目标字段 }}</td><td>{{ b.关系版本 }}</td>
            </tr>
          </tbody>
        </table>
      </section>
    </div>

    <!-- 待核对台账 -->
    <div v-show="activeTab === 'checks'">
      <table class="data-table" data-testid="checks-table">
        <thead><tr>
          <th>id</th><th>来源/游标</th><th>批次</th><th>水位区间</th><th>触发状态</th>
          <th>版本</th><th>说明</th><th>状态</th><th>操作</th>
        </tr></thead>
        <tbody>
          <tr v-for="ck in ledger.checks" :key="ck.id" :data-testid="`check-row-${ck.id}`">
            <td>{{ ck.id }}</td><td>{{ ck.来源标识 }} · {{ ck.游标编号 }}</td><td>{{ ck.批次序号 ?? '—' }}</td>
            <td>{{ ck.水位区间 }}</td><td>{{ ck.触发状态 }}</td><td>v{{ ck.版本 }}</td><td>{{ ck.说明 }}</td>
            <td><em :class="['status-pill', ck.status === '已核对' ? 'pill-已完成' : 'pill-运行中']">{{ ck.status }}</em></td>
            <td><button v-if="ck.status === '待核对'" class="link" type="button" @click="resolveCheck(ck.id)">核对</button></td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- 批次详情：失败批次可打开，查看来源名称与补采范围 -->
    <div v-if="detail" class="modal-backdrop" data-testid="batch-modal" @click.self="detail = null">
      <div class="modal">
        <h3>批次详情 · {{ detail.来源名称 }}</h3>
        <dl class="detail-grid">
          <dt>游标编号</dt><dd>{{ detail.游标编号 }}</dd>
          <dt>来源标识</dt><dd>{{ detail.来源标识 }}</dd>
          <dt>来源名称</dt><dd data-testid="detail-source-name">{{ detail.来源名称 }}（{{ detail.名称解析方式 }}）</dd>
          <dt>批次序号</dt><dd>#{{ detail.批次序号 }}（版本 v{{ detail.版本 }}）</dd>
          <dt>水位区间</dt><dd>{{ detail.水位区间 }}</dd>
          <dt>批次状态</dt><dd><em :class="['status-pill', `pill-${detail.status}`]">{{ detail.status }}</em></dd>
          <dt>已提交水位</dt><dd>{{ detail.已提交水位 }}</dd>
          <dt>下一次采集起点</dt><dd data-testid="detail-next-water">{{ detail.下一次采集起点 }}</dd>
          <dt>补采范围</dt><dd>{{ detail.补采范围 || '—' }}</dd>
          <dt>关联补采单</dt><dd>{{ detail.关联补采单 || '—' }}</dd>
        </dl>
        <pre class="batch-logs" data-testid="timeline-log">{{ detail.logs }}</pre>
        <div class="page-actions">
          <button v-if="detail.status !== '已完成'" class="btn" type="button"
                  :data-testid="`mark-done-${detail.id}`" @click="batchAction(detail.id, '标记完成')">标记完成</button>
          <button v-if="detail.status !== '失败'" class="btn" type="button"
                  :data-testid="`mark-fail-${detail.id}`" @click="batchAction(detail.id, '标记失败')">标记失败</button>
          <button class="btn primary" type="button" @click="detail = null">关闭</button>
        </div>
      </div>
    </div>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记增量游标</h3>
        <label v-for="field in createColumns" :key="field" class="create-field">
          <span>{{ field }}</span><input v-model="createValues[field]" :placeholder="`请输入${field}`" required />
        </label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create">提交</button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>{{ filteredTimeline.length }} 条批次 · {{ ledger.orders.length }} 张补采单 · {{ ledger.checks.length }} 条待核对</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { request } from '@/api/client'

const ENDPOINT = '/api/incremental'
const createColumns = ['游标编号', '来源标识', '增量字段']
const tabs = [
  { key: 'timeline', label: '作业时间线' },
  { key: 'ledger', label: '游标处理台账' },
  { key: 'orders', label: '补采单与血缘' },
  { key: 'checks', label: '待核对台账' },
]

const timeline = ref<any[]>([])
const ledger = ref<{ rules: Record<string, any[]>; cursors: any[]; batches: any[]; orders: any[]; checks: any[]; bindings: any[] }>({
  rules: {}, cursors: [], batches: [], orders: [], checks: [], bindings: [],
})
const activeTab = ref('timeline')
const detail = ref<any>(null)
const dateFrom = ref(''); const dateTo = ref(''); const cursorFilter = ref('')
const errorMessage = ref(''); const showCreate = ref(false)
const createValues = ref<Record<string, string>>({})
const lateMessage = ref('')
const lateForm = ref({ cursor: 'INCR-0002', seq: 3, low: 500, high: 1000, strategy: 'rollback_first' })

const stats = computed(() => [
  { label: '运行中游标', value: ledger.value.cursors.filter(c => c.status === '运行中').length },
  { label: '失败游标', value: ledger.value.cursors.filter(c => c.status === '失败').length },
  { label: '未完成批次', value: ledger.value.cursors.reduce((n, c) => n + (c.未完成批次数 || 0), 0) },
  { label: '待核对记录', value: ledger.value.checks.filter(c => c.status === '待核对').length },
])

const filteredTimeline = computed(() => {
  let items = timeline.value
  if (cursorFilter.value) items = items.filter(i => i.游标编号 === cursorFilter.value)
  if (dateFrom.value) items = items.filter(i => i.start >= dateFrom.value)
  if (dateTo.value) items = items.filter(i => i.end <= dateTo.value)
  return items
})

function sourceName(sourceId: string) {
  return ledger.value.cursors.find(c => c.来源标识 === sourceId)?.来源名称 ?? sourceId
}

function barLeft(item: any) {
  const all = timeline.value.filter(i => i.start && i.end)
  if (!all.length) return 0
  const min = Math.min(...all.map(i => new Date(i.start).getTime()))
  const max = Math.max(...all.map(i => new Date(i.end).getTime()))
  const range = max - min || 86400000
  return ((new Date(item.start).getTime() - min) / range) * 100
}
function barWidth(item: any) {
  const all = timeline.value.filter(i => i.start && i.end)
  if (!all.length) return 20
  const min = Math.min(...all.map(i => new Date(i.start).getTime()))
  const max = Math.max(...all.map(i => new Date(i.end).getTime()))
  const range = max - min || 86400000
  return Math.max(5, ((new Date(item.end).getTime() - new Date(item.start).getTime()) / range) * 100)
}

async function reload() {
  errorMessage.value = ''
  try {
    const query = new URLSearchParams(cursorFilter.value ? { cursor: cursorFilter.value } : {}).toString()
    const [tlRes, ledgerRes] = await Promise.all([
      request(`${ENDPOINT}?${query}`), request(`${ENDPOINT}/ledger`),
    ])
    const tlData = await tlRes.json()
    const ledgerData = await ledgerRes.json()
    timeline.value = tlData.items ?? []
    ledger.value = {
      rules: ledgerData.rules ?? {},
      cursors: ledgerData.cursors ?? [],
      batches: ledgerData.batches ?? [],
      orders: ledgerData.orders ?? [],
      checks: ledgerData.checks ?? [],
      bindings: (ledgerData as any).bindings ?? [],
    }
  } catch (e) {
    errorMessage.value = '台账加载失败'
  }
}

function resetFilters() {
  dateFrom.value = ''; dateTo.value = ''; cursorFilter.value = ''
  void reload()
}

async function openBatch(item: any) {
  try {
    const res = await request(`${ENDPOINT}/batches/${item.id}`)
    if (!res.ok) throw new Error('详情读取失败')
    detail.value = await res.json()
  } catch (e: any) {
    errorMessage.value = e.message
  }
}

async function batchAction(batchId: number, action: string) {
  const res = await request(`${ENDPOINT}/batches/${batchId}/actions`, {
    method: 'POST', body: JSON.stringify({ action }),
  })
  const payload = await res.json()
  if (!payload.ok) { errorMessage.value = payload.message; return }
  await reload()
  await openBatch({ id: batchId })
}

async function submitLate() {
  lateMessage.value = ''
  const f = lateForm.value
  const res = await request(`${ENDPOINT}/cursors/${f.cursor}/late-batch`, {
    method: 'POST',
    body: JSON.stringify({ values: { 批次序号: f.seq, 起始水位: f.low, 结束水位: f.high, strategy: f.strategy } }),
  })
  const payload = await res.json()
  if (!payload.ok) { lateMessage.value = payload.message; return }
  lateMessage.value = `${payload.message}：${payload.entry.说明 ?? ''}`
  await reload()
}

async function recover(orderId: string) {
  const res = await request(`${ENDPOINT}/orders/${orderId}/recover`, { method: 'POST' })
  const payload = await res.json()
  if (!payload.ok) { errorMessage.value = payload.message; return }
  await reload()
}

async function resolveCheck(checkId: number) {
  await request(`${ENDPOINT}/checks/${checkId}/resolve`, { method: 'POST' })
  await reload()
}

function openCreate() { createValues.value = {}; showCreate.value = true }
async function submitCreate() {
  try {
    const res = await request(ENDPOINT, { method: 'POST', body: JSON.stringify({ values: createValues.value }) })
    const payload = await res.json()
    if (!payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (e: any) {
    errorMessage.value = e.message
  }
}

function exportRows() { window.open(`${ENDPOINT}/export`, '_blank') }
onMounted(reload)
</script>

<style scoped>
.tab-bar { display: flex; gap: 6px; margin-bottom: 12px; }
.tab-btn { border: 1px solid var(--border); background: #fff; border-radius: 6px 6px 0 0; padding: 8px 16px; cursor: pointer; font-size: 13px; }
.tab-btn.active { background: var(--brand); color: #fff; border-color: var(--brand); }
.timeline-meta { font-size: 12px; color: var(--muted); margin-left: 8px; white-space: nowrap; display: flex; align-items: center; gap: 6px; }
.status-pill { font-style: normal; font-size: 11px; padding: 2px 8px; border-radius: 10px; background: #e2e8f0; color: #334155; }
.pill-运行中 { background: #dbeafe; color: #1d4ed8; }
.pill-已完成 { background: #dcfce7; color: #15803d; }
.pill-失败 { background: #fee2e2; color: #b91c1c; }
.pill-迟到 { background: #fef9c3; color: #a16207; }
.rules-card { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px 16px; margin-bottom: 12px; }
.rules-card h3 { margin: 0 0 8px; font-size: 14px; }
.rule-group h4 { margin: 8px 0 4px; font-size: 13px; color: var(--brand); }
.rule-group ul { margin: 0; padding-left: 18px; font-size: 12px; color: var(--muted); }
.conflict-panel { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px 16px; margin-top: 12px; }
.conflict-panel h3 { margin: 0 0 6px; font-size: 14px; }
.conflict-form { display: flex; flex-wrap: wrap; gap: 10px; align-items: flex-end; }
.conflict-form label { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: var(--muted); }
.conflict-form input, .conflict-form select { padding: 6px 10px; border: 1px solid var(--border); border-radius: 6px; font-size: 13px; min-width: 120px; }
.detail-grid { display: grid; grid-template-columns: 120px 1fr; gap: 6px 12px; margin: 8px 0; font-size: 13px; }
.detail-grid dt { color: var(--muted); }
.detail-grid dd { margin: 0; }
.batch-logs { background: #1e293b; color: #e2e8f0; border-radius: 6px; padding: 10px; font-size: 12px; white-space: pre-wrap; max-height: 200px; overflow: auto; }
</style>
