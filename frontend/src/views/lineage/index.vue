<template>
  <section class="page" data-module="lineage">
    <header class="page-head">
      <div><h2>数据血缘追踪</h2><p class="page-desc">维护血缘关系，围绕关系编号、上游节点、下游节点、来源字段做登记、筛选与状态流转。</p></div>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记血缘关系</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出关系清单</button>
      </div>
    </header>
    <div class="dag-wrap" ref="dagRef">
      <svg class="dag-svg" :viewBox="`0 0 ${svgW} ${svgH}`">
        <defs><marker id="arrowhead" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6" fill="#94a3b8"/></marker></defs>
        <g v-for="edge in graph.edges" :key="edge.from+edge.to">
          <line class="dag-edge" :x1="nodePos[edge.from]?.x||0" :y1="nodePos[edge.from]?.y||0" :x2="nodePos[edge.to]?.x||0" :y2="nodePos[edge.to]?.y||0"/>
          <text :x="(nodePos[edge.from]?.x+nodePos[edge.to]?.x)/2" :y="(nodePos[edge.from]?.y+nodePos[edge.to]?.y)/2 - 6" font-size="10" fill="#64748b" text-anchor="middle">{{ edge.label }}</text>
        </g>
        <g v-for="node in graph.nodes" :key="node.id" :class="['dag-node', {selected: selectedNode?.id===node.id}]" @click="selectNode(node)" @mousedown="startDrag($event, node)">
          <rect :x="nodePos[node.id]?.x - 60" :y="nodePos[node.id]?.y - 20" width="120" height="40" rx="6" :fill="nodeColor(node.status)"/>
          <text :x="nodePos[node.id]?.x" :y="nodePos[node.id]?.y + 4" text-anchor="middle">{{ node.name || node.label }}</text>
        </g>
      </svg>
    </div>
    <div v-if="selectedNode" class="dag-detail" data-testid="dag-detail">
      <dl><dt>编号</dt><dd>{{ selectedNode.id }}</dd></dl>
      <dl><dt>名称</dt><dd>{{ selectedNode.name || selectedNode.label }}</dd></dl>
      <dl v-for="f in detailFields" :key="f"><dt>{{ f }}</dt><dd>{{ selectedNode[f] ?? '—' }}</dd></dl>
      <div style="grid-column:1/-1; margin-top:8px; display:flex; gap:6px;">
        <button v-for="action in actions" :key="action" class="btn" :data-testid="`action-${selectedNode.id}-${action}`" @click="runAction(action, selectedNode)">{{ action }}</button>
      </div>
    </div>

    <section class="lineage-ledger" data-testid="lineage-ledger">
      <header class="page-head">
        <div>
          <h3>增量游标处理台账 · 血缘联动</h3>
          <p class="page-desc">沿增量采集调用关系读取游标处理台账，把补采批次绑定到下游字段关系，并跟随状态变化更新待核对内容。</p>
        </div>
        <div class="page-actions">
          <button class="btn" type="button" data-testid="refresh-checks" @click="refreshChecks">跟随状态刷新待核对</button>
        </div>
      </header>

      <h4>后续处理编排（按增量作业时间线及游标处理台账）</h4>
      <table class="data-table" data-testid="orchestration-table">
        <thead><tr><th>游标编号</th><th>来源名称</th><th>下一次采集起点</th><th>编排动作</th><th>补采单</th><th>补采范围</th><th>说明</th></tr></thead>
        <tbody>
          <tr v-for="p in orchestration" :key="p.游标编号" :data-testid="`plan-row-${p.游标编号}`">
            <td>{{ p.游标编号 }}</td><td>{{ p.来源名称 }}</td><td>{{ p.下一次采集起点 }}</td>
            <td><em class="lineage-pill">{{ p.编排动作 }}</em></td>
            <td>{{ p.补采单.join(', ') || '—' }}</td><td>{{ p.补采范围.join(', ') || '—' }}</td><td>{{ p.说明 }}</td>
          </tr>
        </tbody>
      </table>

      <h4>补采批次血缘绑定</h4>
      <table class="data-table" data-testid="binding-table">
        <thead><tr><th>补采单</th><th>批次id</th><th>上游节点</th><th>下游节点</th><th>来源字段 → 目标字段</th><th>关系版本</th></tr></thead>
        <tbody>
          <tr v-for="b in bindings" :key="b.id">
            <td>{{ b.补采单 }}</td><td>{{ b.批次id }}</td><td>{{ b.上游节点 }}</td><td>{{ b.下游节点 }}</td>
            <td>{{ b.来源字段 }} → {{ b.目标字段 }}</td><td>{{ b.关系版本 }}</td>
          </tr>
          <tr v-if="!bindings.length"><td colspan="6" class="empty-state">暂无补采批次血缘绑定</td></tr>
        </tbody>
      </table>

      <form class="bind-form" @submit.prevent="submitBind">
        <h4>绑定补采批次到下游字段关系</h4>
        <label><span>补采单</span><select v-model="bindForm.orderId" data-testid="bind-order">
          <option v-for="o in orders" :key="o.id" :value="o.id">{{ o.id }} · {{ o.补采范围 }}</option>
        </select></label>
        <label><span>下游节点</span><input v-model="bindForm.下游节点" data-testid="bind-downstream" placeholder="如 资金域.dwd_pay_detail" required /></label>
        <label><span>来源字段</span><input v-model="bindForm.来源字段" required /></label>
        <label><span>目标字段</span><input v-model="bindForm.目标字段" required /></label>
        <label><span>关系版本</span><input v-model="bindForm.关系版本" /></label>
        <button class="btn primary" type="submit" data-testid="submit-bind">绑定</button>
      </form>

      <h4>待核对台账（跟随时间线状态变化）</h4>
      <table class="data-table" data-testid="lineage-checks">
        <thead><tr><th>id</th><th>来源/游标</th><th>批次</th><th>水位区间</th><th>触发状态</th><th>版本</th><th>说明</th><th>状态</th><th>操作</th></tr></thead>
        <tbody>
          <tr v-for="ck in checks" :key="ck.id" :data-testid="`lineage-check-${ck.id}`">
            <td>{{ ck.id }}</td><td>{{ ck.来源标识 }} · {{ ck.游标编号 }}</td><td>{{ ck.批次序号 ?? '—' }}</td>
            <td>{{ ck.水位区间 }}</td><td>{{ ck.触发状态 }}</td><td>v{{ ck.版本 }}</td><td>{{ ck.说明 }}</td>
            <td>{{ ck.status }}</td>
            <td><button v-if="ck.status === '待核对'" class="link" type="button" @click="resolveCheck(ck.id)">核对</button></td>
          </tr>
        </tbody>
      </table>
    </section>
    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate"><h3>登记血缘关系</h3>
        <label v-for="field in columns.slice(0,3)" :key="field" class="create-field"><span>{{ field }}</span><input v-model="createValues[field]" :placeholder="`请输入${field}`" required /></label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create">提交</button>
        </div>
      </form>
    </div>
    <footer class="page-foot"><span>{{ graph.nodes.length }} 个节点</span></footer>
  </section>
</template>
<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { request } from '@/api/client'
const ENDPOINT = '/api/lineage'
const columns = ["关系编号", "上游节点", "下游节点", "来源字段", "目标字段", "转换说明", "采集批次", "关系版本"]
const actions = ["记录关系", "验证关系", "归档关系"]
const statuses = ["草稿", "已记录", "已验证", "已归档"]
const STATUS_COLORS: Record<string,string> = { '草稿':'#e2e8f0','待校验':'#fef3c7','已启用':'#d1fae5','已停用':'#fee2e2' }
const nodeColor = (s: string) => STATUS_COLORS[s] || '#e2e8f0'
const graph = reactive<{nodes: any[], edges: any[]}>({ nodes: [], edges: [] })
const nodePos = ref<Record<number,{x:number;y:number}>>({})
const selectedNode = ref<any>(null)
const detailFields = computed(() => columns.filter((f: string) => !['id','name','label','status'].includes(f)))
const svgW = ref(800); const svgH = ref(400)
const errorMessage = ref('')
const creating = ref(false); const showCreate = ref(false)
const createValues = ref<Record<string,string>>({})
// 增量台账血缘联动
const orchestration = ref<any[]>([])
const bindings = ref<any[]>([])
const orders = ref<any[]>([])
const checks = ref<any[]>([])
const bindForm = ref({ orderId: '', 下游节点: '', 来源字段: 'event_time', 目标字段: 'pay_time', 关系版本: 'v1' })
let dragNode: any = null; const dagRef = ref<HTMLElement|null>(null)
function layout(nodes: any[]) {
  const w = 800; const h = Math.max(400, nodes.length * 80)
  svgW.value = w; svgH.value = h
  const pos: Record<number,{x:number;y:number}> = {}
  nodes.forEach((n,i) => { pos[n.id] = { x: 120 + (i % 5) * 150, y: 50 + Math.floor(i / 5) * 80 } })
  nodePos.value = pos
}
function selectNode(node: any) { selectedNode.value = selectedNode.value?.id === node.id ? null : node }
function startDrag(e: MouseEvent, node: any) { dragNode = node; const onMove = (ev: MouseEvent) => { if(!dragNode) return; nodePos.value[node.id] = { x: ev.offsetX, y: ev.offsetY } }; const onUp = () => { dragNode = null; document.removeEventListener('mousemove', onMove); document.removeEventListener('mouseup', onUp) }; document.addEventListener('mousemove', onMove); document.addEventListener('mouseup', onUp) }
async function reload() {
  try { const res = await request(ENDPOINT); const data = await res.json(); graph.nodes = data.nodes || []; graph.edges = data.edges || []; layout(graph.nodes) } catch(e) { errorMessage.value = '加载失败' }
}
async function reloadIncrementalLedger() {
  try {
    const [ledgerRes, planRes] = await Promise.all([
      request(`${ENDPOINT}/incremental-ledger`), request(`${ENDPOINT}/orchestration`),
    ])
    const data = await ledgerRes.json()
    bindings.value = data.bindings ?? []
    orders.value = data.orders ?? []
    checks.value = data.checks ?? []
    if (!bindForm.value.orderId && orders.value.length) bindForm.value.orderId = orders.value[0].id
    const plan = await planRes.json()
    orchestration.value = plan.items ?? []
  } catch (e) {
    errorMessage.value = '增量台账联动加载失败'
  }
}
async function submitBind() {
  const f = bindForm.value
  if (!f.orderId) { errorMessage.value = '请选择补采单'; return }
  const res = await request(`${ENDPOINT}/backfill/${f.orderId}/bind`, {
    method: 'POST',
    body: JSON.stringify({ values: { 下游节点: f.下游节点, 来源字段: f.来源字段, 目标字段: f.目标字段, 关系版本: f.关系版本 } }),
  })
  const payload = await res.json()
  if (!payload.ok) { errorMessage.value = payload.message; return }
  await reloadIncrementalLedger()
}
async function refreshChecks() {
  await request(`${ENDPOINT}/checks/refresh`, { method: 'POST' })
  await reloadIncrementalLedger()
}
async function resolveCheck(checkId: number) {
  await request(`${ENDPOINT}/checks/${checkId}/resolve`, { method: 'POST' })
  await reloadIncrementalLedger()
}
async function runAction(action: string, node: any) {
  try { await request(`${ENDPOINT}/${node.id}/actions`, { method: 'POST', body: JSON.stringify({ action }) }); await reload(); await reloadIncrementalLedger() } catch(e) { errorMessage.value = '操作失败' }
}
function openCreate() { createValues.value = {}; showCreate.value = true }
async function submitCreate() {
  try { const res = await request(ENDPOINT, { method: 'POST', body: JSON.stringify({ values: createValues.value }) }); if(!res.ok) throw new Error('创建失败'); showCreate.value = false; await reload() } catch(e: any) { errorMessage.value = e.message }
}
function exportRows() { window.open(`${ENDPOINT}/export`, '_blank') }
onMounted(() => { void reload(); void reloadIncrementalLedger() })
</script>

<style scoped>
.lineage-ledger { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px 16px; margin-top: 16px; }
.lineage-ledger h4 { margin: 14px 0 6px; font-size: 13px; color: var(--brand); }
.lineage-pill { font-style: normal; font-size: 11px; padding: 2px 8px; border-radius: 10px; background: #dbeafe; color: #1d4ed8; }
.bind-form { display: flex; flex-wrap: wrap; gap: 10px; align-items: flex-end; padding: 10px; background: #f8fafc; border-radius: 6px; margin: 8px 0; }
.bind-form label { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: var(--muted); }
.bind-form input, .bind-form select { padding: 6px 10px; border: 1px solid var(--border); border-radius: 6px; font-size: 13px; min-width: 130px; }
</style>
