<script setup>
import { computed, onMounted, onUnmounted, ref } from 'vue'

const agents = ref([]), workflows = ref([]), workflow = ref(null), workflowId = ref(''), followLatest = ref(true), tasks = ref([]), messages = ref([]), selected = ref(null), error = ref(''), floor = ref(null), detailStyle = ref({}), menuOpen = ref(false), acceptanceReason = ref('')
const meta = { 'team-lead':['Team Lead','#172554',6,9], 'task-decomposer':['任务拆分','#8b5cf6',6,38], 'architecture-agent':['架构设计','#3b82f6',36,31], 'product-agent':['产品验收','#e15b93',66,10], 'frontend-agent':['前端开发','#16a36e',36,66], 'backend-agent':['后端开发','#0d9ab7',66,66], 'audit-agent':['代码审计','#dc981f',66,40], 'test-agent':['测试验证','#e15b93',6,66], 'deployment-agent':['部署发布','#374151',36,40] }
const stageNames = { team_lead:'Team Lead', contract_audit:'方案审计', document_review:'文档评审', workflow_validation:'流程验证', frontend:'前端开发', backend:'后端开发', audit:'代码审计', test:'测试验证', acceptance:'产品验收' }
const stagePositions = { team_lead:[6,9], contract_audit:[36,10], document_review:[36,38], workflow_validation:[36,38], acceptance:[66,10], frontend:[36,66], backend:[66,66], audit:[66,40], test:[6,66] }
const currentTask = agent => {
  return tasks.value.find(task => task.id === agent.taskId) || tasks.value.find(task => task.agent === agent.key)
}
const complete = computed(() => tasks.value.filter(task => task.status === 'passed').length)
const acceptanceTask = computed(() => tasks.value.find(task => task.stage === 'acceptance'))
const logs = computed(() => messages.value)
const parseDetail = value => {
  if (!value) return {}
  if (typeof value === 'object') return value
  try { return JSON.parse(value) } catch { return { summary:value } }
}
const selectedTask = computed(() => selected.value ? currentTask(selected.value) : null)
const selectedDetail = computed(() => {
  const task = selectedTask.value || {}
  return parseDetail(task.detail || task.output || task.artifact || task.result)
})
const taskDescription = computed(() => selectedDetail.value.instructions || selectedDetail.value.description || selectedDetail.value.summary || selectedTask.value?.description || selectedTask.value?.instruction || (selected.value?.key === 'team-lead' ? '澄清需求、制定技术方案与 REST API Contract，并维护共享任务板。' : '等待 Team Lead 分派具体任务。'))
const artifacts = computed(() => {
  const value = selectedDetail.value.artifacts || selectedDetail.value.deliverables || selectedTask.value?.artifacts || []
  return Array.isArray(value) ? value : Object.entries(value).map(([name, content]) => ({ name, content }))
})
const contractArtifact = computed(() => artifacts.value.find(artifact => artifact?.type === 'api_contract'))
const steps = computed(() => {
  const value = selectedDetail.value.execution_log || selectedDetail.value.steps || selectedDetail.value.execution_steps || selectedDetail.value.logs || selectedTask.value?.steps || []
  return Array.isArray(value) ? value : [value]
})
const deliveryEvidence = computed(() => steps.value.filter(step => typeof step === 'object' && step.event === '交付证据'))
const apiContract = computed(() => {
  const value = selectedDetail.value.api_contract || selectedDetail.value.apiContract || selectedTask.value?.api_contract || contractArtifact.value?.content || workflow.value?.api_contract
  if (value) return typeof value === 'string' ? value : JSON.stringify(value, null, 2)
  if (selected.value?.key === 'team-lead') return selectedTask.value?.status === 'passed' ? '未记录实际 REST API Contract。' : '等待 Team Lead 交付 REST API Contract。'
  return ''
})
const nodeMessages = computed(() => selected.value ? messages.value.filter(message => message.task_id === selectedTask.value?.id || messageFrom(message) === selected.value.key || messageTo(message) === selected.value.key).slice(-8) : [])
const taskNode = task => ({ key:task.id, taskId:task.id, agentKey:task.agent, name:stageNames[task.stage] || task.agent, role:task.stage })
const displayAgents = computed(() => {
  return tasks.value.map(taskNode)
})
const position = (agent, index) => { const saved = stagePositions[currentTask(agent)?.stage] || meta[agent.agentKey || agent.key]?.slice(2); return saved ? { left:saved[0]+'%', top:saved[1]+'%' } : { left:(8+(index%3)*29)+'%', top:(10+Math.floor(index/3)*25)+'%' } }
const color = agent => meta[agent.agentKey || agent.key]?.[1] || '#64748b'
const label = agent => stageNames[currentTask(agent)?.stage] || meta[agent.key]?.[0] || agent.name
const endpoint = task => { const point = position(taskNode(task), 0); return [Number.parseFloat(point.left)+8, Number.parseFloat(point.top)+8] }
const wireLinks = computed(() => tasks.value.flatMap(task => task.depends_on.map(parent => ({ task, parent:tasks.value.find(item => item.stage === parent) })).filter(link => link.parent)))
const date = value => new Intl.DateTimeFormat('zh-CN', { year:'numeric', month:'2-digit', day:'2-digit', hour:'2-digit', minute:'2-digit', second:'2-digit', hour12:false }).format(new Date(value)).replace(',', ' /')
const statusText = status => ({ ready:'待执行', running:'执行中', repairing:'修复中', acceptance_pending_human:'等待人工验收', passed:'已交付', blocked:'等待依赖', skipped:'已跳过', not_applicable:'不适用', waiting:'等待方案' }[status] || status || '等待方案')
const messageFrom = message => message.from || message.from_agent || 'team-lead'
const messageTo = message => message.to || message.to_agent || 'team-lead'
function select(agent, event) {
  if (selected.value?.key === agent.key) { selected.value = null; return }
  selected.value = agent
  const office = floor.value.getBoundingClientRect(), card = event.currentTarget.getBoundingClientRect()
  const margin = 16, gap = 14, width = 320, preferredHeight = 300
  const cardBox = { left:card.left - office.left, top:card.top - office.top, right:card.right - office.left, bottom:card.bottom - office.top }
  const candidates = [
    { left:cardBox.right + gap, top:cardBox.top, edge:'right' },
    { left:cardBox.left - width - gap, top:cardBox.top, edge:'left' },
    { left:cardBox.left, top:cardBox.bottom + gap, edge:'bottom' },
    { left:cardBox.left, top:cardBox.top - preferredHeight - gap, edge:'top' },
  ].map(candidate => ({
    ...candidate,
    left:Math.min(Math.max(margin, candidate.left), office.width - width - margin),
    top:Math.min(Math.max(margin, candidate.top), office.height - preferredHeight - margin),
  }))
  const overlap = candidate => Math.max(0, Math.min(candidate.left + width, cardBox.right) - Math.max(candidate.left, cardBox.left)) * Math.max(0, Math.min(candidate.top + preferredHeight, cardBox.bottom) - Math.max(candidate.top, cardBox.top))
  const score = candidate => overlap(candidate) * 100 + Math.abs(candidate.left - cardBox.left) + Math.abs(candidate.top - cardBox.top)
  const chosen = candidates.sort((left, right) => score(left) - score(right))[0]
  detailStyle.value = { left:chosen.left+'px', top:chosen.top+'px', right:'auto', bottom:'auto', maxHeight:`${Math.max(180, office.height - chosen.top - margin)}px` }
}
async function decideAcceptance(decision) {
  const reason = acceptanceReason.value.trim()
  if (!reason) { error.value = '请填写验收理由。'; return }
  const response = await fetch(`/api/workflows/${workflow.value.id}/acceptance/decision`, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({decision, actor:'Dashboard 人工验收', reason, idempotency_key:`${workflow.value.id}:acceptance:${Date.now()}`}) })
  if (!response.ok) { error.value = await response.text(); return }
  acceptanceReason.value = ''
  await load()
}
async function load() {
  try {
    const fetchJson = async url => {
      const response = await fetch(url)
      const body = await response.text()
      if (!response.ok) throw Error(body || `${response.status} ${response.statusText}`)
      return body ? JSON.parse(body) : null
    }
    agents.value = await fetchJson('/api/agents')
    workflows.value = await fetchJson('/api/workflows')
    workflows.value.sort((left, right) => new Date(right.last_activity_at) - new Date(left.last_activity_at))
    if (followLatest.value) workflowId.value = workflows.value[0]?.id || ''
    else workflowId.value ||= workflows.value[0]?.id || ''
    workflow.value = workflows.value.find(item => item.id === workflowId.value) || workflows.value[0] || null
    workflowId.value = workflow.value?.id || ''
    const detail = workflow.value ? await fetchJson('/api/workflows/'+workflow.value.id) : { tasks:[], messages:[] }
    tasks.value = detail.tasks; messages.value = detail.messages
    error.value = ''
    if (selected.value && !displayAgents.value.some(agent => agent.key === selected.value.key)) selected.value = null
  } catch (exception) { error.value = exception.message }
}
let timer
onMounted(() => { load(); timer = setInterval(load, 2000) })
onUnmounted(() => clearInterval(timer))
</script>

<template>
  <main class="app">
    <header class="top">
      <div class="brand"><i class="live"></i>A2A Agent Office</div>
      <div class="task"><span>当前确认任务</span><strong>{{ workflow?.title || '等待 Codex 确认执行' }}</strong></div>
      <div class="metrics"><div><b>{{ complete }}/{{ tasks.length }}</b><small>已完成节点</small></div><div><b>{{ logs.length }}</b><small>上下文消息</small></div><div><b>{{ workflow?.status?.toUpperCase() || 'IDLE' }}</b><small>工作流状态</small></div></div>
      <div class="controls"><div class="workflow-picker"><button class="picker" @click="menuOpen = !menuOpen">{{ workflow?.title || '选择任务' }} · {{ workflow?.status }}</button><div v-if="menuOpen" class="workflow-menu"><button v-for="item in workflows" :key="item.id" :class="{ current:item.id === workflowId }" @click="workflowId=item.id; followLatest=false; menuOpen=false; load()">{{ item.title }} · {{ item.status }}</button></div></div><button @click="load">刷新</button></div>
    </header>
    <div class="layout">
      <section ref="floor" class="floor" :class="{ 'has-detail': selected }" @click="selected = null">
        <span class="floor-title">A2A WORKSPACE · 上下文从 agent 到 agent 直接流转</span>
        <svg class="wire"><line v-for="link in wireLinks" :key="link.task.id + link.parent.id" :x1="endpoint(link.parent)[0]+'%'" :y1="endpoint(link.parent)[1]+'%'" :x2="endpoint(link.task)[0]+'%'" :y2="endpoint(link.task)[1]+'%'" /></svg>
        <article v-for="(agent, index) in displayAgents" :key="agent.key" class="agent" :class="{ active:selected?.key === agent.key }" :style="{ ...position(agent,index), '--accent':color(agent) }" @click.stop="select(agent,$event)">
          <div class="agent-head"><i class="avatar">{{ label(agent)[0] }}<em :class="currentTask(agent)?.status"></em></i><div><b>{{ label(agent) }}</b><span>{{ agent.agentKey || agent.key }}</span></div></div>
          <p>{{ currentTask(agent)?.status === 'repairing' ? currentTask(agent)?.detail?.instructions : (currentTask(agent)?.stage || agent.role) + ' · ' + statusText(currentTask(agent)?.status) }}</p><div class="bar"><i :style="{ width: currentTask(agent)?.status === 'passed' ? '100%' : currentTask(agent)?.status === 'repairing' ? '82%' : currentTask(agent)?.status === 'running' ? '72%' : currentTask(agent)?.status === 'ready' ? '58%' : '0%' }"></i></div>
        </article>
        <aside v-if="selected" class="detail" :style="detailStyle" @click.stop><button @click="selected = null">×</button><h3>{{ label(selected) }}</h3><p>Agent ID：{{ selected.key }}</p><p>Task ID：{{ selectedTask?.id || '待 Team Lead 分派' }}</p><p>执行状态：{{ statusText(selectedTask?.status) }}</p><section><h4>正在做什么</h4><p>{{ taskDescription }}</p></section><section v-if="apiContract"><h4>REST API Contract</h4><pre>{{ apiContract }}</pre></section><section v-if="steps.length"><h4>执行步骤 / 日志</h4><ol><li v-for="(step, index) in steps" :key="index">{{ typeof step === 'string' ? step : step.detail || step.text || step.name || JSON.stringify(step) }}</li></ol></section><section v-if="artifacts.length"><h4>{{ deliveryEvidence.length ? '计划交付物 / 对应证据' : '计划交付物（尚未记录交付证据）' }}</h4><ul><li v-for="(artifact, index) in artifacts" :key="index">{{ typeof artifact === 'string' ? artifact : artifact.name || artifact.path || JSON.stringify(artifact) }}</li></ul></section><section v-if="deliveryEvidence.length"><h4>实际交付证据</h4><p v-for="(item, index) in deliveryEvidence" :key="index" class="message">{{ item.detail }}</p></section><section v-if="nodeMessages.length"><h4>协作消息</h4><p v-for="message in nodeMessages" :key="message.id" class="message">{{ label(agents.find(agent => agent.key === messageFrom(message)) || { key:messageFrom(message), name:messageFrom(message) }) }}：{{ message.text }}</p></section><p v-if="selected.key === 'contract-audit'" class="hint">未通过前，前端与后端不会被分派。</p></aside>
        <section v-if="acceptanceTask?.status === 'acceptance_pending_human'" class="acceptance" @click.stop><b>人工验收：请先操作已打开的生图应用</b><p>验收地址：<code>http://127.0.0.1:20003</code>。确认规则、JSON、Prompt、模拟模式标识与图片结果后再决定。</p><textarea v-model="acceptanceReason" placeholder="填写通过或驳回理由（必填）"></textarea><button @click="decideAcceptance('REJECT')">驳回并退回 Team Lead</button><button @click="decideAcceptance('PASS')">验收通过</button></section>
        <div class="legend"><span>● 执行中</span><span>● 等待依赖</span><span>● 已交付</span><span>● 已跳过</span></div><p v-if="error" class="error">{{ error }}</p>
      </section>
      <aside class="side"><div class="side-head"><b>共享上下文 / 协作日志</b><b>{{ logs.length }}</b></div><div v-if="logs.length" class="feed"><article v-for="message in logs" :key="message.id" class="event"><time>{{ date(message.created_at) }}</time><div><b :style="{ background: color(agents.find(agent => agent.key === messageFrom(message)) || { key:messageFrom(message) }) }">{{ label(agents.find(agent => agent.key === messageFrom(message)) || { key:messageFrom(message), name:messageFrom(message) }) }}</b><span>→</span><b class="archive">{{ label(agents.find(agent => agent.key === messageTo(message)) || { key:messageTo(message), name:messageTo(message) }) }}</b><em v-if="message.kind">{{ message.kind }}</em></div><p>{{ message.text }}</p></article></div><div v-else class="empty">Team Lead 确认方案后，<br>这里展示交付、质疑、回复与缺陷回派。</div></aside>
    </div>
  </main>
</template>

<style scoped>
:global(*){box-sizing:border-box}:global(body){margin:0;overflow:hidden;font-family:"Microsoft YaHei",system-ui,sans-serif}.app{height:100vh;color:#1d2939;background:#eef2f6}.top{height:58px;display:flex;align-items:center;gap:25px;padding:10px 18px;background:#fff;border-bottom:1px solid #dbe3ec}.brand{font-weight:800;white-space:nowrap}.live{display:inline-block;width:10px;height:10px;margin-right:9px;border-radius:50%;background:#16a36e;box-shadow:0 0 0 5px #d9f5e9}.task{display:grid;gap:2px;min-width:280px}.task span{font-size:10px;letter-spacing:.1em;color:#97a2b1}.task strong{font-size:13px}.metrics{display:flex;gap:22px;margin-left:auto}.metrics div{text-align:center}.metrics b{display:block;font-size:16px}.metrics small{color:#94a0af;font-size:10px}.controls{display:flex;gap:8px}.controls button{padding:7px 9px;border:1px solid #dbe3ec;border-radius:7px;background:#fff;color:#1d2939}.workflow-picker{position:relative}.picker{width:312px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;text-align:left}.workflow-menu{position:absolute;z-index:30;right:0;top:38px;width:360px;max-height:330px;overflow:auto;border:1px solid #dbe3ec;border-radius:8px;background:#fff;box-shadow:0 14px 30px #24364c26;scrollbar-width:none}.workflow-menu::-webkit-scrollbar{display:none}.workflow-menu button{display:block;width:100%;border:0;border-radius:0;text-align:left;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.workflow-menu button:hover,.workflow-menu .current{background:#eef4ff;color:#172554}.layout{display:grid;grid-template-columns:1fr 355px;height:calc(100vh - 58px);min-height:0}.floor{position:relative;min-height:0;overflow:hidden;background:#f9fbfd radial-gradient(#dce4ec 1px,transparent 1px);background-size:22px 22px}.floor-title{position:absolute;top:15px;left:21px;color:#98a4b3;font-size:10px;letter-spacing:.12em}.wire{position:absolute;inset:0;width:100%;height:100%;pointer-events:none}.wire line{stroke:#c9d4df;stroke-width:1.3;stroke-dasharray:5 5}.agent{position:absolute;width:184px;padding:13px;border:1px solid #dbe3ec;border-radius:13px;background:#fff;box-shadow:0 2px 7px #26384b12;cursor:pointer;transition:.18s}.agent:hover,.agent.active{transform:translateY(-2px);box-shadow:0 0 0 2px var(--accent),0 12px 25px #26384b22}.has-detail .agent:not(.active){transform:scale(.84);opacity:.48}.agent-head{display:flex;align-items:center;gap:10px}.avatar{position:relative;display:grid;place-items:center;width:36px;height:36px;border-radius:50%;background:var(--accent);color:#fff;font-style:normal;font-weight:800}.avatar em{position:absolute;right:-1px;bottom:-1px;width:11px;height:11px;border:2px solid #fff;border-radius:50%;background:#dd9a21}.avatar em.passed{background:#4386ee}.avatar em.ready{background:#16a36e}.agent b{display:block;font-size:13px}.agent span,.agent p{color:#778394;font-size:11px}.agent p{height:28px;margin:10px 0 8px;overflow:hidden}.bar{height:4px;border-radius:5px;background:#edf1f5;overflow:hidden}.bar i{display:block;height:100%;background:var(--accent)}.detail{position:absolute;z-index:10;width:320px;max-height:calc(100% - 32px);overflow:auto;padding:15px;right:18px;bottom:20px;border:1px solid #dbe3ec;border-radius:12px;background:#fff;box-shadow:0 14px 36px #24364c24}.detail button{position:absolute;right:9px;top:7px;border:0;background:none;font-size:20px}.detail h3{margin:0 0 10px}.detail p{margin:5px 0;color:#778394;font-size:12px;line-height:1.5}.detail section{margin-top:12px;padding-top:9px;border-top:1px solid #edf1f5}.detail h4{margin:0 0 5px;font-size:12px;color:#344054}.detail pre{max-height:180px;margin:0;padding:8px;overflow:auto;border-radius:6px;background:#172554;color:#dbeafe;font:10px/1.45 ui-monospace,SFMono-Regular,Consolas,monospace;white-space:pre-wrap}.detail ol,.detail ul{margin:5px 0;padding-left:18px;color:#64748b;font-size:11px;line-height:1.6}.detail .message{padding:5px 7px;background:#f5f8fb;border-radius:5px}.legend{position:absolute;bottom:15px;left:20px;padding:8px 12px;border:1px solid #dbe3ec;border-radius:8px;background:#fffffff0;color:#778394;font-size:11px}.legend span{margin-right:10px}.legend span:nth-child(1){color:#16a36e}.legend span:nth-child(2){color:#dd9a21}.legend span:nth-child(3){color:#4386ee}.legend span:nth-child(4){color:#ec7070}.side{display:flex;min-height:0;flex-direction:column;overflow:hidden;border-left:1px solid #dbe3ec;background:#fff}.side-head{display:flex;justify-content:space-between;padding:14px 17px;border-bottom:1px solid #dbe3ec}.feed{min-height:0;overflow:auto}.event{padding:12px 17px;border-bottom:1px solid #eff2f5;font-size:12px}.event time{color:#a2adba;font:10px ui-monospace,monospace}.event div{display:flex;gap:5px;align-items:center;margin:5px 0}.event b{padding:2px 7px;border-radius:9px;color:#fff;font-size:10px}.event .archive{background:#64748b}.event p{margin:0;color:#5d6979;line-height:1.45}.empty{padding:55px 22px;color:#9aa5b2;text-align:center;line-height:1.8}.error{position:absolute;bottom:10px;left:20px;color:#d44}@media(max-width:900px){.metrics{display:none}.layout{grid-template-columns:1fr}.side{display:none}.top{gap:10px}.task{min-width:0}.controls .picker{max-width:140px}.workflow-menu{max-width:calc(100vw - 20px)}}
.detail{max-height:calc(100% - 32px);overscroll-behavior:contain}.acceptance{position:absolute;z-index:11;left:20px;bottom:58px;width:min(430px,calc(100% - 40px));max-height:calc(100% - 78px);overflow:auto;padding:13px;border:1px solid #f1bd62;border-radius:10px;background:#fffdf7;color:#5b430e;font-size:12px;box-shadow:0 10px 28px #8a5c1522}.acceptance b{display:block;margin-bottom:7px}.acceptance p{line-height:1.55}.acceptance textarea{display:block;width:100%;min-height:60px;margin:8px 0;padding:8px;border:1px solid #d9c48d;border-radius:6px;resize:vertical}.acceptance button{margin-right:8px;padding:7px 10px;border:0;border-radius:6px;background:#975c08;color:#fff;cursor:pointer}.acceptance button:last-child{background:#16724c}
</style>
