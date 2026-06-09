# Phase 5：前端实时大屏（Vue 3）

## 目标

创建 Vue 3 + Vite 前端项目，实现实时大屏，通过 WebSocket 接收数据，ECharts 动态展示。

---

## 步骤 5.1：创建 Vue 3 项目

```bash
cd user-behavior-analytics

# 使用 Vite 创建 Vue 3 项目
npm create vite@latest ui -- --template vue

cd ui
npm install

# 安装依赖
npm install axios echarts vue-router@4 pinia
npm install @stomp/stompjs sockjs-client    # WebSocket 客户端
npm install @iconify/vue @iconify-json/carbon  # 图标库
```

---

## 步骤 5.2：项目结构

创建以下目录和文件：

```
ui/src/
├── App.vue
├── main.js
├── router/
│   └── index.js
├── stores/
│   └── realtime.js          # Pinia 实时数据 store
├── api/
│   ├── analytics.js         # REST API 请求
│   └── websocket.js         # WebSocket 连接管理
├── views/
│   ├── dashboard/
│   │   └── index.vue        # 实时大屏（核心页面）
│   ├── analysis/
│   │   ├── PvAnalysis.vue   # PV 分析
│   │   ├── UvAnalysis.vue   # UV 分析
│   │   ├── RefererAnalysis.vue  # 来源分析
│   │   ├── Performance.vue  # 性能监控
│   │   └── Funnel.vue       # 漏斗分析
│   └── layout/
│       └── Index.vue        # 大屏布局组件
└── utils/
    └── format.js            # 数字格式化工具
```

---

## 步骤 5.3：路由配置

`src/router/index.js`：

```javascript
import { createRouter, createWebHistory } from 'vue-router'

const routes = [
  {
    path: '/',
    redirect: '/dashboard'
  },
  {
    path: '/dashboard',
    name: 'Dashboard',
    component: () => import('@/views/dashboard/index.vue'),
    meta: { title: '实时大屏' }
  },
  {
    path: '/analysis/pv',
    name: 'PvAnalysis',
    component: () => import('@/views/analysis/PvAnalysis.vue'),
    meta: { title: 'PV 分析' }
  },
  {
    path: '/analysis/uv',
    name: 'UvAnalysis',
    component: () => import('@/views/analysis/UvAnalysis.vue'),
    meta: { title: 'UV 分析' }
  },
  {
    path: '/analysis/referer',
    name: 'RefererAnalysis',
    component: () => import('@/views/analysis/RefererAnalysis.vue'),
    meta: { title: '来源分析' }
  },
  {
    path: '/analysis/performance',
    name: 'Performance',
    component: () => import('@/views/analysis/Performance.vue'),
    meta: { title: '性能监控' }
  },
  {
    path: '/analysis/funnel',
    name: 'Funnel',
    component: () => import('@/views/analysis/Funnel.vue'),
    meta: { title: '漏斗分析' }
  }
]

export default createRouter({
  history: createWebHistory(),
  routes
})
```

---

## 步骤 5.4：API 层

`src/api/analytics.js`（REST API）：

```javascript
import axios from 'axios'

const api = axios.create({
  baseURL: '/api',
  timeout: 10000
})

export function getOverview() {
  return api.get('/overview')
}

export function getPvTrend(minutes = 60) {
  return api.get('/pv/trend', { params: { minutes } })
}

export function getTopPages(limit = 10) {
  return api.get('/pv/top', { params: { limit } })
}

export function getRefererAnalysis() {
  return api.get('/referer')
}

export function getFunnelData() {
  return api.get('/funnel')
}
```

`src/api/websocket.js`（WebSocket 连接）：

```javascript
import { Client } from '@stomp/stompjs'
import SockJS from 'sockjs-client'

let stompClient = null

export function connectWebSocket(onMessage) {
  stompClient = new Client({
    webSocketFactory: () => new SockJS('/ws'),
    reconnectDelay: 5000,
    heartbeatIncoming: 10000,
    heartbeatOutgoing: 10000,
    onConnect: () => {
      console.log('[WS] 已连接')
      stompClient.subscribe('/topic/realtime', (message) => {
        const data = JSON.parse(message.body)
        onMessage(data)
      })
    },
    onDisconnect: () => console.log('[WS] 已断开'),
    onStompError: (err) => console.error('[WS] 错误', err)
  })
  stompClient.activate()
}

export function disconnectWebSocket() {
  stompClient?.deactivate()
}
```

---

## 步骤 5.5：Pinia Store（实时数据状态管理）

`src/stores/realtime.js`：

```javascript
import { defineStore } from 'pinia'
import { ref } from 'vue'

export const useRealtimeStore = defineStore('realtime', () => {
  const data = ref({
    pv: 0,
    uv: 0,
    topPages: [],
    statusCodes: {},
    avgResponseTime: 0,
    timestamp: 0
  })

  const pvHistory = ref([])    // PV 趋势数据缓存
  const MAX_HISTORY = 60       // 保留 60 个点（5分钟）

  function update(newData) {
    data.value = {
      ...data.value,
      ...newData
    }

    // 追加 PV 历史点
    pvHistory.value.push({
      time: new Date(newData.timestamp).toLocaleTimeString(),
      value: newData.pv
    })
    if (pvHistory.value.length > MAX_HISTORY) {
      pvHistory.value.shift()
    }
  }

  return { data, pvHistory, update }
})
```

---

## 步骤 5.6：大屏布局组件

`src/views/layout/Index.vue`：

```vue
<template>
  <div class="dashboard-container">
    <!-- 顶部标题栏 -->
    <header class="header">
      <h1>📊 用户行为实时分析平台</h1>
      <div class="header-time">{{ currentTime }}</div>
    </header>

    <!-- 核心指标卡片 -->
    <section class="kpi-row">
      <div class="kpi-card">
        <div class="kpi-label">实时 PV</div>
        <div class="kpi-value" id="pv-number">{{ formatNumber(store.data.pv) }}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">今日 UV</div>
        <div class="kpi-value">{{ formatNumber(store.data.uv) }}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">平均响应时间</div>
        <div class="kpi-value">{{ store.data.avgResponseTime }}ms</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">5xx 错误</div>
        <div class="kpi-value error" :class="{ blink: has5xxError }">
          {{ store.data.statusCodes?.['5xx'] || 0 }}
        </div>
      </div>
    </section>

    <!-- 图表区域 -->
    <section class="charts-row">
      <div class="chart-card large">
        <h3>PV 实时趋势</h3>
        <div ref="pvTrendChart" class="chart-box"></div>
      </div>
      <div class="chart-card">
        <h3>热门页面 TOP10</h3>
        <div ref="topPagesChart" class="chart-box"></div>
      </div>
    </section>

    <section class="charts-row">
      <div class="chart-card">
        <h3>流量来源</h3>
        <div ref="refererChart" class="chart-box"></div>
      </div>
      <div class="chart-card">
        <h3>状态码分布</h3>
        <div ref="statusChart" class="chart-box"></div>
      </div>
      <div class="chart-card">
        <h3>转化漏斗</h3>
        <div ref="funnelChart" class="chart-box"></div>
      </div>
    </section>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted, watch, nextTick } from 'vue'
import * as echarts from 'echarts'
import { useRealtimeStore } from '@/stores/realtime'
import { connectWebSocket, disconnectWebSocket } from '@/api/websocket'
import { getTopPages, getRefererAnalysis, getFunnelData } from '@/api/analytics'
import { formatNumber } from '@/utils/format'

const store = useRealtimeStore()
const currentTime = ref('')
const has5xxError = ref(false)

// 图表 DOM 引用
const pvTrendChart = ref(null)
const topPagesChart = ref(null)
const refererChart = ref(null)
const statusChart = ref(null)
const funnelChart = ref(null)

let pvTrendInstance = null
let topPagesInstance = null
let refererInstance = null
let statusInstance = null
let funnelInstance = null

// 更新时间
function updateClock() {
  currentTime.value = new Date().toLocaleString('zh-CN')
}

// === 渲染 PV 趋势折线图 ===
function renderPvTrend() {
  if (!pvTrendInstance) {
    pvTrendInstance = echarts.init(pvTrendChart.value)
  }
  pvTrendInstance.setOption({
    tooltip: { trigger: 'axis' },
    xAxis: { type: 'category', data: store.pvHistory.map(p => p.time) },
    yAxis: { type: 'value', name: 'PV' },
    series: [{
      data: store.pvHistory.map(p => p.value),
      type: 'line',
      smooth: true,
      areaStyle: { opacity: 0.3 },
      itemStyle: { color: '#409EFF' }
    }],
    grid: { left: 50, right: 20, bottom: 30 }
  })
}

// === 渲染 TOP 页面柱状图 ===
async function renderTopPages() {
  if (!topPagesInstance) {
    topPagesInstance = echarts.init(topPagesChart.value)
  }
  const res = await getTopPages(10)
  const data = res.data || []
  topPagesInstance.setOption({
    tooltip: { trigger: 'axis' },
    xAxis: { type: 'category', data: data.map(d => d.url).reverse() },
    yAxis: { type: 'value' },
    series: [{
      data: data.map(d => d.pv).reverse(),
      type: 'bar',
      itemStyle: {
        color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
          { offset: 0, color: '#409EFF' },
          { offset: 1, color: '#36D399' }
        ])
      }
    }]
  })
}

// === 渲染来源饼图 ===
async function renderReferer() {
  if (!refererInstance) {
    refererInstance = echarts.init(refererChart.value)
  }
  const res = await getRefererAnalysis()
  const data = (res.data || []).map(d => ({
    name: { direct: '直接访问', search_engine: '搜索引擎', external: '外部链接', internal: '站内跳转' }[d.referer_type] || d.referer_type,
    value: d.cnt
  }))
  refererInstance.setOption({
    tooltip: { trigger: 'item', formatter: '{b}: {c} ({d}%)' },
    series: [{
      type: 'pie',
      radius: ['40%', '70%'],
      data,
      label: { show: true, formatter: '{b}\n{d}%' }
    }]
  })
}

// === 渲染状态码环图 ===
async function renderStatus() {
  if (!statusInstance) {
    statusInstance = echarts.init(statusChart.value)
  }
  const codes = store.data.statusCodes || {}
  const data = Object.entries(codes).map(([name, value]) => ({ name, value }))
  statusInstance.setOption({
    tooltip: { trigger: 'item' },
    series: [{
      type: 'pie',
      radius: ['50%', '70%'],
      data,
      label: { show: true, formatter: '{b}: {c}' },
      color: ['#67C23A', '#E6A23C', '#F56C6C', '#C03639'],
      itemStyle: {
        borderRadius: 5,
        borderColor: '#fff',
        borderWidth: 2
      }
    }]
  })
}

// === 渲染漏斗图 ===
async function renderFunnel() {
  if (!funnelInstance) {
    funnelInstance = echarts.init(funnelChart.value)
  }
  const res = await getFunnelData()
  const data = (res.data || []).map(d => ({
    name: d.step_name,
    value: d.user_count
  }))
  funnelInstance.setOption({
    tooltip: { trigger: 'item', formatter: '{b}: {c}' },
    series: [{
      type: 'funnel',
      left: '10%',
      top: 20,
      bottom: 20,
      width: '80%',
      minSize: '20%',
      maxSize: '100%',
      sort: 'descending',
      gap: 2,
      label: { show: true, formatter: '{b}\n{c} ({d}%)' },
      data
    }]
  })
}

// === WebSocket 消息处理 ===
function handleWsMessage(data) {
  store.update(data)
  has5xxError.value = (data.statusCodes?.['5xx'] || 0) > 0
  renderPvTrend()
  renderStatus()
}

// === 生命周期 ===
onMounted(() => {
  updateClock()
  setInterval(updateClock, 1000)

  connectWebSocket(handleWsMessage)

  // 初始加载非实时数据
  setTimeout(async () => {
    await renderTopPages()
    await renderReferer()
    await renderFunnel()
  }, 500)

  // 窗口自适应
  window.addEventListener('resize', () => {
    [pvTrendInstance, topPagesInstance, refererInstance, statusInstance, funnelInstance]
      .forEach(i => i?.resize())
  })
})

onUnmounted(() => {
  disconnectWebSocket()
})
</script>

<style scoped>
.dashboard-container {
  background: #0a1628;
  color: #fff;
  min-height: 100vh;
  padding: 20px;
}
.header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
}
.header h1 { font-size: 24px; margin: 0; }
.header-time { font-size: 16px; color: #8c9db5; }

.kpi-row {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 16px;
  margin-bottom: 20px;
}
.kpi-card {
  background: rgba(255,255,255,0.05);
  border: 1px solid rgba(255,255,255,0.1);
  border-radius: 8px;
  padding: 20px;
  text-align: center;
}
.kpi-label { font-size: 14px; color: #8c9db5; margin-bottom: 8px; }
.kpi-value { font-size: 36px; font-weight: bold; font-family: 'Courier New', monospace; }
.kpi-value.error { color: #F56C6C; }
.blink { animation: blink 1s infinite; }
@keyframes blink {
  0%, 100% { opacity: 1; }
  50% { opacity: 0.3; }
}

.charts-row {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
  gap: 16px;
  margin-bottom: 20px;
}
.chart-card {
  background: rgba(255,255,255,0.05);
  border: 1px solid rgba(255,255,255,0.1);
  border-radius: 8px;
  padding: 16px;
}
.chart-card.large { grid-column: 1 / -1; }
.chart-card h3 { margin: 0 0 12px; font-size: 14px; color: #8c9db5; }
.chart-box { height: 320px; }
</style>
```

---

## 步骤 5.7：工具函数

`src/utils/format.js`：

```javascript
/** 数字格式化：12345 → "12,345" */
export function formatNumber(num) {
  if (num == null) return '0'
  return num.toLocaleString('zh-CN')
}

/** 百分比格式化 */
export function formatPercent(value) {
  return (value * 100).toFixed(1) + '%'
}

/** 时间格式化 */
export function formatTime(ts) {
  return new Date(ts).toLocaleTimeString('zh-CN')
}
```

---

## 步骤 5.8：Vite 配置（代理后端）

`vite.config.js`：

```javascript
import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import path from 'path'

export default defineConfig({
  plugins: [vue()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, 'src')
    }
  },
  server: {
    port: 3000,
    host: true,
    proxy: {
      '/api': {
        target: 'http://localhost:8080',
        changeOrigin: true
      },
      '/ws': {
        target: 'http://localhost:8080',
        ws: true
      }
    }
  }
})
```

---

## 步骤 5.9：main.js 入口

`src/main.js`：

```javascript
import { createApp } from 'vue'
import { createPinia } from 'pinia'
import router from './router'
import App from './App.vue'

const app = createApp(App)
app.use(createPinia())
app.use(router)
app.mount('#app')
```

---

## 步骤 5.10：启动验证

```bash
cd ui
npm run dev

# 浏览器打开 http://localhost:3000
# 应看到实时大屏，图表每 5 秒自动刷新
```

如果后端未启动，可以先 mock 数据测试静态效果。

---

## 本阶段完成标志

- [ ] `npm run dev` 正常启动
- [ ] 浏览器打开 `localhost:3000` 看到大屏布局
- [ ] 数字卡片显示格式化的 PV/UV 值（初步可为 0）
- [ ] WebSocket 连接后在浏览器控制台看到 `[WS] 已连接`
- [ ] 后端运行时，大屏数据每 5 秒自动刷新
- [ ] 窗口缩放时图表自适应
