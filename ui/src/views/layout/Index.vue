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

    <!-- 图表区域 — 固定比例布局，避免空档 -->
    <section class="charts-row-main">
      <div class="chart-card">
        <h3>📈 PV 实时趋势</h3>
        <div ref="pvTrendChart" class="chart-box"></div>
      </div>
      <div class="chart-card">
        <h3>🏆 热门页面 TOP10</h3>
        <div ref="topPagesChart" class="chart-box"></div>
      </div>
    </section>

    <section class="charts-row-three">
      <div class="chart-card">
        <h3>🔗 流量来源</h3>
        <div ref="refererChart" class="chart-box"></div>
      </div>
      <div class="chart-card">
        <h3>📊 状态码分布</h3>
        <div ref="statusChart" class="chart-box"></div>
      </div>
      <div class="chart-card">
        <h3>🔄 转化漏斗</h3>
        <div ref="funnelChart" class="chart-box"></div>
      </div>
    </section>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted, nextTick } from 'vue'
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

// URL 路径 → 中文标签映射
const pageLabels = {
  '/index': '首页',
  '/product/1': '商品详情A',
  '/product/2': '商品详情B',
  '/cart': '购物车',
  '/checkout': '结算页',
  '/about': '关于我们',
  '/': '根路径',
}
function getPageLabel(url) {
  // 精确匹配
  if (pageLabels[url]) return pageLabels[url]
  // 模糊匹配：/api/search → 搜索, /api/login → 登录
  if (url.includes('/api/search')) return '搜索'
  if (url.includes('/api/login')) return '登录'
  // 其他路径截短显示
  return url.length > 12 ? url.substring(0, 10) + '…' : url
}

// === 渲染 TOP 页面柱状图 ===
async function renderTopPages() {
  if (!topPagesInstance) {
    topPagesInstance = echarts.init(topPagesChart.value)
  }
  const res = await getTopPages(10)
  const data = res.data || []
  topPagesInstance.setOption({
    tooltip: { trigger: 'axis', formatter: (params) => {
      const p = params[0]
      return `${p.name}<br/>访问量: ${p.value.toLocaleString()}`
    }},
    xAxis: { type: 'value', name: '访问量' },
    yAxis: {
      type: 'category',
      data: data.map(d => getPageLabel(d.url)).reverse(),
      axisLabel: { fontSize: 12 }
    },
    series: [{
      data: data.map(d => d.pv).reverse(),
      type: 'bar',
      itemStyle: {
        color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
          { offset: 0, color: '#409EFF' },
          { offset: 1, color: '#36D399' }
        ])
      }
    }],
    grid: { left: 80, right: 20, top: 10, bottom: 30 }
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
      left: '5%',
      right: '5%',
      top: 50,
      bottom: 30,
      width: '90%',
      minSize: '20%',
      maxSize: '100%',
      sort: 'descending',
      gap: 4,
      label: {
        show: true,
        position: 'inside',
        formatter: '{b}\n{c} ({d}%)',
        fontSize: 13,
        lineHeight: 20,
        color: '#fff'
      },
      labelLine: { show: false },
      itemStyle: {
        borderColor: '#0a1628',
        borderWidth: 2
      },
      emphasis: {
        label: { fontSize: 15, fontWeight: 'bold' }
      },
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
// === 页面可见性优化（离屏时暂停更新，节省资源）===
let isPageVisible = true
function handleVisibilityChange() {
  if (document.hidden) {
    isPageVisible = false
    // 离屏时暂停 ECharts 动画
    [pvTrendInstance, topPagesInstance, refererInstance, statusInstance, funnelInstance]
      .forEach(i => i?.setOption({ animation: false }))
  } else {
    isPageVisible = true
    // 恢复后刷新所有图表
    [pvTrendInstance, topPagesInstance, refererInstance, statusInstance, funnelInstance]
      .forEach(i => i?.resize())
    // 重新请求静态数据
    setTimeout(async () => {
      await renderTopPages()
      await renderReferer()
      await renderFunnel()
    }, 300)
  }
}

onMounted(() => {
  updateClock()
  const clockTimer = setInterval(updateClock, 1000)

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

  // 页面可见性切换
  document.addEventListener('visibilitychange', handleVisibilityChange)
})

onUnmounted(() => {
  disconnectWebSocket()
  clearInterval(clockTimer)  // eslint-disable-line no-undef
  document.removeEventListener('visibilitychange', handleVisibilityChange)
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

/* 第二行：PV 趋势(2/3) + TOP页面(1/3) — 并排无空档 */
.charts-row-main {
  display: grid;
  grid-template-columns: 2fr 1fr;
  gap: 16px;
  margin-bottom: 20px;
}
/* 第三行：3 个图表等宽并排 */
.charts-row-three {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 16px;
  margin-bottom: 20px;
}
.chart-card {
  background: rgba(255,255,255,0.05);
  border: 1px solid rgba(255,255,255,0.1);
  border-radius: 8px;
  padding: 16px;
  transition: border-color 0.3s;
}
.chart-card:hover {
  border-color: rgba(255,255,255,0.25);
}
.chart-card h3 {
  margin: 0 0 12px;
  font-size: 14px;
  color: #8c9db5;
  font-weight: normal;
}
.chart-box { height: 300px; }

/* 小屏幕 → 纵向堆叠 */
@media (max-width: 900px) {
  .charts-row-main { grid-template-columns: 1fr; }
  .charts-row-three { grid-template-columns: 1fr; }
  .kpi-row { grid-template-columns: repeat(2, 1fr); }
  .chart-box { height: 260px; }
}
</style>
