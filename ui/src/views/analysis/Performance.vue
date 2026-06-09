<template>
  <div class="page-container">
    <header class="page-header">
      <h1>性能监控</h1>
      <router-link to="/dashboard" class="back-link">← 返回大屏</router-link>
    </header>
    <section class="kpi-row">
      <div class="kpi-card">
        <div class="kpi-label">平均响应时间</div>
        <div class="kpi-value" style="color:#409EFF">{{ overview.avgResponseTime }}ms</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">今日 PV</div>
        <div class="kpi-value">{{ formatNumber(overview.pv) }}</div>
      </div>
    </section>
    <section class="chart-card">
      <h3>状态码分布（最近 5 分钟）</h3>
      <div ref="statusChart" class="chart-box"></div>
    </section>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted, nextTick } from 'vue'
import * as echarts from 'echarts'
import { getOverview } from '@/api/analytics'
import { formatNumber } from '@/utils/format'

const statusChart = ref(null)
let statusInstance = null
const overview = ref({})

async function fetchData() {
  const res = await getOverview()
  overview.value = res.data || {}

  const codes = overview.value.statusCodes || {}
  const data = Object.entries(codes).map(([name, value]) => ({ name, value }))

  if (!statusInstance) {
    statusInstance = echarts.init(statusChart.value)
  }
  statusInstance.setOption({
    tooltip: { trigger: 'item', formatter: '{b}: {c}' },
    xAxis: { type: 'category', data: data.map(d => d.name) },
    yAxis: { type: 'value', name: '次数' },
    series: [{
      type: 'bar',
      data: data.map(d => d.value),
      itemStyle: {
        color: (params) => {
          const colors = { '2xx': '#67C23A', '3xx': '#409EFF', '4xx': '#E6A23C', '5xx': '#F56C6C' }
          return colors[d.name] || '#909399'
        }
      }
    }],
    grid: { left: 50, right: 20, bottom: 40 }
  })
}

onMounted(() => {
  nextTick(fetchData)
  window.addEventListener('resize', () => statusInstance?.resize())
})

onUnmounted(() => {
  window.removeEventListener('resize', () => statusInstance?.resize())
  statusInstance?.dispose()
})
</script>

<style scoped>
.page-container { background: #0a1628; color: #fff; min-height: 100vh; padding: 20px; }
.page-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
.page-header h1 { margin: 0; }
.back-link { color: #F56C6C; text-decoration: none; }
.kpi-row { display: grid; grid-template-columns: repeat(2, 1fr); gap: 16px; margin-bottom: 20px; }
.kpi-card { background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.1); border-radius: 8px; padding: 20px; text-align: center; }
.kpi-label { font-size: 14px; color: #8c9db5; margin-bottom: 8px; }
.kpi-value { font-size: 48px; font-weight: bold; font-family: 'Courier New', monospace; }
.chart-card { background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.1); border-radius: 8px; padding: 16px; }
.chart-card h3 { margin: 0 0 12px; color: #8c9db5; }
.chart-box { height: 400px; }
</style>
