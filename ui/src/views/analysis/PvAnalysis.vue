<template>
  <div class="page-container">
    <header class="page-header">
      <h1>PV 分析</h1>
      <router-link to="/dashboard" class="back-link">← 返回大屏</router-link>
    </header>
    <section class="chart-card">
      <h3>PV 趋势（最近 {{ minutes }} 分钟）</h3>
      <div class="controls">
        <button v-for="m in [15, 30, 60, 120]" :key="m" :class="{ active: minutes === m }" @click="changeRange(m)">{{ m }}分钟</button>
      </div>
      <div ref="chartRef" class="chart-box"></div>
    </section>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted, nextTick } from 'vue'
import * as echarts from 'echarts'
import { getPvTrend, getOverview } from '@/api/analytics'

const chartRef = ref(null)
let chartInstance = null
const minutes = ref(60)

function changeRange(m) {
  minutes.value = m
  fetchData()
}

async function fetchData() {
  const res = await getPvTrend(minutes.value)
  const data = res.data || []
  if (!chartInstance) {
    chartInstance = echarts.init(chartRef.value)
  }
  chartInstance.setOption({
    tooltip: { trigger: 'axis' },
    xAxis: { type: 'category', data: data.map(d => d.t) },
    yAxis: { type: 'value', name: 'PV' },
    series: [{
      data: data.map(d => d.pv),
      type: 'line',
      smooth: true,
      areaStyle: { opacity: 0.3 },
      itemStyle: { color: '#409EFF' }
    }],
    grid: { left: 60, right: 20, bottom: 40 }
  })
}

onMounted(() => {
  nextTick(fetchData)
  window.addEventListener('resize', () => chartInstance?.resize())
})

onUnmounted(() => {
  window.removeEventListener('resize', () => chartInstance?.resize())
  chartInstance?.dispose()
})
</script>

<style scoped>
.page-container { background: #0a1628; color: #fff; min-height: 100vh; padding: 20px; }
.page-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
.page-header h1 { margin: 0; }
.back-link { color: #409EFF; text-decoration: none; }
.chart-card { background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.1); border-radius: 8px; padding: 16px; }
.chart-card h3 { margin: 0 0 12px; color: #8c9db5; }
.chart-box { height: 500px; }
.controls { margin-bottom: 12px; display: flex; gap: 8px; }
.controls button { background: rgba(255,255,255,0.1); border: 1px solid rgba(255,255,255,0.2); color: #fff; padding: 4px 12px; border-radius: 4px; cursor: pointer; }
.controls button.active { background: #409EFF; border-color: #409EFF; }
</style>
