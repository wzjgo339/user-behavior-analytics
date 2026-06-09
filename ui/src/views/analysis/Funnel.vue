<template>
  <div class="page-container">
    <header class="page-header">
      <h1>漏斗分析</h1>
      <router-link to="/dashboard" class="back-link">← 返回大屏</router-link>
    </header>
    <section class="chart-card">
      <h3>用户转化漏斗</h3>
      <div ref="chartRef" class="chart-box"></div>
    </section>
    <section class="chart-card" style="margin-top: 16px;">
      <h3>各阶段数据</h3>
      <div class="data-table">
        <div class="table-header">
          <span>步骤</span>
          <span>用户数</span>
          <span>转化率</span>
        </div>
        <div v-for="row in tableData" :key="row.step_name" class="table-row">
          <span>{{ row.step_name }}</span>
          <span>{{ row.user_count }}</span>
          <span>{{ row.conversion_rate }}</span>
        </div>
        <div v-if="tableData.length === 0" class="empty">暂无漏斗数据 — 需生成用户行为事件后由 Flink 漏斗分析作业产出</div>
      </div>
    </section>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted, nextTick } from 'vue'
import * as echarts from 'echarts'
import { getFunnelData } from '@/api/analytics'

const chartRef = ref(null)
let chartInstance = null
const tableData = ref([])

async function fetchData() {
  const res = await getFunnelData()
  tableData.value = res.data || []
  const data = (res.data || []).map(d => ({
    name: d.step_name,
    value: d.user_count
  }))
  if (!chartInstance) {
    chartInstance = echarts.init(chartRef.value)
  }
  chartInstance.setOption({
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
.back-link { color: #67C23A; text-decoration: none; }
.chart-card { background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.1); border-radius: 8px; padding: 16px; }
.chart-card h3 { margin: 0 0 12px; color: #8c9db5; }
.chart-box { height: 400px; }
.data-table { color: #ccc; }
.table-header, .table-row { display: grid; grid-template-columns: 2fr 1fr 1fr; padding: 8px 12px; }
.table-header { color: #8c9db5; border-bottom: 1px solid rgba(255,255,255,0.1); }
.table-row { border-bottom: 1px solid rgba(255,255,255,0.05); }
.empty { text-align: center; padding: 40px; color: #666; }
</style>
