<template>
  <div class="page-container">
    <header class="page-header">
      <h1>来源分析</h1>
      <router-link to="/dashboard" class="back-link">← 返回大屏</router-link>
    </header>
    <section class="charts-row">
      <div class="chart-card">
        <h3>来源分布</h3>
        <div ref="pieChart" class="chart-box"></div>
      </div>
      <div class="chart-card">
        <h3>来源明细</h3>
        <div class="data-table">
          <div class="table-header">
            <span>来源类型</span>
            <span>数量</span>
            <span>占比</span>
          </div>
          <div v-for="row in tableData" :key="row.name" class="table-row">
            <span>{{ row.name }}</span>
            <span>{{ row.value }}</span>
            <span>{{ row.percent }}</span>
          </div>
          <div v-if="tableData.length === 0" class="empty">暂无数据</div>
        </div>
      </div>
    </section>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted, nextTick } from 'vue'
import * as echarts from 'echarts'
import { getRefererAnalysis } from '@/api/analytics'

const pieChart = ref(null)
let pieInstance = null
const rawData = ref([])

const nameMap = { direct: '直接访问', search_engine: '搜索引擎', external: '外部链接', internal: '站内跳转' }

const tableData = computed(() => {
  const total = rawData.value.reduce((s, d) => s + d.cnt, 0) || 1
  return rawData.value.map(d => ({
    name: nameMap[d.referer_type] || d.referer_type,
    value: d.cnt,
    percent: ((d.cnt / total) * 100).toFixed(1) + '%'
  }))
})

async function fetchData() {
  const res = await getRefererAnalysis()
  rawData.value = res.data || []
  const data = rawData.value.map(d => ({
    name: nameMap[d.referer_type] || d.referer_type,
    value: d.cnt
  }))
  if (!pieInstance) {
    pieInstance = echarts.init(pieChart.value)
  }
  pieInstance.setOption({
    tooltip: { trigger: 'item', formatter: '{b}: {c} ({d}%)' },
    series: [{
      type: 'pie',
      radius: ['30%', '60%'],
      data,
      label: { show: true, formatter: '{b}\n{d}%', fontSize: 14 },
      emphasis: { itemStyle: { shadowBlur: 10, shadowOffsetX: 0, shadowColor: 'rgba(0,0,0,0.5)' } }
    }]
  })
}

onMounted(() => {
  nextTick(fetchData)
  window.addEventListener('resize', () => pieInstance?.resize())
})

onUnmounted(() => {
  window.removeEventListener('resize', () => pieInstance?.resize())
  pieInstance?.dispose()
})
</script>

<style scoped>
.page-container { background: #0a1628; color: #fff; min-height: 100vh; padding: 20px; }
.page-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
.page-header h1 { margin: 0; }
.back-link { color: #E6A23C; text-decoration: none; }
.charts-row { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
.chart-card { background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.1); border-radius: 8px; padding: 16px; }
.chart-card h3 { margin: 0 0 12px; color: #8c9db5; }
.chart-box { height: 400px; }
.data-table { color: #ccc; }
.table-header, .table-row { display: grid; grid-template-columns: 2fr 1fr 1fr; padding: 8px 12px; }
.table-header { color: #8c9db5; border-bottom: 1px solid rgba(255,255,255,0.1); }
.table-row { border-bottom: 1px solid rgba(255,255,255,0.05); }
.empty { text-align: center; padding: 40px; color: #666; }
</style>
