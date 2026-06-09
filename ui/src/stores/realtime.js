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
