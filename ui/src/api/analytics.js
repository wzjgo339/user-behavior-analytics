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
