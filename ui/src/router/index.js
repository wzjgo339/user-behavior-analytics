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
