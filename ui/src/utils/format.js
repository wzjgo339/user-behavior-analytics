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
