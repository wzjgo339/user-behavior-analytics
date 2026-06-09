import { Client } from '@stomp/stompjs'
import SockJS from 'sockjs-client'

let stompClient = null

export function connectWebSocket(onMessage) {
  stompClient = new Client({
    // 开发环境直连后端（绕过 Vite 代理的 WebSocket 兼容问题）
    // 生产环境可改为相对路径（通过 Nginx 代理）
    webSocketFactory: () => new SockJS('http://localhost:8080/ws'),
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
