import './style.css'
import { NightVision } from 'night-vision'
import { decode } from '@msgpack/msgpack'

// WebSocket configuration
const WS_BASE_URL = 'wss://svc.aping.tech/feed/api/v1/ws/market'
const USER_ID = 'user_' + Math.random().toString(36).substr(2, 9) // Generate random user ID
// Official subscription format from API documentation
const SUBSCRIPTION_REQUEST = {
  op: "subscribe",
  requestId: "btcusdt-candles",
  args: [{
    exchange: "binance",
    category: "candle",
    topic: "candle.BTCUSDT.1"
  }]
}

// Build WebSocket URL with user identification
function getWebSocketURL() {
  return `${WS_BASE_URL}?userId=${encodeURIComponent(USER_ID)}`
}

// Global variables
let ws = null
let chart = null
let candleData = []
let isConnected = false

// Initialize the application
function init() {
  setupHTML()
  initChart()
  connectWebSocket()
}

// Setup HTML structure
function setupHTML() {
  document.querySelector('#app').innerHTML = `
    <style>
      body {
        background-color: #0c0d0e;
        color: #ffffff;
        font-family: Arial, sans-serif;
        margin: 0;
        padding: 20px;
      }
      #chart-container {
        width: 100%;
        height: 600px;
        margin-top: 20px;
      }
      #status {
        margin-bottom: 10px;
        padding: 10px;
        border-radius: 4px;
        font-weight: bold;
      }
      .connected { background-color: #1a4d1a; }
      .disconnected { background-color: #4d1a1a; }
      .connecting { background-color: #4d4d1a; }
    </style>
    <h1>BTCUSDT Real-time Candlestick Chart</h1>
    <div style="margin-bottom: 10px; font-size: 14px; color: #888;">User ID: ${USER_ID}</div>
    <div id="status" class="disconnected">Disconnected</div>
    <div id="chart-container"></div>
  `
}

// Initialize the night-vision chart
function initChart() {
  chart = new NightVision('chart-container', {
    width: window.innerWidth - 40,
    height: 600
  })

  // Initial empty chart setup
  updateChart()
}

// Update chart with current candle data
function updateChart() {
  if (!chart) return

  chart.data = {
    panes: [{
      overlays: [{
        name: 'BTCUSDT',
        type: 'Candles',
        data: candleData,
        settings: {
          precision: 2,
          colorCandleUp: '#26a69a',
          colorCandleDw: '#ef5350',
          colorWickUp: '#26a69a',
          colorWickDw: '#ef5350'
        }
      }]
    }]
  }
}

// Connect to WebSocket
function connectWebSocket() {
  updateStatus('Connecting...', 'connecting')

  try {
    const wsUrl = getWebSocketURL()
    console.log('Connecting to WebSocket with User ID:', USER_ID)
    ws = new WebSocket(wsUrl)

    ws.onopen = handleWebSocketOpen
    ws.onmessage = handleWebSocketMessage
    ws.onclose = handleWebSocketClose
    ws.onerror = handleWebSocketError

  } catch (error) {
    console.error('WebSocket connection error:', error)
    updateStatus('Connection failed', 'disconnected')
    scheduleReconnect()
  }
}

// Handle WebSocket connection open
function handleWebSocketOpen() {
  console.log('WebSocket connected')
  isConnected = true
  updateStatus('Connected - Subscribing...', 'connecting')

  // Send subscription request using official format
  console.log('Sending subscription request:', SUBSCRIPTION_REQUEST)
  ws.send(JSON.stringify(SUBSCRIPTION_REQUEST))
}

// Handle incoming WebSocket messages
function handleWebSocketMessage(event) {
  try {
    // Handle Blob data (binary MessagePack format)
    if (event.data instanceof Blob) {
      event.data.arrayBuffer().then(buffer => {
        try {
          const message = decode(new Uint8Array(buffer))
          handleParsedMessage(message)
        } catch (error) {
          console.error('Error decoding MessagePack data:', error)
        }
      }).catch(error => {
        console.error('Error reading Blob data:', error)
      })
    } else if (event.data instanceof ArrayBuffer) {
      // Handle ArrayBuffer directly
      try {
        const message = decode(new Uint8Array(event.data))
        handleParsedMessage(message)
      } catch (error) {
        console.error('Error decoding ArrayBuffer:', error)
      }
    } else {
      // Handle text data (JSON format)
      try {
        const message = JSON.parse(event.data)
        handleParsedMessage(message)
      } catch (error) {
        console.error('Error parsing JSON message:', error)
        console.log('Raw message data:', event.data)
      }
    }
  } catch (error) {
    console.error('Error handling WebSocket message:', error)
  }
}

// Handle parsed message content
function handleParsedMessage(message) {
  console.log('Received message:', message)

  switch (message.type) {
    case 'connected':
      console.log('Server connection confirmed:', message.message)
      break

    case 'subscription':
      if (message.status === 'success') {
        console.log('Successfully subscribed to BTCUSDT candles')
        updateStatus('Connected - Receiving data', 'connected')
      } else {
        console.error('Subscription failed:', message)
        updateStatus('Subscription failed', 'disconnected')
      }
      break

    case 'subscribe_response':
      if (message.status === 'success') {
        console.log('Successfully subscribed to BTCUSDT candles')
        console.log('Subscription details:', message.data)
        if (message.data && message.data.successful) {
          console.log('Successful subscriptions:', message.data.successful)
        }
        updateStatus('Connected - Receiving data', 'connected')
      } else {
        console.error('Subscription failed:', message)
        console.error('Error details:', message.data)
        if (message.data && message.data.failed && message.data.failed.length > 0) {
          console.error('Failed subscriptions:', message.data.failed)
          message.data.failed.forEach(failure => {
            console.error('Failure reason:', failure)
          })
        }
        updateStatus('Subscription failed', 'disconnected')
      }
      break

    case 'candle':
      handleCandleData(message.data)
      break

    default:
      // According to documentation, candle data comes with type set to subscription topic
      // Check if this might be candle data
      if (message.type && message.data && Array.isArray(message.data)) {
        console.log('Possible candle data received:', message)
        handleCandleData(message.data)
      } else {
        console.log('Unknown message type:', message)
      }
  }
}

// Handle candle data updates
function handleCandleData(data) {
  if (!data || !Array.isArray(data)) return

  data.forEach(candle => {
    // Convert candle data to night-vision format: [timestamp, open, high, low, close, volume]
    const formattedCandle = [
      candle.timestamp,
      parseFloat(candle.open),
      parseFloat(candle.high),
      parseFloat(candle.low),
      parseFloat(candle.close),
      parseFloat(candle.volume || 0)
    ]

    // Update or add candle data
    const existingIndex = candleData.findIndex(c => c[0] === candle.timestamp)

    if (existingIndex >= 0) {
      // Update existing candle
      candleData[existingIndex] = formattedCandle
    } else {
      // Add new candle
      candleData.push(formattedCandle)

      // Keep only last 200 candles for performance
      if (candleData.length > 200) {
        candleData = candleData.slice(-200)
      }
    }
  })

  // Sort by timestamp to ensure proper order
  candleData.sort((a, b) => a[0] - b[0])

  // Update the chart
  updateChart()
}

// Handle WebSocket connection close
function handleWebSocketClose(event) {
  console.log('WebSocket connection closed:', event.code, event.reason)
  isConnected = false
  updateStatus('Disconnected', 'disconnected')

  // Attempt to reconnect after a delay
  scheduleReconnect()
}

// Handle WebSocket errors
function handleWebSocketError(error) {
  console.error('WebSocket error:', error)
  updateStatus('Connection error', 'disconnected')
}

// Update connection status display
function updateStatus(message, className) {
  const statusElement = document.getElementById('status')
  if (statusElement) {
    statusElement.textContent = message
    statusElement.className = className
  }
}

// Schedule WebSocket reconnection
function scheduleReconnect() {
  if (!isConnected) {
    console.log('Scheduling reconnection in 5 seconds...')
    setTimeout(() => {
      if (!isConnected) {
        connectWebSocket()
      }
    }, 5000)
  }
}

// Handle window resize
function handleResize() {
  if (chart) {
    // NightVision doesn't have a resize method, recreate the chart instead
    initChart()
  }
}

// Start the application
window.addEventListener('load', init)
window.addEventListener('resize', handleResize)