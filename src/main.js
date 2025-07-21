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
    exchange: "bybit",
    category: "linear",
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
let dataUpdateCount = 0

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

  // Start with empty data - real data will come from WebSocket
  candleData = []

  // Initial chart setup
  updateChart()
}



// Update chart with current candle data
function updateChart() {
  if (!chart || candleData.length === 0) {
    return
  }

  // Force chart update by setting new data
  const chartData = {
    panes: [{
      overlays: [{
        name: 'BTCUSDT',
        type: 'Candles',
        data: [...candleData], // Create new array to force update
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

  chart.data = chartData

  // Try different methods to force chart update
  try {
    if (typeof chart.update === 'function') {
      chart.update()
    } else if (typeof chart.render === 'function') {
      chart.render()
    } else if (typeof chart.refresh === 'function') {
      chart.refresh()
    }
  } catch (error) {
    // Ignore chart update errors
  }
}

// Connect to WebSocket
function connectWebSocket() {
  updateStatus('Connecting...', 'connecting')

  try {
    const wsUrl = getWebSocketURL()
    ws = new WebSocket(wsUrl)

    ws.onopen = handleWebSocketOpen
    ws.onmessage = handleWebSocketMessage
    ws.onclose = handleWebSocketClose
    ws.onerror = handleWebSocketError

  } catch (error) {
    updateStatus('Connection failed', 'disconnected')
    scheduleReconnect()
  }
}

// Handle WebSocket connection open
function handleWebSocketOpen() {
  isConnected = true
  updateStatus('Connected - Subscribing...', 'connecting')

  // Send subscription request using official format
  ws.send(JSON.stringify(SUBSCRIPTION_REQUEST))
}

// Recursively decode nested MessagePack data
function decodeNestedMessagePack(obj) {
  if (obj && typeof obj === 'object' && obj.constructor === Uint8Array) {
    // This is a Uint8Array, try to decode it as MessagePack
    try {
      const decoded = decode(obj)
      return decodeNestedMessagePack(decoded) // Recursively decode in case of multiple levels
    } catch (error) {
      return obj // Return original if decoding fails
    }
  } else if (Array.isArray(obj)) {
    // Process each array element
    return obj.map(item => decodeNestedMessagePack(item))
  } else if (obj && typeof obj === 'object') {
    // Process each object property
    const result = {}
    for (const [key, value] of Object.entries(obj)) {
      result[key] = decodeNestedMessagePack(value)
    }
    return result
  } else {
    // Primitive value, return as-is
    return obj
  }
}

// Parse MessagePack binary data
function parseMessagePackData(buffer) {
  try {
    const uint8Array = new Uint8Array(buffer)
    const decoded = decode(uint8Array)

    // Recursively decode any nested MessagePack data
    const fullyDecoded = decodeNestedMessagePack(decoded)

    console.log('Decoded WebSocket data (all levels):', JSON.stringify(fullyDecoded, null, 2))

    return fullyDecoded
  } catch (error) {
    return null
  }
}

// Handle incoming WebSocket messages
function handleWebSocketMessage(event) {
  try {
    // Handle Blob data (binary MessagePack format)
    if (event.data instanceof Blob) {
      event.data.arrayBuffer().then(buffer => {
        const message = parseMessagePackData(buffer)
        if (message) {
          handleParsedMessage(message)
        }
      })
    } else if (event.data instanceof ArrayBuffer) {
      // Handle ArrayBuffer directly
      const message = parseMessagePackData(event.data)
      if (message) {
        handleParsedMessage(message)
      }
    } else if (typeof event.data === 'string') {
      // Handle text data (JSON format)
      try {
        const message = JSON.parse(event.data)
        handleParsedMessage(message)
      } catch (error) {
        // Ignore JSON parse errors
      }
    }
  } catch (error) {
    // Ignore WebSocket message errors
  }
}

// Handle parsed message content
function handleParsedMessage(message) {
  // Handle different message formats

  if (typeof message === 'object' && message !== null && message.type) {
    switch (message.type) {
      case 'connected':
        break

      case 'subscription':
        if (message.status === 'success') {
          updateStatus('Connected - Receiving data', 'connected')
        } else {
          updateStatus('Subscription failed', 'disconnected')
        }
        break

      case 'subscribe_response':
        if (message.status === 'success') {
          updateStatus('Connected - Receiving data', 'connected')
        } else {
          updateStatus('Subscription failed', 'disconnected')
        }
        break

      case 'candle':
        handleCandleData(message.data)
        break

      default:
        // Check if this is candle data by looking for the subscription topic pattern
        if (message.type && message.type.startsWith('candle.')) {
          handleCandleData(message.data)
        } else if (message.type && message.data && Array.isArray(message.data)) {
          handleCandleData(message.data)
        } else if (message.data && message.data.ex && message.data.sy && message.data.o) {
          // ARCA API candle data format detected
          handleCandleData(message.data)
        }
    }
  } else if (Array.isArray(message)) {
    // Handle case where the entire message is an array (possible direct candle data)
    handleCandleData(message)
  }
}

// Handle candle data updates
function handleCandleData(data) {
  // Handle different data formats that might come from MessagePack
  let candleArray = null

  if (Array.isArray(data)) {
    candleArray = data
  } else if (data && typeof data === 'object') {
    // Check if data has a candles property or similar
    if (data.candles && Array.isArray(data.candles)) {
      candleArray = data.candles
    } else if (data.data && Array.isArray(data.data)) {
      candleArray = data.data
    } else if (data.items && Array.isArray(data.items)) {
      candleArray = data.items
    } else if (data.ex && data.sy && data.o && data.h && data.l && data.c) {
      // ARCA API single candle object format
      candleArray = [data]
    } else {
      // Try to convert single object to array
      candleArray = [data]
    }
  } else {
    return
  }

  if (!candleArray || candleArray.length === 0) {
    return
  }

  candleArray.forEach((candle, index) => {
    // Handle different possible data formats
    let timestamp, open, high, low, close, volume

    if (Array.isArray(candle)) {
      // If candle is an array: [timestamp, open, high, low, close, volume]
      [timestamp, open, high, low, close, volume] = candle
    } else if (typeof candle === 'object') {
      // Handle ARCA API format and other common formats
      timestamp = candle.st || candle.et || candle.timestamp || candle.time || candle.t
      open = candle.o || candle.open
      high = candle.h || candle.high
      low = candle.l || candle.low
      close = candle.c || candle.close
      volume = candle.v || candle.volume || 0
    } else {
      return
    }

    // Convert candle data to night-vision format: [timestamp, open, high, low, close, volume]
    // Ensure timestamp is in milliseconds
    let ts = parseInt(timestamp)
    if (ts < 1000000000000) { // If timestamp is in seconds, convert to milliseconds
      ts = ts * 1000
    }

    const formattedCandle = [
      ts,
      parseFloat(open),
      parseFloat(high),
      parseFloat(low),
      parseFloat(close),
      parseFloat(volume || 0)
    ]

    // Validate the formatted candle
    if (formattedCandle.some(val => isNaN(val))) {
      return
    }

    // Update or add candle data
    const existingIndex = candleData.findIndex(c => c[0] === ts)

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

  // Update the chart immediately for real-time data
  dataUpdateCount++
  updateChart()

  // Update status to show data is being received
  updateStatus(`Connected - Receiving data (${dataUpdateCount} updates)`, 'connected')
}

// Handle WebSocket connection close
function handleWebSocketClose(event) {
  isConnected = false
  updateStatus('Disconnected', 'disconnected')

  // Attempt to reconnect after a delay
  scheduleReconnect()
}

// Handle WebSocket errors
function handleWebSocketError(error) {
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
    // Update chart dimensions without recreating the entire chart
    chart.width = window.innerWidth - 40
    chart.height = 600
  }
}

// Start the application
window.addEventListener('load', init)
window.addEventListener('resize', handleResize)