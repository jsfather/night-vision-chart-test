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
let hasRealData = false

// 1-minute candle tracking
let currentCandleStartTime = null
let firstMessageOfCandle = null
let lastMessageOfCandle = null

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
      * {
        margin: 0;
        padding: 0;
        box-sizing: border-box;
      }

      body {
        background: linear-gradient(135deg, #0c0e16 0%, #161b22 100%);
        color: #d1d5db;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
        margin: 0;
        padding: 0;
        min-height: 100vh;
        overflow-x: hidden;
      }

      .header {
        background: rgba(22, 27, 34, 0.95);
        backdrop-filter: blur(10px);
        border-bottom: 1px solid rgba(48, 54, 61, 0.5);
        padding: 16px 24px;
        position: sticky;
        top: 0;
        z-index: 100;
        box-shadow: 0 2px 20px rgba(0, 0, 0, 0.3);
      }

      .header-content {
        display: flex;
        justify-content: space-between;
        align-items: center;
        max-width: 1400px;
        margin: 0 auto;
      }

      .title {
        font-size: 24px;
        font-weight: 600;
        color: #f7fafc;
        display: flex;
        align-items: center;
        gap: 12px;
      }

      .symbol-badge {
        background: linear-gradient(135deg, #f59e0b 0%, #d97706 100%);
        color: #000;
        padding: 4px 12px;
        border-radius: 6px;
        font-size: 14px;
        font-weight: 700;
        letter-spacing: 0.5px;
      }

      .header-info {
        display: flex;
        align-items: center;
        gap: 20px;
        font-size: 14px;
      }

      .price-info {
        display: flex;
        flex-direction: column;
        align-items: flex-end;
        gap: 2px;
      }

      .current-price {
        font-size: 18px;
        font-weight: 600;
        color: #10b981;
      }

      .price-change {
        font-size: 12px;
        color: #6b7280;
      }

      #status {
        padding: 6px 12px;
        border-radius: 20px;
        font-size: 12px;
        font-weight: 500;
        display: flex;
        align-items: center;
        gap: 6px;
      }

      .status-dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        animation: pulse 2s infinite;
      }

      @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.5; }
      }

      .connected {
        background: rgba(16, 185, 129, 0.1);
        color: #10b981;
        border: 1px solid rgba(16, 185, 129, 0.3);
      }
      .connected .status-dot { background: #10b981; }

      .disconnected {
        background: rgba(239, 68, 68, 0.1);
        color: #ef4444;
        border: 1px solid rgba(239, 68, 68, 0.3);
      }
      .disconnected .status-dot { background: #ef4444; }

      .connecting {
        background: rgba(245, 158, 11, 0.1);
        color: #f59e0b;
        border: 1px solid rgba(245, 158, 11, 0.3);
      }
      .connecting .status-dot { background: #f59e0b; }

      .chart-wrapper {
        padding: 24px;
        max-width: 1400px;
        margin: 0 auto;
      }

      #chart-container {
        width: 100%;
        height: calc(100vh - 140px);
        min-height: 600px;
        background: rgba(22, 27, 34, 0.8);
        border-radius: 12px;
        border: 1px solid rgba(48, 54, 61, 0.5);
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
        overflow: hidden;
        position: relative;
      }

      .chart-overlay {
        position: absolute;
        top: 16px;
        left: 16px;
        background: rgba(22, 27, 34, 0.9);
        backdrop-filter: blur(10px);
        padding: 12px 16px;
        border-radius: 8px;
        border: 1px solid rgba(48, 54, 61, 0.5);
        font-size: 12px;
        color: #9ca3af;
        z-index: 10;
      }

      .user-id {
        font-size: 11px;
        color: #6b7280;
        font-family: 'Courier New', monospace;
      }

      @media (max-width: 768px) {
        .header-content {
          flex-direction: column;
          gap: 12px;
        }

        .header-info {
          width: 100%;
          justify-content: space-between;
        }

        .chart-wrapper {
          padding: 16px;
        }

        #chart-container {
          height: calc(100vh - 180px);
        }
      }
    </style>

    <div class="header">
      <div class="header-content">
        <div class="title">
          <span class="symbol-badge">BTCUSDT</span>
          <span>Real-time Chart</span>
        </div>
        <div class="header-info">
          <div class="price-info">
            <div class="current-price" id="current-price">--</div>
            <div class="price-change" id="price-change">--</div>
          </div>
          <div id="status" class="disconnected">
            <div class="status-dot"></div>
            <span>Disconnected</span>
          </div>
        </div>
      </div>
    </div>

    <div class="chart-wrapper">
      <div id="chart-container">
        <div class="chart-overlay">
          <div class="user-id">Session: ${USER_ID}</div>
        </div>
      </div>
    </div>
  `
}

// Initialize the night-vision chart
function initChart() {
  const container = document.getElementById('chart-container')
  const containerRect = container.getBoundingClientRect()

  chart = new NightVision('chart-container', {
    width: containerRect.width,
    height: containerRect.height
  })

  // Add some initial test data to verify chart works
  // addInitialTestData()

  // Initial chart setup
  updateChart()
}

// Add initial test data to show chart is working
function addInitialTestData() {
  const now = Date.now()
  const testData = []

  // Generate 20 test candles
  let basePrice = 118000
  for (let i = 0; i < 20; i++) {
    const timestamp = now - (20 - i) * 60000 // 1 minute intervals
    const open = basePrice + (Math.random() - 0.5) * 100
    const close = open + (Math.random() - 0.5) * 200
    const high = Math.max(open, close) + Math.random() * 100
    const low = Math.min(open, close) - Math.random() * 100
    const volume = Math.random() * 10

    testData.push([timestamp, open, high, low, close, volume])
    basePrice = close // Use previous close as base for next candle
  }

  candleData = testData
}



// Update chart with current candle data
function updateChart() {
  if (!chart) {
    return
  }

  // Update price display
  updatePriceDisplay()

  // Create chart data structure
  const chartData = {
    panes: [{
      overlays: [{
        name: 'BTCUSDT',
        type: 'Candles',
        data: candleData.length > 0 ? [...candleData] : [],
        settings: {
          precision: 1,
          colorCandleUp: '#26a69a',
          colorCandleDw: '#ef5350',
          colorWickUp: '#26a69a',
          colorWickDw: '#ef5350'
        }
      }]
    }]
  }

  // Set chart data
  chart.data = chartData

  // Auto-scroll to show latest candles
  if (candleData.length > 0) {
    try {
      // Get the latest timestamp
      const latestTime = candleData[candleData.length - 1][0]
      const earliestTime = candleData[0][0]

      // Calculate visible range (show last 50 candles or all if less)
      const visibleCandles = Math.min(50, candleData.length)
      const timeRange = latestTime - earliestTime
      const candleWidth = timeRange / Math.max(1, candleData.length - 1)
      const visibleRange = candleWidth * visibleCandles

      // Set the visible range to show latest candles
      const rangeStart = latestTime - visibleRange
      const rangeEnd = latestTime + (candleWidth * 2) // Add some padding

      // Try to set the range for auto-scroll
      if (chart.setRange) {
        chart.setRange(rangeStart, rangeEnd)
      } else if (chart.range) {
        chart.range = [rangeStart, rangeEnd]
      }
    } catch (error) {
      // Ignore range setting errors
    }
  }
}

// Update price display in header
function updatePriceDisplay() {
  if (candleData.length === 0) return

  const latestCandle = candleData[candleData.length - 1]
  const [timestamp, open, high, low, close, volume] = latestCandle

  const currentPriceEl = document.getElementById('current-price')
  const priceChangeEl = document.getElementById('price-change')

  if (currentPriceEl && priceChangeEl) {
    // Format price with proper decimals
    const formattedPrice = close.toLocaleString('en-US', {
      minimumFractionDigits: 1,
      maximumFractionDigits: 1
    })

    currentPriceEl.textContent = `$${formattedPrice}`

    // Calculate price change
    if (candleData.length > 1) {
      const previousCandle = candleData[candleData.length - 2]
      const previousClose = previousCandle[4]
      const change = close - previousClose
      const changePercent = ((change / previousClose) * 100)

      const changeText = `${change >= 0 ? '+' : ''}${change.toFixed(1)} (${changePercent >= 0 ? '+' : ''}${changePercent.toFixed(2)}%)`
      priceChangeEl.textContent = changeText

      // Update colors based on change
      if (change >= 0) {
        currentPriceEl.style.color = '#10b981'
        priceChangeEl.style.color = '#10b981'
      } else {
        currentPriceEl.style.color = '#ef4444'
        priceChangeEl.style.color = '#ef4444'
      }
    }
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
  // Clear test data when real data arrives
  if (!hasRealData) {
    candleData = []
    hasRealData = true
  }

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
    processCandle(candle)
  })
}

// Process each candle - create 1-minute candles
function processCandle(candle) {
  // Extract data from different formats
  let timestamp, open, high, low, close, volume

  if (Array.isArray(candle)) {
    // If candle is an array: [timestamp, open, high, low, close, volume]
    [timestamp, open, high, low, close, volume] = candle
  } else if (typeof candle === 'object') {
    // Handle ARCA API format and other common formats
    timestamp = candle.timestamp || candle.st || candle.time || candle.t
    open = candle.o || candle.open
    high = candle.h || candle.high
    low = candle.l || candle.low
    close = candle.c || candle.close
    volume = candle.v || candle.volume || 0
  } else {
    return
  }

  // Validate required fields
  if (!timestamp || isNaN(parseFloat(open)) || isNaN(parseFloat(high)) ||
      isNaN(parseFloat(low)) || isNaN(parseFloat(close))) {
    console.log('❌ Invalid candle data, skipping:', candle)
    return
  }

  // Convert to milliseconds if needed
  let ts = parseInt(timestamp)
  if (ts < 1000000000000) {
    ts = ts * 1000
  }

  // Create message object
  const message = {
    timestamp: ts,
    open: parseFloat(open),
    high: parseFloat(high),
    low: parseFloat(low),
    close: parseFloat(close),
    volume: parseFloat(volume || 0)
  }

  console.log('📨 Processing message:', new Date(ts).toISOString(), message)

  // Check if we need to start a new 1-minute candle
  if (currentCandleStartTime === null) {
    // First message - start new candle
    startNewCandle(message)
  } else {
    // Check time difference
    const timeDiff = message.timestamp - currentCandleStartTime
    console.log('⏱️ Time diff:', timeDiff, 'ms (', Math.round(timeDiff / 1000), 'seconds )')

    if (timeDiff >= 60000) { // 1 minute = 60000ms
      // Time difference >= 1 minute - create new candle
      console.log('🆕 1 MINUTE PASSED - Creating new candle')
      finishCurrentCandle()
      startNewCandle(message)
    } else {
      // Time difference < 1 minute - update current candle
      console.log('🔄 UPDATING current candle (same minute)')
      updateCurrentCandle(message)
    }
  }
}

// Start a new 1-minute candle
function startNewCandle(message) {
  currentCandleStartTime = message.timestamp
  firstMessageOfCandle = message
  lastMessageOfCandle = message

  console.log('🚀 STARTED new candle at:', new Date(currentCandleStartTime).toISOString())

  // Add to chart immediately
  addOrUpdateCandleInChart()
}

// Update the current candle with new message
function updateCurrentCandle(message) {
  lastMessageOfCandle = message
  console.log('📝 Updated current candle with latest data')

  // Update chart
  addOrUpdateCandleInChart()
}

// Finish current candle (when starting new one)
function finishCurrentCandle() {
  if (currentCandleStartTime && firstMessageOfCandle && lastMessageOfCandle) {
    console.log('✅ FINISHED candle for period:',
      new Date(currentCandleStartTime).toISOString(),
      'to',
      new Date(lastMessageOfCandle.timestamp).toISOString())
  }
}

// Add or update candle in chart
function addOrUpdateCandleInChart() {
  if (!currentCandleStartTime || !firstMessageOfCandle || !lastMessageOfCandle) {
    return
  }

  // Build candle using:
  // - timestamp: start time of the candle
  // - open: from first message
  // - close: from last message
  // - high: max of first and last
  // - low: min of first and last
  // - volume: from last message
  const formattedCandle = [
    currentCandleStartTime,
    firstMessageOfCandle.open,
    Math.max(firstMessageOfCandle.high, lastMessageOfCandle.high),
    Math.min(firstMessageOfCandle.low, lastMessageOfCandle.low),
    lastMessageOfCandle.close,
    lastMessageOfCandle.volume
  ]

  // Check if candle already exists in chart
  const existingIndex = candleData.findIndex(c => c[0] === currentCandleStartTime)

  if (existingIndex >= 0) {
    // UPDATE existing candle
    candleData[existingIndex] = formattedCandle
    console.log('🔄 UPDATED chart candle:', new Date(currentCandleStartTime).toISOString())
  } else {
    // ADD new candle
    candleData.push(formattedCandle)
    console.log('➕ ADDED new chart candle:', new Date(currentCandleStartTime).toISOString())

    // Keep only last 200 candles for performance
    if (candleData.length > 200) {
      candleData = candleData.slice(-200)
    }
  }

  // Sort by timestamp to ensure proper order
  candleData.sort((a, b) => a[0] - b[0])

  // Update the chart
  dataUpdateCount++
  updateChart()

  // Update status
  updateStatus(`Live - ${candleData.length} candles`, 'connected')
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
    statusElement.innerHTML = `
      <div class="status-dot"></div>
      <span>${message}</span>
    `
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
    const container = document.getElementById('chart-container')
    if (container) {
      const containerRect = container.getBoundingClientRect()
      // Update chart dimensions without recreating the entire chart
      chart.width = containerRect.width
      chart.height = containerRect.height
    }
  }
}

// Start the application
window.addEventListener('load', init)
window.addEventListener('resize', handleResize)