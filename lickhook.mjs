import WebSocket from 'ws';
import fs from 'fs';
import { config } from 'dotenv';
import { ema, getDetachSourceFromOHLCV } from 'trading-indicator';
import axios from 'axios';
config();

const likbotsymbols = process.env.likbotsymbols.split(',');  // Split into an array of symbol/exchange pairs
const signalbotexchange = process.env.signalbotexchange;
const windowDuration = process.env.likwindow * 1000;
const signalbotsecret = process.env.signalbotsecret;
const signalbotlonguuid = process.env.signalbotlonguuid;
const signalbotshortuuid = process.env.signalbotshortuuid;
const wsUrl       = 'wss://stream.bybit.com/v5/public/linear';
const webhookUrl  = 'https://api.3commas.io/signal_bots/webhooks';
const webhookUrl2 = 'https://app.3commas.io/trade_signal/trading_view';
const logFile     = 'liquidation_log.txt';
const ematimeframe = process.env.emainterval;
const emalength = parseInt(process.env.emaperiod);
const longband = parseFloat(process.env.likbotlongband);
const shortband = parseFloat(process.env.likbotshortband);
const symbolTurnovers = {};
const symbolminorder = {};
const likbotbuyclustersizeratio = process.env.likbotbuyclustersizeratio;
const likbotsellclustersizeratio = process.env.likbotsellclustersizeratio;
const likbotlong = process.env.likbotlong;
const likbotshort = process.env.likbotshort;
const email_token = process.env.email_token;

class LiquidationQueue {
  constructor(windowDuration) {
    this.windowDuration = windowDuration;
    this.totalSizeLong = 0;
    this.totalSizeShort = 0;
    this.liquidationsLong = new Map();
    this.liquidationsShort = new Map();
    this.lastCleanup = Date.now();
    this.cleanupInterval = 3600000; // 1 hour in milliseconds
  }

  add(size, time, side) {
    const liquidations = side === 'Buy' ? this.liquidationsLong : this.liquidationsShort;
    liquidations.set(time, size);
    if (side === 'Buy') {
      this.totalSizeLong += size;
    } else {
      this.totalSizeShort += size;
    }
    this.removeOld(time);
    if (time - this.lastCleanup > this.cleanupInterval) {
      this.removeOld(time);
      this.lastCleanup = time;
    }
  }

  removeOld(currentTime) {
    this.removeOldSide(currentTime, this.liquidationsLong, 'Long');
    this.removeOldSide(currentTime, this.liquidationsShort, 'Short');
  }

  removeOldSide(currentTime, liquidations, side) {
    for (let [time, size] of liquidations) {
      if (currentTime - time >= this.windowDuration) {
        liquidations.delete(time);
        if (side === 'Long') {
          this.totalSizeLong -= size;
        } else {
          this.totalSizeShort -= size;
        }
      } else {
        break; // Assuming times are added in order
      }
    }
  }

  getTotalSize(side) {
    return side === 'Buy' ? this.totalSizeLong : this.totalSizeShort;
  }

  reset(side) {
    if (side === "Buy") {
      this.liquidationsLong.clear();
      this.totalSizeLong = 0;
    } else if (side === "Sell") {
      this.liquidationsShort.clear();
      this.totalSizeShort = 0;
    }
  }
}

class EMA {
  constructor(period) {
    this.period = period;
    this.values = [];
    this.ema = null;
  }

  async initializeEMA(symbol) {
    try {
      const formattedPair = symbol => `${symbol.slice(0, -4)}/USDT`;
      const { input } = await getDetachSourceFromOHLCV('binance', formattedPair(symbol), `${ematimeframe}m`, false);      
      let emaData = await ema(emalength, "close", input);
      this.ema = emaData[emaData.length - 1];       
    } catch (error) {
      console.error("Error in API TA retrieve", error);
    }
  }

  update(newEma) {
    if (this.ema === null) {
      throw new Error('EMA not initialized. Call initializeEMA first.');
    }
    this.ema = newEma;
    this.values.push(this.ema);
  }

  getCurrentEMA() {
    return this.ema;
  }
}

// Initialize for each symbol-exchange pair
const liquidationQueues = likbotsymbols.map(pair => {
  const [symbol, exchange] = pair.split('/');
  return {
    symbol,
    exchange,
    queue: new LiquidationQueue(windowDuration),
    ema: new EMA(emalength),    
  };
});

// New function to initialize EMA for all trading pairs at startup
async function initializeAllEMAs() {
  for (const { symbol, ema } of liquidationQueues) {
    try {
      await ema.initializeEMA(symbol);
      const currentEMA = ema.getCurrentEMA();
      log(`EMA Initialized for ${symbol}: ${currentEMA.toFixed(2)}`, "file");      
    } catch (error) {
      console.error(`Error initializing EMA for ${symbol}:`, error.message);
    }
  }
}

async function refreshAllEMAs() {
  for (const { symbol, ema } of liquidationQueues) {
    try {
      await ema.initializeEMA(symbol);
      const currentEMA = ema.getCurrentEMA();
      log(`EMA Refreshed for ${symbol}: ${currentEMA.toFixed(2)}`, "file");      
    } catch (error) {
      console.error(`Error refreshing EMA for ${symbol}:`, error.message);
    }
  }
}

function shouldRefreshEMA() {
  const now = new Date();
  const minutes = now.getMinutes();
  const refreshIntervals = [0, 15, 30, 45]; // Refresh at these minutes past the hour

  const timeframeMinutes = parseInt(ematimeframe); // Now directly parse ematimeframe since it's just a number
  if (!refreshIntervals.includes(minutes) || minutes % timeframeMinutes !== 0) {
    return false;
  }
  return true;
}

// Main Execution
log("LickHunter Lite (3Commas webhook version) - Start/Restart");
await initializeAllEMAs();

connectWebSocket();

// Set interval for checking EMA refresh condition
setInterval(async () => {
  if (shouldRefreshEMA()) {
    await refreshAllEMAs();
  }
}, 60000); // Check every minute if it's time to refresh EMA

setInterval(() => {
  const used = process.memoryUsage();
  log(`Memory usage: ${JSON.stringify(used)}`, "file");
}, 3600000); // Every 60 minutes

function connectWebSocket() {
  liquidationQueues.forEach(({ symbol, exchange }) => {
    (function connectForSymbol(symbol, exchange) {
      const ws = new WebSocket(wsUrl);
      const subscriptionMessages = [
        JSON.stringify({ req_id: 'test', op: 'subscribe', args: [`liquidation.${symbol}`] })
      ];

      let pingInterval;
      let pongTimeout;

      function heartbeat() {
        clearTimeout(pongTimeout);
        pongTimeout = setTimeout(() => {
          log(`No pong received for ${symbol}, closing connection`);
          ws.terminate();
        }, 30000); // Wait 30 seconds for a pong before terminating
      }

      ws.on('open', () => {
        subscriptionMessages.forEach(msg => ws.send(msg));

        // Start sending pings every 20 seconds
        pingInterval = setInterval(() => {
          if (ws.readyState === WebSocket.OPEN) {
            ws.ping();
            heartbeat();
          }
        }, 20000);
      });

      ws.on('ping', () => {
        ws.pong();
      });

      ws.on('pong', () => {
        heartbeat();
      });

      ws.on('message', async (data) => {
        const message = JSON.parse(data);
        if (message.success) {
          // Fetch the 24-hour volume for the symbol
          try {
            const volumeData = await fetch24hrVolume(symbol);            
            symbolTurnovers[symbol] = volumeData.turnover24h;
          } catch (error) {
            console.error(`Failed to fetch 24h turnover for ${symbol}: ${error.message}`);
          }
          try {
            const data = await getBinanceData(symbol);
            const minOrderSize = Math.max(5.625, data.minOrderSizeUSD);
            symbolminorder[symbol] = minOrderSize.toFixed(4);
          } catch (error) {
            console.error('Failed to get LOT data:', error);
          }
          log(`WebSoc. ${symbol}\t$${(symbolTurnovers[symbol] / 1000000).toFixed(0)} M Daily. \tCluster target is Long $${(symbolTurnovers[symbol] * likbotbuyclustersizeratio).toFixed(1)}/Short $${(symbolTurnovers[symbol] * likbotsellclustersizeratio).toFixed(1)}   \tMin Order Size: $${symbolminorder[symbol]}`);
        } else if (message.topic && message.topic.startsWith('liquidation.')) {
          handleLiquidationMessage(symbol, message);
        }
      });

      ws.on('error', (error) => {
        console.error(`WebSocket error for symbol: ${symbol}, exchange: ${exchange}:`, error);
      });

      ws.on('close', () => {
        log(`WebSocket for ${symbol} closed. Reconnecting...`);
        clearInterval(pingInterval);
        clearTimeout(pongTimeout);
        setTimeout(() => connectForSymbol(symbol, exchange), 1111);
      });
    })(symbol, exchange);
  });
}

async function handleLiquidationMessage(symbol, message) {
  try {
    if (message.topic.startsWith('liquidation.') && message.data) {
      const { side, size, price, updatedTime } = message.data;
      const matchedQueue = liquidationQueues.find(({ symbol: s }) => s === symbol);
      if (!matchedQueue) return;

      const { queue, exchange, ema} = matchedQueue;
      const liquidationSize = parseFloat(size);
      const liquidationPrice = parseFloat(price);

      const liquidationValueInUSD = liquidationSize * liquidationPrice;

      queue.add(liquidationValueInUSD, updatedTime, side);
      const totalLiquidationSizeUSDlong = queue.getTotalSize("Buy");
      const totalLiquidationSizeUSDshort = queue.getTotalSize("Sell");

      const currentEMA = ema.getCurrentEMA();

      const longthres = currentEMA  *(1 - (longband  / 100));
      const shortthres = currentEMA *(1 + (shortband / 100));
      
      log(`Lick Found: ${symbol} ${side}\t$${liquidationValueInUSD.toFixed(2)},\t$${price}`, "file");
      
      if (side === "Buy" && totalLiquidationSizeUSDlong > (symbolTurnovers[symbol] * likbotbuyclustersizeratio)) {
        if (price < longthres) {
          signalbotlonguuid  !== 0 ? sendWebhook(webhookUrl, createPayload ('enter_long', price, exchange, symbolminorder[symbol])): null;
          signalbotshortuuid !== 0 ? sendWebhook(webhookUrl, createPayload ('exit_short', price, exchange, symbolminorder[symbol])): null;
          sendWebhook(webhookUrl2,createPayload2('enter_long',symbol));
          //sendWebhook(webhookUrl2,createPayload3('exit_short',symbol));
          log(`Valid Cluster LONG  : ${symbol.padEnd(10)}$${totalLiquidationSizeUSDlong.toFixed(2).padStart(10)} @ $${price},  \tBelow Threshold: $${longthres.toFixed(2)}`);
          queue.reset("Buy");
          sendTelegramMessage(`Cluster LONG : ${symbol} Total $${totalLiquidationSizeUSDlong.toFixed(2)} at $${price}, Below Threshold: ${longthres.toFixed(2)}`);
        } else {
          log(`Weak Cluster LONG   : ${symbol.padEnd(10)}$${totalLiquidationSizeUSDlong.toFixed(2).padStart(10)} @ $${price},  \tAbove Threshold: $${longthres.toFixed(2)}`);
        }
      }
      else if (side === "Sell" && totalLiquidationSizeUSDshort > (symbolTurnovers[symbol] * likbotsellclustersizeratio)) {
        if (price > shortthres) {
          signalbotlonguuid  !== 0 ? sendWebhook(webhookUrl, createPayload ('exit_long',   price, exchange, symbolminorder[symbol])): null;
          signalbotshortuuid !== 0 ? sendWebhook(webhookUrl, createPayload ('enter_short', price, exchange, symbolminorder[symbol])): null;
          //sendWebhook(webhookUrl2,createPayload2('enter_short',symbol));
          sendWebhook(webhookUrl2,createPayload3('exit_long',  symbol));
          log(`Valid Cluster SHORT : ${symbol.padEnd(10)}$${totalLiquidationSizeUSDshort.toFixed(2).padStart(10)} @ $${price},  \tAbove Threshold: $${shortthres.toFixed(2)}`);
          queue.reset("Sell");
          sendTelegramMessage(`Cluster SHORT: ${symbol} Total $${totalLiquidationSizeUSDshort.toFixed(2)} at $${price}, Above Threshold: ${shortthres.toFixed(2)}`);
        } else {
          log(`Weak Cluster SHORT  : ${symbol.padEnd(10)}$${totalLiquidationSizeUSDshort.toFixed(2).padStart(10)} @ $${price},  \tBelow Threshold: $${shortthres.toFixed(2)}`);
        }
      }
    }
  } catch (error) {
    console.error(`Error processing liquidation message for ${symbol}: ${error.message}`);
  }
}

function createPayload(action, triggerPrice, exchange, order) {
  return {
    secret: signalbotsecret,
    trigger_price: triggerPrice.toString(),
    tv_exchange: signalbotexchange,
    tv_instrument: exchange,
    action: action,
    bot_uuid: action.includes('long') ? signalbotlonguuid : signalbotshortuuid, // chose long or short uuid
    order: {
      amount: order * 1.1,
      currency_type: "quote",
      order_type: "market"
    }
  };
}
function createPayload2(action, pair) {
  //const transformedPair = pair.replace(/^(.{3})(.{4})$/, '$2_$1'); // for spot
  const transformedPair = pair.replace(/^(.{3,4})(.{4})$/, '$2_$1$2'); // for USDT-M
  console.log(transformedPair);
  return {
    action: "trigger_safety",
    message_type: "bot",
    bot_id: action.includes('long') ? likbotlong : likbotshort, // chose long or short uuid
    email_token: email_token,
    delay_seconds: 0,
    pair: transformedPair,    
  };
}

function createPayload3(action, pair) {
  //const transformedPair = pair.replace(/^(.{3})(.{4})$/, '$2_$1'); // spot
  const transformedPair = pair.replace(/^(.{3,4})(.{4})$/, '$2_$1$2'); // USDT-M
  //console.log(pair,transformedPair);
  return {
    action: "close_at_market_price",
    message_type: "bot",
    bot_id: action.includes('long') ? likbotlong : likbotshort, // chose long or short uuid
    email_token: email_token,
    delay_seconds: 0,
    pair: transformedPair,    
  };
}

async function sendWebhook(url, data) {
  try {
    //console.log(url, data);
    const response = await axios.post(url, data);
    //console.log(response);
  } catch (error) {
    console.error('Error sending webhook:', error.message);
  }
}

function log(message, mode) {
  const now = new Date();
  const options = {
    year: '2-digit',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  };
  const timestamp = now.toLocaleString(undefined, options);

  const existingContent = fs.readFileSync(logFile, 'utf8');
  const lines = existingContent.split('\n');
  const limitedContent = lines.slice(Math.max(lines.length - 50000, 0)).join('\n');
  fs.writeFileSync(logFile, limitedContent, 'utf8');

  if (mode === 'screen') {
    console.log(`${timestamp} - ${message}`);
  } else if (mode === 'file') {
    fs.appendFileSync(logFile, `${timestamp} - ${message}\n`, 'utf8');  
  } else {
    console.log(`${timestamp} - ${message}`);
    fs.appendFileSync(logFile, `${timestamp} - ${message}\n`, 'utf8');
  }
}

async function sendTelegramMessage(message) {
  try {
    const response = await axios.post(`https://api.telegram.org/bot${process.env.telegramtoken}/sendMessage`, {
      chat_id: process.env.telegramid,
      text: message
    });
    return response.data;
  } catch (error) {
    console.error('Error sending message:', error);
    throw error;
  }
}

fs.watchFile('.env', (curr, prev) => {
  if (curr.mtime !== prev.mtime) {
    log('Config. parameters changed, restarting...');
    sleep(1000);
    process.exit(1);
  }
});

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetch24hrVolume(symbol) {
  try {
    const response = await axios.get('https://api.bybit.com/v5/market/tickers', {
      params: {
        category: 'linear',
        symbol: symbol
      }
    });

    if (response.data.retCode === 0 && response.data.result.list) {
      const tickerData = response.data.result.list[0]; // Get the first item, assuming only one symbol is requested
      return {
        turnover24h: tickerData.turnover24h
      };
    } else {
      throw new Error(`Failed to fetch ticker data for symbol: ${symbol}`);
    }
  } catch (error) {
    console.error(`Error fetching 24h turnover for ${symbol}:`, error);
    throw error; // Re-throw the error to handle it in the calling function
  }
}

function testWithSimulatedData() {
  const simulatedData = JSON.stringify({
    topic: "liquidation.BTCUSDT",
    type: "snapshot",
    ts: new Date().getTime(),
    data: {
      updatedTime: new Date().getTime(),
      symbol: "BTCUSDT",
      side: "Buy", // or "Sell"
      size: "5000001",
      price: "100001",
    }
  });
  handleLiquidationMessage("BTCUSDT", simulatedData);
}

async function getBinanceData(symbol) {
  const baseUrl = 'https://fapi.binance.com';

  try {
    // Get exchange info
    const exchangeInfoResponse = await axios.get(`${baseUrl}/fapi/v1/exchangeInfo`, {
      params: { symbol }
    });

    const symbolInfo = exchangeInfoResponse.data.symbols.find(s => s.symbol === symbol);
    if (!symbolInfo) {
      throw new Error(`Symbol ${symbol} not found`);
    }

    const lotSizeFilter = symbolInfo.filters.find(f => f.filterType === 'LOT_SIZE');
    const minQty = parseFloat(lotSizeFilter.minQty);

    // Get 24h ticker data
    const tickerResponse = await axios.get(`${baseUrl}/fapi/v1/ticker/24hr`, {
      params: { symbol }
    });

    const price = parseFloat(tickerResponse.data.lastPrice);
    const volume24h = parseFloat(tickerResponse.data.volume);
    const turnover24h = parseFloat(tickerResponse.data.quoteVolume);

    // Calculate minimum order size in USD
    const minOrderSizeUSD = minQty * price;

    return {
      symbol,
      minOrderSizeUSD,
      turnover24hUSD: turnover24h,
      volume24h,
      currentPrice: price
    };
  } catch (error) {
    console.error('Error:', error.message);
    throw error;
  }
}
