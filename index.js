// index.js
require("dotenv").config();

const WebSocket = require("ws");

const HELIUS_API_KEY = process.env.HELIUS_API_KEY;
const PORT = process.env.PORT || 8080;

// Pump AMM program from Helius docs
const PUMP_AMM_PROGRAM_ID = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";

// Standard Helius WSS endpoint
const WSS_URL = `wss://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;
const RPC_URL = `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;

if (!HELIUS_API_KEY) {
  console.error("Missing HELIUS_API_KEY in environment variables.");
  process.exit(1);
}

let ws = null;
let pingInterval = null;
let reconnectTimeout = null;
let retryCount = 0;

const MAX_RETRIES = Infinity;
const INITIAL_RETRY_DELAY_MS = 1000;
const MAX_RETRY_DELAY_MS = 30000;

// Prevent duplicate processing after reconnects
const seenSignatures = new Set();
const SEEN_SIGNATURE_LIMIT = 10000;

function addSeenSignature(sig) {
  seenSignatures.add(sig);
  if (seenSignatures.size > SEEN_SIGNATURE_LIMIT) {
    const first = seenSignatures.values().next().value;
    seenSignatures.delete(first);
  }
}

function backoffDelay(attempt) {
  return Math.min(INITIAL_RETRY_DELAY_MS * 2 ** attempt, MAX_RETRY_DELAY_MS);
}

async function heliusRpc(method, params) {
  const res = await fetch(RPC_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      jsonrpc: "2.0",
      id: `${method}-${Date.now()}`,
      method,
      params,
    }),
  });

  if (!res.ok) {
    throw new Error(`RPC HTTP error: ${res.status}`);
  }

  const json = await res.json();

  if (json.error) {
    throw new Error(`RPC error: ${JSON.stringify(json.error)}`);
  }

  return json.result;
}

async function fetchFullTransaction(signature) {
  return heliusRpc("getTransaction", [
    signature,
    {
      encoding: "jsonParsed",
      maxSupportedTransactionVersion: 0,
      commitment: "confirmed",
    },
  ]);
}

// TODO: replace this with your real DB insert/upsert
async function saveTradeEvent(event) {
  console.log("SAVE EVENT:", JSON.stringify(event, null, 2));
}

// Lightweight parser to classify buy/sell-ish activity.
// This is intentionally conservative for v1.
function parseTradeFromTransaction(tx, signature) {
  if (!tx || !tx.meta || !tx.transaction) return null;

  const accountKeys =
    tx.transaction?.message?.accountKeys?.map((k) =>
      typeof k === "string" ? k : k.pubkey
    ) || [];

  const logMessages = tx.meta?.logMessages || [];
  const blockTime = tx.blockTime || null;
  const slot = tx.slot || null;

  const preTokenBalances = tx.meta?.preTokenBalances || [];
  const postTokenBalances = tx.meta?.postTokenBalances || [];

  return {
    signature,
    slot,
    blockTime,
    success: tx.meta?.err == null,
    accounts: accountKeys,
    logMessages,
    preTokenBalances,
    postTokenBalances,
    raw: tx,
  };
}

async function processSignature(signature) {
  if (!signature || seenSignatures.has(signature)) return;

  addSeenSignature(signature);

  try {
    const tx = await fetchFullTransaction(signature);

    if (!tx) {
      console.log(`No transaction found yet for signature ${signature}`);
      return;
    }

    const parsed = parseTradeFromTransaction(tx, signature);
    if (!parsed) return;

    await saveTradeEvent(parsed);
    console.log(`Processed transaction ${signature}`);
  } catch (err) {
    console.error(`Failed to process signature ${signature}:`, err.message);
  }
}

function startPing() {
  stopPing();

  pingInterval = setInterval(() => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.ping();
      console.log("Ping sent");
    }
  }, 30000);
}

function stopPing() {
  if (pingInterval) {
    clearInterval(pingInterval);
    pingInterval = null;
  }
}

function subscribe() {
  const request = {
    jsonrpc: "2.0",
    id: 1,
    method: "logsSubscribe",
    params: [
      {
        mentions: [PUMP_AMM_PROGRAM_ID],
      },
      {
        commitment: "confirmed",
      },
    ],
  };

  ws.send(JSON.stringify(request));
  console.log("Sent logsSubscribe request");
}

function connect() {
  console.log(`Connecting to ${WSS_URL}`);
  ws = new WebSocket(WSS_URL);

  ws.on("open", () => {
    console.log("WebSocket opened");
    retryCount = 0;
    subscribe();
    startPing();
  });

  ws.on("message", async (data) => {
    try {
      const msg = JSON.parse(data.toString());

      // Subscription confirmation
      if (typeof msg.result === "number" && msg.id === 1) {
        console.log(`Subscribed successfully. Subscription ID: ${msg.result}`);
        return;
      }

      // Logs notification
      const value = msg?.params?.result?.value;
      if (!value) return;

      const signature = value.signature;
      const logs = value.logs || [];
      const err = value.err;

      if (err) {
        return;
      }

      // Optional extra filtering at log level before expensive RPC hydration
      const looksRelevant =
        logs.some((l) => l.toLowerCase().includes("buy")) ||
        logs.some((l) => l.toLowerCase().includes("sell")) ||
        logs.some((l) => l.toLowerCase().includes("swap")) ||
        true; // keep broad for now

      if (!looksRelevant) return;

      await processSignature(signature);
    } catch (err) {
      console.error("Failed to parse/process message:", err.message);
    }
  });

  ws.on("error", (err) => {
    console.error("WebSocket error:", err.message);
  });

  ws.on("close", () => {
    console.log("WebSocket closed");
    stopPing();
    reconnect();
  });
}

function reconnect() {
  if (reconnectTimeout) return;
  if (retryCount >= MAX_RETRIES) return;

  const delay = backoffDelay(retryCount);
  console.log(`Reconnecting in ${delay}ms...`);

  reconnectTimeout = setTimeout(() => {
    reconnectTimeout = null;
    retryCount += 1;
    connect();
  }, delay);
}

// Simple health server for Railway
const http = require("http");
http
  .createServer((req, res) => {
    if (req.url === "/health") {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(
        JSON.stringify({
          ok: true,
          websocketState: ws ? ws.readyState : null,
          retryCount,
        })
      );
      return;
    }

    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("pumpfun scanner running");
  })
  .listen(PORT, () => {
    console.log(`HTTP health server listening on port ${PORT}`);
  });

connect();

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

function shutdown() {
  console.log("Shutting down...");
  stopPing();

  if (reconnectTimeout) {
    clearTimeout(reconnectTimeout);
    reconnectTimeout = null;
  }

  if (ws) {
    try {
      ws.close();
    } catch (_) {}
  }

  process.exit(0);
}
