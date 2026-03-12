require("dotenv").config();

const http = require("http");
const WebSocket = require("ws");
const { Pool } = require("pg");

const HELIUS_API_KEY = process.env.HELIUS_API_KEY;
const DATABASE_URL = process.env.DATABASE_URL;
const PORT = Number(process.env.PORT || 8080);

const CHAIN = "solana";
const STORE_RAW_EVENTS = String(process.env.STORE_RAW_EVENTS || "false") === "true";
const RAW_RETENTION_COUNT = Number(process.env.RAW_RETENTION_COUNT || 2000);

const PUMP_AMM_PROGRAM_ID =
  process.env.PUMP_AMM_PROGRAM_ID ||
  "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";

/**
 * Ingestion throughput controls
 */
const MAX_TX_PER_SECOND = Number(process.env.MAX_TX_PER_SECOND || 40);
const MAX_QUEUE_SIZE = Number(process.env.MAX_QUEUE_SIZE || 100000);
const SIGNATURE_MAX_AGE_MS = Number(
  process.env.SIGNATURE_MAX_AGE_MS || 30 * 60 * 1000
);
const QUEUE_LOG_EVERY_MS = Number(process.env.QUEUE_LOG_EVERY_MS || 10000);
const WORKER_CONCURRENCY = Number(process.env.WORKER_CONCURRENCY || 20);

/**
 * Trade quality controls
 */
const MIN_SOL_AMOUNT = Number(process.env.MIN_SOL_AMOUNT || 0.1);

/**
 * Retry controls
 */
const RPC_RETRY_COUNT = Number(process.env.RPC_RETRY_COUNT || 3);
const RPC_RETRY_DELAY_MS = Number(process.env.RPC_RETRY_DELAY_MS || 500);

const WSS_URL = `wss://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;
const RPC_URL = `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;

if (!HELIUS_API_KEY) {
  console.error("Missing HELIUS_API_KEY");
  process.exit(1);
}

if (!DATABASE_URL) {
  console.error("Missing DATABASE_URL");
  process.exit(1);
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

let ws = null;
let pingInterval = null;
let reconnectTimeout = null;
let retryCount = 0;
let intentionalShutdown = false;
let currentSocketId = 0;
let last429At = null;

/**
 * Signature dedupe + queue
 */
const seenSignatures = new Set();
const SEEN_SIGNATURE_LIMIT = 100000;

const queuedSignatures = new Set();
const signatureQueue = [];

let queueLogTimer = null;
let workerRunning = false;
const workerPromises = [];

/**
 * Stats
 */
const stats = {
  queued: 0,
  dequeued: 0,
  processed: 0,
  insertedTrades: 0,
  droppedQueueFull: 0,
  droppedStale: 0,
  droppedDuplicate: 0,
  skippedParsed: 0,
  txFetchErrors: 0,
  workerErrors: 0,
  rpcRetries: 0,
  emptyTx: 0,
  belowMinSol: 0,
};

function logInfo(message, extra = {}) {
  const payload = Object.keys(extra).length ? ` ${JSON.stringify(extra)}` : "";
  console.log(`[pump-ws] ${message}${payload}`);
}

function logError(message, extra = {}) {
  const payload = Object.keys(extra).length ? ` ${JSON.stringify(extra)}` : "";
  console.error(`[pump-ws] ${message}${payload}`);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function addSeenSignature(sig) {
  seenSignatures.add(sig);
  if (seenSignatures.size > SEEN_SIGNATURE_LIMIT) {
    const first = seenSignatures.values().next().value;
    seenSignatures.delete(first);
  }
}

function alreadySeen(sig) {
  return seenSignatures.has(sig);
}

function backoffDelay(attempt, wasRateLimited = false) {
  if (wasRateLimited) {
    return Math.min(60000 * 2 ** Math.min(attempt, 4), 600000);
  }
  return Math.min(2000 * 2 ** Math.min(attempt, 5), 60000);
}

async function heliusRpc(method, params) {
  const res = await fetch(RPC_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      jsonrpc: "2.0",
      id: `${method}-${Date.now()}`,
      method,
      params,
    }),
  });

  if (!res.ok) {
    throw new Error(`RPC HTTP error ${res.status}`);
  }

  const json = await res.json();

  if (json.error) {
    throw new Error(`RPC error: ${JSON.stringify(json.error)}`);
  }

  return json.result;
}

async function fetchFullTransaction(signature) {
  let lastErr = null;

  for (let attempt = 0; attempt <= RPC_RETRY_COUNT; attempt += 1) {
    try {
      return await heliusRpc("getTransaction", [
        signature,
        {
          encoding: "jsonParsed",
          maxSupportedTransactionVersion: 0,
          commitment: "confirmed",
        },
      ]);
    } catch (err) {
      lastErr = err;

      if (attempt < RPC_RETRY_COUNT) {
        stats.rpcRetries += 1;
        await sleep(RPC_RETRY_DELAY_MS * (attempt + 1));
      }
    }
  }

  throw lastErr;
}

function getLogMessages(tx) {
  return tx?.meta?.logMessages || [];
}

function getAccountKeys(tx) {
  return (
    tx?.transaction?.message?.accountKeys?.map((k) =>
      typeof k === "string" ? k : k.pubkey
    ) || []
  );
}

function txTouchesPumpAmm(tx) {
  const logs = getLogMessages(tx);
  if (logs.some((l) => l.includes(PUMP_AMM_PROGRAM_ID))) return true;

  const keys = getAccountKeys(tx);
  if (keys.includes(PUMP_AMM_PROGRAM_ID)) return true;

  const instructions = tx?.transaction?.message?.instructions || [];
  for (const ix of instructions) {
    if (ix.programId === PUMP_AMM_PROGRAM_ID) return true;
  }

  const inner = tx?.meta?.innerInstructions || [];
  for (const group of inner) {
    for (const ix of group.instructions || []) {
      if (ix.programId === PUMP_AMM_PROGRAM_ID) return true;
    }
  }

  return false;
}

function getSideFromLogs(tx) {
  const logs = getLogMessages(tx);

  if (logs.some((l) => l.includes("Instruction: Buy"))) return "buy";
  if (logs.some((l) => l.includes("Instruction: Sell"))) return "sell";

  return null;
}

function getSignerWallet(tx) {
  const keys = tx?.transaction?.message?.accountKeys || [];
  for (const key of keys) {
    if (typeof key === "string") continue;
    if (key.signer) return key.pubkey;
  }
  return null;
}

function getWalletSolDelta(tx, walletAddress) {
  const keys = tx?.transaction?.message?.accountKeys || [];
  const preBalances = tx?.meta?.preBalances || [];
  const postBalances = tx?.meta?.postBalances || [];

  for (let i = 0; i < keys.length; i += 1) {
    const pubkey = typeof keys[i] === "string" ? keys[i] : keys[i].pubkey;
    if (pubkey !== walletAddress) continue;

    const pre = preBalances[i] || 0;
    const post = postBalances[i] || 0;
    return (post - pre) / 1e9;
  }

  return 0;
}

function parseTokenBalances(arr) {
  return (arr || []).map((row) => {
    const owner = row.owner || null;
    const mint = row.mint || null;
    const accountIndex = row.accountIndex;
    const decimals = row.uiTokenAmount?.decimals ?? 0;
    const rawAmount = row.uiTokenAmount?.amount || "0";
    const amount = Number(rawAmount) / 10 ** decimals;

    return {
      owner,
      mint,
      accountIndex,
      decimals,
      amount,
    };
  });
}

function buildTokenMap(rows) {
  const map = new Map();
  for (const row of rows) {
    const key = `${row.owner}::${row.mint}::${row.accountIndex}`;
    map.set(key, row);
  }
  return map;
}

function findWalletTokenDelta(tx, walletAddress) {
  const preRows = parseTokenBalances(tx?.meta?.preTokenBalances || []);
  const postRows = parseTokenBalances(tx?.meta?.postTokenBalances || []);

  const preMap = buildTokenMap(preRows);
  const postMap = buildTokenMap(postRows);

  const keys = new Set([...preMap.keys(), ...postMap.keys()]);
  let best = null;

  for (const key of keys) {
    const pre = preMap.get(key);
    const post = postMap.get(key);

    const owner = post?.owner || pre?.owner || null;
    const mint = post?.mint || pre?.mint || null;
    const preAmt = pre?.amount || 0;
    const postAmt = post?.amount || 0;
    const delta = postAmt - preAmt;

    if (!owner || !mint) continue;
    if (owner !== walletAddress) continue;
    if (Math.abs(delta) === 0) continue;

    if (!best || Math.abs(delta) > Math.abs(best.delta)) {
      best = { mint, delta };
    }
  }

  return best;
}

function getFeeSol(tx) {
  return (tx?.meta?.fee || 0) / 1e9;
}

function getTs(tx) {
  return tx?.blockTime ? new Date(tx.blockTime * 1000) : new Date();
}

function parsePumpTrade(tx, signature) {
  if (!tx || !tx.meta || !tx.transaction) {
    return { ok: false, reason: "missing tx fields" };
  }

  if (tx.meta.err) {
    return { ok: false, reason: "tx failed" };
  }

  if (!txTouchesPumpAmm(tx)) {
    return { ok: false, reason: "not pump amm" };
  }

  const sideFromLogs = getSideFromLogs(tx);
  if (!sideFromLogs) {
    return { ok: false, reason: "no buy/sell log" };
  }

  const walletAddress = getSignerWallet(tx);
  if (!walletAddress) {
    return { ok: false, reason: "no signer wallet" };
  }

  const tokenDelta = findWalletTokenDelta(tx, walletAddress);
  if (!tokenDelta) {
    return { ok: false, reason: "no wallet token delta" };
  }

  const walletSolDelta = getWalletSolDelta(tx, walletAddress);
  const solAmount = Math.abs(walletSolDelta);
  const tokenAmount = Math.abs(tokenDelta.delta);

  if (solAmount <= 0) {
    return { ok: false, reason: "zero sol amount" };
  }

  if (solAmount < MIN_SOL_AMOUNT) {
    stats.belowMinSol += 1;
    return { ok: false, reason: "below min sol threshold" };
  }

  if (tokenAmount <= 0) {
    return { ok: false, reason: "zero token amount" };
  }

  const inferredSide =
    tokenDelta.delta > 0 && walletSolDelta < 0
      ? "buy"
      : tokenDelta.delta < 0 && walletSolDelta > 0
      ? "sell"
      : null;

  if (!inferredSide) {
    return { ok: false, reason: "could not infer side" };
  }

  if (sideFromLogs !== inferredSide) {
    return {
      ok: false,
      reason: `log side ${sideFromLogs} != inferred side ${inferredSide}`,
    };
  }

  return {
    ok: true,
    trade: {
      wallet_address: walletAddress,
      token_id: tokenDelta.mint,
      side: inferredSide,
      sol_amount: solAmount,
      token_amount: tokenAmount,
      fee_sol: getFeeSol(tx),
      bonding_progress: 0,
      sol_in_curve: 0,
      ts: getTs(tx),
      signature,
      slot: tx.slot || null,
      raw: STORE_RAW_EVENTS ? tx : null,
    },
  };
}

async function createTables() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS raw_webhook_events (
      id BIGSERIAL PRIMARY KEY,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      signature TEXT,
      slot BIGINT,
      timestamp BIGINT,
      type TEXT,
      payload JSONB
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS wallet_trades (
      id BIGSERIAL PRIMARY KEY,
      wallet_address TEXT NOT NULL,
      token_id TEXT NOT NULL,
      side TEXT NOT NULL,
      sol_amount NUMERIC NOT NULL,
      token_amount NUMERIC NOT NULL,
      fee_sol NUMERIC NOT NULL DEFAULT 0,
      bonding_progress NUMERIC NOT NULL DEFAULT 0,
      sol_in_curve NUMERIC NOT NULL DEFAULT 0,
      ts TIMESTAMPTZ NOT NULL,
      signature TEXT NOT NULL,
      slot BIGINT,
      raw JSONB,
      chain TEXT NOT NULL DEFAULT 'solana'
    );
  `);

  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS raw_webhook_events_signature_idx
    ON raw_webhook_events (signature);
  `);

  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS wallet_trades_signature_wallet_side_idx
    ON wallet_trades (signature, wallet_address, side);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS wallet_trades_ts_idx
    ON wallet_trades (ts);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS wallet_trades_token_ts_idx
    ON wallet_trades (token_id, ts);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS wallet_trades_token_side_ts_idx
    ON wallet_trades (token_id, side, ts);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS wallet_trades_wallet_idx
    ON wallet_trades (wallet_address);
  `);
}

async function insertRawWebhookEvent({ signature, slot, timestamp, type, payload }) {
  if (!STORE_RAW_EVENTS) return;

  await pool.query(
    `
    INSERT INTO raw_webhook_events (signature, slot, timestamp, type, payload)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (signature) DO NOTHING
    `,
    [signature, slot, timestamp, type, payload]
  );
}

async function trimRawEvents() {
  if (!STORE_RAW_EVENTS) return;

  await pool.query(
    `
    DELETE FROM raw_webhook_events
    WHERE id IN (
      SELECT id
      FROM raw_webhook_events
      ORDER BY created_at DESC
      OFFSET $1
    )
    `,
    [RAW_RETENTION_COUNT]
  );
}

async function insertWalletTrade(trade) {
  const result = await pool.query(
    `
    INSERT INTO wallet_trades (
      wallet_address,
      token_id,
      side,
      sol_amount,
      token_amount,
      fee_sol,
      bonding_progress,
      sol_in_curve,
      ts,
      signature,
      slot,
      raw,
      chain
    )
    VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13
    )
    ON CONFLICT (signature, wallet_address, side) DO NOTHING
    RETURNING id
    `,
    [
      trade.wallet_address,
      trade.token_id,
      trade.side,
      trade.sol_amount,
      trade.token_amount,
      trade.fee_sol,
      trade.bonding_progress,
      trade.sol_in_curve,
      trade.ts,
      trade.signature,
      trade.slot,
      trade.raw,
      CHAIN,
    ]
  );

  if (result.rowCount > 0) {
    stats.insertedTrades += 1;
  }
}

function enqueueSignature(signature, slot = null, blockTime = null) {
  if (!signature) return;

  if (alreadySeen(signature) || queuedSignatures.has(signature)) {
    stats.droppedDuplicate += 1;
    return;
  }

  if (signatureQueue.length >= MAX_QUEUE_SIZE) {
    stats.droppedQueueFull += 1;
    return;
  }

  queuedSignatures.add(signature);
  signatureQueue.push({
    signature,
    slot,
    blockTime,
    enqueuedAt: Date.now(),
  });

  stats.queued += 1;
}

async function processQueuedSignature(item) {
  const { signature, slot, blockTime, enqueuedAt } = item;

  queuedSignatures.delete(signature);
  stats.dequeued += 1;

  if (!signature || alreadySeen(signature)) {
    return;
  }

  if (Date.now() - enqueuedAt > SIGNATURE_MAX_AGE_MS) {
    stats.droppedStale += 1;
    return;
  }

  addSeenSignature(signature);

  try {
    const tx = await fetchFullTransaction(signature);
    if (!tx) {
      stats.emptyTx += 1;
      return;
    }

    await insertRawWebhookEvent({
      signature,
      slot: tx.slot || slot,
      timestamp: tx.blockTime || blockTime,
      type: "helius_ws_tx",
      payload: tx,
    });

    const parsed = parsePumpTrade(tx, signature);
    if (!parsed.ok) {
      stats.skippedParsed += 1;
      return;
    }

    await insertWalletTrade(parsed.trade);
    stats.processed += 1;
  } catch (err) {
    stats.txFetchErrors += 1;
    logError("Failed processing signature", {
      signature,
      error: err.message,
    });
  }
}

async function queueWorkerLoop(workerId) {
  const minDelayMs = Math.max(
    Math.floor((1000 / MAX_TX_PER_SECOND) * WORKER_CONCURRENCY),
    10
  );

  while (workerRunning) {
    const item = signatureQueue.shift();

    if (!item) {
      await sleep(100);
      continue;
    }

    try {
      await processQueuedSignature(item);
    } catch (err) {
      stats.workerErrors += 1;
      logError("Queue worker error", {
        workerId,
        error: err.message,
      });
    }

    await sleep(minDelayMs);
  }
}

function startQueueWorker() {
  if (workerRunning) return;

  workerRunning = true;

  for (let i = 0; i < WORKER_CONCURRENCY; i += 1) {
    const workerId = i + 1;
    const promise = queueWorkerLoop(workerId).catch((err) => {
      stats.workerErrors += 1;
      logError("Worker loop crashed", {
        workerId,
        error: err.message,
      });
    });
    workerPromises.push(promise);
  }

  logInfo("Queue workers started", {
    workerConcurrency: WORKER_CONCURRENCY,
    maxTxPerSecond: MAX_TX_PER_SECOND,
    maxQueueSize: MAX_QUEUE_SIZE,
    signatureMaxAgeMs: SIGNATURE_MAX_AGE_MS,
    minSolAmount: MIN_SOL_AMOUNT,
  });
}

function stopQueueWorker() {
  workerRunning = false;
}

function startQueueLogger() {
  if (queueLogTimer) return;

  queueLogTimer = setInterval(() => {
    const now = Date.now();
    const oldestAgeMs = signatureQueue.length
      ? now - signatureQueue[0].enqueuedAt
      : 0;

    logInfo("Queue stats", {
      queueSize: signatureQueue.length,
      oldestAgeMs,
      queued: stats.queued,
      dequeued: stats.dequeued,
      processed: stats.processed,
      insertedTrades: stats.insertedTrades,
      droppedQueueFull: stats.droppedQueueFull,
      droppedStale: stats.droppedStale,
      droppedDuplicate: stats.droppedDuplicate,
      skippedParsed: stats.skippedParsed,
      belowMinSol: stats.belowMinSol,
      txFetchErrors: stats.txFetchErrors,
      workerErrors: stats.workerErrors,
      emptyTx: stats.emptyTx,
      rpcRetries: stats.rpcRetries,
    });
  }, QUEUE_LOG_EVERY_MS);
}

function stopQueueLogger() {
  if (queueLogTimer) {
    clearInterval(queueLogTimer);
    queueLogTimer = null;
  }
}

function subscribe(socket) {
  const request = {
    jsonrpc: "2.0",
    id: 1,
    method: "logsSubscribe",
    params: [
      { mentions: [PUMP_AMM_PROGRAM_ID] },
      { commitment: "confirmed" },
    ],
  };

  socket.send(JSON.stringify(request));
  logInfo("Sent logsSubscribe");
}

function startPing(socketId) {
  stopPing();
  pingInterval = setInterval(() => {
    if (ws && ws.readyState === WebSocket.OPEN && socketId === currentSocketId) {
      try {
        ws.ping();
      } catch (err) {
        logError("Ping failed", { error: err.message });
      }
    }
  }, 30000);
}

function stopPing() {
  if (pingInterval) {
    clearInterval(pingInterval);
    pingInterval = null;
  }
}

function scheduleReconnect(reason = "unknown", wasRateLimited = false) {
  if (intentionalShutdown) return;
  if (reconnectTimeout) return;

  const delay = backoffDelay(retryCount, wasRateLimited);

  logInfo("Scheduling reconnect", {
    reason,
    retryCount,
    delayMs: delay,
    wasRateLimited,
  });

  reconnectTimeout = setTimeout(() => {
    reconnectTimeout = null;
    retryCount += 1;
    connect();
  }, delay);
}

function cleanupSocket(socket) {
  try {
    socket.removeAllListeners();
  } catch (_) {}

  try {
    if (
      socket.readyState === WebSocket.OPEN ||
      socket.readyState === WebSocket.CONNECTING
    ) {
      socket.terminate();
    }
  } catch (_) {}
}

function connect() {
  if (intentionalShutdown) return;

  currentSocketId += 1;
  const socketId = currentSocketId;
  const socket = new WebSocket(WSS_URL);
  ws = socket;

  logInfo("Connecting websocket", {
    socketId,
    url: "wss://mainnet.helius-rpc.com/?api-key=***",
  });

  socket.on("open", () => {
    if (socketId !== currentSocketId) {
      cleanupSocket(socket);
      return;
    }

    retryCount = 0;
    last429At = null;
    logInfo("WebSocket opened", { socketId });
    subscribe(socket);
    startPing(socketId);
  });

  socket.on("message", (data) => {
    if (socketId !== currentSocketId) return;

    try {
      const msg = JSON.parse(data.toString());

      if (typeof msg.result === "number" && msg.id === 1) {
        logInfo("Subscribed successfully", {
          socketId,
          subscriptionId: msg.result,
        });
        return;
      }

      const value = msg?.params?.result?.value;
      if (!value) return;
      if (value.err) return;

      enqueueSignature(
        value.signature,
        msg?.params?.result?.context?.slot || null,
        value?.blockTime || null
      );
    } catch (err) {
      logError("WS message parse error", {
        socketId,
        error: err.message,
      });
    }
  });

  socket.on("error", (err) => {
    if (socketId !== currentSocketId) return;

    const message = err?.message || "unknown websocket error";
    const wasRateLimited = message.includes("429");

    if (wasRateLimited) {
      last429At = new Date().toISOString();
    }

    logError("WebSocket error", {
      socketId,
      error: message,
      wasRateLimited,
    });
  });

  socket.on("close", (code, reasonBuffer) => {
    if (socketId !== currentSocketId) return;

    stopPing();

    const reason =
      reasonBuffer && reasonBuffer.length
        ? reasonBuffer.toString()
        : "no reason";

    const wasRateLimited = reason.includes("429") || Boolean(last429At);

    logInfo("WebSocket closed", {
      socketId,
      code,
      reason,
      wasRateLimited,
    });

    cleanupSocket(socket);
    scheduleReconnect("socket_closed", wasRateLimited);
  });
}

http
  .createServer(async (req, res) => {
    if (req.url === "/health") {
      try {
        const db = await pool.query("SELECT now()");
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(
          JSON.stringify({
            ok: true,
            websocketState: ws ? ws.readyState : null,
            retryCount,
            last429At,
            dbTime: db.rows[0].now,
            queueSize: signatureQueue.length,
            workerRunning,
            stats,
          })
        );
      } catch (err) {
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: false, error: err.message }));
      }
      return;
    }

    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("pumpfun scanner running");
  })
  .listen(PORT, () => {
    logInfo("HTTP server listening", { port: PORT });
  });

async function boot() {
  try {
    const test = await pool.query("SELECT now()");
    logInfo("DB connected", { dbTime: test.rows[0].now });

    await createTables();
    logInfo("Tables ready");

    if (STORE_RAW_EVENTS) {
      setInterval(() => {
        trimRawEvents().catch((err) =>
          logError("Raw trim error", { error: err.message })
        );
      }, 5 * 60 * 1000);
    }

    startQueueWorker();
    startQueueLogger();
    connect();
  } catch (err) {
    logError("Boot failed", { error: err.message });
    process.exit(1);
  }
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

async function shutdown() {
  intentionalShutdown = true;
  logInfo("Shutting down");

  stopPing();
  stopQueueWorker();
  stopQueueLogger();

  if (reconnectTimeout) {
    clearTimeout(reconnectTimeout);
    reconnectTimeout = null;
  }

  if (ws) {
    cleanupSocket(ws);
    ws = null;
  }

  try {
    await Promise.allSettled(workerPromises);
  } catch (_) {}

  try {
    await pool.end();
  } catch (_) {}

  process.exit(0);
}

boot();
