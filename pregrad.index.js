require("dotenv").config();

const http = require("http");
const WebSocket = require("ws");
const { Pool } = require("pg");

const HELIUS_API_KEY = process.env.HELIUS_API_KEY;
const DATABASE_URL = process.env.DATABASE_URL;
const PORT = Number(process.env.PORT || 8081);

if (!HELIUS_API_KEY) {
  console.error("Missing HELIUS_API_KEY");
  process.exit(1);
}

if (!DATABASE_URL) {
  console.error("Missing DATABASE_URL");
  process.exit(1);
}

// IMPORTANT:
// Set this in Railway env vars after confirming the correct Pump launchpad program.
// This should NOT be the Pump AMM / Pump Swap program your current worker uses.
const PUMP_LAUNCHPAD_PROGRAM_ID =
  process.env.PUMP_LAUNCHPAD_PROGRAM_ID || "REPLACE_WITH_PREGRAD_PROGRAM_ID";

const STORE_RAW_EVENTS =
  String(process.env.STORE_RAW_EVENTS || "true") === "true";

const RAW_RETENTION_COUNT = Number(process.env.RAW_RETENTION_COUNT || 5000);
const MAX_QUEUE_SIZE = Number(process.env.MAX_QUEUE_SIZE || 100000);
const WORKER_CONCURRENCY = Number(process.env.WORKER_CONCURRENCY || 12);
const MAX_TX_PER_SECOND = Number(process.env.MAX_TX_PER_SECOND || 25);
const SIGNATURE_MAX_AGE_MS = Number(
  process.env.SIGNATURE_MAX_AGE_MS || 2 * 60 * 1000
);
const QUEUE_LOG_EVERY_MS = Number(process.env.QUEUE_LOG_EVERY_MS || 10000);

const RPC_RETRY_COUNT = Number(process.env.RPC_RETRY_COUNT || 3);
const RPC_RETRY_DELAY_MS = Number(process.env.RPC_RETRY_DELAY_MS || 500);

const WSS_URL = `wss://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;
const RPC_URL = `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;

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

const seenSignatures = new Set();
const SEEN_SIGNATURE_LIMIT = 100000;

const queuedSignatures = new Set();
const signatureQueue = [];

let workerRunning = false;
const workerPromises = [];
let queueLogTimer = null;

const stats = {
  queued: 0,
  dequeued: 0,
  processed: 0,
  insertedEvents: 0,
  insertedTokens: 0,
  droppedQueueFull: 0,
  droppedDuplicate: 0,
  droppedStale: 0,
  skippedIrrelevantLog: 0,
  skippedEmptyTx: 0,
  txFetchErrors: 0,
  workerErrors: 0,
  rpcRetries: 0,
  classifiedCreate: 0,
  classifiedBuy: 0,
  classifiedSell: 0,
  classifiedMigrate: 0,
  classifiedUnknown: 0,
};

function logInfo(message, extra = {}) {
  const payload = Object.keys(extra).length ? ` ${JSON.stringify(extra)}` : "";
  console.log(`[pregrad-ws] ${message}${payload}`);
}

function logError(message, extra = {}) {
  const payload = Object.keys(extra).length ? ` ${JSON.stringify(extra)}` : "";
  console.error(`[pregrad-ws] ${message}${payload}`);
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

function getInstructions(tx) {
  return tx?.transaction?.message?.instructions || [];
}

function getInnerInstructions(tx) {
  return tx?.meta?.innerInstructions || [];
}

function getTs(tx) {
  return tx?.blockTime ? new Date(tx.blockTime * 1000) : new Date();
}

function getSignerWallet(tx) {
  const keys = tx?.transaction?.message?.accountKeys || [];
  for (const key of keys) {
    if (typeof key === "string") continue;
    if (key.signer) return key.pubkey;
  }
  return null;
}

function getMintCandidatesFromTokenBalances(tx) {
  const out = new Set();

  for (const row of tx?.meta?.preTokenBalances || []) {
    if (row?.mint) out.add(row.mint);
  }

  for (const row of tx?.meta?.postTokenBalances || []) {
    if (row?.mint) out.add(row.mint);
  }

  return [...out];
}

function findInstructionProgramIds(tx) {
  const ids = new Set();

  for (const ix of getInstructions(tx)) {
    const pid =
      typeof ix.programId === "string" ? ix.programId : ix.programId?.toString?.();
    if (pid) ids.add(pid);
  }

  for (const group of getInnerInstructions(tx)) {
    for (const ix of group.instructions || []) {
      const pid =
        typeof ix.programId === "string" ? ix.programId : ix.programId?.toString?.();
      if (pid) ids.add(pid);
    }
  }

  return [...ids];
}

function txTouchesLaunchpadProgram(tx) {
  const logs = getLogMessages(tx);
  if (logs.some((l) => l.includes(PUMP_LAUNCHPAD_PROGRAM_ID))) return true;

  const keys = getAccountKeys(tx);
  if (keys.includes(PUMP_LAUNCHPAD_PROGRAM_ID)) return true;

  const programIds = findInstructionProgramIds(tx);
  return programIds.includes(PUMP_LAUNCHPAD_PROGRAM_ID);
}

function isFreshEnough(blockTime) {
  if (!blockTime) return true;
  const eventAgeMs = Date.now() - blockTime * 1000;
  return eventAgeMs <= 60 * 1000;
}

function looksRelevantFromLogs(value) {
  const logs = value?.logs || [];
  if (!Array.isArray(logs) || !logs.length) return false;

  return logs.some(
    (l) =>
      l.includes(PUMP_LAUNCHPAD_PROGRAM_ID) ||
      l.toLowerCase().includes("create") ||
      l.toLowerCase().includes("buy") ||
      l.toLowerCase().includes("sell") ||
      l.toLowerCase().includes("migrate") ||
      l.toLowerCase().includes("graduate")
  );
}

function inferEventTypeFromLogs(tx) {
  const logs = getLogMessages(tx).map((l) => String(l).toLowerCase());

  const hasCreateV2 = logs.some((l) => l.includes("create_v2"));
  const hasCreate = logs.some(
    (l) => l.includes("instruction: create") || l.includes(" create")
  );
  const hasBuy = logs.some((l) => l.includes("instruction: buy"));
  const hasSell = logs.some((l) => l.includes("instruction: sell"));
  const hasMigrate = logs.some(
    (l) => l.includes("migrate") || l.includes("graduate")
  );

  if (hasCreateV2 || hasCreate) return "create";
  if (hasMigrate) return "migrate";
  if (hasBuy && !hasSell) return "buy";
  if (hasSell && !hasBuy) return "sell";
  return "unknown";
}

// Very lightweight mint selection heuristic for v1.
// Honest note: this may need refinement once you inspect raw txs.
function inferPrimaryMint(tx, eventType) {
  const mintCandidates = getMintCandidatesFromTokenBalances(tx);
  if (mintCandidates.length === 1) return mintCandidates[0];
  if (mintCandidates.length > 1) return mintCandidates[0];

  // For create txs, sometimes token balances may not yet make mint obvious.
  // Fallback: look through parsed instructions for mint-like fields.
  for (const ix of getInstructions(tx)) {
    if (ix?.parsed?.info?.mint) return ix.parsed.info.mint;
    if (ix?.parsed?.info?.tokenMint) return ix.parsed.info.tokenMint;
  }

  for (const group of getInnerInstructions(tx)) {
    for (const ix of group.instructions || []) {
      if (ix?.parsed?.info?.mint) return ix.parsed.info.mint;
      if (ix?.parsed?.info?.tokenMint) return ix.parsed.info.tokenMint;
    }
  }

  return null;
}

function parseCreateMetadata(tx) {
  let name = null;
  let symbol = null;

  const instructions = getInstructions(tx);

  for (const ix of instructions) {
    const info = ix?.parsed?.info || {};
    if (!name && typeof info.name === "string") name = info.name;
    if (!symbol && typeof info.symbol === "string") symbol = info.symbol;
  }

  return { name, symbol };
}

function classifyPregradEvent(tx, signature) {
  if (!tx || !tx.meta || !tx.transaction) {
    return { ok: false, reason: "missing tx fields" };
  }

  if (tx.meta.err) {
    return { ok: false, reason: "tx failed" };
  }

  if (!txTouchesLaunchpadProgram(tx)) {
    return { ok: false, reason: "not launchpad program" };
  }

  const eventType = inferEventTypeFromLogs(tx);
  const tokenAddress = inferPrimaryMint(tx, eventType);
  const walletAddress = getSignerWallet(tx);
  const { name, symbol } = parseCreateMetadata(tx);

  const event = {
    token_address: tokenAddress,
    signature,
    slot: tx.slot || null,
    block_time: getTs(tx),
    event_type: eventType,
    wallet_address: walletAddress,
    raw_json: tx,
  };

  const tokenUpsert =
    eventType === "create" && tokenAddress
      ? {
          token_address: tokenAddress,
          creator_wallet: walletAddress,
          symbol: symbol,
          name: name,
          token_program: null,
          created_at: getTs(tx),
          first_seen_signature: signature,
          first_seen_slot: tx.slot || null,
          graduation_status: "pre_grad",
          market_phase: "PRE_GRAD",
        }
      : null;

  return {
    ok: true,
    event,
    tokenUpsert,
  };
}

async function createTables() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS pump_launchpad_tokens (
      token_address TEXT PRIMARY KEY,
      creator_wallet TEXT,
      symbol TEXT,
      name TEXT,
      token_program TEXT,
      created_at TIMESTAMPTZ,
      first_seen_signature TEXT,
      first_seen_slot BIGINT,
      graduation_status TEXT NOT NULL DEFAULT 'pre_grad',
      graduated_at TIMESTAMPTZ,
      market_phase TEXT NOT NULL DEFAULT 'PRE_GRAD',
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS pump_launchpad_events (
      id BIGSERIAL PRIMARY KEY,
      token_address TEXT,
      signature TEXT NOT NULL UNIQUE,
      slot BIGINT,
      block_time TIMESTAMPTZ,
      event_type TEXT NOT NULL,
      wallet_address TEXT,
      raw_json JSONB,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS pump_launchpad_events_token_time_idx
    ON pump_launchpad_events (token_address, block_time DESC);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS pump_launchpad_events_type_time_idx
    ON pump_launchpad_events (event_type, block_time DESC);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS pump_launchpad_tokens_status_idx
    ON pump_launchpad_tokens (graduation_status, created_at DESC);
  `);

  if (STORE_RAW_EVENTS) {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS raw_pregrad_events (
        id BIGSERIAL PRIMARY KEY,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        signature TEXT UNIQUE,
        slot BIGINT,
        timestamp BIGINT,
        type TEXT,
        payload JSONB
      );
    `);
  }
}

async function insertRawPregradEvent({ signature, slot, timestamp, type, payload }) {
  if (!STORE_RAW_EVENTS) return;

  await pool.query(
    `
    INSERT INTO raw_pregrad_events (signature, slot, timestamp, type, payload)
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
    DELETE FROM raw_pregrad_events
    WHERE id IN (
      SELECT id
      FROM raw_pregrad_events
      ORDER BY created_at DESC
      OFFSET $1
    )
    `,
    [RAW_RETENTION_COUNT]
  );
}

async function upsertLaunchpadToken(token) {
  if (!token?.token_address) return false;

  const result = await pool.query(
    `
    INSERT INTO pump_launchpad_tokens (
      token_address,
      creator_wallet,
      symbol,
      name,
      token_program,
      created_at,
      first_seen_signature,
      first_seen_slot,
      graduation_status,
      market_phase,
      updated_at
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW())
    ON CONFLICT (token_address)
    DO UPDATE SET
      creator_wallet = COALESCE(pump_launchpad_tokens.creator_wallet, EXCLUDED.creator_wallet),
      symbol = COALESCE(pump_launchpad_tokens.symbol, EXCLUDED.symbol),
      name = COALESCE(pump_launchpad_tokens.name, EXCLUDED.name),
      token_program = COALESCE(pump_launchpad_tokens.token_program, EXCLUDED.token_program),
      created_at = COALESCE(pump_launchpad_tokens.created_at, EXCLUDED.created_at),
      first_seen_signature = COALESCE(pump_launchpad_tokens.first_seen_signature, EXCLUDED.first_seen_signature),
      first_seen_slot = COALESCE(pump_launchpad_tokens.first_seen_slot, EXCLUDED.first_seen_slot),
      updated_at = NOW()
    RETURNING token_address
    `,
    [
      token.token_address,
      token.creator_wallet,
      token.symbol,
      token.name,
      token.token_program,
      token.created_at,
      token.first_seen_signature,
      token.first_seen_slot,
      token.graduation_status || "pre_grad",
      token.market_phase || "PRE_GRAD",
    ]
  );

  if (result.rowCount > 0) {
    stats.insertedTokens += 1;
    return true;
  }

  return false;
}

async function insertLaunchpadEvent(event) {
  const result = await pool.query(
    `
    INSERT INTO pump_launchpad_events (
      token_address,
      signature,
      slot,
      block_time,
      event_type,
      wallet_address,
      raw_json
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7)
    ON CONFLICT (signature) DO NOTHING
    RETURNING id
    `,
    [
      event.token_address,
      event.signature,
      event.slot,
      event.block_time,
      event.event_type,
      event.wallet_address,
      STORE_RAW_EVENTS ? event.raw_json : null,
    ]
  );

  if (result.rowCount > 0) {
    stats.insertedEvents += 1;
    return true;
  }

  return false;
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

  if (!signature || alreadySeen(signature)) return;

  if (Date.now() - enqueuedAt > SIGNATURE_MAX_AGE_MS) {
    stats.droppedStale += 1;
    return;
  }

  addSeenSignature(signature);

  try {
    const tx = await fetchFullTransaction(signature);
    if (!tx) {
      stats.skippedEmptyTx += 1;
      return;
    }

    await insertRawPregradEvent({
      signature,
      slot: tx.slot || slot,
      timestamp: tx.blockTime || blockTime,
      type: "helius_ws_pregrad_tx",
      payload: tx,
    });

    const classified = classifyPregradEvent(tx, signature);
    if (!classified.ok) return;

    if (classified.tokenUpsert) {
      await upsertLaunchpadToken(classified.tokenUpsert);
    }

    const inserted = await insertLaunchpadEvent(classified.event);
    if (!inserted) return;

    stats.processed += 1;

    switch (classified.event.event_type) {
      case "create":
        stats.classifiedCreate += 1;
        break;
      case "buy":
        stats.classifiedBuy += 1;
        break;
      case "sell":
        stats.classifiedSell += 1;
        break;
      case "migrate":
        stats.classifiedMigrate += 1;
        if (classified.event.token_address) {
          await pool.query(
            `
            UPDATE pump_launchpad_tokens
            SET graduation_status = 'graduated',
                market_phase = 'JUST_GRADUATED',
                graduated_at = COALESCE(graduated_at, $2),
                updated_at = NOW()
            WHERE token_address = $1
            `,
            [classified.event.token_address, classified.event.block_time]
          );
        }
        break;
      default:
        stats.classifiedUnknown += 1;
        break;
    }
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
    15
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
  });
}

function stopQueueWorker() {
  workerRunning = false;
}

let lastQueued = 0;
let lastDequeued = 0;
let lastInserted = 0;
let lastLogTime = Date.now();

function startQueueLogger() {
  if (queueLogTimer) return;

  queueLogTimer = setInterval(() => {
    const now = Date.now();
    const seconds = (now - lastLogTime) / 1000;

    const incomingRate = (stats.queued - lastQueued) / seconds;
    const drainRate = (stats.dequeued - lastDequeued) / seconds;
    const insertRate = (stats.insertedEvents - lastInserted) / seconds;

    const oldestAgeMs = signatureQueue.length
      ? now - signatureQueue[0].enqueuedAt
      : 0;

    logInfo("Queue stats", {
      queueSize: signatureQueue.length,
      oldestAgeMs,
      queued: stats.queued,
      dequeued: stats.dequeued,
      processed: stats.processed,
      insertedEvents: stats.insertedEvents,
      insertedTokens: stats.insertedTokens,
      incomingPerSec: incomingRate.toFixed(2),
      drainedPerSec: drainRate.toFixed(2),
      insertedPerSec: insertRate.toFixed(2),
      droppedQueueFull: stats.droppedQueueFull,
      droppedDuplicate: stats.droppedDuplicate,
      droppedStale: stats.droppedStale,
      skippedIrrelevantLog: stats.skippedIrrelevantLog,
      skippedEmptyTx: stats.skippedEmptyTx,
      txFetchErrors: stats.txFetchErrors,
      workerErrors: stats.workerErrors,
      rpcRetries: stats.rpcRetries,
      classifiedCreate: stats.classifiedCreate,
      classifiedBuy: stats.classifiedBuy,
      classifiedSell: stats.classifiedSell,
      classifiedMigrate: stats.classifiedMigrate,
      classifiedUnknown: stats.classifiedUnknown,
    });

    lastQueued = stats.queued;
    lastDequeued = stats.dequeued;
    lastInserted = stats.insertedEvents;
    lastLogTime = now;
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
      { mentions: [PUMP_LAUNCHPAD_PROGRAM_ID] },
      { commitment: "confirmed" },
    ],
  };

  socket.send(JSON.stringify(request));
  logInfo("Sent logsSubscribe", {
    programId: PUMP_LAUNCHPAD_PROGRAM_ID,
  });
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

function stopPing() {
  if (pingInterval) {
    clearInterval(pingInterval);
    pingInterval = null;
  }
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

function connect() {
  if (intentionalShutdown) return;

  if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
    return;
  }

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

      const result = msg?.params?.result;
      const value = result?.value;
      const context = result?.context;

      if (!value || value.err) return;
      if (!value.signature) return;

      if (!isFreshEnough(value.blockTime)) {
        stats.droppedStale += 1;
        return;
      }

      if (!looksRelevantFromLogs(value)) {
        stats.skippedIrrelevantLog += 1;
        return;
      }

      enqueueSignature(
        value.signature,
        context?.slot || null,
        value.blockTime || null
      );
    } catch (err) {
      logError("WS message parse error", {
        socketId,
        error: err.message,
      });
    }
  });

  socket.on("error", (err) => {
    const message = err?.message || "unknown websocket error";
    const wasRateLimited = message.includes("429");

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

    const wasRateLimited = reason.includes("429");

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
            dbTime: db.rows[0].now,
            queueSize: signatureQueue.length,
            workerRunning,
            programId: PUMP_LAUNCHPAD_PROGRAM_ID,
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
    res.end("pregrad pump scanner running");
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
