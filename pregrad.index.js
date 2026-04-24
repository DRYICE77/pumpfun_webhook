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

const PUMP_LAUNCHPAD_PROGRAM_ID =
  process.env.PUMP_LAUNCHPAD_PROGRAM_ID || "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

const STORE_RAW_EVENTS = String(process.env.STORE_RAW_EVENTS || "true") === "true";
const RAW_RETENTION_COUNT = Number(process.env.RAW_RETENTION_COUNT || 5000);

const MAX_QUEUE_SIZE = Number(process.env.MAX_QUEUE_SIZE || 5000);
const RESUME_QUEUE_SIZE = Number(process.env.RESUME_QUEUE_SIZE || 2500);
const WORKER_CONCURRENCY = Number(process.env.WORKER_CONCURRENCY || 8);
const MAX_TX_PER_SECOND = Number(process.env.MAX_TX_PER_SECOND || 30);
const SIGNATURE_MAX_AGE_MS = Number(process.env.SIGNATURE_MAX_AGE_MS || 120000);
const STALE_DRAIN_INTERVAL_MS = Number(process.env.STALE_DRAIN_INTERVAL_MS || 1000);
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

let intakePaused = false;
let workerRunning = false;

const seenSignatures = new Set();
const queuedSignatures = new Set();
const signatureQueue = [];
const workerPromises = [];

const SEEN_SIGNATURE_LIMIT = 100000;

let queueLogTimer = null;
let staleDrainTimer = null;

const stats = {
  queued: 0,
  dequeued: 0,
  processed: 0,
  insertedEvents: 0,
  insertedTokens: 0,

  intakePausedCount: 0,
  intakeResumedCount: 0,
  droppedQueueFull: 0,
  droppedDuplicate: 0,
  droppedStale: 0,
  droppedDuringPause: 0,

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
  console.log(`[pregrad-ws] ${message}${Object.keys(extra).length ? " " + JSON.stringify(extra) : ""}`);
}

function logError(message, extra = {}) {
  console.error(`[pregrad-ws] ${message}${Object.keys(extra).length ? " " + JSON.stringify(extra) : ""}`);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function addSeenSignature(sig) {
  seenSignatures.add(sig);
  if (seenSignatures.size > SEEN_SIGNATURE_LIMIT) {
    seenSignatures.delete(seenSignatures.values().next().value);
  }
}

function alreadySeen(sig) {
  return seenSignatures.has(sig);
}

function maybePauseIntake() {
  if (!intakePaused && signatureQueue.length >= MAX_QUEUE_SIZE) {
    intakePaused = true;
    stats.intakePausedCount += 1;
    logInfo("Intake paused", { queueSize: signatureQueue.length, maxQueueSize: MAX_QUEUE_SIZE });
  }
}

function maybeResumeIntake() {
  if (intakePaused && signatureQueue.length <= RESUME_QUEUE_SIZE) {
    intakePaused = false;
    stats.intakeResumedCount += 1;
    logInfo("Intake resumed", { queueSize: signatureQueue.length, resumeQueueSize: RESUME_QUEUE_SIZE });
  }
}

function drainStaleQueueItems() {
  const now = Date.now();
  let dropped = 0;

  while (signatureQueue.length && now - signatureQueue[0].enqueuedAt > SIGNATURE_MAX_AGE_MS) {
    const item = signatureQueue.shift();
    if (item?.signature) queuedSignatures.delete(item.signature);
    dropped += 1;
  }

  if (dropped > 0) {
    stats.droppedStale += dropped;
    logInfo("Dropped stale queue items", { dropped, queueSize: signatureQueue.length });
  }

  maybeResumeIntake();
}

function startStaleDrainer() {
  if (staleDrainTimer) return;
  staleDrainTimer = setInterval(drainStaleQueueItems, STALE_DRAIN_INTERVAL_MS);
}

function stopStaleDrainer() {
  if (staleDrainTimer) {
    clearInterval(staleDrainTimer);
    staleDrainTimer = null;
  }
}

function backoffDelay(attempt, wasRateLimited = false) {
  if (wasRateLimited) return Math.min(60000 * 2 ** Math.min(attempt, 4), 600000);
  return Math.min(2000 * 2 ** Math.min(attempt, 5), 60000);
}

async function heliusRpc(method, params) {
  const res = await fetch(RPC_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ jsonrpc: "2.0", id: `${method}-${Date.now()}`, method, params }),
  });

  if (!res.ok) throw new Error(`RPC HTTP error ${res.status}`);

  const json = await res.json();
  if (json.error) throw new Error(`RPC error: ${JSON.stringify(json.error)}`);

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

function getInstructions(tx) {
  return tx?.transaction?.message?.instructions || [];
}

function getInnerInstructions(tx) {
  return tx?.meta?.innerInstructions || [];
}

function getAccountKeys(tx) {
  return tx?.transaction?.message?.accountKeys?.map((k) => (typeof k === "string" ? k : k.pubkey)) || [];
}

function getTs(tx) {
  return tx?.blockTime ? new Date(tx.blockTime * 1000) : new Date();
}

function getSignerWallet(tx) {
  const keys = tx?.transaction?.message?.accountKeys || [];
  for (const key of keys) {
    if (typeof key !== "string" && key.signer) return key.pubkey;
  }
  return null;
}

function txTouchesLaunchpadProgram(tx) {
  if (getLogMessages(tx).some((l) => l.includes(PUMP_LAUNCHPAD_PROGRAM_ID))) return true;
  if (getAccountKeys(tx).includes(PUMP_LAUNCHPAD_PROGRAM_ID)) return true;

  for (const ix of getInstructions(tx)) {
    const pid = typeof ix.programId === "string" ? ix.programId : ix.programId?.toString?.();
    if (pid === PUMP_LAUNCHPAD_PROGRAM_ID) return true;
  }

  for (const group of getInnerInstructions(tx)) {
    for (const ix of group.instructions || []) {
      const pid = typeof ix.programId === "string" ? ix.programId : ix.programId?.toString?.();
      if (pid === PUMP_LAUNCHPAD_PROGRAM_ID) return true;
    }
  }

  return false;
}

function looksRelevantFromLogs(value) {
  const logs = value?.logs || [];
  if (!Array.isArray(logs) || !logs.length) return false;

  return logs.some((l) => {
    const s = String(l).toLowerCase();
    return (
      l.includes(PUMP_LAUNCHPAD_PROGRAM_ID) ||
      s.includes("instruction: buy") ||
      s.includes("instruction: sell") ||
      s.includes("instruction: create") ||
      s.includes("migrate") ||
      s.includes("graduate")
    );
  });
}

function inferEventTypeFromLogs(tx) {
  const logs = getLogMessages(tx).map((l) => String(l).toLowerCase());

  const hasCreate = logs.some((l) => l.includes("instruction: create") || l.includes("create_v2"));
  const hasBuy = logs.some((l) => l.includes("instruction: buy"));
  const hasSell = logs.some((l) => l.includes("instruction: sell"));
  const hasMigrate = logs.some((l) => l.includes("migrate") || l.includes("graduate"));

  if (hasCreate) return "create";
  if (hasMigrate) return "migrate";
  if (hasBuy && !hasSell) return "buy";
  if (hasSell && !hasBuy) return "sell";
  return "unknown";
}

function getMintCandidatesFromTokenBalances(tx) {
  const out = new Set();

  for (const row of tx?.meta?.preTokenBalances || []) {
    if (row?.mint && !row.mint.startsWith("So111111")) out.add(row.mint);
  }

  for (const row of tx?.meta?.postTokenBalances || []) {
    if (row?.mint && !row.mint.startsWith("So111111")) out.add(row.mint);
  }

  return [...out];
}

function inferPrimaryMint(tx) {
  const candidates = getMintCandidatesFromTokenBalances(tx).filter((m) => m.endsWith("pump"));
  if (candidates.length) return candidates[0];

  const all = getMintCandidatesFromTokenBalances(tx);
  return all[0] || null;
}

function parseCreateMetadata(tx) {
  let name = null;
  let symbol = null;

  for (const ix of getInstructions(tx)) {
    const info = ix?.parsed?.info || {};
    if (!name && typeof info.name === "string") name = info.name;
    if (!symbol && typeof info.symbol === "string") symbol = info.symbol;
  }

  return { name, symbol };
}

function extractSolAmount(tx, eventType) {
  const SOL_MINT = "So11111111111111111111111111111111111111112";

  let maxSolUiAmount = null;

  const scanIx = (ix) => {
    const info = ix?.parsed?.info || {};
    if (ix?.parsed?.type !== "transferChecked" && ix?.parsed?.type !== "transfer") return;

    if (info.mint === SOL_MINT) {
      const raw =
        info?.tokenAmount?.uiAmount ??
        info?.uiAmount ??
        (info?.amount && info?.decimals === 9 ? Number(info.amount) / 1e9 : null);

      const n = Number(raw);
      if (Number.isFinite(n) && n > 0) {
        maxSolUiAmount = maxSolUiAmount === null ? n : Math.max(maxSolUiAmount, n);
      }
    }

    if (info.lamports) {
      const n = Number(info.lamports) / 1e9;
      if (Number.isFinite(n) && n > 0) {
        maxSolUiAmount = maxSolUiAmount === null ? n : Math.max(maxSolUiAmount, n);
      }
    }
  };

  for (const ix of getInstructions(tx)) scanIx(ix);
  for (const group of getInnerInstructions(tx)) {
    for (const ix of group.instructions || []) scanIx(ix);
  }

  if (maxSolUiAmount !== null) return maxSolUiAmount;

  const logs = getLogMessages(tx).join("\n");
  const amountInMatch = logs.match(/amount_in:\s*([0-9]+)/i);
  if (amountInMatch) {
    const raw = Number(amountInMatch[1]);

    if (Number.isFinite(raw) && raw > 0) {
      if (eventType === "buy") return raw / 1e9;
      return raw / 1e6;
    }
  }

  return null;
}

function extractTokenAmount(tx, tokenAddress) {
  if (!tokenAddress) return null;

  let maxTokenUiAmount = null;

  const scanIx = (ix) => {
    const info = ix?.parsed?.info || {};
    if (info.mint !== tokenAddress) return;

    const raw =
      info?.tokenAmount?.uiAmount ??
      info?.uiAmount ??
      (info?.amount && info?.decimals != null ? Number(info.amount) / 10 ** Number(info.decimals) : null);

    const n = Number(raw);
    if (Number.isFinite(n) && n > 0) {
      maxTokenUiAmount = maxTokenUiAmount === null ? n : Math.max(maxTokenUiAmount, n);
    }
  };

  for (const ix of getInstructions(tx)) scanIx(ix);
  for (const group of getInnerInstructions(tx)) {
    for (const ix of group.instructions || []) scanIx(ix);
  }

  return maxTokenUiAmount;
}

function classifyPregradEvent(tx, signature) {
  if (!tx || !tx.meta || !tx.transaction) return { ok: false, reason: "missing tx fields" };
  if (tx.meta.err) return { ok: false, reason: "tx failed" };
  if (!txTouchesLaunchpadProgram(tx)) return { ok: false, reason: "not launchpad program" };

  const eventType = inferEventTypeFromLogs(tx);
  const tokenAddress = inferPrimaryMint(tx);
  const walletAddress = getSignerWallet(tx);
  const { name, symbol } = parseCreateMetadata(tx);

  const solAmount = extractSolAmount(tx, eventType);
  const tokenAmount = extractTokenAmount(tx, tokenAddress);
  const pricePerToken = solAmount && tokenAmount ? solAmount / tokenAmount : null;

  const blockTime = getTs(tx);
  const isMigrate = eventType === "migrate";

  const event = {
    token_address: tokenAddress,
    signature,
    slot: tx.slot || null,
    block_time: blockTime,
    event_type: eventType,
    wallet_address: walletAddress,
    sol_amount: solAmount,
    token_amount: tokenAmount,
    price_per_token: pricePerToken,
    raw_json: tx,
  };

  const tokenUpsert = tokenAddress
    ? {
        token_address: tokenAddress,
        creator_wallet: walletAddress,
        symbol,
        name,
        token_program: null,
        created_at: blockTime,
        first_seen_signature: signature,
        first_seen_slot: tx.slot || null,
        graduation_status: isMigrate ? "graduated" : "pre_grad",
        market_phase: isMigrate ? "JUST_GRADUATED" : "PRE_GRAD",
        last_event_type: eventType,
        last_seen_at: blockTime,
        graduated_at: isMigrate ? blockTime : null,
      }
    : null;

  return { ok: true, event, tokenUpsert };
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
      last_event_type TEXT,
      last_seen_at TIMESTAMPTZ,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    ALTER TABLE pump_launchpad_tokens
    ADD COLUMN IF NOT EXISTS market_cap_usd NUMERIC,
    ADD COLUMN IF NOT EXISTS liquidity_usd NUMERIC,
    ADD COLUMN IF NOT EXISTS latest_price NUMERIC,
    ADD COLUMN IF NOT EXISTS fdv_usd NUMERIC,
    ADD COLUMN IF NOT EXISTS bonding_progress_pct NUMERIC,
    ADD COLUMN IF NOT EXISTS ath_market_cap_usd NUMERIC,
    ADD COLUMN IF NOT EXISTS atl_market_cap_usd NUMERIC,
    ADD COLUMN IF NOT EXISTS updated_market_data_at TIMESTAMPTZ;
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
    ALTER TABLE pump_launchpad_events
    ADD COLUMN IF NOT EXISTS sol_amount NUMERIC,
    ADD COLUMN IF NOT EXISTS token_amount NUMERIC,
    ADD COLUMN IF NOT EXISTS price_per_token NUMERIC,
    ADD COLUMN IF NOT EXISTS market_cap_usd NUMERIC,
    ADD COLUMN IF NOT EXISTS sol_price_usd NUMERIC;
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

  await pool.query(`
    CREATE INDEX IF NOT EXISTS pump_launchpad_tokens_market_idx
    ON pump_launchpad_tokens (graduation_status, market_cap_usd DESC, updated_market_data_at DESC);
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

async function upsertLaunchpadToken(token) {
  if (!token?.token_address) return false;

  const result = await pool.query(
    `
    INSERT INTO pump_launchpad_tokens (
      token_address, creator_wallet, symbol, name, token_program,
      created_at, first_seen_signature, first_seen_slot,
      graduation_status, graduated_at, market_phase,
      last_event_type, last_seen_at, updated_at
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,NOW())
    ON CONFLICT (token_address)
    DO UPDATE SET
      creator_wallet = COALESCE(pump_launchpad_tokens.creator_wallet, EXCLUDED.creator_wallet),
      symbol = COALESCE(pump_launchpad_tokens.symbol, EXCLUDED.symbol),
      name = COALESCE(pump_launchpad_tokens.name, EXCLUDED.name),
      token_program = COALESCE(pump_launchpad_tokens.token_program, EXCLUDED.token_program),
      created_at = COALESCE(pump_launchpad_tokens.created_at, EXCLUDED.created_at),
      first_seen_signature = COALESCE(pump_launchpad_tokens.first_seen_signature, EXCLUDED.first_seen_signature),
      first_seen_slot = COALESCE(pump_launchpad_tokens.first_seen_slot, EXCLUDED.first_seen_slot),
      graduation_status = CASE
        WHEN pump_launchpad_tokens.graduation_status = 'graduated' THEN 'graduated'
        ELSE EXCLUDED.graduation_status
      END,
      graduated_at = COALESCE(pump_launchpad_tokens.graduated_at, EXCLUDED.graduated_at),
      market_phase = CASE
        WHEN pump_launchpad_tokens.market_phase IN ('JUST_GRADUATED', 'POST_GRAD') THEN pump_launchpad_tokens.market_phase
        ELSE EXCLUDED.market_phase
      END,
      last_event_type = COALESCE(EXCLUDED.last_event_type, pump_launchpad_tokens.last_event_type),
      last_seen_at = COALESCE(EXCLUDED.last_seen_at, pump_launchpad_tokens.last_seen_at),
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
      token.graduated_at || null,
      token.market_phase || "PRE_GRAD",
      token.last_event_type || null,
      token.last_seen_at || null,
    ]
  );

  if (result.rowCount > 0) {
    stats.insertedTokens += 1;
    return true;
  }

  return false;
}

async function markTokenGraduated(tokenAddress, graduatedAt) {
  if (!tokenAddress) return;

  await pool.query(
    `
    UPDATE pump_launchpad_tokens
    SET graduation_status = 'graduated',
        market_phase = 'JUST_GRADUATED',
        graduated_at = COALESCE(graduated_at, $2),
        last_event_type = 'migrate',
        last_seen_at = COALESCE($2, NOW()),
        updated_at = NOW()
    WHERE token_address = $1
    `,
    [tokenAddress, graduatedAt]
  );
}

async function insertLaunchpadEvent(event) {
  const result = await pool.query(
    `
    INSERT INTO pump_launchpad_events (
      token_address, signature, slot, block_time, event_type,
      wallet_address, sol_amount, token_amount, price_per_token, raw_json
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
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
      event.sol_amount,
      event.token_amount,
      event.price_per_token,
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

  maybeResumeIntake();
  maybePauseIntake();

  if (intakePaused) {
    stats.droppedDuringPause += 1;
    return;
  }

  if (alreadySeen(signature) || queuedSignatures.has(signature)) {
    stats.droppedDuplicate += 1;
    return;
  }

  if (signatureQueue.length >= MAX_QUEUE_SIZE) {
    stats.droppedQueueFull += 1;
    maybePauseIntake();
    return;
  }

  queuedSignatures.add(signature);
  signatureQueue.push({ signature, slot, blockTime, enqueuedAt: Date.now() });
  stats.queued += 1;
}

async function processQueuedSignature(item) {
  queuedSignatures.delete(item.signature);
  stats.dequeued += 1;

  if (!item.signature || alreadySeen(item.signature)) return;

  if (Date.now() - item.enqueuedAt > SIGNATURE_MAX_AGE_MS) {
    stats.droppedStale += 1;
    return;
  }

  addSeenSignature(item.signature);

  try {
    const tx = await fetchFullTransaction(item.signature);

    if (!tx) {
      stats.skippedEmptyTx += 1;
      return;
    }

    await insertRawPregradEvent({
      signature: item.signature,
      slot: tx.slot || item.slot,
      timestamp: tx.blockTime || item.blockTime,
      type: "helius_ws_pregrad_tx",
      payload: tx,
    });

    const classified = classifyPregradEvent(tx, item.signature);
    if (!classified.ok) return;
    const MIN_SOL_AMOUNT = Number(process.env.MIN_SOL_AMOUNT || 0.1);

if (
  ["buy", "sell"].includes(classified.event?.event_type) &&
  (
    classified.event?.sol_amount === null ||
    classified.event?.sol_amount < MIN_SOL_AMOUNT
  )
) {
  stats.skippedSmallSolAmount = (stats.skippedSmallSolAmount || 0) + 1;
  return;
}

    if (classified.event?.token_address) {
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
        await markTokenGraduated(classified.event.token_address, classified.event.block_time);
        break;
      default:
        stats.classifiedUnknown += 1;
    }
  } catch (err) {
    stats.txFetchErrors += 1;
    logError("Failed processing signature", { signature: item.signature, error: err.message });
  }
}

async function queueWorkerLoop(workerId) {
  const minDelayMs = Math.max(Math.floor((1000 / MAX_TX_PER_SECOND) * WORKER_CONCURRENCY), 15);

  while (workerRunning) {
    drainStaleQueueItems();

    const item = signatureQueue.shift();

    if (!item) {
      maybeResumeIntake();
      await sleep(100);
      continue;
    }

    try {
      await processQueuedSignature(item);
    } catch (err) {
      stats.workerErrors += 1;
      logError("Queue worker error", { workerId, error: err.message });
    }

    maybeResumeIntake();
    await sleep(minDelayMs);
  }
}

function startQueueWorker() {
  if (workerRunning) return;
  workerRunning = true;

  for (let i = 0; i < WORKER_CONCURRENCY; i += 1) {
    workerPromises.push(queueWorkerLoop(i + 1));
  }

  logInfo("Queue workers started", {
    workerConcurrency: WORKER_CONCURRENCY,
    maxTxPerSecond: MAX_TX_PER_SECOND,
    maxQueueSize: MAX_QUEUE_SIZE,
    resumeQueueSize: RESUME_QUEUE_SIZE,
    signatureMaxAgeMs: SIGNATURE_MAX_AGE_MS,
  });
}

function stopQueueWorker() {
  workerRunning = false;
}

let lastQueued = 0;
let lastDequeued = 0;
let lastInserted = 0;
let lastDroppedPause = 0;
let lastLogTime = Date.now();

function startQueueLogger() {
  if (queueLogTimer) return;

  queueLogTimer = setInterval(() => {
    const now = Date.now();
    const seconds = Math.max((now - lastLogTime) / 1000, 1);

    const oldestAgeMs = signatureQueue.length ? now - signatureQueue[0].enqueuedAt : 0;

    logInfo("Queue stats", {
      intakePaused,
      queueSize: signatureQueue.length,
      oldestAgeMs,
      queued: stats.queued,
      dequeued: stats.dequeued,
      processed: stats.processed,
      insertedEvents: stats.insertedEvents,
      insertedTokens: stats.insertedTokens,
      incomingPerSec: ((stats.queued - lastQueued) / seconds).toFixed(2),
      drainedPerSec: ((stats.dequeued - lastDequeued) / seconds).toFixed(2),
      insertedPerSec: ((stats.insertedEvents - lastInserted) / seconds).toFixed(2),
      droppedDuringPausePerSec: ((stats.droppedDuringPause - lastDroppedPause) / seconds).toFixed(2),
      droppedQueueFull: stats.droppedQueueFull,
      droppedDuplicate: stats.droppedDuplicate,
      droppedStale: stats.droppedStale,
      droppedDuringPause: stats.droppedDuringPause,
      txFetchErrors: stats.txFetchErrors,
      workerErrors: stats.workerErrors,
      classifiedCreate: stats.classifiedCreate,
      classifiedBuy: stats.classifiedBuy,
      classifiedSell: stats.classifiedSell,
      classifiedMigrate: stats.classifiedMigrate,
      classifiedUnknown: stats.classifiedUnknown,
    });

    lastQueued = stats.queued;
    lastDequeued = stats.dequeued;
    lastInserted = stats.insertedEvents;
    lastDroppedPause = stats.droppedDuringPause;
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
  socket.send(
    JSON.stringify({
      jsonrpc: "2.0",
      id: 1,
      method: "logsSubscribe",
      params: [{ mentions: [PUMP_LAUNCHPAD_PROGRAM_ID] }, { commitment: "confirmed" }],
    })
  );

  logInfo("Sent logsSubscribe", { programId: PUMP_LAUNCHPAD_PROGRAM_ID });
}

function cleanupSocket(socket) {
  try {
    socket.removeAllListeners();
    if (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING) {
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
  if (intentionalShutdown || reconnectTimeout) return;

  const delay = backoffDelay(retryCount, wasRateLimited);

  logInfo("Scheduling reconnect", { reason, retryCount, delayMs: delay, wasRateLimited });

  reconnectTimeout = setTimeout(() => {
    reconnectTimeout = null;
    retryCount += 1;
    connect();
  }, delay);
}

function connect() {
  if (intentionalShutdown) return;

  if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;

  currentSocketId += 1;
  const socketId = currentSocketId;
  const socket = new WebSocket(WSS_URL);
  ws = socket;

  logInfo("Connecting websocket", { socketId, url: "wss://mainnet.helius-rpc.com/?api-key=***" });

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
        logInfo("Subscribed successfully", { socketId, subscriptionId: msg.result });
        return;
      }

      const result = msg?.params?.result;
      const value = result?.value;
      const context = result?.context;

      if (!value || value.err || !value.signature) return;

      if (intakePaused) {
        stats.droppedDuringPause += 1;
        maybeResumeIntake();
        return;
      }

      if (!looksRelevantFromLogs(value)) {
        stats.skippedIrrelevantLog += 1;
        return;
      }

      enqueueSignature(value.signature, context?.slot || null, value.blockTime || null);
    } catch (err) {
      logError("WS message parse error", { socketId, error: err.message });
    }
  });

  socket.on("error", (err) => {
    const message = err?.message || "unknown websocket error";
    logError("WebSocket error", { socketId, error: message, wasRateLimited: message.includes("429") });
  });

  socket.on("close", (code, reasonBuffer) => {
    if (socketId !== currentSocketId) return;

    stopPing();

    const reason = reasonBuffer?.length ? reasonBuffer.toString() : "no reason";
    const wasRateLimited = reason.includes("429");

    logInfo("WebSocket closed", { socketId, code, reason, wasRateLimited });

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
            intakePaused,
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

    startQueueWorker();
    startQueueLogger();
    startStaleDrainer();
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
  stopStaleDrainer();

  if (reconnectTimeout) clearTimeout(reconnectTimeout);

  if (ws) cleanupSocket(ws);

  try {
    await Promise.allSettled(workerPromises);
  } catch (_) {}

  try {
    await pool.end();
  } catch (_) {}

  process.exit(0);
}

boot();
