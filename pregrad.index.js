// ==================================================
// PRE-GRAD TOKENS.JS
// NORTH STAR — HELIUS PUMP.FUN PRE-GRAD SCANNER
//
// Purpose:
//
// 1. Subscribe to Pump.fun program logs through Helius.
// 2. Queue and hydrate matching transactions safely.
// 3. Preserve every valid trade event, including small trades.
// 4. Calculate trade amounts from wallet balance deltas first.
// 5. Maintain live SOL-denominated and USD market data.
// 6. Capture scheduled 1-minute and 3-minute early-supply snapshots.
// 7. Run holder enrichment through a separately throttled queue.
// 8. Recover cleanly from null RPC responses and WebSocket failures.
// ==================================================

require("dotenv").config();

const http = require("http");
const WebSocket = require("ws");
const { Pool } = require("pg");

// ==================================================
// 1. ENVIRONMENT
// ==================================================

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
  process.env.PUMP_LAUNCHPAD_PROGRAM_ID ||
  "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

const WSS_URL =
  `wss://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;

const RPC_URL =
  `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;

const STORE_RAW_EVENTS =
  String(process.env.STORE_RAW_EVENTS || "true") === "true";

const RAW_RETENTION_COUNT = Number(
  process.env.RAW_RETENTION_COUNT || 25000
);

const RAW_RETENTION_CLEANUP_MS = Number(
  process.env.RAW_RETENTION_CLEANUP_MS || 15 * 60 * 1000
);

const MAX_QUEUE_SIZE = Number(
  process.env.MAX_QUEUE_SIZE || 5000
);

const RESUME_QUEUE_SIZE = Number(
  process.env.RESUME_QUEUE_SIZE || 2500
);

const WORKER_CONCURRENCY = Number(
  process.env.WORKER_CONCURRENCY || 8
);

const MAX_TX_PER_SECOND = Number(
  process.env.MAX_TX_PER_SECOND || 30
);

const SIGNATURE_MAX_AGE_MS = Number(
  process.env.SIGNATURE_MAX_AGE_MS || 120000
);

const STALE_DRAIN_INTERVAL_MS = Number(
  process.env.STALE_DRAIN_INTERVAL_MS || 1000
);

const QUEUE_LOG_EVERY_MS = Number(
  process.env.QUEUE_LOG_EVERY_MS || 10000
);

const RPC_RETRY_COUNT = Number(
  process.env.RPC_RETRY_COUNT || 5
);

const RPC_RETRY_DELAY_MS = Number(
  process.env.RPC_RETRY_DELAY_MS || 350
);

const CONTROL_REFRESH_MS = Number(
  process.env.CONTROL_REFRESH_MS || 5000
);

const DEFAULT_MIN_SOL_AMOUNT = Number(
  process.env.MIN_SOL_AMOUNT || 0.05
);

const PREGRAD_TOKEN_SUPPLY = Number(
  process.env.PREGRAD_TOKEN_SUPPLY || 1000000000
);

const SOL_PRICE_USD = Number(
  process.env.SOL_PRICE_USD || 0
);

const TOKEN_SAFETY_ENRICHMENT_ENABLED =
  String(process.env.TOKEN_SAFETY_ENRICHMENT_ENABLED || "true") === "true";

const HOLDER_ENRICHMENT_ENABLED =
  String(process.env.HOLDER_ENRICHMENT_ENABLED || "true") === "true";

const HOLDER_REFRESH_COOLDOWN_MS = Number(
  process.env.HOLDER_REFRESH_COOLDOWN_MS || 10 * 60 * 1000
);

const HOLDER_WORKER_CONCURRENCY = Number(
  process.env.HOLDER_WORKER_CONCURRENCY || 2
);

const HOLDER_MAX_REQUESTS_PER_SECOND = Number(
  process.env.HOLDER_MAX_REQUESTS_PER_SECOND || 5
);

const HOLDER_MIN_TOP1_RISK_PCT = Number(
  process.env.HOLDER_MIN_TOP1_RISK_PCT || 15
);

const HOLDER_MIN_TOP5_RISK_PCT = Number(
  process.env.HOLDER_MIN_TOP5_RISK_PCT || 50
);

const HOLDER_MIN_TOP10_RISK_PCT = Number(
  process.env.HOLDER_MIN_TOP10_RISK_PCT || 80
);

const EARLY_SNAPSHOT_1M_DELAY_MS = Number(
  process.env.EARLY_SNAPSHOT_1M_DELAY_MS || 65 * 1000
);

const EARLY_SNAPSHOT_3M_DELAY_MS = Number(
  process.env.EARLY_SNAPSHOT_3M_DELAY_MS || 185 * 1000
);

const EARLY_SNAPSHOT_MAX_LATE_MS = Number(
  process.env.EARLY_SNAPSHOT_MAX_LATE_MS || 15 * 60 * 1000
);

const SEEN_SIGNATURE_LIMIT = Number(
  process.env.SEEN_SIGNATURE_LIMIT || 100000
);

// ==================================================
// 2. DATABASE
// ==================================================

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: Number(process.env.PG_POOL_MAX || 20),
  idleTimeoutMillis: Number(process.env.PG_IDLE_TIMEOUT_MS || 30000),
  connectionTimeoutMillis: Number(process.env.PG_CONNECT_TIMEOUT_MS || 10000),
});

// ==================================================
// 3. RUNTIME STATE
// ==================================================

let pregradControl = {
  helius_enabled: false,
  manual_override: "OFF",
  max_queue_size: MAX_QUEUE_SIZE,
  min_sol_threshold: DEFAULT_MIN_SOL_AMOUNT,
  updated_at: null,
};

let lastControlFetchAt = 0;

let ws = null;
let reconnectTimeout = null;
let retryCount = 0;
let intentionalShutdown = false;
let currentSocketId = 0;
let pingInterval = null;
let socketAlive = false;

let intakePaused = false;
let workerRunning = false;
let holderWorkerRunning = false;

const seenSignatures = new Set();
const inFlightSignatures = new Set();
const queuedSignatures = new Set();

const signatureQueue = [];
let signatureQueueHead = 0;

const holderQueue = [];
let holderQueueHead = 0;
const holderQueuedKeys = new Set();

const tokenLastHolderEnrichedAt = new Map();
const tokenScheduledSnapshots = new Map();

const workerPromises = [];
const holderWorkerPromises = [];

let queueLogTimer = null;
let staleDrainTimer = null;
let retentionTimer = null;

const stats = {
  queued: 0,
  dequeued: 0,
  processed: 0,
  insertedEvents: 0,
  insertedTokens: 0,
  updatedMarketData: 0,

  intakePausedCount: 0,
  intakeResumedCount: 0,
  droppedQueueFull: 0,
  droppedDuplicate: 0,
  droppedStale: 0,
  droppedDuringPause: 0,

  smallTradesStored: 0,
  marketDataUnavailable: 0,
  txNullRetries: 0,
  txFetchErrors: 0,
  workerErrors: 0,
  rpcRetries: 0,
  controlFetchErrors: 0,

  classifiedCreate: 0,
  classifiedBuy: 0,
  classifiedSell: 0,
  classifiedMigrate: 0,
  classifiedUnknown: 0,

  holderJobsQueued: 0,
  holderJobsProcessed: 0,
  holderJobsSkippedCooldown: 0,
  holderJobsFailed: 0,
  earlySnapshot1mRuns: 0,
  earlySnapshot3mRuns: 0,

  skippedIrrelevantLog: 0,
  skippedFailedTx: 0,
  skippedUnresolvedMint: 0,
  skippedUnresolvedTradeAmount: 0,
};

// ==================================================
// 4. LOGGING / HELPERS
// ==================================================

function logInfo(message, extra = {}) {
  const suffix = Object.keys(extra).length
    ? ` ${JSON.stringify(extra)}`
    : "";

  console.log(`[pregrad-ws] ${message}${suffix}`);
}

function logError(message, extra = {}) {
  const suffix = Object.keys(extra).length
    ? ` ${JSON.stringify(extra)}`
    : "";

  console.error(`[pregrad-ws] ${message}${suffix}`);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function toNumber(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function normalizePct(value) {
  const n = toNumber(value, null);
  return n === null ? null : Number(n.toFixed(4));
}

function nowIso() {
  return new Date().toISOString();
}

function getSignatureQueueLength() {
  return signatureQueue.length - signatureQueueHead;
}

function getHolderQueueLength() {
  return holderQueue.length - holderQueueHead;
}

function compactQueue(queue, headRefName) {
  if (headRefName === "signature") {
    if (signatureQueueHead > 1000 && signatureQueueHead * 2 > signatureQueue.length) {
      signatureQueue.splice(0, signatureQueueHead);
      signatureQueueHead = 0;
    }
    return;
  }

  if (holderQueueHead > 500 && holderQueueHead * 2 > holderQueue.length) {
    holderQueue.splice(0, holderQueueHead);
    holderQueueHead = 0;
  }
}

function addSeenSignature(signature) {
  seenSignatures.add(signature);

  if (seenSignatures.size > SEEN_SIGNATURE_LIMIT) {
    const oldest = seenSignatures.values().next().value;
    seenSignatures.delete(oldest);
  }
}

function isSignatureKnown(signature) {
  return (
    seenSignatures.has(signature) ||
    inFlightSignatures.has(signature) ||
    queuedSignatures.has(signature)
  );
}

function backoffDelay(attempt, rateLimited = false) {
  if (rateLimited) {
    return Math.min(15000 * 2 ** Math.min(attempt, 5), 300000);
  }

  return Math.min(1500 * 2 ** Math.min(attempt, 6), 60000);
}

// ==================================================
// 5. CONTROL TABLE
// ==================================================

async function getPregradControl(force = false) {
  const now = Date.now();

  if (!force && now - lastControlFetchAt < CONTROL_REFRESH_MS) {
    return pregradControl;
  }

  try {
    const result = await pool.query(`
      SELECT
        helius_enabled,
        manual_override,
        max_queue_size,
        min_sol_threshold,
        updated_at
      FROM pregrad_system_control
      WHERE id = 1
    `);

    if (result.rows[0]) {
      pregradControl = {
        helius_enabled: result.rows[0].helius_enabled === true,
        manual_override: result.rows[0].manual_override || "OFF",
        max_queue_size: Number(
          result.rows[0].max_queue_size || MAX_QUEUE_SIZE
        ),
        min_sol_threshold: Number(
          result.rows[0].min_sol_threshold || DEFAULT_MIN_SOL_AMOUNT
        ),
        updated_at: result.rows[0].updated_at,
      };
    }

    lastControlFetchAt = now;
  } catch (error) {
    stats.controlFetchErrors += 1;

    logError("Failed to fetch pregrad control", {
      error: error.message,
    });

    pregradControl = {
      ...pregradControl,
      helius_enabled: false,
      manual_override: "CONTROL_FETCH_FAILED",
    };
  }

  return pregradControl;
}

function isPregradEnabled() {
  return (
    pregradControl.helius_enabled === true &&
    pregradControl.manual_override !== "OFF"
  );
}

function effectiveMaxQueueSize() {
  return Number(pregradControl.max_queue_size || MAX_QUEUE_SIZE);
}

function effectiveMinSolAmount() {
  return Number(
    pregradControl.min_sol_threshold || DEFAULT_MIN_SOL_AMOUNT
  );
}

// ==================================================
// 6. HELIUS RPC
// ==================================================

async function heliusRpc(method, params) {
  const response = await fetch(RPC_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      jsonrpc: "2.0",
      id: `${method}-${Date.now()}-${Math.random()}`,
      method,
      params,
    }),
  });

  if (!response.ok) {
    const error = new Error(`RPC HTTP error ${response.status}`);
    error.status = response.status;
    throw error;
  }

  const json = await response.json();

  if (json.error) {
    const error = new Error(`RPC error: ${JSON.stringify(json.error)}`);
    error.rpcError = json.error;
    throw error;
  }

  return json.result;
}

async function fetchFullTransaction(signature) {
  let lastError = null;

  for (let attempt = 0; attempt <= RPC_RETRY_COUNT; attempt += 1) {
    try {
      const transaction = await heliusRpc("getTransaction", [
        signature,
        {
          encoding: "jsonParsed",
          maxSupportedTransactionVersion: 0,
          commitment: "confirmed",
        },
      ]);

      if (transaction) {
        return transaction;
      }

      if (attempt < RPC_RETRY_COUNT) {
        stats.txNullRetries += 1;
        await sleep(RPC_RETRY_DELAY_MS * (attempt + 1));
        continue;
      }

      return null;
    } catch (error) {
      lastError = error;

      if (attempt < RPC_RETRY_COUNT) {
        stats.rpcRetries += 1;

        const rateLimited =
          error?.status === 429 ||
          String(error?.message || "").includes("429");

        await sleep(
          rateLimited
            ? backoffDelay(attempt, true)
            : RPC_RETRY_DELAY_MS * (attempt + 1)
        );
      }
    }
  }

  throw lastError;
}

async function fetchTokenSupply(mintAddress) {
  return heliusRpc("getTokenSupply", [mintAddress]);
}

async function fetchLargestTokenAccounts(mintAddress) {
  return heliusRpc("getTokenLargestAccounts", [mintAddress]);
}

// ==================================================
// 7. TRANSACTION PARSING
// ==================================================

function getAccountKeyRows(tx) {
  return tx?.transaction?.message?.accountKeys || [];
}

function getAccountKeys(tx) {
  return getAccountKeyRows(tx).map((row) =>
    typeof row === "string" ? row : row.pubkey
  );
}

function getSignerIndex(tx) {
  const rows = getAccountKeyRows(tx);

  for (let index = 0; index < rows.length; index += 1) {
    const row = rows[index];

    if (typeof row !== "string" && row.signer === true) {
      return index;
    }
  }

  return 0;
}

function getSignerWallet(tx) {
  const rows = getAccountKeyRows(tx);
  const signerIndex = getSignerIndex(tx);
  const row = rows[signerIndex];

  return typeof row === "string" ? row : row?.pubkey || null;
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

function getBlockTime(tx) {
  return tx?.blockTime
    ? new Date(tx.blockTime * 1000)
    : new Date();
}

function txTouchesPumpProgram(tx) {
  if (getAccountKeys(tx).includes(PUMP_LAUNCHPAD_PROGRAM_ID)) {
    return true;
  }

  if (
    getLogMessages(tx).some((line) =>
      String(line).includes(PUMP_LAUNCHPAD_PROGRAM_ID)
    )
  ) {
    return true;
  }

  return false;
}

function looksRelevantFromLogs(value) {
  const logs = Array.isArray(value?.logs) ? value.logs : [];

  return logs.some((line) => {
    const lower = String(line).toLowerCase();

    return (
      String(line).includes(PUMP_LAUNCHPAD_PROGRAM_ID) ||
      lower.includes("instruction: buy") ||
      lower.includes("instruction: sell") ||
      lower.includes("instruction: create") ||
      lower.includes("create_v2") ||
      lower.includes("migrate") ||
      lower.includes("graduate")
    );
  });
}

function inferEventType(tx) {
  const logs = getLogMessages(tx).map((line) =>
    String(line).toLowerCase()
  );

  const hasCreate = logs.some(
    (line) =>
      line.includes("instruction: create") ||
      line.includes("create_v2")
  );

  const hasBuy = logs.some((line) =>
    line.includes("instruction: buy")
  );

  const hasSell = logs.some((line) =>
    line.includes("instruction: sell")
  );

  const hasMigrate = logs.some(
    (line) =>
      line.includes("migrate") ||
      line.includes("graduate")
  );

  if (hasCreate) return "create";
  if (hasMigrate) return "migrate";
  if (hasBuy && !hasSell) return "buy";
  if (hasSell && !hasBuy) return "sell";

  return "unknown";
}

function getMintCandidates(tx) {
  const candidates = new Set();

  for (const balance of tx?.meta?.preTokenBalances || []) {
    if (
      balance?.mint &&
      balance.mint !== "So11111111111111111111111111111111111111112"
    ) {
      candidates.add(balance.mint);
    }
  }

  for (const balance of tx?.meta?.postTokenBalances || []) {
    if (
      balance?.mint &&
      balance.mint !== "So11111111111111111111111111111111111111112"
    ) {
      candidates.add(balance.mint);
    }
  }

  return [...candidates];
}

function inferPrimaryMint(tx) {
  const candidates = getMintCandidates(tx);
  const pumpMint = candidates.find((mint) => mint.endsWith("pump"));

  return pumpMint || candidates[0] || null;
}

function parseCreateMetadata(tx) {
  let name = null;
  let symbol = null;

  const scan = (instruction) => {
    const info = instruction?.parsed?.info || {};

    if (!name && typeof info.name === "string") {
      name = info.name;
    }

    if (!symbol && typeof info.symbol === "string") {
      symbol = info.symbol;
    }
  };

  for (const instruction of getInstructions(tx)) {
    scan(instruction);
  }

  for (const group of getInnerInstructions(tx)) {
    for (const instruction of group.instructions || []) {
      scan(instruction);
    }
  }

  return { name, symbol };
}

function tokenBalanceUiAmount(balance) {
  const uiTokenAmount = balance?.uiTokenAmount;

  if (uiTokenAmount?.uiAmount != null) {
    return toNumber(uiTokenAmount.uiAmount, 0);
  }

  if (
    uiTokenAmount?.amount != null &&
    uiTokenAmount?.decimals != null
  ) {
    return (
      Number(uiTokenAmount.amount) /
      10 ** Number(uiTokenAmount.decimals)
    );
  }

  return 0;
}

function getOwnerTokenAmount(tx, owner, mint, side) {
  const balances =
    side === "pre"
      ? tx?.meta?.preTokenBalances || []
      : tx?.meta?.postTokenBalances || [];

  return balances
    .filter(
      (balance) =>
        balance?.owner === owner &&
        balance?.mint === mint
    )
    .reduce(
      (sum, balance) => sum + tokenBalanceUiAmount(balance),
      0
    );
}

function getSignerSolDelta(tx) {
  const signerIndex = getSignerIndex(tx);
  const preBalance = toNumber(
    tx?.meta?.preBalances?.[signerIndex],
    null
  );
  const postBalance = toNumber(
    tx?.meta?.postBalances?.[signerIndex],
    null
  );

  if (preBalance === null || postBalance === null) {
    return null;
  }

  return (postBalance - preBalance) / 1e9;
}

function getTradeAmountsFromBalanceDeltas(
  tx,
  eventType,
  tokenAddress,
  walletAddress
) {
  if (
    !["buy", "sell"].includes(eventType) ||
    !tokenAddress ||
    !walletAddress
  ) {
    return {
      solAmount: null,
      tokenAmount: null,
      signerSolDelta: null,
      signerTokenDelta: null,
      source: null,
    };
  }

  const preToken = getOwnerTokenAmount(
    tx,
    walletAddress,
    tokenAddress,
    "pre"
  );

  const postToken = getOwnerTokenAmount(
    tx,
    walletAddress,
    tokenAddress,
    "post"
  );

  const signerTokenDelta = postToken - preToken;
  const signerSolDelta = getSignerSolDelta(tx);

  const tokenAmount = Math.abs(signerTokenDelta);
  const solAmount =
    signerSolDelta === null
      ? null
      : Math.abs(signerSolDelta);

  const directionLooksValid =
    eventType === "buy"
      ? signerTokenDelta > 0
      : signerTokenDelta < 0;

  return {
    solAmount:
      directionLooksValid && solAmount > 0
        ? solAmount
        : null,
    tokenAmount:
      directionLooksValid && tokenAmount > 0
        ? tokenAmount
        : null,
    signerSolDelta,
    signerTokenDelta,
    source:
      directionLooksValid
        ? "signer_balance_delta"
        : null,
  };
}

function extractLargestTransferFallback(tx, tokenAddress) {
  let largestSol = null;
  let largestToken = null;

  const scan = (instruction) => {
    const info = instruction?.parsed?.info || {};

    if (info.lamports != null) {
      const sol = Number(info.lamports) / 1e9;

      if (Number.isFinite(sol) && sol > 0) {
        largestSol =
          largestSol === null
            ? sol
            : Math.max(largestSol, sol);
      }
    }

    if (info.mint === tokenAddress) {
      const raw =
        info?.tokenAmount?.uiAmount ??
        info?.uiAmount ??
        (
          info?.amount != null &&
          info?.decimals != null
            ? Number(info.amount) /
              10 ** Number(info.decimals)
            : null
        );

      const amount = Number(raw);

      if (Number.isFinite(amount) && amount > 0) {
        largestToken =
          largestToken === null
            ? amount
            : Math.max(largestToken, amount);
      }
    }
  };

  for (const instruction of getInstructions(tx)) {
    scan(instruction);
  }

  for (const group of getInnerInstructions(tx)) {
    for (const instruction of group.instructions || []) {
      scan(instruction);
    }
  }

  return {
    solAmount: largestSol,
    tokenAmount: largestToken,
    source:
      largestSol !== null || largestToken !== null
        ? "largest_transfer_fallback"
        : null,
  };
}

function classifyPregradTransaction(tx, signature) {
  if (!tx?.meta || !tx?.transaction) {
    return { ok: false, reason: "missing_transaction_fields" };
  }

  if (tx.meta.err) {
    return { ok: false, reason: "transaction_failed" };
  }

  if (!txTouchesPumpProgram(tx)) {
    return { ok: false, reason: "not_pump_program" };
  }

  const eventType = inferEventType(tx);
  const tokenAddress = inferPrimaryMint(tx);
  const walletAddress = getSignerWallet(tx);
  const blockTime = getBlockTime(tx);
  const { name, symbol } = parseCreateMetadata(tx);

  if (!tokenAddress) {
    return { ok: false, reason: "unresolved_token_mint" };
  }

  const deltaAmounts = getTradeAmountsFromBalanceDeltas(
    tx,
    eventType,
    tokenAddress,
    walletAddress
  );

  const fallbackAmounts = extractLargestTransferFallback(
    tx,
    tokenAddress
  );

  const solAmount =
    deltaAmounts.solAmount ??
    fallbackAmounts.solAmount ??
    null;

  const tokenAmount =
    deltaAmounts.tokenAmount ??
    fallbackAmounts.tokenAmount ??
    null;

  const amountSource =
    deltaAmounts.source ||
    fallbackAmounts.source ||
    null;

  const pricePerTokenSol =
    Number.isFinite(solAmount) &&
    solAmount > 0 &&
    Number.isFinite(tokenAmount) &&
    tokenAmount > 0
      ? solAmount / tokenAmount
      : null;

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
    price_per_token_sol: pricePerTokenSol,
    signer_sol_delta: deltaAmounts.signerSolDelta,
    signer_token_delta: deltaAmounts.signerTokenDelta,
    amount_source: amountSource,
    raw_json: tx,
  };

  const tokenUpsert = {
    token_address: tokenAddress,
    creator_wallet:
      eventType === "create"
        ? walletAddress
        : null,
    symbol,
    name,
    token_program: null,
    created_at: blockTime,
    first_seen_signature: signature,
    first_seen_slot: tx.slot || null,
    graduation_status:
      isMigrate
        ? "graduated"
        : "pre_grad",
    market_phase:
      isMigrate
        ? "JUST_GRADUATED"
        : "PRE_GRAD",
    last_event_type: eventType,
    last_seen_at: blockTime,
    graduated_at:
      isMigrate
        ? blockTime
        : null,
  };

  return {
    ok: true,
    event,
    tokenUpsert,
  };
}

// ==================================================
// 8. TABLES
// ==================================================

async function createTables() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS pregrad_system_control (
      id BIGINT PRIMARY KEY DEFAULT 1,
      helius_enabled BOOLEAN DEFAULT false,
      manual_override TEXT DEFAULT 'OFF',
      max_queue_size INTEGER DEFAULT 2500,
      min_sol_threshold NUMERIC DEFAULT 0.05,
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      CONSTRAINT single_pregrad_control_row CHECK (id = 1)
    );
  `);

  await pool.query(`
    INSERT INTO pregrad_system_control (
      id,
      helius_enabled,
      manual_override,
      max_queue_size,
      min_sol_threshold,
      updated_at
    )
    VALUES (1, false, 'OFF', 2500, 0.05, NOW())
    ON CONFLICT (id) DO NOTHING;
  `);

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
    ADD COLUMN IF NOT EXISTS market_cap_sol NUMERIC,
    ADD COLUMN IF NOT EXISTS market_cap_usd NUMERIC,
    ADD COLUMN IF NOT EXISTS liquidity_usd NUMERIC,
    ADD COLUMN IF NOT EXISTS latest_price_sol NUMERIC,
    ADD COLUMN IF NOT EXISTS latest_price NUMERIC,
    ADD COLUMN IF NOT EXISTS fdv_usd NUMERIC,
    ADD COLUMN IF NOT EXISTS bonding_progress_pct NUMERIC,
    ADD COLUMN IF NOT EXISTS ath_market_cap_sol NUMERIC,
    ADD COLUMN IF NOT EXISTS atl_market_cap_sol NUMERIC,
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
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    ALTER TABLE pump_launchpad_events
    ADD COLUMN IF NOT EXISTS sol_amount NUMERIC,
    ADD COLUMN IF NOT EXISTS token_amount NUMERIC,
    ADD COLUMN IF NOT EXISTS price_per_token NUMERIC,
    ADD COLUMN IF NOT EXISTS price_per_token_sol NUMERIC,
    ADD COLUMN IF NOT EXISTS market_cap_sol NUMERIC,
    ADD COLUMN IF NOT EXISTS market_cap_usd NUMERIC,
    ADD COLUMN IF NOT EXISTS sol_price_usd NUMERIC,
    ADD COLUMN IF NOT EXISTS signer_sol_delta NUMERIC,
    ADD COLUMN IF NOT EXISTS signer_token_delta NUMERIC,
    ADD COLUMN IF NOT EXISTS amount_source TEXT,
    ADD COLUMN IF NOT EXISTS passes_volume_threshold BOOLEAN,
    ADD COLUMN IF NOT EXISTS raw_json JSONB;
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
    CREATE INDEX IF NOT EXISTS pump_launchpad_events_wallet_time_idx
    ON pump_launchpad_events (wallet_address, block_time DESC);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS pump_launchpad_tokens_status_idx
    ON pump_launchpad_tokens (graduation_status, created_at DESC);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS pump_launchpad_tokens_market_idx
    ON pump_launchpad_tokens (
      graduation_status,
      market_cap_usd DESC,
      updated_market_data_at DESC
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS token_safety_enrichment (
      token_id TEXT PRIMARY KEY,
      token_address TEXT NOT NULL,
      dev_hold_pct NUMERIC,
      insiders_pct NUMERIC,
      phishing_pct NUMERIC,
      bundler_pct NUMERIC,
      sniper_pct NUMERIC,
      dex_paid BOOLEAN,
      burnt BOOLEAN,
      no_mint BOOLEAN,
      no_blacklist BOOLEAN,
      source TEXT,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    ALTER TABLE token_safety_enrichment
    ADD COLUMN IF NOT EXISTS top_holder_pct NUMERIC,
    ADD COLUMN IF NOT EXISTS top_5_holders_pct NUMERIC,
    ADD COLUMN IF NOT EXISTS top_10_holders_pct NUMERIC,
    ADD COLUMN IF NOT EXISTS largest_account_sample_count INTEGER,
    ADD COLUMN IF NOT EXISTS holder_count_estimate INTEGER,
    ADD COLUMN IF NOT EXISTS concentration_risk TEXT,
    ADD COLUMN IF NOT EXISTS early_holder_wallet_count_1m INTEGER,
    ADD COLUMN IF NOT EXISTS early_holder_wallet_count_3m INTEGER,
    ADD COLUMN IF NOT EXISTS early_supply_pct_1m NUMERIC,
    ADD COLUMN IF NOT EXISTS early_supply_pct_3m NUMERIC,
    ADD COLUMN IF NOT EXISTS early_supply_recorded_at_1m TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS early_supply_recorded_at_3m TIMESTAMPTZ;
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS token_safety_enrichment_updated_idx
    ON token_safety_enrichment (updated_at DESC);
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

    await pool.query(`
      CREATE INDEX IF NOT EXISTS raw_pregrad_events_created_idx
      ON raw_pregrad_events (created_at DESC);
    `);
  }
}

// ==================================================
// 9. CORE DATABASE WRITES
// ==================================================

async function insertRawPregradEvent({
  signature,
  slot,
  timestamp,
  type,
  payload,
}) {
  if (!STORE_RAW_EVENTS) return;

  await pool.query(
    `
    INSERT INTO raw_pregrad_events (
      signature,
      slot,
      timestamp,
      type,
      payload
    )
    VALUES ($1,$2,$3,$4,$5)
    ON CONFLICT (signature) DO NOTHING
    `,
    [signature, slot, timestamp, type, payload]
  );
}

async function persistClassifiedTransaction(
  event,
  token,
  passesVolumeThreshold
) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const tokenResult = await client.query(
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
        graduated_at,
        market_phase,
        last_event_type,
        last_seen_at,
        updated_at
      )
      VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,NOW()
      )
      ON CONFLICT (token_address)
      DO UPDATE SET
        creator_wallet = COALESCE(
          pump_launchpad_tokens.creator_wallet,
          EXCLUDED.creator_wallet
        ),
        symbol = COALESCE(
          pump_launchpad_tokens.symbol,
          EXCLUDED.symbol
        ),
        name = COALESCE(
          pump_launchpad_tokens.name,
          EXCLUDED.name
        ),
        token_program = COALESCE(
          pump_launchpad_tokens.token_program,
          EXCLUDED.token_program
        ),
        created_at = LEAST(
          COALESCE(pump_launchpad_tokens.created_at, EXCLUDED.created_at),
          EXCLUDED.created_at
        ),
        first_seen_signature = COALESCE(
          pump_launchpad_tokens.first_seen_signature,
          EXCLUDED.first_seen_signature
        ),
        first_seen_slot = COALESCE(
          pump_launchpad_tokens.first_seen_slot,
          EXCLUDED.first_seen_slot
        ),
        graduation_status = CASE
          WHEN pump_launchpad_tokens.graduation_status = 'graduated'
            THEN 'graduated'
          ELSE EXCLUDED.graduation_status
        END,
        graduated_at = COALESCE(
          pump_launchpad_tokens.graduated_at,
          EXCLUDED.graduated_at
        ),
        market_phase = CASE
          WHEN pump_launchpad_tokens.market_phase IN (
            'JUST_GRADUATED',
            'POST_GRAD'
          )
            THEN pump_launchpad_tokens.market_phase
          ELSE EXCLUDED.market_phase
        END,
        last_event_type = EXCLUDED.last_event_type,
        last_seen_at = GREATEST(
          COALESCE(pump_launchpad_tokens.last_seen_at, EXCLUDED.last_seen_at),
          EXCLUDED.last_seen_at
        ),
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
        token.graduation_status,
        token.graduated_at,
        token.market_phase,
        token.last_event_type,
        token.last_seen_at,
      ]
    );

    const eventResult = await client.query(
      `
      INSERT INTO pump_launchpad_events (
        token_address,
        signature,
        slot,
        block_time,
        event_type,
        wallet_address,
        sol_amount,
        token_amount,
        price_per_token,
        price_per_token_sol,
        signer_sol_delta,
        signer_token_delta,
        amount_source,
        passes_volume_threshold,
        raw_json
      )
      VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$9,$10,$11,$12,$13,$14
      )
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
        event.price_per_token_sol,
        event.signer_sol_delta,
        event.signer_token_delta,
        event.amount_source,
        passesVolumeThreshold,
        null,
      ]
    );

    let marketDataUpdated = false;

    if (
      eventResult.rowCount > 0 &&
      ["buy", "sell"].includes(event.event_type) &&
      Number.isFinite(event.price_per_token_sol) &&
      event.price_per_token_sol > 0
    ) {
      const marketCapSol =
        event.price_per_token_sol * PREGRAD_TOKEN_SUPPLY;

      const hasUsd =
        Number.isFinite(SOL_PRICE_USD) &&
        SOL_PRICE_USD > 0;

      const latestPriceUsd = hasUsd
        ? event.price_per_token_sol * SOL_PRICE_USD
        : null;

      const marketCapUsd = hasUsd
        ? marketCapSol * SOL_PRICE_USD
        : null;

      await client.query(
        `
        UPDATE pump_launchpad_tokens
        SET
          latest_price_sol = $2,
          market_cap_sol = $3,
          latest_price = COALESCE($4, latest_price),
          market_cap_usd = COALESCE($5, market_cap_usd),
          fdv_usd = COALESCE($5, fdv_usd),

          ath_market_cap_sol = GREATEST(
            COALESCE(ath_market_cap_sol, 0),
            $3
          ),

          atl_market_cap_sol = CASE
            WHEN atl_market_cap_sol IS NULL OR atl_market_cap_sol = 0
              THEN $3
            ELSE LEAST(atl_market_cap_sol, $3)
          END,

          ath_market_cap_usd = CASE
            WHEN $5 IS NULL THEN ath_market_cap_usd
            ELSE GREATEST(
              COALESCE(ath_market_cap_usd, 0),
              $5
            )
          END,

          atl_market_cap_usd = CASE
            WHEN $5 IS NULL THEN atl_market_cap_usd
            WHEN atl_market_cap_usd IS NULL OR atl_market_cap_usd = 0
              THEN $5
            ELSE LEAST(atl_market_cap_usd, $5)
          END,

          updated_market_data_at = NOW(),
          updated_at = NOW()
        WHERE token_address = $1
        `,
        [
          event.token_address,
          event.price_per_token_sol,
          marketCapSol,
          latestPriceUsd,
          marketCapUsd,
        ]
      );

      await client.query(
        `
        UPDATE pump_launchpad_events
        SET
          market_cap_sol = $2,
          market_cap_usd = $3,
          sol_price_usd = $4
        WHERE signature = $1
        `,
        [
          event.signature,
          marketCapSol,
          marketCapUsd,
          hasUsd ? SOL_PRICE_USD : null,
        ]
      );

      marketDataUpdated = true;
    }

    await client.query("COMMIT");

    if (tokenResult.rowCount > 0) {
      stats.insertedTokens += 1;
    }

    if (eventResult.rowCount > 0) {
      stats.insertedEvents += 1;
    }

    if (marketDataUpdated) {
      stats.updatedMarketData += 1;
    } else if (["buy", "sell"].includes(event.event_type)) {
      stats.marketDataUnavailable += 1;
    }

    return {
      inserted: eventResult.rowCount > 0,
      marketDataUpdated,
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

// ==================================================
// 10. HOLDER CONCENTRATION
// ==================================================

function parseLargestAccountUiAmount(row) {
  if (row?.uiAmount != null) {
    return toNumber(row.uiAmount, 0);
  }

  if (row?.uiAmountString != null) {
    return toNumber(row.uiAmountString, 0);
  }

  if (row?.amount != null && row?.decimals != null) {
    return (
      Number(row.amount) /
      10 ** Number(row.decimals)
    );
  }

  return 0;
}

function calculateLargestAccountConcentration(
  largestAccounts,
  totalSupplyUi
) {
  const supply = toNumber(totalSupplyUi, 0);

  if (
    !Array.isArray(largestAccounts) ||
    largestAccounts.length === 0 ||
    supply <= 0
  ) {
    return {
      top_holder_pct: null,
      top_5_holders_pct: null,
      top_10_holders_pct: null,
      largest_account_sample_count: 0,
    };
  }

  const amounts = largestAccounts
    .map(parseLargestAccountUiAmount)
    .filter((amount) => amount > 0)
    .sort((a, b) => b - a);

  const sum = (values) =>
    values.reduce((total, value) => total + value, 0);

  return {
    top_holder_pct: normalizePct(
      (amounts[0] / supply) * 100
    ),
    top_5_holders_pct: normalizePct(
      (sum(amounts.slice(0, 5)) / supply) * 100
    ),
    top_10_holders_pct: normalizePct(
      (sum(amounts.slice(0, 10)) / supply) * 100
    ),
    largest_account_sample_count: amounts.length,
  };
}

function classifyConcentrationRisk(concentration) {
  const top1 = toNumber(concentration.top_holder_pct, 0);
  const top5 = toNumber(concentration.top_5_holders_pct, 0);
  const top10 = toNumber(concentration.top_10_holders_pct, 0);

  if (top1 >= 25 || top5 >= 70 || top10 >= 90) {
    return "high";
  }

  if (
    top1 >= HOLDER_MIN_TOP1_RISK_PCT ||
    top5 >= HOLDER_MIN_TOP5_RISK_PCT ||
    top10 >= HOLDER_MIN_TOP10_RISK_PCT
  ) {
    return "medium";
  }

  return "low";
}

async function upsertHolderConcentration(
  tokenAddress,
  concentration,
  concentrationRisk
) {
  await pool.query(
    `
    INSERT INTO token_safety_enrichment (
      token_id,
      token_address,
      top_holder_pct,
      top_5_holders_pct,
      top_10_holders_pct,
      largest_account_sample_count,
      holder_count_estimate,
      concentration_risk,
      source,
      updated_at
    )
    VALUES ($1,$1,$2,$3,$4,$5,$5,$6,'pregrad_largest_account_scan',NOW())
    ON CONFLICT (token_id)
    DO UPDATE SET
      token_address = EXCLUDED.token_address,
      top_holder_pct = COALESCE(
        EXCLUDED.top_holder_pct,
        token_safety_enrichment.top_holder_pct
      ),
      top_5_holders_pct = COALESCE(
        EXCLUDED.top_5_holders_pct,
        token_safety_enrichment.top_5_holders_pct
      ),
      top_10_holders_pct = COALESCE(
        EXCLUDED.top_10_holders_pct,
        token_safety_enrichment.top_10_holders_pct
      ),
      largest_account_sample_count = COALESCE(
        EXCLUDED.largest_account_sample_count,
        token_safety_enrichment.largest_account_sample_count
      ),
      holder_count_estimate = COALESCE(
        EXCLUDED.holder_count_estimate,
        token_safety_enrichment.holder_count_estimate
      ),
      concentration_risk = COALESCE(
        EXCLUDED.concentration_risk,
        token_safety_enrichment.concentration_risk
      ),
      source = EXCLUDED.source,
      updated_at = NOW()
    `,
    [
      tokenAddress,
      concentration.top_holder_pct,
      concentration.top_5_holders_pct,
      concentration.top_10_holders_pct,
      concentration.largest_account_sample_count,
      concentrationRisk,
    ]
  );
}

async function runHolderConcentrationScan(tokenAddress) {
  const [supplyResult, largestResult] = await Promise.all([
    fetchTokenSupply(tokenAddress),
    fetchLargestTokenAccounts(tokenAddress),
  ]);

  const supplyValue = supplyResult?.value;

  const totalSupplyUi =
    supplyValue?.uiAmount != null
      ? Number(supplyValue.uiAmount)
      : supplyValue?.amount != null &&
        supplyValue?.decimals != null
        ? Number(supplyValue.amount) /
          10 ** Number(supplyValue.decimals)
        : 0;

  const concentration = calculateLargestAccountConcentration(
    largestResult?.value || [],
    totalSupplyUi
  );

  const concentrationRisk =
    classifyConcentrationRisk(concentration);

  await upsertHolderConcentration(
    tokenAddress,
    concentration,
    concentrationRisk
  );

  tokenLastHolderEnrichedAt.set(tokenAddress, Date.now());

  logInfo("Holder concentration updated", {
    tokenAddress,
    totalSupplyUi,
    ...concentration,
    concentrationRisk,
  });
}

// ==================================================
// 11. EARLY SUPPLY SNAPSHOTS
// ==================================================

async function calculateEarlySupplySnapshot(
  tokenAddress,
  windowMinutes
) {
  const result = await pool.query(
    `
    WITH token AS (
      SELECT created_at
      FROM pump_launchpad_tokens
      WHERE token_address = $1
      LIMIT 1
    ),

    early_wallets AS (
      SELECT DISTINCT e.wallet_address
      FROM pump_launchpad_events e
      CROSS JOIN token t
      WHERE e.token_address = $1
        AND e.event_type = 'buy'
        AND e.wallet_address IS NOT NULL
        AND e.block_time >= t.created_at
        AND e.block_time <=
          t.created_at + ($2::text || ' minutes')::interval
    ),

    current_positions AS (
      SELECT
        w.wallet_address,
        w.net_tokens
      FROM wallet_token_positions w
      WHERE w.token_id = $1
        AND w.net_tokens > 0
    ),

    total_supply AS (
      SELECT COALESCE(SUM(net_tokens), 0) AS total_net_tokens
      FROM current_positions
    ),

    early_positions AS (
      SELECT
        COUNT(DISTINCT p.wallet_address) AS early_holder_wallet_count,
        COALESCE(SUM(p.net_tokens), 0) AS early_net_tokens
      FROM current_positions p
      JOIN early_wallets ew
        ON ew.wallet_address = p.wallet_address
    )

    SELECT
      ep.early_holder_wallet_count,
      CASE
        WHEN ts.total_net_tokens > 0
          THEN (ep.early_net_tokens / ts.total_net_tokens) * 100
        ELSE NULL
      END AS early_supply_pct
    FROM total_supply ts
    CROSS JOIN early_positions ep
    `,
    [tokenAddress, windowMinutes]
  );

  const row = result.rows[0] || {};

  return {
    early_holder_wallet_count: toNumber(
      row.early_holder_wallet_count,
      null
    ),
    early_supply_pct: normalizePct(
      row.early_supply_pct
    ),
  };
}

async function saveEarlySupplySnapshot(
  tokenAddress,
  windowMinutes
) {
  const snapshot = await calculateEarlySupplySnapshot(
    tokenAddress,
    windowMinutes
  );

  if (windowMinutes === 1) {
    await pool.query(
      `
      INSERT INTO token_safety_enrichment (
        token_id,
        token_address,
        early_holder_wallet_count_1m,
        early_supply_pct_1m,
        early_supply_recorded_at_1m,
        source,
        updated_at
      )
      VALUES ($1,$1,$2,$3,NOW(),'pregrad_early_supply_1m',NOW())
      ON CONFLICT (token_id)
      DO UPDATE SET
        token_address = EXCLUDED.token_address,
        early_holder_wallet_count_1m = COALESCE(
          token_safety_enrichment.early_holder_wallet_count_1m,
          EXCLUDED.early_holder_wallet_count_1m
        ),
        early_supply_pct_1m = COALESCE(
          token_safety_enrichment.early_supply_pct_1m,
          EXCLUDED.early_supply_pct_1m
        ),
        early_supply_recorded_at_1m = COALESCE(
          token_safety_enrichment.early_supply_recorded_at_1m,
          EXCLUDED.early_supply_recorded_at_1m
        ),
        source = CASE
          WHEN token_safety_enrichment.early_supply_recorded_at_1m IS NULL
            THEN EXCLUDED.source
          ELSE token_safety_enrichment.source
        END,
        updated_at = NOW()
      `,
      [
        tokenAddress,
        snapshot.early_holder_wallet_count,
        snapshot.early_supply_pct,
      ]
    );

    stats.earlySnapshot1mRuns += 1;
  } else {
    await pool.query(
      `
      INSERT INTO token_safety_enrichment (
        token_id,
        token_address,
        early_holder_wallet_count_3m,
        early_supply_pct_3m,
        early_supply_recorded_at_3m,
        source,
        updated_at
      )
      VALUES ($1,$1,$2,$3,NOW(),'pregrad_early_supply_3m',NOW())
      ON CONFLICT (token_id)
      DO UPDATE SET
        token_address = EXCLUDED.token_address,
        early_holder_wallet_count_3m = COALESCE(
          token_safety_enrichment.early_holder_wallet_count_3m,
          EXCLUDED.early_holder_wallet_count_3m
        ),
        early_supply_pct_3m = COALESCE(
          token_safety_enrichment.early_supply_pct_3m,
          EXCLUDED.early_supply_pct_3m
        ),
        early_supply_recorded_at_3m = COALESCE(
          token_safety_enrichment.early_supply_recorded_at_3m,
          EXCLUDED.early_supply_recorded_at_3m
        ),
        source = CASE
          WHEN token_safety_enrichment.early_supply_recorded_at_3m IS NULL
            THEN EXCLUDED.source
          ELSE token_safety_enrichment.source
        END,
        updated_at = NOW()
      `,
      [
        tokenAddress,
        snapshot.early_holder_wallet_count,
        snapshot.early_supply_pct,
      ]
    );

    stats.earlySnapshot3mRuns += 1;
  }

  logInfo("Early supply snapshot saved", {
    tokenAddress,
    windowMinutes,
    ...snapshot,
  });
}

function scheduleEarlySupplySnapshots(tokenAddress, createdAt) {
  if (!tokenAddress || !createdAt) return;

  if (tokenScheduledSnapshots.has(tokenAddress)) {
    return;
  }

  const createdAtMs = new Date(createdAt).getTime();

  if (!Number.isFinite(createdAtMs)) {
    return;
  }

  const ageMs = Date.now() - createdAtMs;

  if (ageMs > EARLY_SNAPSHOT_MAX_LATE_MS) {
    return;
  }

  const timers = [];

  const scheduleWindow = (windowMinutes, targetDelayMs) => {
    const delay = Math.max(targetDelayMs - ageMs, 0);

    const timer = setTimeout(() => {
      enqueueHolderJob({
        tokenAddress,
        jobType: `early_${windowMinutes}m`,
        force: true,
      });
    }, delay);

    timers.push(timer);
  };

  scheduleWindow(1, EARLY_SNAPSHOT_1M_DELAY_MS);
  scheduleWindow(3, EARLY_SNAPSHOT_3M_DELAY_MS);

  tokenScheduledSnapshots.set(tokenAddress, timers);
}

// ==================================================
// 12. HOLDER ENRICHMENT QUEUE
// ==================================================

function holderJobKey(job) {
  return `${job.jobType}:${job.tokenAddress}`;
}

function enqueueHolderJob(job) {
  if (
    !TOKEN_SAFETY_ENRICHMENT_ENABLED ||
    !HOLDER_ENRICHMENT_ENABLED ||
    !job?.tokenAddress
  ) {
    return;
  }

  const key = holderJobKey(job);

  if (holderQueuedKeys.has(key)) {
    return;
  }

  holderQueuedKeys.add(key);
  holderQueue.push({
    ...job,
    enqueuedAt: Date.now(),
  });

  stats.holderJobsQueued += 1;
}

async function processHolderJob(job) {
  const { tokenAddress, jobType, force } = job;

  if (jobType === "early_1m") {
    await saveEarlySupplySnapshot(tokenAddress, 1);
    return;
  }

  if (jobType === "early_3m") {
    await saveEarlySupplySnapshot(tokenAddress, 3);
    return;
  }

  const lastRun =
    tokenLastHolderEnrichedAt.get(tokenAddress) || 0;

  if (
    !force &&
    Date.now() - lastRun < HOLDER_REFRESH_COOLDOWN_MS
  ) {
    stats.holderJobsSkippedCooldown += 1;
    return;
  }

  await runHolderConcentrationScan(tokenAddress);
}

async function holderWorkerLoop(workerId) {
  const minimumDelayMs = Math.max(
    Math.floor(
      (1000 / HOLDER_MAX_REQUESTS_PER_SECOND) *
      HOLDER_WORKER_CONCURRENCY
    ),
    50
  );

  while (holderWorkerRunning) {
    const job = holderQueue[holderQueueHead];

    if (!job) {
      await sleep(150);
      continue;
    }

    holderQueueHead += 1;
    holderQueuedKeys.delete(holderJobKey(job));
    compactQueue(holderQueue, "holder");

    try {
      await processHolderJob(job);
      stats.holderJobsProcessed += 1;
    } catch (error) {
      stats.holderJobsFailed += 1;

      logError("Holder enrichment job failed", {
        workerId,
        tokenAddress: job.tokenAddress,
        jobType: job.jobType,
        error: error.message,
      });
    }

    await sleep(minimumDelayMs);
  }
}

function startHolderWorkers() {
  if (holderWorkerRunning) return;

  holderWorkerRunning = true;

  for (
    let index = 0;
    index < HOLDER_WORKER_CONCURRENCY;
    index += 1
  ) {
    holderWorkerPromises.push(
      holderWorkerLoop(index + 1)
    );
  }

  logInfo("Holder workers started", {
    concurrency: HOLDER_WORKER_CONCURRENCY,
    maxRequestsPerSecond: HOLDER_MAX_REQUESTS_PER_SECOND,
  });
}

// ==================================================
// 13. SIGNATURE QUEUE
// ==================================================

function maybePauseIntake() {
  const maxQueueSize = effectiveMaxQueueSize();

  if (
    !intakePaused &&
    getSignatureQueueLength() >= maxQueueSize
  ) {
    intakePaused = true;
    stats.intakePausedCount += 1;

    logInfo("Intake paused", {
      queueSize: getSignatureQueueLength(),
      maxQueueSize,
    });
  }
}

function maybeResumeIntake() {
  if (
    intakePaused &&
    getSignatureQueueLength() <= RESUME_QUEUE_SIZE &&
    isPregradEnabled()
  ) {
    intakePaused = false;
    stats.intakeResumedCount += 1;

    logInfo("Intake resumed", {
      queueSize: getSignatureQueueLength(),
      resumeQueueSize: RESUME_QUEUE_SIZE,
    });
  }
}

function enqueueSignature(signature, slot = null, blockTime = null) {
  if (!signature) return;

  maybeResumeIntake();
  maybePauseIntake();

  if (!isPregradEnabled()) {
    stats.droppedDuringPause += 1;
    return;
  }

  if (intakePaused) {
    stats.droppedDuringPause += 1;
    return;
  }

  if (isSignatureKnown(signature)) {
    stats.droppedDuplicate += 1;
    return;
  }

  if (
    getSignatureQueueLength() >= effectiveMaxQueueSize()
  ) {
    stats.droppedQueueFull += 1;
    maybePauseIntake();
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

function drainStaleQueueItems() {
  const now = Date.now();
  let dropped = 0;

  while (true) {
    const item = signatureQueue[signatureQueueHead];

    if (!item) break;

    if (
      now - item.enqueuedAt <= SIGNATURE_MAX_AGE_MS
    ) {
      break;
    }

    signatureQueueHead += 1;
    queuedSignatures.delete(item.signature);
    dropped += 1;
  }

  if (dropped > 0) {
    stats.droppedStale += dropped;

    logInfo("Dropped stale queue items", {
      dropped,
      queueSize: getSignatureQueueLength(),
    });
  }

  compactQueue(signatureQueue, "signature");
  maybeResumeIntake();
}

async function processQueuedSignature(item) {
  queuedSignatures.delete(item.signature);
  inFlightSignatures.add(item.signature);
  stats.dequeued += 1;

  let permanentlySeen = false;

  try {
    if (
      Date.now() - item.enqueuedAt > SIGNATURE_MAX_AGE_MS
    ) {
      stats.droppedStale += 1;
      return;
    }

    const control = await getPregradControl();

    if (
      !control.helius_enabled ||
      control.manual_override === "OFF"
    ) {
      stats.droppedDuringPause += 1;
      return;
    }

    const tx = await fetchFullTransaction(item.signature);

    if (!tx) {
      throw new Error(
        "Transaction remained null after RPC retries"
      );
    }

    if (tx.meta?.err) {
      stats.skippedFailedTx += 1;
      permanentlySeen = true;
      return;
    }

    const classified = classifyPregradTransaction(
      tx,
      item.signature
    );

    if (!classified.ok) {
      if (classified.reason === "unresolved_token_mint") {
        stats.skippedUnresolvedMint += 1;
      }

      permanentlySeen = true;
      return;
    }

    const event = classified.event;
    const token = classified.tokenUpsert;

    const passesVolumeThreshold =
      !["buy", "sell"].includes(event.event_type) ||
      (
        Number.isFinite(event.sol_amount) &&
        event.sol_amount >= effectiveMinSolAmount()
      );

    if (
      ["buy", "sell"].includes(event.event_type) &&
      !passesVolumeThreshold
    ) {
      stats.smallTradesStored += 1;
    }

    if (
      ["buy", "sell"].includes(event.event_type) &&
      (
        !Number.isFinite(event.sol_amount) ||
        !Number.isFinite(event.token_amount)
      )
    ) {
      stats.skippedUnresolvedTradeAmount += 1;
    }

    await insertRawPregradEvent({
      signature: item.signature,
      slot: tx.slot || item.slot,
      timestamp: tx.blockTime || item.blockTime,
      type: "helius_ws_pregrad_tx",
      payload: tx,
    });

    const persisted = await persistClassifiedTransaction(
      event,
      token,
      passesVolumeThreshold
    );

    permanentlySeen = true;

    if (!persisted.inserted) {
      return;
    }

    if (event.event_type === "create") {
      scheduleEarlySupplySnapshots(
        event.token_address,
        event.block_time
      );

      enqueueHolderJob({
        tokenAddress: event.token_address,
        jobType: "concentration",
        force: true,
      });
    } else {
      enqueueHolderJob({
        tokenAddress: event.token_address,
        jobType: "concentration",
        force: false,
      });
    }

    stats.processed += 1;

    switch (event.event_type) {
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
        break;
      default:
        stats.classifiedUnknown += 1;
    }
  } catch (error) {
    stats.txFetchErrors += 1;

    logError("Failed processing signature", {
      signature: item.signature,
      error: error.message,
    });
  } finally {
    inFlightSignatures.delete(item.signature);

    if (permanentlySeen) {
      addSeenSignature(item.signature);
    }
  }
}

async function queueWorkerLoop(workerId) {
  const minimumDelayMs = Math.max(
    Math.floor(
      (1000 / MAX_TX_PER_SECOND) *
      WORKER_CONCURRENCY
    ),
    15
  );

  while (workerRunning) {
    await getPregradControl();
    drainStaleQueueItems();

    const item = signatureQueue[signatureQueueHead];

    if (!item) {
      maybeResumeIntake();
      await sleep(100);
      continue;
    }

    signatureQueueHead += 1;
    compactQueue(signatureQueue, "signature");

    try {
      await processQueuedSignature(item);
    } catch (error) {
      stats.workerErrors += 1;

      logError("Queue worker error", {
        workerId,
        error: error.message,
      });
    }

    maybeResumeIntake();
    await sleep(minimumDelayMs);
  }
}

function startQueueWorkers() {
  if (workerRunning) return;

  workerRunning = true;

  for (
    let index = 0;
    index < WORKER_CONCURRENCY;
    index += 1
  ) {
    workerPromises.push(
      queueWorkerLoop(index + 1)
    );
  }

  logInfo("Transaction workers started", {
    workerConcurrency: WORKER_CONCURRENCY,
    maxTransactionsPerSecond: MAX_TX_PER_SECOND,
    maxQueueSize: MAX_QUEUE_SIZE,
    signatureMaxAgeMs: SIGNATURE_MAX_AGE_MS,
    pregradTokenSupply: PREGRAD_TOKEN_SUPPLY,
    solPriceUsd:
      SOL_PRICE_USD > 0
        ? SOL_PRICE_USD
        : null,
  });
}

// ==================================================
// 14. WEBSOCKET
// ==================================================

function subscribe(socket) {
  socket.send(
    JSON.stringify({
      jsonrpc: "2.0",
      id: 1,
      method: "logsSubscribe",
      params: [
        {
          mentions: [PUMP_LAUNCHPAD_PROGRAM_ID],
        },
        {
          commitment: "confirmed",
        },
      ],
    })
  );

  logInfo("Sent logsSubscribe", {
    programId: PUMP_LAUNCHPAD_PROGRAM_ID,
  });
}

function cleanupSocket(socket) {
  try {
    socket.removeAllListeners();

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
  socketAlive = true;

  pingInterval = setInterval(() => {
    if (
      !ws ||
      ws.readyState !== WebSocket.OPEN ||
      socketId !== currentSocketId
    ) {
      return;
    }

    if (!socketAlive) {
      logError("WebSocket heartbeat missed", {
        socketId,
      });

      ws.terminate();
      return;
    }

    socketAlive = false;

    try {
      ws.ping();
    } catch (error) {
      logError("WebSocket ping failed", {
        socketId,
        error: error.message,
      });
    }
  }, 30000);
}

function scheduleReconnect(
  reason = "unknown",
  rateLimited = false
) {
  if (intentionalShutdown || reconnectTimeout) {
    return;
  }

  const delay = backoffDelay(retryCount, rateLimited);

  logInfo("Scheduling reconnect", {
    reason,
    retryCount,
    delayMs: delay,
    rateLimited,
  });

  reconnectTimeout = setTimeout(() => {
    reconnectTimeout = null;
    retryCount += 1;
    connect();
  }, delay);
}

function connect() {
  if (intentionalShutdown) return;

  if (
    ws &&
    (
      ws.readyState === WebSocket.OPEN ||
      ws.readyState === WebSocket.CONNECTING
    )
  ) {
    return;
  }

  currentSocketId += 1;

  const socketId = currentSocketId;
  const socket = new WebSocket(WSS_URL);
  ws = socket;

  logInfo("Connecting WebSocket", {
    socketId,
    url: "wss://mainnet.helius-rpc.com/?api-key=***",
  });

  socket.on("open", () => {
    if (socketId !== currentSocketId) {
      cleanupSocket(socket);
      return;
    }

    retryCount = 0;
    socketAlive = true;

    logInfo("WebSocket opened", {
      socketId,
    });

    subscribe(socket);
    startPing(socketId);
  });

  socket.on("pong", () => {
    if (socketId === currentSocketId) {
      socketAlive = true;
    }
  });

  socket.on("message", async (data) => {
    if (socketId !== currentSocketId) {
      return;
    }

    socketAlive = true;

    try {
      const message = JSON.parse(data.toString());

      if (
        typeof message.result === "number" &&
        message.id === 1
      ) {
        logInfo("Subscribed successfully", {
          socketId,
          subscriptionId: message.result,
        });
        return;
      }

      const result = message?.params?.result;
      const value = result?.value;
      const context = result?.context;

      if (
        !value ||
        value.err ||
        !value.signature
      ) {
        return;
      }

      const control = await getPregradControl();

      if (
        !control.helius_enabled ||
        control.manual_override === "OFF"
      ) {
        stats.droppedDuringPause += 1;
        return;
      }

      if (intakePaused) {
        stats.droppedDuringPause += 1;
        maybeResumeIntake();
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
    } catch (error) {
      logError("WebSocket message parse error", {
        socketId,
        error: error.message,
      });
    }
  });

  socket.on("error", (error) => {
    const message =
      error?.message ||
      "unknown_websocket_error";

    logError("WebSocket error", {
      socketId,
      error: message,
      rateLimited: message.includes("429"),
    });
  });

  socket.on("close", (code, reasonBuffer) => {
    if (socketId !== currentSocketId) {
      return;
    }

    stopPing();

    const reason = reasonBuffer?.length
      ? reasonBuffer.toString()
      : "no_reason";

    const rateLimited = reason.includes("429");

    logInfo("WebSocket closed", {
      socketId,
      code,
      reason,
      rateLimited,
    });

    cleanupSocket(socket);
    scheduleReconnect("socket_closed", rateLimited);
  });
}

// ==================================================
// 15. MAINTENANCE / LOGGING
// ==================================================

function startStaleDrainer() {
  if (staleDrainTimer) return;

  staleDrainTimer = setInterval(
    drainStaleQueueItems,
    STALE_DRAIN_INTERVAL_MS
  );
}

async function cleanupRawRetention() {
  if (!STORE_RAW_EVENTS || RAW_RETENTION_COUNT <= 0) {
    return;
  }

  try {
    const result = await pool.query(
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

    if (result.rowCount > 0) {
      logInfo("Raw retention cleanup completed", {
        deleted: result.rowCount,
        retained: RAW_RETENTION_COUNT,
      });
    }
  } catch (error) {
    logError("Raw retention cleanup failed", {
      error: error.message,
    });
  }
}

function startRetentionCleanup() {
  if (!STORE_RAW_EVENTS || retentionTimer) return;

  retentionTimer = setInterval(
    cleanupRawRetention,
    RAW_RETENTION_CLEANUP_MS
  );
}

let lastStats = {
  queued: 0,
  dequeued: 0,
  insertedEvents: 0,
  processed: 0,
};

let lastLogAt = Date.now();

function startQueueLogger() {
  if (queueLogTimer) return;

  queueLogTimer = setInterval(async () => {
    await getPregradControl();

    const now = Date.now();
    const seconds = Math.max(
      (now - lastLogAt) / 1000,
      1
    );

    const oldest = signatureQueue[signatureQueueHead];

    logInfo("Scanner stats", {
      time: nowIso(),
      systemEnabled: isPregradEnabled(),
      manualOverride: pregradControl.manual_override,
      websocketState: ws?.readyState ?? null,
      intakePaused,
      signatureQueueSize: getSignatureQueueLength(),
      holderQueueSize: getHolderQueueLength(),
      oldestSignatureAgeMs: oldest
        ? now - oldest.enqueuedAt
        : 0,

      incomingPerSecond: Number(
        (
          (stats.queued - lastStats.queued) /
          seconds
        ).toFixed(2)
      ),

      drainedPerSecond: Number(
        (
          (stats.dequeued - lastStats.dequeued) /
          seconds
        ).toFixed(2)
      ),

      insertedPerSecond: Number(
        (
          (
            stats.insertedEvents -
            lastStats.insertedEvents
          ) /
          seconds
        ).toFixed(2)
      ),

      processedPerSecond: Number(
        (
          (stats.processed - lastStats.processed) /
          seconds
        ).toFixed(2)
      ),

      ...stats,
    });

    lastStats = {
      queued: stats.queued,
      dequeued: stats.dequeued,
      insertedEvents: stats.insertedEvents,
      processed: stats.processed,
    };

    lastLogAt = now;
  }, QUEUE_LOG_EVERY_MS);
}

function stopTimers() {
  stopPing();

  if (queueLogTimer) {
    clearInterval(queueLogTimer);
    queueLogTimer = null;
  }

  if (staleDrainTimer) {
    clearInterval(staleDrainTimer);
    staleDrainTimer = null;
  }

  if (retentionTimer) {
    clearInterval(retentionTimer);
    retentionTimer = null;
  }

  for (const timers of tokenScheduledSnapshots.values()) {
    for (const timer of timers) {
      clearTimeout(timer);
    }
  }

  tokenScheduledSnapshots.clear();
}

// ==================================================
// 16. HEALTH SERVER
// ==================================================

http
  .createServer(async (request, response) => {
    if (request.url === "/health") {
      try {
        await getPregradControl(true);
        const db = await pool.query("SELECT NOW()");

        response.writeHead(200, {
          "Content-Type": "application/json",
        });

        response.end(
          JSON.stringify({
            ok: true,
            service: "pregrad-pump-scanner",
            dbTime: db.rows[0].now,
            websocketState: ws?.readyState ?? null,
            retryCount,
            socketAlive,
            queueSize: getSignatureQueueLength(),
            holderQueueSize: getHolderQueueLength(),
            intakePaused,
            workerRunning,
            holderWorkerRunning,
            programId: PUMP_LAUNCHPAD_PROGRAM_ID,
            systemEnabled: isPregradEnabled(),
            pregradTokenSupply: PREGRAD_TOKEN_SUPPLY,
            solPriceUsd:
              SOL_PRICE_USD > 0
                ? SOL_PRICE_USD
                : null,
            pregradControl,
            stats,
          })
        );
      } catch (error) {
        response.writeHead(500, {
          "Content-Type": "application/json",
        });

        response.end(
          JSON.stringify({
            ok: false,
            error: error.message,
          })
        );
      }

      return;
    }

    response.writeHead(200, {
      "Content-Type": "text/plain",
    });

    response.end("pregrad pump scanner running");
  })
  .listen(PORT, () => {
    logInfo("HTTP server listening", {
      port: PORT,
    });
  });

// ==================================================
// 17. BOOT / SHUTDOWN
// ==================================================

async function restorePendingEarlySnapshots() {
  const result = await pool.query(
    `
    SELECT
      p.token_address,
      p.created_at,
      s.early_supply_recorded_at_1m,
      s.early_supply_recorded_at_3m
    FROM pump_launchpad_tokens p
    LEFT JOIN token_safety_enrichment s
      ON s.token_id = p.token_address
    WHERE p.created_at >= NOW() - INTERVAL '15 minutes'
      AND p.graduation_status = 'pre_grad'
      AND (
        s.early_supply_recorded_at_1m IS NULL
        OR s.early_supply_recorded_at_3m IS NULL
      )
    `
  );

  for (const row of result.rows) {
    scheduleEarlySupplySnapshots(
      row.token_address,
      row.created_at
    );
  }

  if (result.rowCount > 0) {
    logInfo("Restored pending early snapshots", {
      count: result.rowCount,
    });
  }
}

async function boot() {
  try {
    const test = await pool.query("SELECT NOW()");

    logInfo("Database connected", {
      dbTime: test.rows[0].now,
    });

    await createTables();
    logInfo("Tables ready");

    await getPregradControl(true);

    logInfo("PreGrad control loaded", {
      ...pregradControl,
    });

    await restorePendingEarlySnapshots();

    startQueueWorkers();
    startHolderWorkers();
    startQueueLogger();
    startStaleDrainer();
    startRetentionCleanup();
    connect();
  } catch (error) {
    logError("Boot failed", {
      error: error.message,
    });

    process.exit(1);
  }
}

async function shutdown() {
  if (intentionalShutdown) return;

  intentionalShutdown = true;

  logInfo("Shutting down");

  workerRunning = false;
  holderWorkerRunning = false;

  stopTimers();

  if (reconnectTimeout) {
    clearTimeout(reconnectTimeout);
    reconnectTimeout = null;
  }

  if (ws) {
    cleanupSocket(ws);
  }

  try {
    await Promise.allSettled([
      ...workerPromises,
      ...holderWorkerPromises,
    ]);
  } catch (_) {}

  try {
    await pool.end();
  } catch (_) {}

  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

boot();
