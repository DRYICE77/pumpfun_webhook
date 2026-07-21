// ==================================================
// PRE-GRAD INDEX.JS — PRESERVED INGESTION VERSION
// PART 1 OF 4
//
// Paste Parts 1–4 together in order.
// Core philosophy:
// • Preserve the proven ingestion path.
// • Keep worker concurrency modest.
// • Keep enrichment outside the ingestion critical path.
// • Do not run schema migrations during live startup.
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

// ==================================================
// 2. INGESTION CONTROLS
//
// Purpose:
//
// Configure the live Helius ingestion pipeline.
//
// Philosophy:
//
// • Preserve the proven ingestion flow.
// • Keep worker concurrency conservative.
// • Prevent raw-event storage from blocking trades.
// • Keep retention cleanup outside this service.
// • Allow Railway environment variables to override
//   operational limits without changing code.
//
// ==================================================


// ==================================================
// 2A. RAW EVENT STORAGE
//
// Raw transaction storage is disabled by default.
//
// The scanner's primary responsibility is:
//
// • pump_launchpad_tokens
// • pump_launchpad_events
//
// Raw archival must never block live ingestion.
//
// Any future raw-event retention or cleanup should run
// through a separate maintenance job.
// ==================================================

const STORE_RAW_EVENTS =
  String(
    process.env.STORE_RAW_EVENTS || "false"
  ) === "true";


// ==================================================
// 2B. SIGNATURE QUEUE
//
// Protect the scanner during periods of unusually
// heavy Pump.fun activity.
//
// Intake pauses when the queue reaches its maximum
// and resumes after the backlog falls sufficiently.
// ==================================================

const MAX_QUEUE_SIZE = Number(
  process.env.MAX_QUEUE_SIZE || 5000
);

const RESUME_QUEUE_SIZE = Number(
  process.env.RESUME_QUEUE_SIZE || 2500
);


// ==================================================
// 2C. TRANSACTION WORKERS
//
// Preserve the proven operating profile.
//
// Eight workers provide healthy parallelism without
// overwhelming the Railway PostgreSQL connection pool.
// ==================================================

const WORKER_CONCURRENCY = Number(
  process.env.WORKER_CONCURRENCY || 8
);

const MAX_TX_PER_SECOND = Number(
  process.env.MAX_TX_PER_SECOND || 30
);


// ==================================================
// 2D. QUEUE AGE / MAINTENANCE
//
// Signatures that remain queued too long may no longer
// be useful for real-time analysis.
//
// The stale drainer runs independently from the
// transaction workers.
// ==================================================

const SIGNATURE_MAX_AGE_MS = Number(
  process.env.SIGNATURE_MAX_AGE_MS || 120000
);

const STALE_DRAIN_INTERVAL_MS = Number(
  process.env.STALE_DRAIN_INTERVAL_MS || 1000
);


// ==================================================
// 2E. RUNTIME LOGGING
//
// Emit scanner-health statistics every ten seconds by
// default.
//
// This includes:
//
// • Queue depth
// • Incoming rate
// • Processing rate
// • Insert rate
// • Error counts
// ==================================================

const QUEUE_LOG_EVERY_MS = Number(
  process.env.QUEUE_LOG_EVERY_MS || 10000
);


// ==================================================
// 2F. HELIUS RPC RETRIES
//
// Retry temporary null responses and RPC failures
// without allowing a single transaction to block a
// worker indefinitely.
// ==================================================

const RPC_RETRY_COUNT = Number(
  process.env.RPC_RETRY_COUNT || 3
);

const RPC_RETRY_DELAY_MS = Number(
  process.env.RPC_RETRY_DELAY_MS || 500
);


// ==================================================
// 2G. SYSTEM CONTROL REFRESH
//
// Refresh the shared pre-grad control state at a
// conservative interval.
//
// The safe control cache prevents multiple workers
// from launching duplicate control queries.
// ==================================================

const CONTROL_REFRESH_MS = Number(
  process.env.CONTROL_REFRESH_MS || 5000
);


// ==================================================
// 2H. MINIMUM TRADE SIZE
//
// Buy and sell events below this SOL amount are not
// inserted into the primary event table.
//
// This remains adjustable through:
//
// MIN_SOL_AMOUNT
// ==================================================

const DEFAULT_MIN_SOL_AMOUNT = Number(
  process.env.MIN_SOL_AMOUNT || 0.1
);


// ==================================================
// 2I. PRE-GRAD TOKEN SUPPLY
//
// Pump.fun token supply used for live market-cap
// calculations.
// ==================================================

const PREGRAD_TOKEN_SUPPLY = Number(
  process.env.PREGRAD_TOKEN_SUPPLY || 10000000000
);

// ==================================================
// 2J. SOL PRICE
//
// Optional SOL/USD conversion value.
//
// When unavailable or zero:
//
// • SOL-denominated price remains available.
// • SOL-denominated market cap remains available.
// • USD market fields are left unchanged.
// ==================================================

const SOL_PRICE_USD = Number(
  process.env.SOL_PRICE_USD || 0
);
// ==================================================
// 3. ENRICHMENT CONTROLS
// ==================================================

const TOKEN_SAFETY_ENRICHMENT_ENABLED =
  String(
    process.env.TOKEN_SAFETY_ENRICHMENT_ENABLED || "true"
  ) === "true";

const HOLDER_ENRICHMENT_ENABLED =
  String(
    process.env.HOLDER_ENRICHMENT_ENABLED || "true"
  ) === "true";

const HOLDER_REFRESH_COOLDOWN_MS = Number(
  process.env.HOLDER_REFRESH_COOLDOWN_MS ||
  10 * 60 * 1000
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

// ==================================================
// 4. DATABASE
// ==================================================

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },

  // Keep the pool larger than worker concurrency,
  // but do not encourage excessive parallelism.
  max: Number(process.env.PG_POOL_MAX || 20),

  idleTimeoutMillis: Number(
    process.env.PG_IDLE_TIMEOUT_MS || 30000
  ),

  connectionTimeoutMillis: Number(
    process.env.PG_CONNECT_TIMEOUT_MS || 10000
  ),
});

pool.on("error", (error) => {
  console.error(
    `[pregrad-ws] Unexpected idle PostgreSQL client error ${JSON.stringify({
      error: error.message,
    })}`
  );
});

// ==================================================
// 5. RUNTIME STATE
// ==================================================

let pregradControl = {
  helius_enabled: false,
  manual_override: "OFF",
  max_queue_size: MAX_QUEUE_SIZE,
  min_sol_threshold: DEFAULT_MIN_SOL_AMOUNT,
  updated_at: null,
};

let lastControlFetchAt = 0;
let controlFetchPromise = null;

let ws = null;
let pingInterval = null;
let reconnectTimeout = null;
let retryCount = 0;
let intentionalShutdown = false;
let currentSocketId = 0;
let socketAlive = false;

let intakePaused = false;
let workerRunning = false;

const seenSignatures = new Set();
const queuedSignatures = new Set();
const inFlightSignatures = new Set();

const signatureQueue = [];
const workerPromises = [];

const tokenSafetyEnrichmentInFlight = new Map();
const tokenLastHolderEnrichedAt = new Map();

const SEEN_SIGNATURE_LIMIT = Number(
  process.env.SEEN_SIGNATURE_LIMIT || 100000
);

let queueLogTimer = null;
let staleDrainTimer = null;


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

  skippedSmallSolAmount: 0,
  skippedMarketDataUpdate: 0,
  skippedIrrelevantLog: 0,
  skippedEmptyTx: 0,
  skippedFailedTx: 0,
  skippedUnresolvedMint: 0,

  txFetchErrors: 0,
  workerErrors: 0,
  rpcRetries: 0,
  controlFetchErrors: 0,

  classifiedCreate: 0,
  classifiedBuy: 0,
  classifiedSell: 0,
  classifiedMigrate: 0,
  classifiedUnknown: 0,

  safetyEnrichmentRuns: 0,
  safetyEnrichmentSkippedCooldown: 0,
  safetyEnrichmentErrors: 0,
};

// ==================================================
// 6. LOGGING / BASIC HELPERS
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
  const number = Number(value);
  return Number.isFinite(number)
    ? number
    : fallback;
}

function normalizePct(value) {
  const number = toNumber(value, null);

  return number === null
    ? null
    : Number(number.toFixed(4));
}

function nowIso() {
  return new Date().toISOString();
}

function addSeenSignature(signature) {
  seenSignatures.add(signature);

  if (seenSignatures.size > SEEN_SIGNATURE_LIMIT) {
    const oldest =
      seenSignatures.values().next().value;

    seenSignatures.delete(oldest);
  }
}

function signatureIsKnown(signature) {
  return (
    seenSignatures.has(signature) ||
    queuedSignatures.has(signature) ||
    inFlightSignatures.has(signature)
  );
}

function backoffDelay(
  attempt,
  wasRateLimited = false
) {
  if (wasRateLimited) {
    return Math.min(
      60000 * 2 ** Math.min(attempt, 4),
      600000
    );
  }

  return Math.min(
    2000 * 2 ** Math.min(attempt, 5),
    60000
  );
}

// ==================================================
// 7. SAFE CONTROL CACHE
// ==================================================

async function getPregradControl(force = false) {
  const now = Date.now();

  if (
    !force &&
    now - lastControlFetchAt < CONTROL_REFRESH_MS
  ) {
    return pregradControl;
  }

  // Prevent multiple workers from launching the same
  // control query simultaneously.
  if (controlFetchPromise) {
    return controlFetchPromise;
  }

  controlFetchPromise = (async () => {
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
          helius_enabled:
            result.rows[0].helius_enabled === true,

          manual_override:
            result.rows[0].manual_override || "OFF",

          max_queue_size: Number(
            result.rows[0].max_queue_size ||
            MAX_QUEUE_SIZE
          ),

          min_sol_threshold: Number(
            result.rows[0].min_sol_threshold ||
            DEFAULT_MIN_SOL_AMOUNT
          ),

          updated_at:
            result.rows[0].updated_at,
        };
      }

      lastControlFetchAt = Date.now();
    } catch (error) {
      stats.controlFetchErrors += 1;

      logError(
        "Failed to fetch pregrad control",
        {
          error: error.message,
        }
      );

      // Preserve the last known state.
      // A temporary DB timeout must not automatically
      // switch Helius ingestion off.
    } finally {
      controlFetchPromise = null;
    }

    return pregradControl;
  })();

  return controlFetchPromise;
}

function isPregradEnabled() {
  return (
    pregradControl.helius_enabled === true &&
    pregradControl.manual_override !== "OFF"
  );
}

function effectiveMaxQueueSize() {
  return Number(
    pregradControl.max_queue_size ||
    MAX_QUEUE_SIZE
  );
}

function effectiveMinSolAmount() {
  return Number(
    pregradControl.min_sol_threshold ||
    DEFAULT_MIN_SOL_AMOUNT
  );
}
// ==================================================
// PRE-GRAD INDEX.JS — PRESERVED INGESTION VERSION
// PART 2 OF 4
// Paste immediately after Part 1.
// ==================================================

// ==================================================
// 8. QUEUE MANAGEMENT
// ==================================================

function maybePauseIntake() {
  const maxQueueSize =
    effectiveMaxQueueSize();

  if (
    !intakePaused &&
    signatureQueue.length >= maxQueueSize
  ) {
    intakePaused = true;
    stats.intakePausedCount += 1;

    logInfo("Intake paused", {
      queueSize: signatureQueue.length,
      maxQueueSize,
    });
  }
}

function maybeResumeIntake() {
  if (
    intakePaused &&
    signatureQueue.length <= RESUME_QUEUE_SIZE &&
    isPregradEnabled()
  ) {
    intakePaused = false;
    stats.intakeResumedCount += 1;

    logInfo("Intake resumed", {
      queueSize: signatureQueue.length,
      resumeQueueSize: RESUME_QUEUE_SIZE,
    });
  }
}

function drainStaleQueueItems() {
  const now = Date.now();
  let dropped = 0;

  while (
    signatureQueue.length > 0 &&
    now - signatureQueue[0].enqueuedAt >
      SIGNATURE_MAX_AGE_MS
  ) {
    const item = signatureQueue.shift();

    if (item?.signature) {
      queuedSignatures.delete(item.signature);
    }

    dropped += 1;
  }

  if (dropped > 0) {
    stats.droppedStale += dropped;

    logInfo("Dropped stale queue items", {
      dropped,
      queueSize: signatureQueue.length,
    });
  }

  maybeResumeIntake();
}

function enqueueSignature(
  signature,
  slot = null,
  blockTime = null
) {
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

  if (signatureIsKnown(signature)) {
    stats.droppedDuplicate += 1;
    return;
  }

  if (
    signatureQueue.length >=
    effectiveMaxQueueSize()
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

// ==================================================
// 9. HELIUS RPC
// ==================================================

async function heliusRpc(method, params) {
  const response = await fetch(RPC_URL, {
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

  if (!response.ok) {
    const error = new Error(
      `RPC HTTP error ${response.status}`
    );

    error.status = response.status;
    throw error;
  }

  const json = await response.json();

  if (json.error) {
    throw new Error(
      `RPC error: ${JSON.stringify(json.error)}`
    );
  }

  return json.result;
}

async function fetchFullTransaction(signature) {
  let lastError = null;

  for (
    let attempt = 0;
    attempt <= RPC_RETRY_COUNT;
    attempt += 1
  ) {
    try {
      const transaction = await heliusRpc(
        "getTransaction",
        [
          signature,
          {
            encoding: "jsonParsed",
            maxSupportedTransactionVersion: 0,
            commitment: "confirmed",
          },
        ]
      );

      if (transaction) {
        return transaction;
      }

      if (attempt < RPC_RETRY_COUNT) {
        stats.rpcRetries += 1;

        await sleep(
          RPC_RETRY_DELAY_MS *
          (attempt + 1)
        );
      }
    } catch (error) {
      lastError = error;

      if (attempt < RPC_RETRY_COUNT) {
        stats.rpcRetries += 1;

        const wasRateLimited =
          error?.status === 429 ||
          String(error?.message || "")
            .includes("429");

        await sleep(
          wasRateLimited
            ? backoffDelay(attempt, true)
            : RPC_RETRY_DELAY_MS *
              (attempt + 1)
        );
      }
    }
  }

  if (lastError) {
    throw lastError;
  }

  return null;
}

async function fetchTokenSupply(
  mintAddress
) {
  return heliusRpc(
    "getTokenSupply",
    [mintAddress]
  );
}

async function fetchLargestTokenAccounts(
  mintAddress
) {
  return heliusRpc(
    "getTokenLargestAccounts",
    [mintAddress]
  );
}

// ==================================================
// 10. TRANSACTION PARSING
//
// Purpose:
//
// Convert a hydrated Solana transaction into one
// normalized Pump.fun pre-grad event.
//
// Philosophy:
//
// • Preserve the proven ingestion parser.
// • Require a confirmed Pump.fun mint address.
// • Never guess using an unrelated token account.
// • Keep parsing lightweight and deterministic.
// • Return null for unresolved amounts rather than
//   inventing trade data.
// ==================================================


// ==================================================
// 10A. TRANSACTION ACCESS HELPERS
// ==================================================

function getLogMessages(tx) {
  return tx?.meta?.logMessages || [];
}

function getInstructions(tx) {
  return (
    tx?.transaction?.message?.instructions ||
    []
  );
}

function getInnerInstructions(tx) {
  return tx?.meta?.innerInstructions || [];
}

function getAccountKeyRows(tx) {
  return (
    tx?.transaction?.message?.accountKeys ||
    []
  );
}

function getAccountKeys(tx) {
  return getAccountKeyRows(tx)
    .map((key) =>
      typeof key === "string"
        ? key
        : key?.pubkey
    )
    .filter(Boolean);
}

function getBlockTime(tx) {
  return tx?.blockTime
    ? new Date(tx.blockTime * 1000)
    : new Date();
}

function getSignerWallet(tx) {
  for (const key of getAccountKeyRows(tx)) {
    if (
      typeof key !== "string" &&
      key?.signer === true &&
      key?.pubkey
    ) {
      return key.pubkey;
    }
  }

  return null;
}


// ==================================================
// 10B. PUMP PROGRAM DETECTION
// ==================================================

function txTouchesLaunchpadProgram(tx) {
  if (
    getAccountKeys(tx).includes(
      PUMP_LAUNCHPAD_PROGRAM_ID
    )
  ) {
    return true;
  }

  if (
    getLogMessages(tx).some((line) =>
      String(line).includes(
        PUMP_LAUNCHPAD_PROGRAM_ID
      )
    )
  ) {
    return true;
  }

  return false;
}

function looksRelevantFromLogs(value) {
  const logs = Array.isArray(value?.logs)
    ? value.logs
    : [];

  if (!logs.length) {
    return false;
  }

  return logs.some((line) => {
    const text = String(line);
    const lower = text.toLowerCase();

    return (
      text.includes(
        PUMP_LAUNCHPAD_PROGRAM_ID
      ) ||
      lower.includes("instruction: buy") ||
      lower.includes("instruction: sell") ||
      lower.includes("instruction: create") ||
      lower.includes("create_v2") ||
      lower.includes("migrate") ||
      lower.includes("graduate")
    );
  });
}


// ==================================================
// 10C. EVENT TYPE
// ==================================================

function inferEventTypeFromLogs(tx) {
  const logs = getLogMessages(tx).map(
    (line) =>
      String(line).toLowerCase()
  );

  const hasCreate = logs.some(
    (line) =>
      line.includes("instruction: create") ||
      line.includes("create_v2")
  );

  const hasBuy = logs.some(
    (line) =>
      line.includes("instruction: buy")
  );

  const hasSell = logs.some(
    (line) =>
      line.includes("instruction: sell")
  );

  const hasMigrate = logs.some(
    (line) =>
      line.includes("migrate") ||
      line.includes("graduate")
  );

  if (hasCreate) {
    return "create";
  }

  if (hasMigrate) {
    return "migrate";
  }

  if (hasBuy && !hasSell) {
    return "buy";
  }

  if (hasSell && !hasBuy) {
    return "sell";
  }

  return "unknown";
}


// ==================================================
// 10D. PUMP MINT IDENTIFICATION
//
// Only confirmed Pump.fun mint addresses are accepted.
//
// We intentionally do not fall back to the first
// arbitrary token-balance address because that can be:
//
// • A wrapped SOL mint
// • An unrelated token mint
// • A vault-related account
// • Another token touched by the transaction
//
// This prevents invalid holder-enrichment requests.
// ==================================================

function getMintCandidatesFromTokenBalances(tx) {
  const candidates = new Set();

  const collect = (rows) => {
    for (const row of rows || []) {
      const mint = row?.mint;

      if (
        typeof mint !== "string" ||
        !mint
      ) {
        continue;
      }

      if (
        mint ===
        "So11111111111111111111111111111111111111112"
      ) {
        continue;
      }

      candidates.add(mint);
    }
  };

  collect(
    tx?.meta?.preTokenBalances
  );

  collect(
    tx?.meta?.postTokenBalances
  );

  return [...candidates];
}

function inferPrimaryMint(tx) {
  const candidates =
    getMintCandidatesFromTokenBalances(tx);

  const pumpMint = candidates.find(
    (mint) =>
      typeof mint === "string" &&
      mint.endsWith("pump")
  );

  return pumpMint || null;
}


// ==================================================
// 10E. CREATE METADATA
// ==================================================

function parseCreateMetadata(tx) {
  let name = null;
  let symbol = null;

  const scan = (instruction) => {
    const info =
      instruction?.parsed?.info || {};

    if (
      !name &&
      typeof info.name === "string"
    ) {
      name = info.name;
    }

    if (
      !symbol &&
      typeof info.symbol === "string"
    ) {
      symbol = info.symbol;
    }
  };

  for (
    const instruction of getInstructions(tx)
  ) {
    scan(instruction);
  }

  for (
    const group of getInnerInstructions(tx)
  ) {
    for (
      const instruction of
      group?.instructions || []
    ) {
      scan(instruction);
    }
  }

  return {
    name,
    symbol,
  };
}


// ==================================================
// 10F. SOL AMOUNT
//
// Preserve the proven transfer-based parser.
//
// Priority:
//
// 1. Parsed wrapped-SOL token transfer
// 2. Native lamport transfer
// 3. Pump log fallback
// ==================================================

function extractSolAmount(
  tx,
  eventType
) {
  const SOL_MINT =
    "So11111111111111111111111111111111111111112";

  let largestSolAmount = null;

  const recordAmount = (value) => {
    const amount = Number(value);

    if (
      !Number.isFinite(amount) ||
      amount <= 0
    ) {
      return;
    }

    largestSolAmount =
      largestSolAmount === null
        ? amount
        : Math.max(
            largestSolAmount,
            amount
          );
  };

  const scan = (instruction) => {
    const info =
      instruction?.parsed?.info || {};

    if (info.mint === SOL_MINT) {
      const raw =
        info?.tokenAmount?.uiAmount ??
        info?.uiAmount ??
        (
          info?.amount != null &&
          info?.decimals === 9
            ? Number(info.amount) / 1e9
            : null
        );

      recordAmount(raw);
    }

    if (info.lamports != null) {
      recordAmount(
        Number(info.lamports) / 1e9
      );
    }
  };

  for (
    const instruction of getInstructions(tx)
  ) {
    scan(instruction);
  }

  for (
    const group of getInnerInstructions(tx)
  ) {
    for (
      const instruction of
      group?.instructions || []
    ) {
      scan(instruction);
    }
  }

  if (largestSolAmount !== null) {
    return largestSolAmount;
  }

  const logs =
    getLogMessages(tx).join("\n");

  const amountInMatch =
    logs.match(
      /amount_in:\s*([0-9]+)/i
    );

  if (!amountInMatch) {
    return null;
  }

  const raw =
    Number(amountInMatch[1]);

  if (
    !Number.isFinite(raw) ||
    raw <= 0
  ) {
    return null;
  }

  return eventType === "buy"
    ? raw / 1e9
    : raw / 1e6;
}


// ==================================================
// 10G. TOKEN AMOUNT
// ==================================================

function extractTokenAmount(
  tx,
  tokenAddress
) {
  if (!tokenAddress) {
    return null;
  }

  let largestTokenAmount = null;

  const recordAmount = (value) => {
    const amount = Number(value);

    if (
      !Number.isFinite(amount) ||
      amount <= 0
    ) {
      return;
    }

    largestTokenAmount =
      largestTokenAmount === null
        ? amount
        : Math.max(
            largestTokenAmount,
            amount
          );
  };

  const scan = (instruction) => {
    const info =
      instruction?.parsed?.info || {};

    if (
      info.mint !== tokenAddress
    ) {
      return;
    }

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

    recordAmount(raw);
  };

  for (
    const instruction of getInstructions(tx)
  ) {
    scan(instruction);
  }

  for (
    const group of getInnerInstructions(tx)
  ) {
    for (
      const instruction of
      group?.instructions || []
    ) {
      scan(instruction);
    }
  }

  return largestTokenAmount;
}


// ==================================================
// 10H. FINAL EVENT CLASSIFICATION
// ==================================================

function classifyPregradEvent(
  tx,
  signature
) {
  if (
    !tx ||
    !tx.meta ||
    !tx.transaction
  ) {
    return {
      ok: false,
      reason:
        "missing_transaction_fields",
    };
  }

  if (tx.meta.err) {
    return {
      ok: false,
      reason:
        "transaction_failed",
    };
  }

  if (!txTouchesLaunchpadProgram(tx)) {
    return {
      ok: false,
      reason:
        "not_launchpad_program",
    };
  }

  const eventType =
    inferEventTypeFromLogs(tx);

  const tokenAddress =
    inferPrimaryMint(tx);

  if (!tokenAddress) {
    return {
      ok: false,
      reason:
        "unresolved_token_mint",
    };
  }

  const walletAddress =
    getSignerWallet(tx);

  const { name, symbol } =
    parseCreateMetadata(tx);

  const solAmount =
    extractSolAmount(
      tx,
      eventType
    );

  const tokenAmount =
    extractTokenAmount(
      tx,
      tokenAddress
    );

  const pricePerToken =
    Number.isFinite(solAmount) &&
    solAmount > 0 &&
    Number.isFinite(tokenAmount) &&
    tokenAmount > 0
      ? solAmount / tokenAmount
      : null;

  const blockTime =
    getBlockTime(tx);

  const isMigrate =
    eventType === "migrate";

  return {
    ok: true,

    event: {
      token_address:
        tokenAddress,

      signature,

      slot:
        tx.slot || null,

      block_time:
        blockTime,

      event_type:
        eventType,

      wallet_address:
        walletAddress,

      sol_amount:
        solAmount,

      token_amount:
        tokenAmount,

      price_per_token:
        pricePerToken,

      raw_json:
        tx,
    },

    tokenUpsert: {
      token_address:
        tokenAddress,

      creator_wallet:
        eventType === "create"
          ? walletAddress
          : null,

      symbol,
      name,

      token_program:
        null,

      created_at:
        blockTime,

      first_seen_signature:
        signature,

      first_seen_slot:
        tx.slot || null,

      graduation_status:
        isMigrate
          ? "graduated"
          : "pre_grad",

      market_phase:
        isMigrate
          ? "JUST_GRADUATED"
          : "PRE_GRAD",

      last_event_type:
        eventType,

      last_seen_at:
        blockTime,

      graduated_at:
        isMigrate
          ? blockTime
          : null,
    },
  };
}
// ==================================================
// PRE-GRAD INDEX.JS — PRESERVED INGESTION VERSION
// PART 3 OF 4
// Paste immediately after Part 2.
// ==================================================

// ==================================================
// 11. PROVEN DATABASE WRITE PATH
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
    [
      signature,
      slot,
      timestamp,
      type,
      payload,
    ]
  );
}

async function upsertLaunchpadToken(token) {
  if (!token?.token_address) {
    return false;
  }

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
      graduated_at,
      market_phase,
      last_event_type,
      last_seen_at,
      updated_at
    )
    VALUES (
      $1,$2,$3,$4,$5,$6,$7,
      $8,$9,$10,$11,$12,$13,NOW()
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

      created_at = COALESCE(
        pump_launchpad_tokens.created_at,
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
        WHEN
          pump_launchpad_tokens.graduation_status =
          'graduated'
          THEN 'graduated'
        ELSE EXCLUDED.graduation_status
      END,

      graduated_at = COALESCE(
        pump_launchpad_tokens.graduated_at,
        EXCLUDED.graduated_at
      ),

      market_phase = CASE
        WHEN
          pump_launchpad_tokens.market_phase IN (
            'JUST_GRADUATED',
            'POST_GRAD'
          )
          THEN
            pump_launchpad_tokens.market_phase
        ELSE EXCLUDED.market_phase
      END,

      last_event_type = COALESCE(
        EXCLUDED.last_event_type,
        pump_launchpad_tokens.last_event_type
      ),

      last_seen_at = COALESCE(
        EXCLUDED.last_seen_at,
        pump_launchpad_tokens.last_seen_at
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
      sol_amount,
      token_amount,
      price_per_token,
      raw_json
    )
    VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10
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
      event.price_per_token,
      STORE_RAW_EVENTS
        ? event.raw_json
        : null,
    ]
  );

  if (result.rowCount > 0) {
    stats.insertedEvents += 1;
    return true;
  }

  return false;
}

async function updateLaunchpadMarketDataFromEvent(
  event
) {
  if (!event?.token_address) {
    return false;
  }

  const priceSol = Number(
    event.price_per_token || 0
  );

  if (
    !Number.isFinite(priceSol) ||
    priceSol <= 0
  ) {
    stats.skippedMarketDataUpdate += 1;
    return false;
  }

  const marketCapSol =
    priceSol * PREGRAD_TOKEN_SUPPLY;

  const hasUsd =
    Number.isFinite(SOL_PRICE_USD) &&
    SOL_PRICE_USD > 0;

  const latestPriceUsd =
    hasUsd
      ? priceSol * SOL_PRICE_USD
      : null;

  const marketCapUsd =
    hasUsd
      ? marketCapSol * SOL_PRICE_USD
      : null;

  await pool.query(
    `
    UPDATE pump_launchpad_tokens
    SET
      latest_price_sol = $2,
      market_cap_sol = $3,

      latest_price = COALESCE(
        $4,
        latest_price
      ),

      market_cap_usd = COALESCE(
        $5,
        market_cap_usd
      ),

      fdv_usd = COALESCE(
        $5,
        fdv_usd
      ),

      ath_market_cap_sol = GREATEST(
        COALESCE(ath_market_cap_sol, 0),
        $3
      ),

      atl_market_cap_sol = CASE
        WHEN
          atl_market_cap_sol IS NULL
          OR atl_market_cap_sol = 0
          THEN $3
        ELSE LEAST(
          atl_market_cap_sol,
          $3
        )
      END,

      ath_market_cap_usd = CASE
        WHEN $5 IS NULL
          THEN ath_market_cap_usd
        ELSE GREATEST(
          COALESCE(ath_market_cap_usd, 0),
          $5
        )
      END,

      atl_market_cap_usd = CASE
        WHEN $5 IS NULL
          THEN atl_market_cap_usd
        WHEN
          atl_market_cap_usd IS NULL
          OR atl_market_cap_usd = 0
          THEN $5
        ELSE LEAST(
          atl_market_cap_usd,
          $5
        )
      END,

      updated_market_data_at = NOW(),
      updated_at = NOW()

    WHERE token_address = $1
    `,
    [
      event.token_address,
      priceSol,
      marketCapSol,
      latestPriceUsd,
      marketCapUsd,
    ]
  );

  await pool.query(
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
      hasUsd
        ? SOL_PRICE_USD
        : null,
    ]
  );

  stats.updatedMarketData += 1;
  return true;
}

async function markTokenGraduated(
  tokenAddress,
  graduatedAt
) {
  if (!tokenAddress) return;

  await pool.query(
    `
    UPDATE pump_launchpad_tokens
    SET
      graduation_status = 'graduated',
      market_phase = 'JUST_GRADUATED',

      graduated_at = COALESCE(
        graduated_at,
        $2
      ),

      last_event_type = 'migrate',

      last_seen_at = COALESCE(
        $2,
        NOW()
      ),

      updated_at = NOW()

    WHERE token_address = $1
    `,
    [
      tokenAddress,
      graduatedAt,
    ]
  );
}

// ==================================================
// 12. HOLDER ENRICHMENT
//
// Purpose:
//
// Collect lightweight holder-concentration data for
// confirmed Pump.fun token mints.
//
// Philosophy:
//
// • Never block live trade ingestion.
// • Never await enrichment from the event pipeline.
// • Prevent duplicate concurrent scans.
// • Respect the existing refresh cooldown.
// • Treat unavailable mint accounts as recoverable.
// • Preserve the last successful enrichment data.
// ==================================================


// ==================================================
// 12A. LARGEST ACCOUNT AMOUNT PARSER
// ==================================================

function parseLargestAccountUiAmount(row) {
  if (row?.uiAmount != null) {
    return toNumber(
      row.uiAmount,
      0
    );
  }

  if (row?.uiAmountString != null) {
    return toNumber(
      row.uiAmountString,
      0
    );
  }

  if (
    row?.amount != null &&
    row?.decimals != null
  ) {
    const amount =
      Number(row.amount);

    const decimals =
      Number(row.decimals);

    if (
      Number.isFinite(amount) &&
      Number.isFinite(decimals)
    ) {
      return (
        amount /
        10 ** decimals
      );
    }
  }

  return 0;
}


// ==================================================
// 12B. HOLDER CONCENTRATION
//
// Helius getTokenLargestAccounts returns only a
// limited account sample.
//
// holder_count_estimate therefore represents the
// sampled account count, not the total token-holder
// population.
// ==================================================

function calculateHolderConcentration(
  largestAccounts,
  totalSupplyUi
) {
  const supply =
    toNumber(
      totalSupplyUi,
      0
    );

  if (
    !Array.isArray(largestAccounts) ||
    largestAccounts.length === 0 ||
    supply <= 0
  ) {
    return {
      top_holder_pct:
        null,

      top_5_holders_pct:
        null,

      top_10_holders_pct:
        null,

      holder_count_estimate:
        0,
    };
  }

  const amounts = largestAccounts
    .map(
      parseLargestAccountUiAmount
    )
    .filter(
      (amount) =>
        Number.isFinite(amount) &&
        amount > 0
    )
    .sort(
      (a, b) =>
        b - a
    );

  if (!amounts.length) {
    return {
      top_holder_pct:
        null,

      top_5_holders_pct:
        null,

      top_10_holders_pct:
        null,

      holder_count_estimate:
        0,
    };
  }

  const sum = (values) =>
    values.reduce(
      (total, value) =>
        total + value,
      0
    );

  return {
    top_holder_pct:
      normalizePct(
        (
          amounts[0] /
          supply
        ) * 100
      ),

    top_5_holders_pct:
      normalizePct(
        (
          sum(
            amounts.slice(0, 5)
          ) /
          supply
        ) * 100
      ),

    top_10_holders_pct:
      normalizePct(
        (
          sum(
            amounts.slice(0, 10)
          ) /
          supply
        ) * 100
      ),

    holder_count_estimate:
  null,
  };
}


// ==================================================
// 12C. CONCENTRATION RISK
// ==================================================

function classifyConcentrationRisk(
  concentration
) {
  const top1 = toNumber(
    concentration?.top_holder_pct,
    0
  );

  const top5 = toNumber(
    concentration?.top_5_holders_pct,
    0
  );

  const top10 = toNumber(
    concentration?.top_10_holders_pct,
    0
  );

  if (
    top1 >= 25 ||
    top5 >= 70 ||
    top10 >= 90
  ) {
    return "high";
  }

  if (
    top1 >=
      HOLDER_MIN_TOP1_RISK_PCT ||
    top5 >=
      HOLDER_MIN_TOP5_RISK_PCT ||
    top10 >=
      HOLDER_MIN_TOP10_RISK_PCT
  ) {
    return "medium";
  }

  return "low";
}


// ==================================================
// 12D. SAFETY ENRICHMENT WRITE
//
// New non-null values replace prior values.
//
// Missing values never erase existing enrichment.
// ==================================================

async function upsertTokenSafetyEnrichment({
  tokenAddress,
  concentration,
  concentrationRisk,
}) {
  if (!tokenAddress) {
    return;
  }

  await pool.query(
    `
    INSERT INTO token_safety_enrichment (
      token_id,
      token_address,
      top_holder_pct,
      top_5_holders_pct,
      top_10_holders_pct,
      holder_count_estimate,
      concentration_risk,
      source,
      updated_at
    )
    VALUES (
      $1,
      $1,
      $2,
      $3,
      $4,
      $5,
      $6,
      'pregrad_holder_scan',
      NOW()
    )

    ON CONFLICT (token_id)
    DO UPDATE SET
      token_address =
        EXCLUDED.token_address,

      top_holder_pct =
        COALESCE(
          EXCLUDED.top_holder_pct,
          token_safety_enrichment.top_holder_pct
        ),

      top_5_holders_pct =
        COALESCE(
          EXCLUDED.top_5_holders_pct,
          token_safety_enrichment.top_5_holders_pct
        ),

      top_10_holders_pct =
        COALESCE(
          EXCLUDED.top_10_holders_pct,
          token_safety_enrichment.top_10_holders_pct
        ),

      holder_count_estimate =
        COALESCE(
          EXCLUDED.holder_count_estimate,
          token_safety_enrichment.holder_count_estimate
        ),

      concentration_risk =
        COALESCE(
          EXCLUDED.concentration_risk,
          token_safety_enrichment.concentration_risk
        ),

      source =
        EXCLUDED.source,

      updated_at =
        NOW()
    `,
    [
      tokenAddress,

      concentration?.top_holder_pct ??
        null,

      concentration?.top_5_holders_pct ??
        null,

      concentration?.top_10_holders_pct ??
        null,

      concentration?.holder_count_estimate ??
        null,

      concentrationRisk ??
        null,
    ]
  );
}


// ==================================================
// 12E. RPC ERROR CLASSIFICATION
//
// Some very new token mints may not yet be available
// through getTokenSupply or getTokenLargestAccounts.
//
// These should be treated as temporary unavailable
// accounts rather than fatal scanner errors.
// ==================================================

function isHolderAccountUnavailableError(
  error
) {
  const message =
    String(
      error?.message || ""
    ).toLowerCase();

  return (
    message.includes(
      "could not find account"
    ) ||
    message.includes(
      "account not found"
    ) ||
    message.includes(
      "invalid param"
    )
  );
}


// ==================================================
// 12F. HOLDER ENRICHMENT RUN
// ==================================================

async function enrichTokenHolderConcentration(
  tokenAddress
) {
  if (
    !TOKEN_SAFETY_ENRICHMENT_ENABLED ||
    !HOLDER_ENRICHMENT_ENABLED ||
    !tokenAddress
  ) {
    return;
  }

  const now = Date.now();

  const lastRun =
    tokenLastHolderEnrichedAt.get(
      tokenAddress
    ) || 0;

  if (
    now - lastRun <
    HOLDER_REFRESH_COOLDOWN_MS
  ) {
    stats.safetyEnrichmentSkippedCooldown += 1;
    return;
  }

  if (
    tokenSafetyEnrichmentInFlight.has(
      tokenAddress
    )
  ) {
    return tokenSafetyEnrichmentInFlight.get(
      tokenAddress
    );
  }

  const runPromise = (async () => {
    try {
      const [
        supplyResult,
        largestResult,
      ] = await Promise.all([
        fetchTokenSupply(
          tokenAddress
        ),

        fetchLargestTokenAccounts(
          tokenAddress
        ),
      ]);

      const supplyValue =
        supplyResult?.value;

      const totalSupplyUi =
        supplyValue?.uiAmount != null
          ? Number(
              supplyValue.uiAmount
            )
          : (
              supplyValue?.amount != null &&
              supplyValue?.decimals != null
            )
            ? (
                Number(
                  supplyValue.amount
                ) /
                10 **
                Number(
                  supplyValue.decimals
                )
              )
            : 0;

      if (
        !Number.isFinite(
          totalSupplyUi
        ) ||
        totalSupplyUi <= 0
      ) {
        throw new Error(
          "Holder supply unavailable"
        );
      }

      const concentration =
        calculateHolderConcentration(
          largestResult?.value || [],
          totalSupplyUi
        );

      const concentrationRisk =
        classifyConcentrationRisk(
          concentration
        );

      await upsertTokenSafetyEnrichment({
        tokenAddress,
        concentration,
        concentrationRisk,
      });

      tokenLastHolderEnrichedAt.set(
        tokenAddress,
        Date.now()
      );

      stats.safetyEnrichmentRuns += 1;

      logInfo(
        "Token holder enrichment updated",
        {
          tokenAddress,
          totalSupplyUi,
          ...concentration,
          concentrationRisk,
        }
      );
    } catch (error) {
      stats.safetyEnrichmentErrors += 1;

      if (
        isHolderAccountUnavailableError(
          error
        )
      ) {
        // Apply the normal cooldown so the same
        // unavailable account is not retried on
        // every incoming transaction.
        tokenLastHolderEnrichedAt.set(
          tokenAddress,
          Date.now()
        );

        logInfo(
          "Holder enrichment account unavailable",
          {
            tokenAddress,
          }
        );

        return;
      }

      logError(
        "Failed token holder enrichment",
        {
          tokenAddress,

          error:
            String(
              error?.message ||
              error
            ),
        }
      );
    } finally {
      tokenSafetyEnrichmentInFlight.delete(
        tokenAddress
      );
    }
  })();

  tokenSafetyEnrichmentInFlight.set(
    tokenAddress,
    runPromise
  );

  return runPromise;
}


// ==================================================
// 12G. ASYNC ENRICHMENT DISPATCH
//
// This function must never be awaited by the live
// transaction-ingestion pipeline.
// ==================================================

function dispatchTokenSafetyEnrichment(
  tokenAddress
) {
  if (
    !tokenAddress ||
    !TOKEN_SAFETY_ENRICHMENT_ENABLED ||
    !HOLDER_ENRICHMENT_ENABLED
  ) {
    return;
  }

  enrichTokenHolderConcentration(
    tokenAddress
  ).catch((error) => {
    logError(
      "Async holder enrichment dispatch failed",
      {
        tokenAddress,

        error:
          String(
            error?.message ||
            error
          ),
      }
    );
  });
}

// ==================================================
// 13. SIGNATURE PROCESSING
//
// Purpose:
//
// Process one queued Helius transaction signature
// through the proven pre-grad ingestion pipeline.
//
// Order:
//
// 1. Validate queue item
// 2. Fetch hydrated transaction
// 3. Optionally store raw transaction
// 4. Classify Pump.fun event
// 5. Apply minimum trade threshold
// 6. Upsert token
// 7. Insert event
// 8. Update market data
// 9. Mark migration
// 10. Dispatch async holder enrichment
//
// Philosophy:
//
// • Preserve successful trade ingestion.
// • Never let enrichment block event insertion.
// • Never retry permanently invalid transactions.
// • Allow temporary fetch failures to be retried later.
// • Enrich only create and buy events.
// ==================================================


// ==================================================
// 13A. PROCESS QUEUED SIGNATURE
// ==================================================

async function processQueuedSignature(item) {
  if (!item?.signature) {
    return;
  }

  const signature =
    item.signature;

  queuedSignatures.delete(
    signature
  );

  if (
    seenSignatures.has(signature) ||
    inFlightSignatures.has(signature)
  ) {
    stats.droppedDuplicate += 1;
    return;
  }

  if (
    Date.now() -
      item.enqueuedAt >
    SIGNATURE_MAX_AGE_MS
  ) {
    stats.droppedStale += 1;
    return;
  }

  if (!isPregradEnabled()) {
    stats.droppedDuringPause += 1;
    return;
  }

  inFlightSignatures.add(
    signature
  );

  stats.dequeued += 1;

  let permanentlySeen = false;

  try {
    // ----------------------------------------------
    // FETCH HYDRATED TRANSACTION
    // ----------------------------------------------

    const tx =
      await fetchFullTransaction(
        signature
      );

    if (!tx) {
      stats.skippedEmptyTx += 1;

      // Leave the signature eligible for a future
      // retry because this may be a temporary RPC
      // availability issue.
      return;
    }

    if (tx.meta?.err) {
      stats.skippedFailedTx += 1;
      permanentlySeen = true;
      return;
    }


    // ----------------------------------------------
    // OPTIONAL RAW STORAGE
    //
    // STORE_RAW_EVENTS is disabled by default.
    // ----------------------------------------------

    if (STORE_RAW_EVENTS) {
      await insertRawPregradEvent({
        signature,

        slot:
          tx.slot ||
          item.slot ||
          null,

        timestamp:
          tx.blockTime ||
          item.blockTime ||
          null,

        type:
          "helius_ws_pregrad_tx",

        payload:
          tx,
      });
    }


    // ----------------------------------------------
    // CLASSIFY PUMP.FUN EVENT
    // ----------------------------------------------

    const classified =
      classifyPregradEvent(
        tx,
        signature
      );

    if (!classified.ok) {
      if (
        classified.reason ===
        "unresolved_token_mint"
      ) {
        stats.skippedUnresolvedMint += 1;
      }

      permanentlySeen = true;
      return;
    }

    const event =
      classified.event;

    const token =
      classified.tokenUpsert;


    // ----------------------------------------------
    // MINIMUM TRADE SIZE
    //
    // Create and migrate events are always allowed.
    // Only buy and sell events use the SOL threshold.
    // ----------------------------------------------

    if (
      ["buy", "sell"].includes(
        event.event_type
      )
    ) {
      const minSolAmount =
        effectiveMinSolAmount();

      const solAmount =
        Number(
          event.sol_amount
        );

      if (
        !Number.isFinite(solAmount) ||
        solAmount <
          minSolAmount
      ) {
        stats.skippedSmallSolAmount += 1;
        permanentlySeen = true;
        return;
      }
    }


    // ----------------------------------------------
    // PRIMARY TOKEN WRITE
    // ----------------------------------------------

    await upsertLaunchpadToken(
      token
    );


    // ----------------------------------------------
    // PRIMARY EVENT WRITE
    // ----------------------------------------------

    const inserted =
      await insertLaunchpadEvent(
        event
      );

    permanentlySeen = true;

    if (!inserted) {
      return;
    }


    // ----------------------------------------------
    // LIVE MARKET DATA
    // ----------------------------------------------

    if (
      ["buy", "sell"].includes(
        event.event_type
      )
    ) {
      await updateLaunchpadMarketDataFromEvent(
        event
      );
    }


    // ----------------------------------------------
    // GRADUATION
    // ----------------------------------------------

    if (
      event.event_type ===
      "migrate"
    ) {
      await markTokenGraduated(
        event.token_address,
        event.block_time
      );
    }


    // ----------------------------------------------
    // ASYNC HOLDER ENRICHMENT
    //
    // Only creates and buys initiate holder scans.
    //
    // Sells and migrations do not need to trigger
    // another holder request.
    //
    // This call is intentionally never awaited.
    // ----------------------------------------------

    if (
      event.event_type ===
        "create" ||
      event.event_type ===
        "buy"
    ) {
      dispatchTokenSafetyEnrichment(
        event.token_address
      );
    }


    // ----------------------------------------------
    // SUCCESS STATS
    // ----------------------------------------------

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
        break;
    }
  } catch (error) {
    stats.txFetchErrors += 1;

    logError(
      "Failed processing signature",
      {
        signature,

        error:
          String(
            error?.message ||
            error
          ),
      }
    );
  } finally {
    inFlightSignatures.delete(
      signature
    );

    if (permanentlySeen) {
      addSeenSignature(
        signature
      );
    }
  }
}
// ==================================================
// PRE-GRAD INDEX.JS — PRESERVED INGESTION VERSION
// PART 4 OF 4
// Paste immediately after Part 3.
// ==================================================

// ==================================================
// 14. WORKERS
// ==================================================

async function queueWorkerLoop(workerId) {
  const minimumDelayMs = Math.max(
    Math.floor(
      (
        1000 /
        MAX_TX_PER_SECOND
      ) *
      WORKER_CONCURRENCY
    ),
    15
  );

  while (workerRunning) {
    drainStaleQueueItems();

    const item =
      signatureQueue.shift();

    if (!item) {
      maybeResumeIntake();
      await sleep(100);
      continue;
    }

    try {
      await processQueuedSignature(item);
    } catch (error) {
      stats.workerErrors += 1;

      logError(
        "Queue worker error",
        {
          workerId,
          error: error.message,
        }
      );
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
    workerConcurrency:
      WORKER_CONCURRENCY,

    maxTransactionsPerSecond:
      MAX_TX_PER_SECOND,

    maxQueueSize:
      MAX_QUEUE_SIZE,

    resumeQueueSize:
      RESUME_QUEUE_SIZE,

    signatureMaxAgeMs:
      SIGNATURE_MAX_AGE_MS,

    pregradTokenSupply:
      PREGRAD_TOKEN_SUPPLY,

    solPriceUsd:
      SOL_PRICE_USD > 0
        ? SOL_PRICE_USD
        : null,
  });
}

// ==================================================
// 15. WEBSOCKET
// ==================================================

function subscribe(socket) {
  socket.send(
    JSON.stringify({
      jsonrpc: "2.0",
      id: 1,
      method: "logsSubscribe",

      params: [
        {
          mentions: [
            PUMP_LAUNCHPAD_PROGRAM_ID,
          ],
        },

        {
          commitment: "confirmed",
        },
      ],
    })
  );

  logInfo("Sent logsSubscribe", {
    programId:
      PUMP_LAUNCHPAD_PROGRAM_ID,
  });
}

function cleanupSocket(socket) {
  try {
    socket.removeAllListeners();

    if (
      socket.readyState ===
        WebSocket.OPEN ||
      socket.readyState ===
        WebSocket.CONNECTING
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
      ws.readyState !==
        WebSocket.OPEN ||
      socketId !== currentSocketId
    ) {
      return;
    }

    if (!socketAlive) {
      logError(
        "WebSocket heartbeat missed",
        {
          socketId,
        }
      );

      ws.terminate();
      return;
    }

    socketAlive = false;

    try {
      ws.ping();
    } catch (error) {
      logError(
        "WebSocket ping failed",
        {
          socketId,
          error: error.message,
        }
      );
    }
  }, 30000);
}

function scheduleReconnect(
  reason = "unknown",
  wasRateLimited = false
) {
  if (
    intentionalShutdown ||
    reconnectTimeout
  ) {
    return;
  }

  const delay = backoffDelay(
    retryCount,
    wasRateLimited
  );

  logInfo("Scheduling reconnect", {
    reason,
    retryCount,
    delayMs: delay,
    wasRateLimited,
  });

  reconnectTimeout = setTimeout(
    () => {
      reconnectTimeout = null;
      retryCount += 1;
      connect();
    },
    delay
  );
}

function connect() {
  if (intentionalShutdown) {
    return;
  }

  if (
    ws &&
    (
      ws.readyState ===
        WebSocket.OPEN ||
      ws.readyState ===
        WebSocket.CONNECTING
    )
  ) {
    return;
  }

  currentSocketId += 1;

  const socketId =
    currentSocketId;

  const socket =
    new WebSocket(WSS_URL);

  ws = socket;

  logInfo("Connecting WebSocket", {
    socketId,
    url:
      "wss://mainnet.helius-rpc.com/?api-key=***",
  });

  socket.on("open", () => {
    if (
      socketId !==
      currentSocketId
    ) {
      cleanupSocket(socket);
      return;
    }

    retryCount = 0;
    socketAlive = true;

    logInfo(
      "WebSocket opened",
      {
        socketId,
      }
    );

    subscribe(socket);
    startPing(socketId);
  });

  socket.on("pong", () => {
    if (
      socketId ===
      currentSocketId
    ) {
      socketAlive = true;
    }
  });

  socket.on("message", (data) => {
    if (
      socketId !==
      currentSocketId
    ) {
      return;
    }

    socketAlive = true;

    try {
      const message =
        JSON.parse(
          data.toString()
        );

      if (
        typeof message.result ===
          "number" &&
        message.id === 1
      ) {
        logInfo(
          "Subscribed successfully",
          {
            socketId,
            subscriptionId:
              message.result,
          }
        );

        return;
      }

      const result =
        message?.params?.result;

      const value =
        result?.value;

      const context =
        result?.context;

      if (
        !value ||
        value.err ||
        !value.signature
      ) {
        return;
      }

      if (!isPregradEnabled()) {
        stats.droppedDuringPause += 1;
        return;
      }

      if (intakePaused) {
        stats.droppedDuringPause += 1;
        maybeResumeIntake();
        return;
      }

      if (
        !looksRelevantFromLogs(
          value
        )
      ) {
        stats.skippedIrrelevantLog += 1;
        return;
      }

      enqueueSignature(
        value.signature,
        context?.slot || null,
        value.blockTime || null
      );
    } catch (error) {
      logError(
        "WebSocket message parse error",
        {
          socketId,
          error: error.message,
        }
      );
    }
  });

  socket.on("error", (error) => {
    const message =
      error?.message ||
      "unknown_websocket_error";

    logError(
      "WebSocket error",
      {
        socketId,
        error: message,
        wasRateLimited:
          message.includes("429"),
      }
    );
  });

  socket.on(
    "close",
    (
      code,
      reasonBuffer
    ) => {
      if (
        socketId !==
        currentSocketId
      ) {
        return;
      }

      stopPing();

      const reason =
        reasonBuffer?.length
          ? reasonBuffer.toString()
          : "no_reason";

      const wasRateLimited =
        reason.includes("429");

      logInfo(
        "WebSocket closed",
        {
          socketId,
          code,
          reason,
          wasRateLimited,
        }
      );

      cleanupSocket(socket);

      scheduleReconnect(
        "socket_closed",
        wasRateLimited
      );
    }
  );
}

// ==================================================
// 16. MAINTENANCE / STATS
//
// Purpose:
//
// Maintain queue health and expose scanner throughput
// without adding database-heavy maintenance work to
// the live ingestion service.
//
// Philosophy:
//
// • Drain stale signatures regularly.
// • Refresh control state through one shared query.
// • Log real-time ingestion health.
// • Never run raw-table retention here.
// • Keep shutdown cleanup simple and predictable.
// ==================================================


// ==================================================
// 16A. STALE QUEUE DRAINER
//
// Removes signatures that have remained in the queue
// beyond SIGNATURE_MAX_AGE_MS.
//
// This prevents old transactions from consuming worker
// capacity after the scanner falls temporarily behind.
// ==================================================

function startStaleDrainer() {
  if (staleDrainTimer) {
    return;
  }

  staleDrainTimer = setInterval(
    drainStaleQueueItems,
    STALE_DRAIN_INTERVAL_MS
  );

  logInfo(
    "Stale queue drainer started",
    {
      intervalMs:
        STALE_DRAIN_INTERVAL_MS,

      signatureMaxAgeMs:
        SIGNATURE_MAX_AGE_MS,
    }
  );
}


// ==================================================
// 16B. THROUGHPUT SNAPSHOT
//
// Stores the previous cumulative counters so each
// scanner log can calculate recent per-second rates.
// ==================================================

let previousStats = {
  queued: 0,
  dequeued: 0,
  insertedEvents: 0,
  processed: 0,
};

let previousLogAt =
  Date.now();


// ==================================================
// 16C. SCANNER STATS LOGGER
//
// Produces one compact scanner-health record per log
// interval.
//
// Includes:
//
// • WebSocket health
// • Queue depth
// • Oldest queued signature age
// • In-flight transaction count
// • Incoming transaction rate
// • Worker drain rate
// • Database insert rate
// • Completed processing rate
// • Cumulative scanner counters
//
// The control query is shared through the safe control
// cache and cannot fan out across workers.
// ==================================================

function startQueueLogger() {
  if (queueLogTimer) {
    return;
  }

  queueLogTimer = setInterval(
    async () => {
      try {
        // One shared control refresh.
        await getPregradControl();

        const now =
          Date.now();

        const seconds =
          Math.max(
            (
              now -
              previousLogAt
            ) / 1000,
            1
          );

        const oldest =
          signatureQueue[0];

        const incomingPerSecond =
          Number(
            (
              (
                stats.queued -
                previousStats.queued
              ) /
              seconds
            ).toFixed(2)
          );

        const drainedPerSecond =
          Number(
            (
              (
                stats.dequeued -
                previousStats.dequeued
              ) /
              seconds
            ).toFixed(2)
          );

        const insertedPerSecond =
          Number(
            (
              (
                stats.insertedEvents -
                previousStats.insertedEvents
              ) /
              seconds
            ).toFixed(2)
          );

        const processedPerSecond =
          Number(
            (
              (
                stats.processed -
                previousStats.processed
              ) /
              seconds
            ).toFixed(2)
          );

        logInfo(
          "Scanner stats",
          {
            time:
              nowIso(),

            systemEnabled:
              isPregradEnabled(),

            manualOverride:
              pregradControl.manual_override,

            websocketState:
              ws?.readyState ??
              null,

            socketAlive,

            intakePaused,

            workerRunning,

            queueSize:
              signatureQueue.length,

            queuedSignatureCount:
              queuedSignatures.size,

            inFlightCount:
              inFlightSignatures.size,

            seenSignatureCount:
              seenSignatures.size,

            holderEnrichmentInFlight:
              tokenSafetyEnrichmentInFlight.size,

            oldestSignatureAgeMs:
              oldest
                ? Math.max(
                    now -
                    oldest.enqueuedAt,
                    0
                  )
                : 0,

            incomingPerSecond,

            drainedPerSecond,

            insertedPerSecond,

            processedPerSecond,

            ...stats,
          }
        );

        previousStats = {
          queued:
            stats.queued,

          dequeued:
            stats.dequeued,

          insertedEvents:
            stats.insertedEvents,

          processed:
            stats.processed,
        };

        previousLogAt =
          now;
      } catch (error) {
        logError(
          "Scanner stats logging failed",
          {
            error:
              String(
                error?.message ||
                error
              ),
          }
        );
      }
    },
    QUEUE_LOG_EVERY_MS
  );

  logInfo(
    "Scanner stats logger started",
    {
      intervalMs:
        QUEUE_LOG_EVERY_MS,
    }
  );
}


// ==================================================
// 16D. TIMER SHUTDOWN
//
// Stops every recurring timer owned by this service.
//
// Raw-retention cleanup is intentionally absent.
// Database cleanup must run through a separate,
// lock-safe maintenance process.
// ==================================================

function stopTimers() {
  stopPing();

  if (queueLogTimer) {
    clearInterval(
      queueLogTimer
    );

    queueLogTimer =
      null;
  }

  if (staleDrainTimer) {
    clearInterval(
      staleDrainTimer
    );

    staleDrainTimer =
      null;
  }
}
// ==================================================
// 17. HEALTH SERVER
// ==================================================

http
  .createServer(
    async (
      request,
      response
    ) => {
      if (
        request.url ===
        "/health"
      ) {
        try {
          await getPregradControl(
            true
          );

          const db =
            await pool.query(
              "SELECT NOW()"
            );

          response.writeHead(
            200,
            {
              "Content-Type":
                "application/json",
            }
          );

          response.end(
            JSON.stringify({
              ok: true,
              service:
                "pregrad-pump-scanner",

              dbTime:
                db.rows[0].now,

              websocketState:
                ws?.readyState ??
                null,

              socketAlive,
              retryCount,

              queueSize:
                signatureQueue.length,

              inFlightCount:
                inFlightSignatures.size,

              intakePaused,
              workerRunning,

              programId:
                PUMP_LAUNCHPAD_PROGRAM_ID,

              systemEnabled:
                isPregradEnabled(),

              pregradTokenSupply:
                PREGRAD_TOKEN_SUPPLY,

              solPriceUsd:
                SOL_PRICE_USD > 0
                  ? SOL_PRICE_USD
                  : null,

              pregradControl,
              stats,
            })
          );
        } catch (error) {
          response.writeHead(
            500,
            {
              "Content-Type":
                "application/json",
            }
          );

          response.end(
            JSON.stringify({
              ok: false,
              error:
                error.message,
            })
          );
        }

        return;
      }

      response.writeHead(
        200,
        {
          "Content-Type":
            "text/plain",
        }
      );

      response.end(
        "pregrad pump scanner running"
      );
    }
  )
  .listen(PORT, () => {
    logInfo(
      "HTTP server listening",
      {
        port: PORT,
      }
    );
  });

// ==================================================
// 18. BOOT / SHUTDOWN
//
// Purpose:
//
// Start and stop the PreGrad scanner safely.
//
// Boot order:
//
// 1. Verify PostgreSQL connectivity
// 2. Skip all runtime schema migrations
// 3. Load the shared PreGrad control state
// 4. Start queue workers
// 5. Start maintenance and health logging
// 6. Connect to the Helius WebSocket
//
// Shutdown order:
//
// 1. Prevent duplicate shutdown attempts
// 2. Stop new WebSocket intake
// 3. Stop recurring timers
// 4. Stop queue workers
// 5. Wait for active workers to settle
// 6. Close the PostgreSQL pool
//
// Raw-event retention is intentionally excluded from
// this service.
// ==================================================


// ==================================================
// 18A. BOOT
// ==================================================

async function boot() {
  try {
    // ----------------------------------------------
    // DATABASE HEALTH CHECK
    // ----------------------------------------------

    const test =
      await pool.query(
        "SELECT NOW()"
      );

    logInfo(
      "Database connected",
      {
        dbTime:
          test.rows[0]?.now ||
          null,
      }
    );


    // ----------------------------------------------
    // NO RUNTIME DDL
    //
    // Schema migrations, index creation, and table
    // cleanup must run outside the live scanner.
    // ----------------------------------------------

    logInfo(
      "Skipping runtime table migrations"
    );


    // ----------------------------------------------
    // INITIAL CONTROL LOAD
    // ----------------------------------------------

    await getPregradControl(true);

    logInfo(
      "PreGrad control loaded",
      {
        ...pregradControl,
      }
    );


    // ----------------------------------------------
    // START INTERNAL SERVICES
    // ----------------------------------------------

    startQueueWorkers();
    startQueueLogger();
    startStaleDrainer();


    // ----------------------------------------------
    // START HELIUS INTAKE
    //
    // Connect only after the database, control cache,
    // workers, and maintenance timers are ready.
    // ----------------------------------------------

    connect();

    logInfo(
      "PreGrad scanner boot completed",
      {
        workerConcurrency:
          WORKER_CONCURRENCY,

        maxQueueSize:
          MAX_QUEUE_SIZE,

        rawEventStorage:
          STORE_RAW_EVENTS,

        holderEnrichment:
          HOLDER_ENRICHMENT_ENABLED,
      }
    );
  } catch (error) {
    logError(
      "Boot failed",
      {
        error:
          String(
            error?.message ||
            error
          ),

        stack:
          error?.stack ||
          null,
      }
    );

    try {
      stopTimers();
    } catch (_) {}

    try {
      await pool.end();
    } catch (_) {}

    process.exit(1);
  }
}


// ==================================================
// 18B. SHUTDOWN
// ==================================================

async function shutdown(signal = "unknown") {
  if (intentionalShutdown) {
    return;
  }

  intentionalShutdown = true;

  logInfo(
    "Shutting down PreGrad scanner",
    {
      signal,

      queueSize:
        signatureQueue.length,

      inFlightCount:
        inFlightSignatures.size,
    }
  );


  // ----------------------------------------------
  // STOP NEW WORK
  // ----------------------------------------------

  intakePaused = true;
  workerRunning = false;

  stopTimers();


  // ----------------------------------------------
  // CANCEL RECONNECT
  // ----------------------------------------------

  if (reconnectTimeout) {
    clearTimeout(
      reconnectTimeout
    );

    reconnectTimeout =
      null;
  }


  // ----------------------------------------------
  // CLOSE WEBSOCKET
  // ----------------------------------------------

  if (ws) {
    try {
      cleanupSocket(ws);
    } catch (_) {}

    try {
      if (
        ws.readyState ===
          WebSocket.OPEN ||
        ws.readyState ===
          WebSocket.CONNECTING
      ) {
        ws.close(
          1000,
          "Scanner shutting down"
        );
      }
    } catch (_) {}

    ws = null;
  }


  // ----------------------------------------------
  // WAIT FOR WORKERS
  // ----------------------------------------------

  try {
    await Promise.allSettled(
      workerPromises
    );
  } catch (_) {}


  // ----------------------------------------------
  // CLOSE DATABASE POOL
  // ----------------------------------------------

  try {
    await pool.end();

    logInfo(
      "Database pool closed"
    );
  } catch (error) {
    logError(
      "Database pool shutdown failed",
      {
        error:
          String(
            error?.message ||
            error
          ),
      }
    );
  }

  process.exit(0);
}


// ==================================================
// 18C. PROCESS SIGNALS
// ==================================================

process.once(
  "SIGINT",
  () =>
    shutdown("SIGINT")
);

process.once(
  "SIGTERM",
  () =>
    shutdown("SIGTERM")
);


// ==================================================
// 18D. START SERVICE
// ==================================================

boot();
