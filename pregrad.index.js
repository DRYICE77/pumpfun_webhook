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
  // ==========================================
  // INGESTION
  // ==========================================

  queued: 0,
  dequeued: 0,
  processed: 0,
  insertedEvents: 0,
  insertedTokens: 0,
  updatedMarketData: 0,

  // ==========================================
  // QUEUE SAFEGUARDS
  // ==========================================

  intakePausedCount: 0,
  intakeResumedCount: 0,
  droppedQueueFull: 0,
  droppedDuplicate: 0,
  droppedStale: 0,
  droppedDuringPause: 0,

  // ==========================================
  // FILTERING
  // ==========================================

  skippedSmallSolAmount: 0,
  skippedMarketDataUpdate: 0,
  skippedIrrelevantLog: 0,
  skippedEmptyTx: 0,
  skippedFailedTx: 0,
  skippedUnsupportedPumpInstruction: 0,
  skippedUnresolvedMint: 0,

  // Individual SQL operation performance
sqlTokenUpsertSamples: 0,
sqlTokenUpsertTotalMs: 0,
sqlTokenUpsertMaxMs: 0,

sqlEventInsertSamples: 0,
sqlEventInsertTotalMs: 0,
sqlEventInsertMaxMs: 0,

sqlMarketTokenUpdateSamples: 0,
sqlMarketTokenUpdateTotalMs: 0,
sqlMarketTokenUpdateMaxMs: 0,

sqlMarketEventUpdateSamples: 0,
sqlMarketEventUpdateTotalMs: 0,
sqlMarketEventUpdateMaxMs: 0,

sqlGraduationUpdateSamples: 0,
sqlGraduationUpdateTotalMs: 0,
sqlGraduationUpdateMaxMs: 0,

  // ==========================================
  // UNRESOLVED MINT DIAGNOSTICS
  // ==========================================
  resolvedOneCandidatePumpConfirmed: 0,
  resolvedMultipleCandidatePumpConfirmed: 0,
  resolvedMultipleCandidateRule4: 0,
  unresolvedCreate: 0,
  unresolvedBuy: 0,
  unresolvedSell: 0,
  unresolvedMigrate: 0,
  unresolvedUnknown: 0,

  unresolvedNoTokenBalances: 0,
  unresolvedZeroCandidates: 0,
  unresolvedOneCandidate: 0,
  unresolvedMultipleCandidates: 0,
  unresolvedCandidatesNoPumpSuffix: 0,
  unresolvedCandidatesWithPumpSuffix: 0,

  // ==========================================
  // ERRORS
  // ==========================================

  txFetchErrors: 0,
  workerErrors: 0,
  rpcRetries: 0,
  controlFetchErrors: 0,

  // ==========================================
  // EVENT CLASSIFICATION
  // ==========================================

  classifiedCreate: 0,
  classifiedBuy: 0,
  classifiedSell: 0,
  classifiedMigrate: 0,
  classifiedUnknown: 0,

  // ==========================================
  // ENRICHMENT
  // ==========================================

  safetyEnrichmentRuns: 0,
  safetyEnrichmentSkippedCooldown: 0,
  safetyEnrichmentErrors: 0,

  // ==========================================
  // RPC FETCH PERFORMANCE
  // ==========================================

  rpcFetchSamples: 0,
  rpcFetchTotalMs: 0,
  rpcFetchMaxMs: 0,

  // ==========================================
  // DATABASE WRITE PERFORMANCE
  // ==========================================

  dbWriteSamples: 0,
  dbWriteTotalMs: 0,
  dbWriteMaxMs: 0,

  // ==========================================
  // TOTAL SIGNATURE PROCESSING PERFORMANCE
  // ==========================================

  processingSamples: 0,
  processingTotalMs: 0,
  processingMaxMs: 0,

  // ==========================================
  // INTAKE PAUSE PERFORMANCE
  // ==========================================

  intakePauseSamples: 0,
  intakePauseTotalMs: 0,
  intakePauseMaxMs: 0,
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


// ==================================================
// 6A. PERFORMANCE DIAGNOSTICS
//
// Observation only:
// • No additional database queries
// • No per-transaction logging
// • No changes to ingestion behavior
//
// Measures:
//
// • Helius transaction fetch
// • Complete database-write phase
// • Complete signature processing
// • Intake pauses
//
// Individual SQL operations:
//
// • Token upsert
// • Event insert
// • Market token update
// • Market event update
// • Graduation update
//
// Uses counters defined in const stats.
// ==================================================

function performanceNow() {
  return Number(
    process.hrtime.bigint()
  ) / 1e6;
}


function getPerformanceCounterMap() {
  return {
    rpcFetch: {
      samples: "rpcFetchSamples",
      total: "rpcFetchTotalMs",
      max: "rpcFetchMaxMs",
    },

    dbWrite: {
      samples: "dbWriteSamples",
      total: "dbWriteTotalMs",
      max: "dbWriteMaxMs",
    },

    processing: {
      samples: "processingSamples",
      total: "processingTotalMs",
      max: "processingMaxMs",
    },

    intakePause: {
      samples: "intakePauseSamples",
      total: "intakePauseTotalMs",
      max: "intakePauseMaxMs",
    },

    sqlTokenUpsert: {
      samples: "sqlTokenUpsertSamples",
      total: "sqlTokenUpsertTotalMs",
      max: "sqlTokenUpsertMaxMs",
    },

    sqlEventInsert: {
      samples: "sqlEventInsertSamples",
      total: "sqlEventInsertTotalMs",
      max: "sqlEventInsertMaxMs",
    },

    sqlMarketTokenUpdate: {
      samples: "sqlMarketTokenUpdateSamples",
      total: "sqlMarketTokenUpdateTotalMs",
      max: "sqlMarketTokenUpdateMaxMs",
    },

    sqlMarketEventUpdate: {
      samples: "sqlMarketEventUpdateSamples",
      total: "sqlMarketEventUpdateTotalMs",
      max: "sqlMarketEventUpdateMaxMs",
    },

    sqlGraduationUpdate: {
      samples: "sqlGraduationUpdateSamples",
      total: "sqlGraduationUpdateTotalMs",
      max: "sqlGraduationUpdateMaxMs",
    },
  };
}


function recordPerformanceTiming(
  category,
  durationMs
) {
  if (
    !Number.isFinite(durationMs) ||
    durationMs < 0
  ) {
    return;
  }

  const counters =
    getPerformanceCounterMap()[category];

  if (!counters) {
    return;
  }

  stats[counters.samples] += 1;

  stats[counters.total] +=
    durationMs;

  stats[counters.max] = Math.max(
    stats[counters.max],
    durationMs
  );
}


function getPerformanceSummary(
  category
) {
  const counters =
    getPerformanceCounterMap()[category];

  if (!counters) {
    return null;
  }

  const samples =
    stats[counters.samples];

  const totalMs =
    stats[counters.total];

  const maxMs =
    stats[counters.max];

  return {
    samples,

    avgMs:
      samples > 0
        ? Number(
            (
              totalMs /
              samples
            ).toFixed(2)
          )
        : null,

    maxMs:
      samples > 0
        ? Number(
            maxMs.toFixed(2)
          )
        : null,
  };
}


// ==================================================
// 6B. TIMED DATABASE QUERY
//
// Wraps an existing pool.query() without changing
// the SQL, parameters, return value, or error behavior.
//
// Timing includes:
//
// • Waiting for an available pool connection
// • PostgreSQL query execution
// • Result delivery back to Node
//
// The finally block records failed queries too.
//
// IMPORTANT:
//
// This helper does not:
// • Retry queries
// • Catch/suppress query errors
// • Open transactions
// • Add database queries
// • Change SQL behavior
// ==================================================

async function timedPoolQuery(
  category,
  sql,
  params = []
) {
  const startedAt =
    performanceNow();

  try {
    return await pool.query(
      sql,
      params
    );
  } finally {
    recordPerformanceTiming(
      category,
      performanceNow() - startedAt
    );
  }
}


// ==================================================
// 6C. SIGNATURE HELPERS
// ==================================================

function addSeenSignature(signature) {
  seenSignatures.add(signature);

  if (
    seenSignatures.size >
    SEEN_SIGNATURE_LIMIT
  ) {
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


// ==================================================
// 6D. RETRY BACKOFF
// ==================================================

function backoffDelay(
  attempt,
  wasRateLimited = false
) {
  if (wasRateLimited) {
    return Math.min(
      60000 *
        2 ** Math.min(
          attempt,
          4
        ),
      600000
    );
  }

  return Math.min(
    2000 *
      2 ** Math.min(
        attempt,
        5
      ),
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
//
// Performance diagnostics:
// • Measure completed intake-pause durations
// • Preserve existing queue safeguards
// • Preserve existing signature handling
// • Do not change queue limits or ingestion behavior
// ==================================================


// ==================================================
// 8A. INTAKE PAUSE STATE
//
// Monotonic timestamp for the current pause.
// Null means no pause is being measured.
//
// This is separate from intakePaused, which remains
// the existing source of truth for queue behavior.
// ==================================================

let intakePausedAt = null;


// ==================================================
// 8B. PAUSE INTAKE
// ==================================================

function maybePauseIntake() {
  const maxQueueSize =
    effectiveMaxQueueSize();

  if (
    !intakePaused &&
    signatureQueue.length >= maxQueueSize
  ) {
    intakePaused = true;

    // Begin measuring this pause.
    intakePausedAt = performanceNow();

    stats.intakePausedCount += 1;

    logInfo("Intake paused", {
      queueSize: signatureQueue.length,
      maxQueueSize,
    });
  }
}


// ==================================================
// 8C. RESUME INTAKE
// ==================================================

function maybeResumeIntake() {
  if (
    intakePaused &&
    signatureQueue.length <= RESUME_QUEUE_SIZE &&
    isPregradEnabled()
  ) {
    // Capture duration before clearing pause state.
    const pauseDurationMs =
      intakePausedAt === null
        ? null
        : performanceNow() - intakePausedAt;

    intakePaused = false;

    // Record one sample per completed pause.
    if (pauseDurationMs !== null) {
      recordPerformanceTiming(
        "intakePause",
        pauseDurationMs
      );
    }

    intakePausedAt = null;

    stats.intakeResumedCount += 1;

    logInfo("Intake resumed", {
      queueSize: signatureQueue.length,
      resumeQueueSize: RESUME_QUEUE_SIZE,

      pauseDurationMs:
        pauseDurationMs === null
          ? null
          : Number(
              pauseDurationMs.toFixed(2)
            ),
    });
  }
}


// ==================================================
// 8D. STALE QUEUE MAINTENANCE
// ==================================================

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
      queuedSignatures.delete(
        item.signature
      );
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


// ==================================================
// 8E. ENQUEUE SIGNATURE
// ==================================================

function enqueueSignature(
  signature,
  slot = null,
  blockTime = null
) {
  if (!signature) {
    return;
  }

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
//
// Performance diagnostics:
// • Measure full transaction-fetch duration
// • Include retries and retry delays
// • Record successful and failed fetches
// • Preserve existing RPC and retry behavior
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

// ==================================================
// 9A. FETCH FULL TRANSACTION
//
// The timer covers the entire function, including:
// • Helius request time
// • Null-response retries
// • RPC error retries
// • Retry backoff delays
//
// The finally block records timing on every exit.
// ==================================================

async function fetchFullTransaction(signature) {
  const fetchStartedAt = performanceNow();

  let lastError = null;

  try {
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
              maxSupportedTransactionVersion: 1,
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
            RPC_RETRY_DELAY_MS * (attempt + 1)
          );
        }
      } catch (error) {
        lastError = error;

        if (attempt < RPC_RETRY_COUNT) {
          stats.rpcRetries += 1;

          const wasRateLimited =
            error?.status === 429 ||
            String(error?.message || "").includes("429");

          await sleep(
            wasRateLimited
              ? backoffDelay(attempt, true)
              : RPC_RETRY_DELAY_MS * (attempt + 1)
          );
        }
      }
    }

    if (lastError) {
      throw lastError;
    }

    return null;
  } finally {
    recordPerformanceTiming(
      "rpcFetch",
      performanceNow() - fetchStartedAt
    );
  }
}

// ==================================================
// 9B. TOKEN SUPPLY
// ==================================================

async function fetchTokenSupply(
  mintAddress
) {
  return heliusRpc(
    "getTokenSupply",
    [mintAddress]
  );
}

// ==================================================
// 9C. LARGEST TOKEN ACCOUNTS
// ==================================================

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
    line =>
      String(line)
        .trim()
        .toLowerCase()
  );

  const hasCreate = logs.some(
    line =>
      line.endsWith(
        "instruction: create"
      ) ||
      line.endsWith(
        "instruction: createv2"
      ) ||
      line.endsWith(
        "instruction: create_v2"
      )
  );

  const hasBuy = logs.some(
    line =>
      line.includes(
        "instruction: buy"
      )
  );

  const hasSell = logs.some(
    line =>
      line.includes(
        "instruction: sell"
      )
  );

  const hasMigrate = logs.some(
    line =>
      line.endsWith(
        "instruction: migrate"
      ) ||
      line.endsWith(
        "instruction: graduate"
      )
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

function isScoringEventType(eventType) {
  return (
    eventType === "create" ||
    eventType === "buy" ||
    eventType === "sell" ||
    eventType === "migrate"
  );
}

// ==================================================
// 10D. PUMP MINT IDENTIFICATION
//
// Production behavior:
//
// • Only confirmed Pump.fun mint addresses are accepted.
// • The current proven resolver accepts a token-balance
//   candidate only when the mint ends in "pump".
// • We do NOT promote unresolved candidates into production
//   events yet.
//
// Diagnostic behavior:
//
// • Classify unresolved transactions by candidate count.
// • Capture a bounded sample of one-candidate failures.
// • Preserve enough raw transaction structure to determine
//   whether the sole candidate can be independently confirmed
//   from the Pump.fun instruction layout.
// • Diagnostics never change mint selection.
//
// This lets us study unresolved mint completeness safely
// before adding any new resolution path.
// ==================================================

const WSOL_MINT =
  "So11111111111111111111111111111111111111112";

// Keep the diagnostic samples bounded so a long-running
// process cannot accumulate unlimited transaction data.
const ONE_CANDIDATE_SAMPLE_LIMIT = 100;

const oneCandidateMintSamples = [];

const MULTIPLE_CANDIDATE_SAMPLE_LIMIT = 100;

const multipleCandidateMintSamples = [];

const ZERO_CANDIDATE_SAMPLE_LIMIT = 100;
const zeroCandidateMintSamples = [];

const AMBIGUOUS_MULTIPLE_SAMPLE_LIMIT = 100;

const ambiguousMultipleMintSamples = [];

const PUMP_V2_MINT_ROLE_SAMPLE_LIMIT = 500;

const pumpV2MintRoleSamples = [];

const RULE4_SAMPLE_LIMIT = 500;

const rule4DiagnosticSamples = [];

let rule4DiagnosticSummary = {
  examined: 0,
  resolvable: 0,
  stillUnresolved: 0,
  expectedMintNotCandidate: 0,
  unsupportedSchema: 0,

  bySchema: {},
};

// ============================================================
// RULE 5 SHADOW DIAGNOSTIC
//
// PURPOSE:
//
// Test whether unresolved genuine Create transactions can be
// safely resolved using agreement between:
//
//   Pump Create / CreateV2 account[0]
//              +
//   SPL Token InitializeMint / InitializeMint2 mint
//
// This diagnostic NEVER changes production mint resolution.
// ============================================================

const RULE5_SHADOW_SAMPLE_LIMIT = 50;

const rule5ShadowSamples = [];

let rule5ShadowDiagnosticComplete = false;

// ============================================================
// UNRESOLVED CREATE MINT DIAGNOSTIC
// ============================================================

const UNRESOLVED_CREATE_SAMPLE_LIMIT = 50;

const unresolvedCreateMintSamples = [];

let unresolvedCreateMintDiagnosticComplete = false;

function recordUnresolvedCreateMintSample(
  tx,
  signature
) {
  // ----------------------------------------------
  // STOP AFTER SAMPLE LIMIT
  // ----------------------------------------------

  if (
    unresolvedCreateMintDiagnosticComplete ||
    unresolvedCreateMintSamples.length >=
      UNRESOLVED_CREATE_SAMPLE_LIMIT
  ) {
    return;
  }

  // ----------------------------------------------
  // ONLY UNRESOLVED CREATE EVENTS
  // ----------------------------------------------

  const eventType =
    inferEventTypeFromLogs(tx);

  if (eventType !== "create") {
    return;
  }

  // ----------------------------------------------
  // TOKEN-BALANCE MINT CANDIDATES
  // ----------------------------------------------

  const candidates =
    getMintCandidatesFromTokenBalances(tx);

  const candidateSet =
    new Set(candidates);

  // ----------------------------------------------
  // COLLECT OUTER + INNER PUMP INSTRUCTIONS
  // ----------------------------------------------

  const pumpInstructions = [];

  const outerInstructions =
    getInstructions(tx) || [];

  for (
    let outerIndex = 0;
    outerIndex < outerInstructions.length;
    outerIndex += 1
  ) {
    const ix =
      outerInstructions[outerIndex];

    if (
      ix?.programId !==
      PUMP_LAUNCHPAD_PROGRAM_ID
    ) {
      continue;
    }

    const accounts =
      Array.isArray(ix.accounts)
        ? ix.accounts
        : [];

    pumpInstructions.push({
      location: "outer",
      outerIndex,
      innerIndex: null,

      accountCount:
        accounts.length,

      accounts:
        accounts.map(
          (account, index) => ({
            index,
            account,

            isCandidate:
              candidateSet.has(account),
          })
        ),

      data:
        ix?.data ?? null,
    });
  }

  const innerGroups =
    getInnerInstructions(tx) || [];

  for (const group of innerGroups) {
    const instructions =
      group?.instructions || [];

    for (
      let innerIndex = 0;
      innerIndex < instructions.length;
      innerIndex += 1
    ) {
      const ix =
        instructions[innerIndex];

      if (
        ix?.programId !==
        PUMP_LAUNCHPAD_PROGRAM_ID
      ) {
        continue;
      }

      const accounts =
        Array.isArray(ix.accounts)
          ? ix.accounts
          : [];

      pumpInstructions.push({
        location: "inner",

        outerIndex:
          group?.index ?? null,

        innerIndex,

        accountCount:
          accounts.length,

        accounts:
          accounts.map(
            (account, index) => ({
              index,
              account,

              isCandidate:
                candidateSet.has(account),
            })
          ),

        data:
          ix?.data ?? null,
      });
    }
  }

  function recordRule5ShadowDiagnostic(
  tx,
  signature = null
) {
  // ----------------------------------------------
  // STOP AFTER SAMPLE LIMIT
  // ----------------------------------------------

  if (
    rule5ShadowDiagnosticComplete ||
    rule5ShadowSamples.length >=
      RULE5_SHADOW_SAMPLE_LIMIT
  ) {
    return;
  }

  // ----------------------------------------------
  // ONLY CREATE EVENTS
  // ----------------------------------------------

  const eventType =
    inferEventTypeFromLogs(tx);

  if (eventType !== "create") {
    return;
  }

  // ----------------------------------------------
  // TOKEN-BALANCE CANDIDATES
  // ----------------------------------------------

  const candidates =
    getMintCandidatesFromTokenBalances(tx);

  const candidateSet =
    new Set(candidates);

  // ----------------------------------------------
  // COLLECT INSTRUCTIONS
  // ----------------------------------------------

  const outerInstructions =
    getInstructions(tx) || [];

  const innerGroups =
    getInnerInstructions(tx) || [];

  // ----------------------------------------------
  // FIND PUMP CREATE INSTRUCTIONS
  //
  // We identify the actual Create instruction by
  // pairing Pump instructions with the transaction
  // logs conservatively.
  //
  // For the shadow test, account[0] is the mint
  // hypothesis we are testing.
  // ----------------------------------------------

  const pumpCreateInstructions = [];

  // Genuine CreateV2 transactions observed so far
  // use a 20-account Pump instruction.
  //
  // IMPORTANT:
  // This is diagnostic-only. We are testing this
  // schema, not promoting it to production.
  function inspectPumpInstruction(
    ix,
    location,
    outerIndex,
    innerIndex
  ) {
    if (
      ix?.programId !==
      PUMP_LAUNCHPAD_PROGRAM_ID
    ) {
      return;
    }

    const accounts =
      Array.isArray(ix.accounts)
        ? ix.accounts
        : [];

    // --------------------------------------------
    // SHADOW CREATE SCHEMA
    //
    // Current observed CreateV2:
    // accountCount = 20
    // expected mint = account[0]
    // --------------------------------------------

    if (accounts.length !== 20) {
      return;
    }

    const expectedMint =
      accounts[0] || null;

    pumpCreateInstructions.push({
      location,
      outerIndex,
      innerIndex,

      accountCount:
        accounts.length,

      expectedMint,

      expectedMintIsCandidate:
        typeof expectedMint === "string" &&
        candidateSet.has(expectedMint),

      accounts,
    });
  }

  // ----------------------------------------------
  // OUTER PUMP INSTRUCTIONS
  // ----------------------------------------------

  for (
    let outerIndex = 0;
    outerIndex < outerInstructions.length;
    outerIndex += 1
  ) {
    inspectPumpInstruction(
      outerInstructions[outerIndex],
      "outer",
      outerIndex,
      null
    );
  }

  // ----------------------------------------------
  // INNER PUMP INSTRUCTIONS
  // ----------------------------------------------

  for (const group of innerGroups) {
    const instructions =
      group?.instructions || [];

    for (
      let innerIndex = 0;
      innerIndex < instructions.length;
      innerIndex += 1
    ) {
      inspectPumpInstruction(
        instructions[innerIndex],
        "inner",
        group?.index ?? null,
        innerIndex
      );
    }
  }

  // ----------------------------------------------
  // FIND INITIALIZE MINT INSTRUCTIONS
  // ----------------------------------------------

  const initializeMintInstructions = [];

  // ----------------------------------------------
  // FIND MINTTO INSTRUCTIONS
  //
  // This is supporting evidence only.
  // ----------------------------------------------

  const mintToInstructions = [];

  function inspectParsedTokenInstruction(
    ix,
    location,
    outerIndex,
    innerIndex
  ) {
    const parsed =
      ix?.parsed ?? null;

    if (!parsed) {
      return;
    }

    const type =
      parsed?.type ?? null;

    const info =
      parsed?.info ?? null;

    if (
      type === "initializeMint" ||
      type === "initializeMint2"
    ) {
      const mint =
        info?.mint ?? null;

      initializeMintInstructions.push({
        location,
        outerIndex,
        innerIndex,
        type,
        mint,

        mintIsCandidate:
          typeof mint === "string" &&
          candidateSet.has(mint),
      });

      return;
    }

    if (
      type === "mintTo" ||
      type === "mintToChecked"
    ) {
      const mint =
        info?.mint ?? null;

      mintToInstructions.push({
        location,
        outerIndex,
        innerIndex,
        type,
        mint,

        mintIsCandidate:
          typeof mint === "string" &&
          candidateSet.has(mint),
      });
    }
  }

  // ----------------------------------------------
  // OUTER PARSED TOKEN INSTRUCTIONS
  // ----------------------------------------------

  for (
    let outerIndex = 0;
    outerIndex < outerInstructions.length;
    outerIndex += 1
  ) {
    inspectParsedTokenInstruction(
      outerInstructions[outerIndex],
      "outer",
      outerIndex,
      null
    );
  }

  // ----------------------------------------------
  // INNER PARSED TOKEN INSTRUCTIONS
  // ----------------------------------------------

  for (const group of innerGroups) {
    const instructions =
      group?.instructions || [];

    for (
      let innerIndex = 0;
      innerIndex < instructions.length;
      innerIndex += 1
    ) {
      inspectParsedTokenInstruction(
        instructions[innerIndex],
        "inner",
        group?.index ?? null,
        innerIndex
      );
    }
  }

  // ----------------------------------------------
  // UNIQUE CREATE MINT HYPOTHESES
  // ----------------------------------------------

  const createExpectedMints =
    new Set(
      pumpCreateInstructions
        .map(ix => ix.expectedMint)
        .filter(
          mint =>
            typeof mint === "string"
        )
    );

  // ----------------------------------------------
  // UNIQUE INITIALIZE-MINT HYPOTHESES
  // ----------------------------------------------

  const initializeMints =
    new Set(
      initializeMintInstructions
        .map(ix => ix.mint)
        .filter(
          mint =>
            typeof mint === "string"
        )
    );

  // ----------------------------------------------
  // UNIQUE MINTTO HYPOTHESES
  // ----------------------------------------------

  const mintToMints =
    new Set(
      mintToInstructions
        .map(ix => ix.mint)
        .filter(
          mint =>
            typeof mint === "string"
        )
    );

  // ----------------------------------------------
  // DERIVE SHADOW RESULT
  // ----------------------------------------------

  const createMint =
    createExpectedMints.size === 1
      ? Array.from(
          createExpectedMints
        )[0]
      : null;

  const initializeMint =
    initializeMints.size === 1
      ? Array.from(
          initializeMints
        )[0]
      : null;

  const mintToMint =
    mintToMints.size === 1
      ? Array.from(
          mintToMints
        )[0]
      : null;

  const createMintIsCandidate =
    typeof createMint === "string" &&
    candidateSet.has(createMint);

  const initializeMintIsCandidate =
    typeof initializeMint === "string" &&
    candidateSet.has(initializeMint);

  const createVsInitializeMatch =
    createMint !== null &&
    initializeMint !== null &&
    createMint === initializeMint;

  const initializeVsMintToMatch =
    initializeMint !== null &&
    mintToMint !== null &&
    initializeMint === mintToMint;

  // ----------------------------------------------
  // STRICT RULE 5 SHADOW RESOLUTION
  //
  // Production candidate would require:
  //
  // 1. exactly one Create account[0] hypothesis
  // 2. exactly one InitializeMint hypothesis
  // 3. both are token-balance candidates
  // 4. both independently identify same mint
  //
  // MintTo is recorded as additional evidence but
  // is NOT required yet.
  // ----------------------------------------------

  const resolvable =
    createExpectedMints.size === 1 &&
    initializeMints.size === 1 &&
    createMintIsCandidate &&
    initializeMintIsCandidate &&
    createVsInitializeMatch;

  // ----------------------------------------------
  // RECORD SAMPLE
  // ----------------------------------------------

  const sample = {
    signature,

    eventType,

    candidateCount:
      candidates.length,

    candidates,

    pumpCreateInstructionCount:
      pumpCreateInstructions.length,

    pumpCreateInstructions,

    initializeMintInstructionCount:
      initializeMintInstructions.length,

    initializeMintInstructions,

    mintToInstructionCount:
      mintToInstructions.length,

    mintToInstructions,

    uniqueCreateExpectedMintCount:
      createExpectedMints.size,

    uniqueInitializeMintCount:
      initializeMints.size,

    uniqueMintToMintCount:
      mintToMints.size,

    createMint,

    initializeMint,

    mintToMint,

    createMintIsCandidate,

    initializeMintIsCandidate,

    createVsInitializeMatch,

    initializeVsMintToMatch,

    resolvable,
  };

  rule5ShadowSamples.push(sample);

  // ----------------------------------------------
  // LOG EACH SAMPLE
  // ----------------------------------------------

  logInfo(
    "Rule 5 shadow diagnostic sample",
    {
      sampleNumber:
        rule5ShadowSamples.length,

      signature,

      candidateCount:
        candidates.length,

      pumpCreateInstructionCount:
        pumpCreateInstructions.length,

      initializeMintInstructionCount:
        initializeMintInstructions.length,

      mintToInstructionCount:
        mintToInstructions.length,

      createMint,

      initializeMint,

      mintToMint,

      createMintIsCandidate,

      initializeMintIsCandidate,

      createVsInitializeMatch,

      initializeVsMintToMatch,

      resolvable,
    }
  );

  // ----------------------------------------------
  // FINAL SUMMARY
  // ----------------------------------------------

  if (
    rule5ShadowSamples.length >=
    RULE5_SHADOW_SAMPLE_LIMIT
  ) {
    rule5ShadowDiagnosticComplete =
      true;

    let resolvableCount = 0;

    let stillUnresolved = 0;

    let createInstructionMissing = 0;

    let initializeMintMissing = 0;

    let mintToMissing = 0;

    let createMintNotCandidate = 0;

    let initializeMintNotCandidate = 0;

    let createVsInitializeMismatch = 0;

    let initializeVsMintToMismatch = 0;

    let multipleCreateExpectedMints = 0;

    let multipleInitializeMints = 0;

    let multipleMintToMints = 0;

    const candidateCountDistribution = {};

    const createAccountCountDistribution = {};

    for (
      const row
      of rule5ShadowSamples
    ) {
      // ------------------------------------------
      // RESOLUTION
      // ------------------------------------------

      if (row.resolvable) {
        resolvableCount += 1;
      } else {
        stillUnresolved += 1;
      }

      // ------------------------------------------
      // MISSING SIGNALS
      // ------------------------------------------

      if (
        row.pumpCreateInstructionCount === 0
      ) {
        createInstructionMissing += 1;
      }

      if (
        row.initializeMintInstructionCount === 0
      ) {
        initializeMintMissing += 1;
      }

      if (
        row.mintToInstructionCount === 0
      ) {
        mintToMissing += 1;
      }

      // ------------------------------------------
      // CANDIDATE VALIDATION
      // ------------------------------------------

      if (
        row.createMint !== null &&
        !row.createMintIsCandidate
      ) {
        createMintNotCandidate += 1;
      }

      if (
        row.initializeMint !== null &&
        !row.initializeMintIsCandidate
      ) {
        initializeMintNotCandidate += 1;
      }

      // ------------------------------------------
      // DISAGREEMENTS
      // ------------------------------------------

      if (
        row.createMint !== null &&
        row.initializeMint !== null &&
        !row.createVsInitializeMatch
      ) {
        createVsInitializeMismatch += 1;
      }

      if (
        row.initializeMint !== null &&
        row.mintToMint !== null &&
        !row.initializeVsMintToMatch
      ) {
        initializeVsMintToMismatch += 1;
      }

      // ------------------------------------------
      // MULTIPLE HYPOTHESES
      // ------------------------------------------

      if (
        row.uniqueCreateExpectedMintCount > 1
      ) {
        multipleCreateExpectedMints += 1;
      }

      if (
        row.uniqueInitializeMintCount > 1
      ) {
        multipleInitializeMints += 1;
      }

      if (
        row.uniqueMintToMintCount > 1
      ) {
        multipleMintToMints += 1;
      }

      // ------------------------------------------
      // CANDIDATE COUNT DISTRIBUTION
      // ------------------------------------------

      const candidateKey =
        String(row.candidateCount);

      candidateCountDistribution[
        candidateKey
      ] =
        (
          candidateCountDistribution[
            candidateKey
          ] || 0
        ) + 1;

      // ------------------------------------------
      // CREATE ACCOUNT COUNT DISTRIBUTION
      // ------------------------------------------

      for (
        const ix
        of row.pumpCreateInstructions
      ) {
        const accountKey =
          String(ix.accountCount);

        createAccountCountDistribution[
          accountKey
        ] =
          (
            createAccountCountDistribution[
              accountKey
            ] || 0
          ) + 1;
      }
    }

    logInfo(
      "Rule 5 shadow diagnostic complete",
      {
        examined:
          rule5ShadowSamples.length,

        resolvable:
          resolvableCount,

        stillUnresolved,

        resolutionRate:
          rule5ShadowSamples.length > 0
            ? resolvableCount /
              rule5ShadowSamples.length
            : 0,

        createInstructionMissing,

        initializeMintMissing,

        mintToMissing,

        createMintNotCandidate,

        initializeMintNotCandidate,

        createVsInitializeMismatch,

        initializeVsMintToMismatch,

        multipleCreateExpectedMints,

        multipleInitializeMints,

        multipleMintToMints,

        candidateCountDistribution,

        createAccountCountDistribution,

        samples:
          rule5ShadowSamples,
      }
    );
  }
}

  // ----------------------------------------------
  // COLLECT ALL PARSED TOKEN INSTRUCTIONS
  //
  // We specifically want to see whether Create
  // transactions expose InitializeMint,
  // InitializeMint2, MintTo, etc.
  // ----------------------------------------------

  const tokenInstructions = [];

  function inspectTokenInstruction(
    ix,
    location,
    outerIndex,
    innerIndex
  ) {
    const parsed =
      ix?.parsed ?? null;

    if (!parsed) {
      return;
    }

    const type =
      parsed?.type ?? null;

    const info =
      parsed?.info ?? null;

    const interestingTypes =
      new Set([
        "initializeMint",
        "initializeMint2",
        "mintTo",
        "mintToChecked",
        "initializeAccount",
        "initializeAccount2",
        "initializeAccount3",
      ]);

    if (
      !interestingTypes.has(type)
    ) {
      return;
    }

    tokenInstructions.push({
      location,
      outerIndex,
      innerIndex,

      program:
        ix?.program ?? null,

      programId:
        ix?.programId ?? null,

      type,

      info,
    });
  }

  // ----------------------------------------------
  // PARSED OUTER TOKEN INSTRUCTIONS
  // ----------------------------------------------

  for (
    let outerIndex = 0;
    outerIndex < outerInstructions.length;
    outerIndex += 1
  ) {
    inspectTokenInstruction(
      outerInstructions[outerIndex],
      "outer",
      outerIndex,
      null
    );
  }

  // ----------------------------------------------
  // PARSED INNER TOKEN INSTRUCTIONS
  // ----------------------------------------------

  for (const group of innerGroups) {
    const instructions =
      group?.instructions || [];

    for (
      let innerIndex = 0;
      innerIndex < instructions.length;
      innerIndex += 1
    ) {
      inspectTokenInstruction(
        instructions[innerIndex],
        "inner",
        group?.index ?? null,
        innerIndex
      );
    }
  }

  // ----------------------------------------------
  // RAW PRE / POST TOKEN BALANCES
  //
  // Important for the zero-candidate cases.
  // ----------------------------------------------

  const preTokenBalances =
    tx?.meta?.preTokenBalances ?? [];

  const postTokenBalances =
    tx?.meta?.postTokenBalances ?? [];

  // ----------------------------------------------
  // ACCOUNT KEYS
  //
  // Useful if Create initializes a mint but no
  // token balance exists yet.
  // ----------------------------------------------

  const accountKeys =
    Array.isArray(
      tx?.transaction?.message?.accountKeys
    )
      ? tx.transaction.message.accountKeys
      : [];

  // ----------------------------------------------
  // LOGS
  //
  // Keep only useful instruction / Pump-related
  // lines so diagnostic output stays manageable.
  // ----------------------------------------------

  const logs =
    tx?.meta?.logMessages ?? [];

  const relevantLogs =
    logs.filter(log => {
      if (typeof log !== "string") {
        return false;
      }

      const lower =
        log.toLowerCase();

      return (
        lower.includes("instruction:") ||
        lower.includes("initialize") ||
        lower.includes("mint") ||
        lower.includes("create")
      );
    });

  // ----------------------------------------------
  // RECORD SAMPLE
  // ----------------------------------------------

  unresolvedCreateMintSamples.push({
    signature:
      signature || null,

    eventType,

    candidateCount:
      candidates.length,

    candidates,

    pumpInstructionCount:
      pumpInstructions.length,

    pumpInstructions,

    tokenInstructionCount:
      tokenInstructions.length,

    tokenInstructions,

    preTokenBalances,

    postTokenBalances,

    accountKeys,

    relevantLogs,
  });

  // ----------------------------------------------
  // LOG EACH SAMPLE IMMEDIATELY
  //
  // Creates are rare enough that we don't want
  // to wait for all 50 before seeing anything.
  // ----------------------------------------------

  logInfo(
    "Unresolved create mint diagnostic sample",
    {
      sampleNumber:
        unresolvedCreateMintSamples.length,

      sample:
        unresolvedCreateMintSamples[
          unresolvedCreateMintSamples.length - 1
        ],
    }
  );

  // ----------------------------------------------
  // FINAL SUMMARY
  // ----------------------------------------------

  if (
    unresolvedCreateMintSamples.length >=
    UNRESOLVED_CREATE_SAMPLE_LIMIT
  ) {
    unresolvedCreateMintDiagnosticComplete =
      true;

    const candidateCountDistribution =
      {};

    const pumpAccountCountDistribution =
      {};

    const tokenInstructionTypeCounts =
      {};

    for (
      const sample
      of unresolvedCreateMintSamples
    ) {
      const candidateKey =
        String(sample.candidateCount);

      candidateCountDistribution[
        candidateKey
      ] =
        (
          candidateCountDistribution[
            candidateKey
          ] || 0
        ) + 1;

      for (
        const ix
        of sample.pumpInstructions
      ) {
        const accountKey =
          String(ix.accountCount);

        pumpAccountCountDistribution[
          accountKey
        ] =
          (
            pumpAccountCountDistribution[
              accountKey
            ] || 0
          ) + 1;
      }

      for (
        const ix
        of sample.tokenInstructions
      ) {
        const typeKey =
          ix.type || "unknown";

        tokenInstructionTypeCounts[
          typeKey
        ] =
          (
            tokenInstructionTypeCounts[
              typeKey
            ] || 0
          ) + 1;
      }
    }

    logInfo(
      "Unresolved create mint diagnostic complete",
      {
        sampleCount:
          unresolvedCreateMintSamples.length,

        candidateCountDistribution,

        pumpAccountCountDistribution,

        tokenInstructionTypeCounts,

        samples:
          unresolvedCreateMintSamples,
      }
    );
  }
}

function recordPumpV2MintRoleSample(
  tx,
  signature,
  eventType,
  resolvedMint
) {
  if (
    pumpV2MintRoleSamples.length >=
    PUMP_V2_MINT_ROLE_SAMPLE_LIMIT
  ) {
    return;
  }

  if (
    eventType !== "buy" &&
    eventType !== "sell"
  ) {
    return;
  }

  if (
    typeof resolvedMint !== "string" ||
    !resolvedMint
  ) {
    return;
  }

  const pumpInstructions = [];

  const outerInstructions =
    getInstructions(tx) || [];

  for (
    let outerIndex = 0;
    outerIndex < outerInstructions.length;
    outerIndex += 1
  ) {
    const ix =
      outerInstructions[outerIndex];

    if (
      ix?.programId !==
      PUMP_LAUNCHPAD_PROGRAM_ID
    ) {
      continue;
    }

    pumpInstructions.push({
      location: "outer",
      outerIndex,
      innerIndex: null,
      accounts:
        Array.isArray(ix.accounts)
          ? ix.accounts
          : [],
      data:
        ix?.data ?? null,
    });
  }

  const innerGroups =
    getInnerInstructions(tx) || [];

  for (const group of innerGroups) {
    const instructions =
      group?.instructions || [];

    for (
      let innerIndex = 0;
      innerIndex < instructions.length;
      innerIndex += 1
    ) {
      const ix =
        instructions[innerIndex];

      if (
        ix?.programId !==
        PUMP_LAUNCHPAD_PROGRAM_ID
      ) {
        continue;
      }

      pumpInstructions.push({
        location: "inner",
        outerIndex:
          group?.index ?? null,
        innerIndex,
        accounts:
          Array.isArray(ix.accounts)
            ? ix.accounts
            : [],
        data:
          ix?.data ?? null,
      });
    }
  }

  // We only care about Pump instructions that:
  //
  // 1. have account indexes 1 and 2
  // 2. contain the mint we already resolved
  //
  // This keeps unrelated Pump CPIs out of the sample.

  const matchingInstructions =
    pumpInstructions.filter(ix => {
      if (ix.accounts.length < 3) {
        return false;
      }

      return (
        ix.accounts[1] === resolvedMint ||
        ix.accounts[2] === resolvedMint
      );
    });

  if (!matchingInstructions.length) {
    return;
  }

  for (const ix of matchingInstructions) {
    if (
      pumpV2MintRoleSamples.length >=
      PUMP_V2_MINT_ROLE_SAMPLE_LIMIT
    ) {
      break;
    }

    const resolvedMintIndex =
      ix.accounts[1] === resolvedMint
        ? 1
        : ix.accounts[2] === resolvedMint
          ? 2
          : null;

    pumpV2MintRoleSamples.push({
      signature:
        signature || null,

      eventType,

      resolvedMint,

      resolvedMintIndex,

      account1:
        ix.accounts[1] || null,

      account2:
        ix.accounts[2] || null,

      account1EqualsResolvedMint:
        ix.accounts[1] === resolvedMint,

      account2EqualsResolvedMint:
        ix.accounts[2] === resolvedMint,

      accountCount:
        ix.accounts.length,

      instructionData:
        ix.data,

      // Short prefix makes grouping instruction
      // variants much easier in the output.
      instructionDataPrefix:
        typeof ix.data === "string"
          ? ix.data.slice(0, 16)
          : null,

      location:
        ix.location,

      outerIndex:
        ix.outerIndex,

      innerIndex:
        ix.innerIndex,
    });
  }

  if (
    pumpV2MintRoleSamples.length ===
    PUMP_V2_MINT_ROLE_SAMPLE_LIMIT
  ) {
    const summary = {};

    for (const row of pumpV2MintRoleSamples) {
      const key = [
        row.eventType,
        row.instructionDataPrefix,
        row.accountCount,
      ].join("|");

      if (!summary[key]) {
        summary[key] = {
          eventType:
            row.eventType,

          instructionDataPrefix:
            row.instructionDataPrefix,

          accountCount:
            row.accountCount,

          sampleCount: 0,

          resolvedAtIndex1: 0,

          resolvedAtIndex2: 0,
        };
      }

      summary[key].sampleCount += 1;

      if (row.resolvedMintIndex === 1) {
        summary[key].resolvedAtIndex1 += 1;
      }

      if (row.resolvedMintIndex === 2) {
        summary[key].resolvedAtIndex2 += 1;
      }
    }

    logInfo(
      "Pump V2 mint-role sample complete",
      {
        sampleCount:
          pumpV2MintRoleSamples.length,

        summary:
          Object.values(summary),

        // Only include a few examples.
        // No more enormous Railway line.
        examples:
          pumpV2MintRoleSamples.slice(0, 10),
      }
    );
  }
}
// ================================================
// RULE 4 SHADOW DIAGNOSTIC
//
// PURPOSE:
//
// Test the proposed Pump trade account-schema rule
// against transactions that CURRENTLY remain
// unresolved.
//
// IMPORTANT:
//
// This diagnostic does NOT resolve the mint.
// It only measures whether Rule 4 WOULD have
// resolved it.
//
// CONTROL EVIDENCE:
//
// BUY:
//   18 accounts -> mint at index 2
//   19 accounts -> mint at index 2
//   27 accounts -> mint at index 1
//   28 accounts -> mint at index 1
//
// SELL:
//   16 accounts -> mint at index 2
//   17 accounts -> mint at index 2
//   26 accounts -> mint at index 1
//   27 accounts -> mint at index 1
//
// SAFETY:
//
// The expected account must also exist in the
// token-balance candidate set.
// ================================================

function getRule4ExpectedMintIndex(
  eventType,
  accountCount
) {
  if (eventType === "buy") {
    if (
      accountCount === 18 ||
      accountCount === 19
    ) {
      return 2;
    }

    if (
      accountCount === 27 ||
      accountCount === 28
    ) {
      return 1;
    }
  }

  if (eventType === "sell") {
    if (
      accountCount === 16 ||
      accountCount === 17
    ) {
      return 2;
    }

    if (
      accountCount === 26 ||
      accountCount === 27
    ) {
      return 1;
    }
  }

  return null;
}


function recordRule4ShadowDiagnostic(
  tx,
  signature,
  eventType,
  candidates
) {
  if (
    rule4DiagnosticSummary.examined >=
    RULE4_SAMPLE_LIMIT
  ) {
    return;
  }

  if (
    eventType !== "buy" &&
    eventType !== "sell"
  ) {
    return;
  }

  if (
    !Array.isArray(candidates) ||
    candidates.length < 2
  ) {
    return;
  }

  rule4DiagnosticSummary.examined += 1;

  const candidateSet =
    new Set(candidates);

  const pumpInstructions = [];

  // ----------------------------------------------
  // OUTER PUMP INSTRUCTIONS
  // ----------------------------------------------

  const outerInstructions =
    getInstructions(tx) || [];

  for (
    let outerIndex = 0;
    outerIndex < outerInstructions.length;
    outerIndex += 1
  ) {
    const ix =
      outerInstructions[outerIndex];

    if (
      ix?.programId !==
      PUMP_LAUNCHPAD_PROGRAM_ID
    ) {
      continue;
    }

    pumpInstructions.push({
      location: "outer",
      outerIndex,
      innerIndex: null,

      accounts:
        Array.isArray(ix.accounts)
          ? ix.accounts
          : [],

      data:
        ix?.data ?? null,
    });
  }

  // ----------------------------------------------
  // INNER PUMP INSTRUCTIONS
  // ----------------------------------------------

  const innerGroups =
    getInnerInstructions(tx) || [];

  for (const group of innerGroups) {
    const instructions =
      group?.instructions || [];

    for (
      let innerIndex = 0;
      innerIndex < instructions.length;
      innerIndex += 1
    ) {
      const ix =
        instructions[innerIndex];

      if (
        ix?.programId !==
        PUMP_LAUNCHPAD_PROGRAM_ID
      ) {
        continue;
      }

      pumpInstructions.push({
        location: "inner",

        outerIndex:
          group?.index ?? null,

        innerIndex,

        accounts:
          Array.isArray(ix.accounts)
            ? ix.accounts
            : [],

        data:
          ix?.data ?? null,
      });
    }
  }

  // ----------------------------------------------
  // TEST RECOGNIZED RULE-4 SCHEMAS
  // ----------------------------------------------

  const matches = [];

  for (const ix of pumpInstructions) {
    const accountCount =
      ix.accounts.length;

    const expectedMintIndex =
      getRule4ExpectedMintIndex(
        eventType,
        accountCount
      );

    if (expectedMintIndex === null) {
      continue;
    }

    const expectedMint =
      ix.accounts[expectedMintIndex] || null;

    const expectedMintIsCandidate =
      typeof expectedMint === "string" &&
      candidateSet.has(expectedMint);

    matches.push({
      location:
        ix.location,

      outerIndex:
        ix.outerIndex,

      innerIndex:
        ix.innerIndex,

      accountCount,

      expectedMintIndex,

      expectedMint,

      expectedMintIsCandidate,

      instructionData:
        ix.data,
    });
  }

  // ----------------------------------------------
  // NO RECOGNIZED SCHEMA
  // ----------------------------------------------

  if (!matches.length) {
    rule4DiagnosticSummary.unsupportedSchema += 1;
    rule4DiagnosticSummary.stillUnresolved += 1;

    maybeFinishRule4Diagnostic();

    return;
  }

  // ----------------------------------------------
  // ONLY ACCEPT EXPECTED MINTS THAT ARE ALSO
  // TOKEN-BALANCE CANDIDATES
  // ----------------------------------------------

  const validMatches =
    matches.filter(
      row =>
        row.expectedMintIsCandidate
    );

  if (!validMatches.length) {
    rule4DiagnosticSummary.expectedMintNotCandidate += 1;
    rule4DiagnosticSummary.stillUnresolved += 1;

    maybeFinishRule4Diagnostic();

    return;
  }

  // ----------------------------------------------
  // DEDUPLICATE EXPECTED MINTS
  //
  // Multiple Pump instructions can agree on the
  // same mint. That's still deterministic.
  // ----------------------------------------------

  const expectedMints =
    [
      ...new Set(
        validMatches.map(
          row => row.expectedMint
        )
      ),
    ];

  // ----------------------------------------------
  // RULE 4 WOULD RESOLVE
  // ----------------------------------------------

  if (expectedMints.length === 1) {
    const expectedMint =
      expectedMints[0];

    rule4DiagnosticSummary.resolvable += 1;

    for (const match of validMatches) {
      const schemaKey =
        [
          eventType,
          match.accountCount,
          match.expectedMintIndex,
        ].join("|");

      if (
        !rule4DiagnosticSummary.bySchema[
          schemaKey
        ]
      ) {
        rule4DiagnosticSummary.bySchema[
          schemaKey
        ] = {
          eventType,
          accountCount:
            match.accountCount,
          expectedMintIndex:
            match.expectedMintIndex,
          count: 0,
        };
      }

      rule4DiagnosticSummary.bySchema[
        schemaKey
      ].count += 1;
    }

    if (
      rule4DiagnosticSamples.length < 20
    ) {
      rule4DiagnosticSamples.push({
        signature:
          signature || null,

        eventType,

        candidateCount:
          candidates.length,

        candidates,

        expectedMint,

        validMatches,
      });
    }
  }

  // ----------------------------------------------
  // CONFLICT:
  // recognized schemas pointed to >1 candidate
  // ----------------------------------------------

  else {
    rule4DiagnosticSummary.stillUnresolved += 1;

    if (
      rule4DiagnosticSamples.length < 20
    ) {
      rule4DiagnosticSamples.push({
        signature:
          signature || null,

        eventType,

        candidateCount:
          candidates.length,

        candidates,

        conflict: true,

        expectedMints,

        validMatches,
      });
    }
  }

  maybeFinishRule4Diagnostic();
}


// ================================================
// RULE 4 DIAGNOSTIC COMPLETION LOGGER
// ================================================

function maybeFinishRule4Diagnostic() {
  if (
    rule4DiagnosticSummary.examined !==
    RULE4_SAMPLE_LIMIT
  ) {
    return;
  }

  logInfo(
    "Rule 4 shadow diagnostic complete",
    {
      examined:
        rule4DiagnosticSummary.examined,

      resolvable:
        rule4DiagnosticSummary.resolvable,

      stillUnresolved:
        rule4DiagnosticSummary.stillUnresolved,

      expectedMintNotCandidate:
        rule4DiagnosticSummary
          .expectedMintNotCandidate,

      unsupportedSchema:
        rule4DiagnosticSummary
          .unsupportedSchema,

      resolutionRate:
        rule4DiagnosticSummary.examined > 0
          ? (
              rule4DiagnosticSummary.resolvable /
              rule4DiagnosticSummary.examined
            )
          : 0,

      bySchema:
        Object.values(
          rule4DiagnosticSummary.bySchema
        ),

      examples:
        rule4DiagnosticSamples,
    }
  );
}

// ==================================================
// 10D-1. TOKEN-BALANCE MINT CANDIDATES
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

      // Wrapped SOL is never the Pump token mint.
      if (mint === WSOL_MINT) {
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


// ==================================================
// 10D-2. CURRENT PRODUCTION MINT RESOLVER
//
// IMPORTANT:
//
// Preserve current production behavior while diagnostics
// are running.
//
// We are intentionally NOT using "one candidate" as a
// resolution rule yet.
// ==================================================

// ==================================================
// INFER PRIMARY PUMP.FUN MINT
//
// Resolution order:
//
// 1. Preserve the existing high-confidence rule:
//    if a token-balance candidate ends in "pump",
//    resolve it immediately.
//
// 2. If there is exactly one non-wSOL candidate,
//    inspect both outer and inner instructions.
//
//    If the Pump.fun program explicitly references
//    that candidate as one of its instruction accounts,
//    resolve it.
//
// 3. Otherwise remain unresolved.
//
// IMPORTANT:
// - We do NOT resolve a mint merely because it is the
//   only token-balance candidate.
// - We do NOT assume a fixed Pump account position.
// - We do NOT require parsed SPL-token confirmation.
// ==================================================

function inferPrimaryMint(tx) {
  const candidates =
    getMintCandidatesFromTokenBalances(tx);

  // ----------------------------------------------
  // RULE 1:
  // EXISTING PUMP-SUFFIX RESOLUTION
  // ----------------------------------------------

  const pumpSuffixMint =
    candidates.find(
      mint =>
        typeof mint === "string" &&
        mint.endsWith("pump")
    );

  if (pumpSuffixMint) {
    return pumpSuffixMint;
  }

  // ----------------------------------------------
  // NO TOKEN-BALANCE CANDIDATES
  // ----------------------------------------------

  if (candidates.length === 0) {
    return null;
  }

  // ----------------------------------------------
  // COLLECT ALL CANDIDATES REFERENCED BY
  // OUTER OR INNER PUMP INSTRUCTIONS
  //
  // Also preserve the Pump instructions themselves
  // for the Rule 4 schema check below.
  // ----------------------------------------------

  const pumpConfirmedCandidates =
    new Set();

  const pumpInstructions = [];

  // ----------------------------------------------
  // OUTER INSTRUCTIONS
  // ----------------------------------------------

  const outerInstructions =
    getInstructions(tx) || [];

  for (const ix of outerInstructions) {
    if (
      ix?.programId !==
      PUMP_LAUNCHPAD_PROGRAM_ID
    ) {
      continue;
    }

    const accounts =
      Array.isArray(ix.accounts)
        ? ix.accounts
        : [];

    pumpInstructions.push({
      accounts,
    });

    for (const candidateMint of candidates) {
      if (
        accounts.includes(candidateMint)
      ) {
        pumpConfirmedCandidates.add(
          candidateMint
        );
      }
    }
  }

  // ----------------------------------------------
  // INNER INSTRUCTIONS
  // ----------------------------------------------

  const innerGroups =
    getInnerInstructions(tx) || [];

  for (const group of innerGroups) {
    const instructions =
      group?.instructions || [];

    for (const ix of instructions) {
      if (
        ix?.programId !==
        PUMP_LAUNCHPAD_PROGRAM_ID
      ) {
        continue;
      }

      const accounts =
        Array.isArray(ix.accounts)
          ? ix.accounts
          : [];

      pumpInstructions.push({
        accounts,
      });

      for (
        const candidateMint
        of candidates
      ) {
        if (
          accounts.includes(candidateMint)
        ) {
          pumpConfirmedCandidates.add(
            candidateMint
          );
        }
      }
    }
  }

  const confirmedCandidates =
    Array.from(
      pumpConfirmedCandidates
    );

  // ----------------------------------------------
  // RULE 2:
  // PUMP-CONFIRMED ONE-CANDIDATE FALLBACK
  // ----------------------------------------------

  if (
    candidates.length === 1 &&
    confirmedCandidates.length === 1
  ) {
    stats.resolvedOneCandidatePumpConfirmed += 1;

    return confirmedCandidates[0];
  }

  // ----------------------------------------------
  // RULE 3:
  // MULTIPLE TOKEN-BALANCE CANDIDATES,
  // BUT EXACTLY ONE PUMP-CONFIRMED CANDIDATE
  // ----------------------------------------------

  if (
    candidates.length > 1 &&
    confirmedCandidates.length === 1
  ) {
    stats.resolvedMultipleCandidatePumpConfirmed += 1;

    return confirmedCandidates[0];
  }

  // ----------------------------------------------
  // RULE 4:
  // VALIDATED MULTIPLE-PUMP-MINT TRADE SCHEMA
  //
  // Shadow diagnostic result:
  //
  //   500 examined
  //   500 resolvable
  //   0 conflicts
  //   0 expected-mint-not-candidate
  //   0 unsupported schemas
  //
  // Validated unresolved schemas:
  //
  //   BUY:
  //     27 accounts -> mint at index 1
  //     28 accounts -> mint at index 1
  //
  //   SELL:
  //     26 accounts -> mint at index 1
  //     27 accounts -> mint at index 1
  //
  // SAFETY:
  //
  // - Only applies to multiple-candidate cases.
  // - Only applies when Rules 1-3 failed.
  // - Expected mint MUST be a token-balance candidate.
  // - Every qualifying Pump instruction must agree
  //   on exactly one expected mint.
  // - Anything outside the validated schemas remains
  //   unresolved.
  // ----------------------------------------------

  if (
    candidates.length > 1 &&
    confirmedCandidates.length > 1
  ) {
    const candidateSet =
      new Set(candidates);

    const rule4ExpectedMints =
      new Set();

    let qualifyingInstructionCount = 0;

    for (const pumpIx of pumpInstructions) {
      const accounts =
        pumpIx.accounts || [];

      const accountCount =
        accounts.length;

      let schemaMatches = false;

      // ------------------------------------------
      // VALIDATED BUY SCHEMAS
      // ------------------------------------------

      if (
        inferEventTypeFromLogs(tx) === "buy" &&
        (
          accountCount === 27 ||
          accountCount === 28
        )
      ) {
        schemaMatches = true;
      }

      // ------------------------------------------
      // VALIDATED SELL SCHEMAS
      // ------------------------------------------

      else if (
        inferEventTypeFromLogs(tx) === "sell" &&
        (
          accountCount === 26 ||
          accountCount === 27
        )
      ) {
        schemaMatches = true;
      }

      if (!schemaMatches) {
        continue;
      }

      qualifyingInstructionCount += 1;

      // All four validated schemas use account[1].
      const expectedMint =
        accounts[1] || null;

      // ------------------------------------------
      // EXPECTED MINT MUST BE A REAL
      // TOKEN-BALANCE CANDIDATE
      // ------------------------------------------

      if (
        typeof expectedMint !== "string" ||
        !candidateSet.has(expectedMint)
      ) {
        // A qualifying schema produced something
        // outside our candidate set.
        //
        // Fail closed rather than guessing.
        return null;
      }

      rule4ExpectedMints.add(
        expectedMint
      );
    }

    // --------------------------------------------
    // REQUIRE AT LEAST ONE QUALIFYING INSTRUCTION
    // --------------------------------------------

    if (
      qualifyingInstructionCount === 0
    ) {
      return null;
    }

    // --------------------------------------------
    // ALL QUALIFYING INSTRUCTIONS MUST AGREE
    //
    // Exactly one unique expected mint means the
    // schema produced a deterministic answer.
    // --------------------------------------------

    if (
      rule4ExpectedMints.size === 1
    ) {
      const resolvedMint =
        Array.from(
          rule4ExpectedMints
        )[0];

      stats.resolvedMultipleCandidateRule4 += 1;

      return resolvedMint;
    }

    // Multiple qualifying instructions disagreed.
    // Fail closed.
    return null;
  }

  // ----------------------------------------------
  // NO SAFE RESOLUTION
  //
  // Includes:
  // - no Pump-confirmed candidate
  // - unsupported multiple-candidate schema
  // - Rule 4 expected mint not in candidate set
  // - Rule 4 qualifying instructions disagree
  // ----------------------------------------------

  return null;
}
// ==================================================
// 10D-3. ONE-CANDIDATE VALIDATION DIAGNOSTIC
//
// For unresolved transactions with exactly one non-wSOL
// token-balance candidate, collect a compact validation
// record showing whether that candidate is independently
// confirmed by:
//
//   1. A Pump.fun instruction account
//   2. A parsed SPL-token instruction mint
//
// Both outer and inner instructions are inspected.
//
// This diagnostic does NOT affect event classification
// or production mint selection.
// ==================================================

function recordOneCandidateMintSample(
  tx,
  signature,
  eventType,
  candidateMint
) {
  // Stop after the configured sample size.
  if (
    oneCandidateMintSamples.length >=
    ONE_CANDIDATE_SAMPLE_LIMIT
  ) {
    return;
  }

  // ----------------------------------------------
  // 1. COLLECT OUTER + INNER INSTRUCTIONS
  // ----------------------------------------------

  const outerInstructions =
    getInstructions(tx) || [];

  const innerGroups =
    getInnerInstructions(tx) || [];

  const innerInstructions =
    innerGroups.flatMap(
      group => group?.instructions || []
    );

  const allInstructions = [
    ...outerInstructions.map(ix => ({
      ...ix,
      diagnosticLocation: "outer",
    })),

    ...innerInstructions.map(ix => ({
      ...ix,
      diagnosticLocation: "inner",
    })),
  ];

  // ----------------------------------------------
  // 2. FIND PUMP INSTRUCTIONS REFERENCING CANDIDATE
  // ----------------------------------------------

  const pumpMatches = [];

  for (const ix of allInstructions) {
    if (
      ix?.programId !==
      PUMP_LAUNCHPAD_PROGRAM_ID
    ) {
      continue;
    }

    const accounts =
      Array.isArray(ix.accounts)
        ? ix.accounts
        : [];

    const candidateIndexes = [];

    for (
      let i = 0;
      i < accounts.length;
      i += 1
    ) {
      if (
        accounts[i] ===
        candidateMint
      ) {
        candidateIndexes.push(i);
      }
    }

    if (candidateIndexes.length) {
      pumpMatches.push({
        location:
          ix.diagnosticLocation,

        candidateIndexes,

        accountCount:
          accounts.length,
      });
    }
  }

  // ----------------------------------------------
  // 3. FIND PARSED TOKEN INSTRUCTIONS
  //    THAT EXPLICITLY NAME CANDIDATE AS MINT
  // ----------------------------------------------

  const tokenMintMatches = [];

  for (const ix of allInstructions) {
    const parsed =
      ix?.parsed || null;

    const info =
      parsed?.info || null;

    if (
      info?.mint !==
      candidateMint
    ) {
      continue;
    }

    tokenMintMatches.push({
      location:
        ix.diagnosticLocation,

      programId:
        ix?.programId || null,

      type:
        parsed?.type || null,
    });
  }

  // ----------------------------------------------
  // 4. BUILD COMPACT VALIDATION RECORD
  // ----------------------------------------------

  const pumpConfirmed =
    pumpMatches.length > 0;

  const tokenInstructionConfirmed =
    tokenMintMatches.length > 0;

  const confirmedByBoth =
    pumpConfirmed &&
    tokenInstructionConfirmed;

  oneCandidateMintSamples.push({
    signature:
      signature || null,

    eventType:
      eventType || "unknown",

    candidateMint:
      candidateMint || null,

    pumpConfirmed,

    pumpMatches,

    tokenInstructionConfirmed,

    tokenMintMatches,

    confirmedByBoth,
  });

  // ----------------------------------------------
  // 5. OUTPUT SUMMARY ONCE SAMPLE IS COMPLETE
  // ----------------------------------------------

  if (
    oneCandidateMintSamples.length ===
    ONE_CANDIDATE_SAMPLE_LIMIT
  ) {
    const pumpConfirmedCount =
      oneCandidateMintSamples.filter(
        row => row.pumpConfirmed
      ).length;

    const tokenInstructionConfirmedCount =
      oneCandidateMintSamples.filter(
        row =>
          row.tokenInstructionConfirmed
      ).length;

    const confirmedByBothCount =
      oneCandidateMintSamples.filter(
        row => row.confirmedByBoth
      ).length;

    const neitherConfirmedCount =
      oneCandidateMintSamples.filter(
        row =>
          !row.pumpConfirmed &&
          !row.tokenInstructionConfirmed
      ).length;

    logInfo(
      "One-candidate mint validation sample complete",
      {
        sampleCount:
          oneCandidateMintSamples.length,

        pumpConfirmedCount,

        tokenInstructionConfirmedCount,

        confirmedByBothCount,

        neitherConfirmedCount,

        samples:
          oneCandidateMintSamples,
      }
    );
  }
}

// ==================================================
// MULTIPLE-CANDIDATE MINT VALIDATION DIAGNOSTIC
//
// For unresolved transactions containing 2+ non-wSOL
// token-balance candidates:
//
// 1. Inspect all outer + inner Pump.fun instructions.
// 2. Determine which candidate mints are explicitly
//    referenced by Pump.fun.
// 3. Record whether Pump references:
//      - zero candidates
//      - exactly one candidate
//      - multiple candidates
//
// Diagnostic only.
// Does NOT affect production mint resolution.
// ==================================================

function recordMultipleCandidateMintSample(
  tx,
  signature,
  eventType,
  candidates
) {
  if (
    multipleCandidateMintSamples.length >=
    MULTIPLE_CANDIDATE_SAMPLE_LIMIT
  ) {
    return;
  }

  // ----------------------------------------------
  // COLLECT OUTER + INNER PUMP INSTRUCTIONS
  // ----------------------------------------------

  const pumpInstructions = [];

  const outerInstructions =
    getInstructions(tx) || [];

  for (
    let outerIndex = 0;
    outerIndex < outerInstructions.length;
    outerIndex += 1
  ) {
    const ix =
      outerInstructions[outerIndex];

    if (
      ix?.programId !==
      PUMP_LAUNCHPAD_PROGRAM_ID
    ) {
      continue;
    }

    pumpInstructions.push({
      location: "outer",
      outerIndex,
      innerIndex: null,
      accounts:
        Array.isArray(ix.accounts)
          ? ix.accounts
          : [],
    });
  }

  const innerGroups =
    getInnerInstructions(tx) || [];

  for (const group of innerGroups) {
    const instructions =
      group?.instructions || [];

    for (
      let innerIndex = 0;
      innerIndex < instructions.length;
      innerIndex += 1
    ) {
      const ix =
        instructions[innerIndex];

      if (
        ix?.programId !==
        PUMP_LAUNCHPAD_PROGRAM_ID
      ) {
        continue;
      }

      pumpInstructions.push({
        location: "inner",
        outerIndex:
          group?.index ?? null,
        innerIndex,
        accounts:
          Array.isArray(ix.accounts)
            ? ix.accounts
            : [],
      });
    }
  }

  // ----------------------------------------------
  // CHECK EACH CANDIDATE AGAINST PUMP
  // ----------------------------------------------

  const candidateResults =
    candidates.map(candidateMint => {
      const pumpMatches = [];

      for (
        const pumpIx
        of pumpInstructions
      ) {
        const candidateIndexes = [];

        for (
          let i = 0;
          i < pumpIx.accounts.length;
          i += 1
        ) {
          if (
            pumpIx.accounts[i] ===
            candidateMint
          ) {
            candidateIndexes.push(i);
          }
        }

        if (
          candidateIndexes.length > 0
        ) {
          pumpMatches.push({
            location:
              pumpIx.location,

            outerIndex:
              pumpIx.outerIndex,

            innerIndex:
              pumpIx.innerIndex,

            candidateIndexes,
          });
        }
      }

      return {
        candidateMint,

        pumpConfirmed:
          pumpMatches.length > 0,

        pumpMatches,
      };
    });

  // ----------------------------------------------
  // FIND ALL PUMP-CONFIRMED CANDIDATES
  // ----------------------------------------------

  const pumpConfirmedCandidates =
    candidateResults
      .filter(
        row => row.pumpConfirmed
      )
      .map(
        row => row.candidateMint
      );

  const pumpConfirmedCandidateCount =
    pumpConfirmedCandidates.length;

  // ----------------------------------------------
  // STORE COMPACT SAMPLE
  // ----------------------------------------------

  multipleCandidateMintSamples.push({
    signature:
      signature || null,

    eventType:
      eventType || "unknown",

    candidateCount:
      candidates.length,

    candidates,

    pumpConfirmedCandidateCount,

    pumpConfirmedCandidates,

    candidateResults,
  });

  // ----------------------------------------------
  // OUTPUT SUMMARY WHEN SAMPLE REACHES LIMIT
  // ----------------------------------------------

  if (
    multipleCandidateMintSamples.length ===
    MULTIPLE_CANDIDATE_SAMPLE_LIMIT
  ) {
    const zeroPumpConfirmedCount =
      multipleCandidateMintSamples.filter(
        row =>
          row.pumpConfirmedCandidateCount === 0
      ).length;

    const onePumpConfirmedCount =
      multipleCandidateMintSamples.filter(
        row =>
          row.pumpConfirmedCandidateCount === 1
      ).length;

    const multiplePumpConfirmedCount =
      multipleCandidateMintSamples.filter(
        row =>
          row.pumpConfirmedCandidateCount > 1
      ).length;

    logInfo(
      "Multiple-candidate mint validation sample complete",
      {
        sampleCount:
          multipleCandidateMintSamples.length,

        zeroPumpConfirmedCount,

        onePumpConfirmedCount,

        multiplePumpConfirmedCount,

        samples:
          multipleCandidateMintSamples,
      }
    );
  }
}

// ==================================================
// AMBIGUOUS MULTIPLE-CANDIDATE DIAGNOSTIC
//
// Diagnostic only.
//
// Runs only for unresolved scoring events where:
//
// • 2+ non-wSOL token-balance candidates exist
// • 2+ candidates are explicitly referenced by Pump
//
// Goal:
//
// Determine whether the real traded Pump mint can be
// identified deterministically using:
//
// • Pump instruction account position
// • Pump instruction data / discriminator
// • Candidate token-balance changes
// • Parsed token instructions
// • Event type
//
// This function NEVER changes production mint
// resolution.
// ==================================================

function recordAmbiguousMultipleMintSample(
  tx,
  signature,
  eventType,
  candidates
) {
  if (
    ambiguousMultipleMintSamples.length >=
    AMBIGUOUS_MULTIPLE_SAMPLE_LIMIT
  ) {
    return;
  }

  if (
    !Array.isArray(candidates) ||
    candidates.length < 2
  ) {
    return;
  }

  // ----------------------------------------------
  // TOKEN BALANCES
  // ----------------------------------------------

  const preTokenBalances =
    tx?.meta?.preTokenBalances || [];

  const postTokenBalances =
    tx?.meta?.postTokenBalances || [];

  // ----------------------------------------------
  // COLLECT PUMP INSTRUCTIONS
  // ----------------------------------------------

  const pumpInstructions = [];

  const outerInstructions =
    getInstructions(tx) || [];

  for (
    let outerIndex = 0;
    outerIndex < outerInstructions.length;
    outerIndex += 1
  ) {
    const ix =
      outerInstructions[outerIndex];

    if (
      ix?.programId !==
      PUMP_LAUNCHPAD_PROGRAM_ID
    ) {
      continue;
    }

    pumpInstructions.push({
      location: "outer",

      outerIndex,

      innerIndex: null,

      accounts:
        Array.isArray(ix.accounts)
          ? ix.accounts
          : [],

      data:
        ix?.data ?? null,

      parsed:
        ix?.parsed ?? null,
    });
  }

  const innerGroups =
    getInnerInstructions(tx) || [];

  for (const group of innerGroups) {
    const instructions =
      group?.instructions || [];

    for (
      let innerIndex = 0;
      innerIndex < instructions.length;
      innerIndex += 1
    ) {
      const ix =
        instructions[innerIndex];

      if (
        ix?.programId !==
        PUMP_LAUNCHPAD_PROGRAM_ID
      ) {
        continue;
      }

      pumpInstructions.push({
        location: "inner",

        outerIndex:
          group?.index ?? null,

        innerIndex,

        accounts:
          Array.isArray(ix.accounts)
            ? ix.accounts
            : [],

        data:
          ix?.data ?? null,

        parsed:
          ix?.parsed ?? null,
      });
    }
  }

  // ----------------------------------------------
  // DETERMINE WHICH CANDIDATES ARE
  // EXPLICITLY REFERENCED BY PUMP
  // ----------------------------------------------

  const candidateResults =
    candidates.map(candidateMint => {
      const pumpMatches = [];

      for (const pumpIx of pumpInstructions) {
        const candidateIndexes = [];

        for (
          let i = 0;
          i < pumpIx.accounts.length;
          i += 1
        ) {
          if (
            pumpIx.accounts[i] ===
            candidateMint
          ) {
            candidateIndexes.push(i);
          }
        }

        if (candidateIndexes.length > 0) {
          pumpMatches.push({
            location:
              pumpIx.location,

            outerIndex:
              pumpIx.outerIndex,

            innerIndex:
              pumpIx.innerIndex,

            candidateIndexes,

            accountCount:
              pumpIx.accounts.length,

            instructionData:
              pumpIx.data,
          });
        }
      }

      return {
        candidateMint,

        pumpConfirmed:
          pumpMatches.length > 0,

        pumpMatches,
      };
    });

  const pumpConfirmedCandidates =
    candidateResults
      .filter(
        row => row.pumpConfirmed
      )
      .map(
        row => row.candidateMint
      );

  // ----------------------------------------------
  // THIS DIAGNOSTIC ONLY WANTS TRUE AMBIGUOUS
  // MULTIPLE-CONFIRMED CASES
  // ----------------------------------------------

  if (
    pumpConfirmedCandidates.length < 2
  ) {
    return;
  }

  // ----------------------------------------------
  // TOKEN-BALANCE DELTA BY CANDIDATE
  //
  // We aggregate raw integer token amounts across
  // all accounts belonging to each mint.
  //
  // rawDelta:
  //   positive = aggregate token balance increased
  //   negative = aggregate token balance decreased
  // ----------------------------------------------

  const getRawAmount = row => {
    const raw =
      row?.uiTokenAmount?.amount;

    if (
      typeof raw !== "string" &&
      typeof raw !== "number"
    ) {
      return 0n;
    }

    try {
      return BigInt(raw);
    } catch {
      return 0n;
    }
  };

  const candidateBalanceChanges =
    candidates.map(candidateMint => {
      let preRaw = 0n;
      let postRaw = 0n;

      let decimals = null;

      const preRows =
        preTokenBalances.filter(
          row =>
            row?.mint === candidateMint
        );

      const postRows =
        postTokenBalances.filter(
          row =>
            row?.mint === candidateMint
        );

      for (const row of preRows) {
        preRaw += getRawAmount(row);

        if (
          decimals === null &&
          Number.isFinite(
            row?.uiTokenAmount?.decimals
          )
        ) {
          decimals =
            row.uiTokenAmount.decimals;
        }
      }

      for (const row of postRows) {
        postRaw += getRawAmount(row);

        if (
          decimals === null &&
          Number.isFinite(
            row?.uiTokenAmount?.decimals
          )
        ) {
          decimals =
            row.uiTokenAmount.decimals;
        }
      }

      const rawDelta =
        postRaw - preRaw;

      return {
        candidateMint,

        decimals,

        preRaw:
          preRaw.toString(),

        postRaw:
          postRaw.toString(),

        rawDelta:
          rawDelta.toString(),

        direction:
          rawDelta > 0n
            ? "increase"
            : rawDelta < 0n
              ? "decrease"
              : "unchanged",

        preAccountCount:
          preRows.length,

        postAccountCount:
          postRows.length,
      };
    });

  // ----------------------------------------------
  // PARSED TOKEN INSTRUCTIONS
  //
  // Capture parsed instructions that explicitly
  // identify one of our candidate mints.
  // ----------------------------------------------

  const parsedCandidateInstructions = [];

  const inspectParsedInstruction = (
    ix,
    location,
    outerIndex = null,
    innerIndex = null
  ) => {
    const parsed =
      ix?.parsed || null;

    const info =
      parsed?.info || null;

    const mint =
      info?.mint || null;

    if (
      typeof mint !== "string" ||
      !candidates.includes(mint)
    ) {
      return;
    }

    parsedCandidateInstructions.push({
      candidateMint:
        mint,

      location,

      outerIndex,

      innerIndex,

      programId:
        ix?.programId || null,

      type:
        parsed?.type || null,

      source:
        info?.source || null,

      destination:
        info?.destination || null,

      authority:
        info?.authority || null,

      owner:
        info?.owner || null,

      amount:
        info?.amount ??
        info?.tokenAmount?.amount ??
        null,
    });
  };

  for (
    let outerIndex = 0;
    outerIndex < outerInstructions.length;
    outerIndex += 1
  ) {
    inspectParsedInstruction(
      outerInstructions[outerIndex],
      "outer",
      outerIndex,
      null
    );
  }

  for (const group of innerGroups) {
    const instructions =
      group?.instructions || [];

    for (
      let innerIndex = 0;
      innerIndex < instructions.length;
      innerIndex += 1
    ) {
      inspectParsedInstruction(
        instructions[innerIndex],
        "inner",
        group?.index ?? null,
        innerIndex
      );
    }
  }

  // ----------------------------------------------
  // SAVE SAMPLE
  // ----------------------------------------------

  ambiguousMultipleMintSamples.push({
    signature:
      signature || null,

    slot:
      tx?.slot ?? null,

    blockTime:
      tx?.blockTime ?? null,

    eventType:
      eventType || "unknown",

    candidateCount:
      candidates.length,

    candidates,

    pumpConfirmedCandidateCount:
      pumpConfirmedCandidates.length,

    pumpConfirmedCandidates,

    candidateResults,

    candidateBalanceChanges,

    parsedCandidateInstructions,

    pumpInstructions,

    logs:
      getLogMessages(tx) || [],
  });

  // ----------------------------------------------
  // OUTPUT SUMMARY WHEN SAMPLE IS COMPLETE
  // ----------------------------------------------

  if (
    ambiguousMultipleMintSamples.length ===
    AMBIGUOUS_MULTIPLE_SAMPLE_LIMIT
  ) {
    const eventTypeCounts = {};

    const confirmedCountDistribution = {};

    const candidateCountDistribution = {};

    for (
      const row
      of ambiguousMultipleMintSamples
    ) {
      eventTypeCounts[row.eventType] =
        (
          eventTypeCounts[row.eventType] ||
          0
        ) + 1;

      const confirmedKey =
        String(
          row.pumpConfirmedCandidateCount
        );

      confirmedCountDistribution[
        confirmedKey
      ] =
        (
          confirmedCountDistribution[
            confirmedKey
          ] ||
          0
        ) + 1;

      const candidateKey =
        String(row.candidateCount);

      candidateCountDistribution[
        candidateKey
      ] =
        (
          candidateCountDistribution[
            candidateKey
          ] ||
          0
        ) + 1;
    }

    logInfo(
      "Ambiguous multiple-candidate mint sample complete",
      {
        sampleCount:
          ambiguousMultipleMintSamples.length,

        eventTypeCounts,

        candidateCountDistribution,

        confirmedCountDistribution,

        samples:
          ambiguousMultipleMintSamples,
      }
    );
  }
}

// ==================================================
// ZERO-CANDIDATE EVENT DIAGNOSTIC
//
// Diagnostic only.
//
// Investigates transactions where token balances
// produce zero usable non-wSOL mint candidates.
//
// Goals:
// 1. Determine whether token balances are absent
//    or merely unusable.
// 2. Inspect Pump instructions directly.
// 3. Determine whether these appear to be events
//    NorthStar actually wants to capture.
// 4. Gather evidence for a future Pump-only
//    mint resolver.
//
// This function does NOT modify mint selection.
// ==================================================

function recordZeroCandidateMintSample(
  tx,
  signature,
  eventType
) {
  if (
    zeroCandidateMintSamples.length >=
    ZERO_CANDIDATE_SAMPLE_LIMIT
  ) {
    return;
  }

  const preTokenBalances =
    tx?.meta?.preTokenBalances || [];

  const postTokenBalances =
    tx?.meta?.postTokenBalances || [];

  const hasAnyTokenBalances =
    preTokenBalances.length > 0 ||
    postTokenBalances.length > 0;

  // ----------------------------------------------
  // OUTER PUMP INSTRUCTIONS
  // ----------------------------------------------

  const pumpInstructions = [];

  const outerInstructions =
    getInstructions(tx) || [];

  for (
    let outerIndex = 0;
    outerIndex < outerInstructions.length;
    outerIndex += 1
  ) {
    const ix =
      outerInstructions[outerIndex];

    if (
      ix?.programId !==
      PUMP_LAUNCHPAD_PROGRAM_ID
    ) {
      continue;
    }

    pumpInstructions.push({
      location: "outer",

      outerIndex,

      innerIndex: null,

      accounts:
        Array.isArray(ix.accounts)
          ? ix.accounts
          : [],

      data:
        ix?.data ?? null,

      parsed:
        ix?.parsed ?? null,
    });
  }

  // ----------------------------------------------
  // INNER PUMP INSTRUCTIONS
  // ----------------------------------------------

  const innerGroups =
    getInnerInstructions(tx) || [];

  for (const group of innerGroups) {
    const instructions =
      group?.instructions || [];

    for (
      let innerIndex = 0;
      innerIndex < instructions.length;
      innerIndex += 1
    ) {
      const ix =
        instructions[innerIndex];

      if (
        ix?.programId !==
        PUMP_LAUNCHPAD_PROGRAM_ID
      ) {
        continue;
      }

      pumpInstructions.push({
        location: "inner",

        outerIndex:
          group?.index ?? null,

        innerIndex,

        accounts:
          Array.isArray(ix.accounts)
            ? ix.accounts
            : [],

        data:
          ix?.data ?? null,

        parsed:
          ix?.parsed ?? null,
      });
    }
  }

  // ----------------------------------------------
  // PARSED TOKEN-RELATED INSTRUCTIONS
  //
  // Useful for seeing whether a mint appears in
  // parsed instruction data even though it did not
  // appear in pre/post token balances.
  // ----------------------------------------------

  const parsedMintReferences = [];

  const inspectParsedInstruction = (
    ix,
    location,
    outerIndex = null,
    innerIndex = null
  ) => {
    const parsed =
      ix?.parsed || null;

    const info =
      parsed?.info || null;

    const mint =
      info?.mint || null;

    if (
      typeof mint !== "string" ||
      !mint
    ) {
      return;
    }

    parsedMintReferences.push({
      mint,

      location,

      outerIndex,

      innerIndex,

      programId:
        ix?.programId || null,

      type:
        parsed?.type || null,
    });
  };

  for (
    let outerIndex = 0;
    outerIndex < outerInstructions.length;
    outerIndex += 1
  ) {
    inspectParsedInstruction(
      outerInstructions[outerIndex],
      "outer",
      outerIndex,
      null
    );
  }

  for (const group of innerGroups) {
    const instructions =
      group?.instructions || [];

    for (
      let innerIndex = 0;
      innerIndex < instructions.length;
      innerIndex += 1
    ) {
      inspectParsedInstruction(
        instructions[innerIndex],
        "inner",
        group?.index ?? null,
        innerIndex
      );
    }
  }

  // ----------------------------------------------
  // LOGS
  // ----------------------------------------------

  const logs =
    getLogMessages(tx) || [];

  // ----------------------------------------------
  // SAVE SAMPLE
  // ----------------------------------------------

  zeroCandidateMintSamples.push({
    signature:
      signature || null,

    slot:
      tx?.slot ?? null,

    blockTime:
      tx?.blockTime ?? null,

    eventType:
      eventType || "unknown",

    hasAnyTokenBalances,

    preTokenBalanceCount:
      preTokenBalances.length,

    postTokenBalanceCount:
      postTokenBalances.length,

    preTokenBalances,

    postTokenBalances,

    pumpInstructionCount:
      pumpInstructions.length,

    pumpInstructions,

    parsedMintReferenceCount:
      parsedMintReferences.length,

    parsedMintReferences,

    logs,
  });

  // ----------------------------------------------
  // SUMMARY AFTER 100 SAMPLES
  // ----------------------------------------------

  if (
    zeroCandidateMintSamples.length ===
    ZERO_CANDIDATE_SAMPLE_LIMIT
  ) {
    const noTokenBalancesCount =
      zeroCandidateMintSamples.filter(
        row =>
          !row.hasAnyTokenBalances
      ).length;

    const hasTokenBalancesCount =
      zeroCandidateMintSamples.filter(
        row =>
          row.hasAnyTokenBalances
      ).length;

    const hasPumpInstructionCount =
      zeroCandidateMintSamples.filter(
        row =>
          row.pumpInstructionCount > 0
      ).length;

    const hasParsedMintReferenceCount =
      zeroCandidateMintSamples.filter(
        row =>
          row.parsedMintReferenceCount > 0
      ).length;

    const eventTypeCounts = {};

    for (
      const row
      of zeroCandidateMintSamples
    ) {
      const key =
        row.eventType || "unknown";

      eventTypeCounts[key] =
        (eventTypeCounts[key] || 0) + 1;
    }

    logInfo(
      "Zero-candidate event validation sample complete",
      {
        sampleCount:
          zeroCandidateMintSamples.length,

        noTokenBalancesCount,

        hasTokenBalancesCount,

        hasPumpInstructionCount,

        hasParsedMintReferenceCount,

        eventTypeCounts,

        samples:
          zeroCandidateMintSamples,
      }
    );
  }
}

// ==================================================
// 10D-4. UNRESOLVED MINT DIAGNOSTICS
//
// Count unresolved mint populations and collect
// diagnostic samples for later inspection.
//
// IMPORTANT:
//
// This function does NOT modify mint selection.
//
// Rule 4 is tested here only in SHADOW MODE against
// multiple-candidate transactions that remain
// unresolved after the production mint resolver.
// ==================================================

function recordUnresolvedMintDiagnostics(
  tx,
  signature = null
) {
  // ----------------------------------------------
  // EVENT TYPE
  // ----------------------------------------------

  const eventType =
    inferEventTypeFromLogs(tx);

  const eventCounter = {
    create: "unresolvedCreate",
    buy: "unresolvedBuy",
    sell: "unresolvedSell",
    migrate: "unresolvedMigrate",
  }[eventType] || "unresolvedUnknown";

  if (
    typeof stats[eventCounter] === "number"
  ) {
    stats[eventCounter] += 1;
  }

  // ----------------------------------------------
  // TOKEN BALANCE AVAILABILITY
  // ----------------------------------------------

  const preBalances =
    tx?.meta?.preTokenBalances || [];

  const postBalances =
    tx?.meta?.postTokenBalances || [];

  if (
    preBalances.length === 0 &&
    postBalances.length === 0
  ) {
    stats.unresolvedNoTokenBalances += 1;
  }

  // ----------------------------------------------
  // MINT CANDIDATES
  // ----------------------------------------------

  const candidates =
    getMintCandidatesFromTokenBalances(tx);

  // ----------------------------------------------
  // UNRESOLVED CREATE DIAGNOSTICS
  //
  // These run only for unresolved Create events.
  //
  // 1. General Create diagnostic captures the
  //    full transaction structure.
  //
  // 2. Rule 5 shadow diagnostic tests whether
  //    CreateV2 account[0] and InitializeMint /
  //    InitializeMint2 independently identify
  //    the same mint.
  //
  // Neither diagnostic changes production mint
  // resolution.
  // ----------------------------------------------

  if (eventType === "create") {
    recordUnresolvedCreateMintSample(
      tx,
      signature
    );

    recordRule5ShadowDiagnostic(
      tx,
      signature
    );
  }

  // ----------------------------------------------
  // CANDIDATE POPULATION
  // ----------------------------------------------

  if (candidates.length === 0) {
    stats.unresolvedZeroCandidates += 1;

    recordZeroCandidateMintSample(
      tx,
      signature,
      eventType
    );
  }

  else if (candidates.length === 1) {
    stats.unresolvedOneCandidate += 1;

    recordOneCandidateMintSample(
      tx,
      signature,
      eventType,
      candidates[0]
    );
  }

  else {
    stats.unresolvedMultipleCandidates += 1;

    // --------------------------------------------
    // GENERAL MULTIPLE-CANDIDATE DIAGNOSTIC
    // --------------------------------------------

    recordMultipleCandidateMintSample(
      tx,
      signature,
      eventType,
      candidates
    );

    // --------------------------------------------
    // TRUE AMBIGUOUS MULTIPLE-MINT DIAGNOSTIC
    //
    // Inspect cases where multiple candidates are
    // explicitly referenced by Pump instructions.
    // --------------------------------------------

    recordAmbiguousMultipleMintSample(
      tx,
      signature,
      eventType,
      candidates
    );
  }

  // ----------------------------------------------
  // PUMP SUFFIX DIAGNOSTIC
  // ----------------------------------------------

  const hasPumpSuffix =
    candidates.some(
      mint =>
        typeof mint === "string" &&
        mint.endsWith("pump")
    );

  if (hasPumpSuffix) {
    stats.unresolvedCandidatesWithPumpSuffix += 1;
  }

  else if (candidates.length > 0) {
    stats.unresolvedCandidatesNoPumpSuffix += 1;
  }
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
  // ----------------------------------------------
  // BASIC TRANSACTION VALIDATION
  // ----------------------------------------------

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

  // ----------------------------------------------
  // CONFIRM PUMP / LAUNCHPAD ACTIVITY
  // ----------------------------------------------

  if (!txTouchesLaunchpadProgram(tx)) {
    return {
      ok: false,
      reason:
        "not_launchpad_program",
    };
  }

  // ----------------------------------------------
  // EVENT TYPE
  // ----------------------------------------------

  const eventType =
    inferEventTypeFromLogs(tx);

  // ----------------------------------------------
  // SCORING EVENT FILTER
  //
  // A transaction can touch the Pump program
  // without representing a token event that
  // NorthStar wants to score.
  //
  // Examples observed:
  // - creator fee collection
  // - fee distribution
  // - cashback claims
  // - accumulator/account maintenance
  //
  // Only recognized scoring events continue
  // into mint resolution.
  // ----------------------------------------------

  if (!isScoringEventType(eventType)) {
    return {
      ok: false,
      reason:
        "unsupported_pump_instruction",

      eventType,
    };
  }

  // ----------------------------------------------
  // MINT RESOLUTION
  //
  // Only attempt this after confirming that the
  // transaction represents a scoring event.
  // ----------------------------------------------

  const tokenAddress =
    inferPrimaryMint(tx);

  if (!tokenAddress) {
    return {
      ok: false,
      reason:
        "unresolved_token_mint",

      eventType,
    };
  }

  // ----------------------------------------------
  // PUMP V2 MINT-ROLE DIAGNOSTIC
  //
  // Diagnostic only.
  //
  // Uses successfully resolved buy/sell events as
  // the control population so we can determine
  // which Pump V2 instruction account position
  // corresponds to the resolved traded mint.
  //
  // This does NOT affect mint resolution.
  // ----------------------------------------------

  recordPumpV2MintRoleSample(
    tx,
    signature,
    eventType,
    tokenAddress
  );
// ================================================
// RULE 4 SHADOW DIAGNOSTIC
//
// PURPOSE:
//
// Test the proposed Pump trade account-schema rule
// against transactions that CURRENTLY remain
// unresolved.
//
// IMPORTANT:
//
// This diagnostic does NOT resolve the mint.
// It only measures whether Rule 4 WOULD have
// resolved it.
//
// CONTROL EVIDENCE:
//
// BUY:
//   18 accounts -> mint at index 2
//   19 accounts -> mint at index 2
//   27 accounts -> mint at index 1
//   28 accounts -> mint at index 1
//
// SELL:
//   16 accounts -> mint at index 2
//   17 accounts -> mint at index 2
//   26 accounts -> mint at index 1
//   27 accounts -> mint at index 1
//
// SAFETY:
//
// The expected account must also exist in the
// token-balance candidate set.
// ================================================

function getRule4ExpectedMintIndex(
  eventType,
  accountCount
) {
  if (eventType === "buy") {
    if (
      accountCount === 18 ||
      accountCount === 19
    ) {
      return 2;
    }

    if (
      accountCount === 27 ||
      accountCount === 28
    ) {
      return 1;
    }
  }

  if (eventType === "sell") {
    if (
      accountCount === 16 ||
      accountCount === 17
    ) {
      return 2;
    }

    if (
      accountCount === 26 ||
      accountCount === 27
    ) {
      return 1;
    }
  }

  return null;
}


function recordRule4ShadowDiagnostic(
  tx,
  signature,
  eventType,
  candidates
) {
  if (
    rule4DiagnosticSummary.examined >=
    RULE4_SAMPLE_LIMIT
  ) {
    return;
  }

  if (
    eventType !== "buy" &&
    eventType !== "sell"
  ) {
    return;
  }

  if (
    !Array.isArray(candidates) ||
    candidates.length < 2
  ) {
    return;
  }

  rule4DiagnosticSummary.examined += 1;

  const candidateSet =
    new Set(candidates);

  const pumpInstructions = [];

  // ----------------------------------------------
  // OUTER PUMP INSTRUCTIONS
  // ----------------------------------------------

  const outerInstructions =
    getInstructions(tx) || [];

  for (
    let outerIndex = 0;
    outerIndex < outerInstructions.length;
    outerIndex += 1
  ) {
    const ix =
      outerInstructions[outerIndex];

    if (
      ix?.programId !==
      PUMP_LAUNCHPAD_PROGRAM_ID
    ) {
      continue;
    }

    pumpInstructions.push({
      location: "outer",
      outerIndex,
      innerIndex: null,

      accounts:
        Array.isArray(ix.accounts)
          ? ix.accounts
          : [],

      data:
        ix?.data ?? null,
    });
  }

  // ----------------------------------------------
  // INNER PUMP INSTRUCTIONS
  // ----------------------------------------------

  const innerGroups =
    getInnerInstructions(tx) || [];

  for (const group of innerGroups) {
    const instructions =
      group?.instructions || [];

    for (
      let innerIndex = 0;
      innerIndex < instructions.length;
      innerIndex += 1
    ) {
      const ix =
        instructions[innerIndex];

      if (
        ix?.programId !==
        PUMP_LAUNCHPAD_PROGRAM_ID
      ) {
        continue;
      }

      pumpInstructions.push({
        location: "inner",

        outerIndex:
          group?.index ?? null,

        innerIndex,

        accounts:
          Array.isArray(ix.accounts)
            ? ix.accounts
            : [],

        data:
          ix?.data ?? null,
      });
    }
  }

  // ----------------------------------------------
  // TEST RECOGNIZED RULE-4 SCHEMAS
  // ----------------------------------------------

  const matches = [];

  for (const ix of pumpInstructions) {
    const accountCount =
      ix.accounts.length;

    const expectedMintIndex =
      getRule4ExpectedMintIndex(
        eventType,
        accountCount
      );

    if (expectedMintIndex === null) {
      continue;
    }

    const expectedMint =
      ix.accounts[expectedMintIndex] || null;

    const expectedMintIsCandidate =
      typeof expectedMint === "string" &&
      candidateSet.has(expectedMint);

    matches.push({
      location:
        ix.location,

      outerIndex:
        ix.outerIndex,

      innerIndex:
        ix.innerIndex,

      accountCount,

      expectedMintIndex,

      expectedMint,

      expectedMintIsCandidate,

      instructionData:
        ix.data,
    });
  }

  // ----------------------------------------------
  // NO RECOGNIZED SCHEMA
  // ----------------------------------------------

  if (!matches.length) {
    rule4DiagnosticSummary.unsupportedSchema += 1;
    rule4DiagnosticSummary.stillUnresolved += 1;

    maybeFinishRule4Diagnostic();

    return;
  }

  // ----------------------------------------------
  // ONLY ACCEPT EXPECTED MINTS THAT ARE ALSO
  // TOKEN-BALANCE CANDIDATES
  // ----------------------------------------------

  const validMatches =
    matches.filter(
      row =>
        row.expectedMintIsCandidate
    );

  if (!validMatches.length) {
    rule4DiagnosticSummary.expectedMintNotCandidate += 1;
    rule4DiagnosticSummary.stillUnresolved += 1;

    maybeFinishRule4Diagnostic();

    return;
  }

  // ----------------------------------------------
  // DEDUPLICATE EXPECTED MINTS
  //
  // Multiple Pump instructions can agree on the
  // same mint. That's still deterministic.
  // ----------------------------------------------

  const expectedMints =
    [
      ...new Set(
        validMatches.map(
          row => row.expectedMint
        )
      ),
    ];

  // ----------------------------------------------
  // RULE 4 WOULD RESOLVE
  // ----------------------------------------------

  if (expectedMints.length === 1) {
    const expectedMint =
      expectedMints[0];

    rule4DiagnosticSummary.resolvable += 1;

    for (const match of validMatches) {
      const schemaKey =
        [
          eventType,
          match.accountCount,
          match.expectedMintIndex,
        ].join("|");

      if (
        !rule4DiagnosticSummary.bySchema[
          schemaKey
        ]
      ) {
        rule4DiagnosticSummary.bySchema[
          schemaKey
        ] = {
          eventType,
          accountCount:
            match.accountCount,
          expectedMintIndex:
            match.expectedMintIndex,
          count: 0,
        };
      }

      rule4DiagnosticSummary.bySchema[
        schemaKey
      ].count += 1;
    }

    if (
      rule4DiagnosticSamples.length < 20
    ) {
      rule4DiagnosticSamples.push({
        signature:
          signature || null,

        eventType,

        candidateCount:
          candidates.length,

        candidates,

        expectedMint,

        validMatches,
      });
    }
  }

  // ----------------------------------------------
  // CONFLICT:
  // recognized schemas pointed to >1 candidate
  // ----------------------------------------------

  else {
    rule4DiagnosticSummary.stillUnresolved += 1;

    if (
      rule4DiagnosticSamples.length < 20
    ) {
      rule4DiagnosticSamples.push({
        signature:
          signature || null,

        eventType,

        candidateCount:
          candidates.length,

        candidates,

        conflict: true,

        expectedMints,

        validMatches,
      });
    }
  }

  maybeFinishRule4Diagnostic();
}


// ================================================
// RULE 4 DIAGNOSTIC COMPLETION LOGGER
// ================================================

function maybeFinishRule4Diagnostic() {
  if (
    rule4DiagnosticSummary.examined !==
    RULE4_SAMPLE_LIMIT
  ) {
    return;
  }

  logInfo(
    "Rule 4 shadow diagnostic complete",
    {
      examined:
        rule4DiagnosticSummary.examined,

      resolvable:
        rule4DiagnosticSummary.resolvable,

      stillUnresolved:
        rule4DiagnosticSummary.stillUnresolved,

      expectedMintNotCandidate:
        rule4DiagnosticSummary
          .expectedMintNotCandidate,

      unsupportedSchema:
        rule4DiagnosticSummary
          .unsupportedSchema,

      resolutionRate:
        rule4DiagnosticSummary.examined > 0
          ? (
              rule4DiagnosticSummary.resolvable /
              rule4DiagnosticSummary.examined
            )
          : 0,

      bySchema:
        Object.values(
          rule4DiagnosticSummary.bySchema
        ),

      examples:
        rule4DiagnosticSamples,
    }
  );
}
  // ----------------------------------------------
  // WALLET
  // ----------------------------------------------

  const walletAddress =
    getSignerWallet(tx);

  // ----------------------------------------------
  // CREATE METADATA
  // ----------------------------------------------

  const { name, symbol } =
    parseCreateMetadata(tx);

  // ----------------------------------------------
  // TRADE AMOUNTS
  // ----------------------------------------------

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

  // ----------------------------------------------
  // PRICE
  // ----------------------------------------------

  const pricePerToken =
    Number.isFinite(solAmount) &&
    solAmount > 0 &&
    Number.isFinite(tokenAmount) &&
    tokenAmount > 0
      ? solAmount / tokenAmount
      : null;

  // ----------------------------------------------
  // BLOCK TIME
  // ----------------------------------------------

  const blockTime =
    getBlockTime(tx);

  const isMigrate =
    eventType === "migrate";

  // ----------------------------------------------
  // FINAL CLASSIFICATION
  // ----------------------------------------------

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
//
// Performance diagnostics:
//
// Individual primary SQL operations are timed through
// timedPoolQuery().
//
// This does NOT change:
// • SQL statements
// • SQL parameters
// • Query ordering
// • Return values
// • Error behavior
// • Database transaction behavior
//
// Timed operations:
//
// • Token upsert
// • Event insert
// • Market token update
// • Market event update
// • Graduation update
//
// Raw-event storage remains unchanged because it is
// disabled by default and is outside the current
// primary ingestion bottleneck investigation.
// ==================================================


// ==================================================
// 11A. OPTIONAL RAW EVENT STORAGE
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


// ==================================================
// 11B. TOKEN UPSERT
// ==================================================

async function upsertLaunchpadToken(token) {
  if (!token?.token_address) {
    return false;
  }

  const result = await timedPoolQuery(
    "sqlTokenUpsert",
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


// ==================================================
// 11C. EVENT INSERT
// ==================================================

async function insertLaunchpadEvent(event) {
  const result = await timedPoolQuery(
    "sqlEventInsert",
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


// ==================================================
// 11D. LIVE MARKET DATA
// ==================================================

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


  // ----------------------------------------------
  // TOKEN MARKET-DATA UPDATE
  // ----------------------------------------------

  await timedPoolQuery(
    "sqlMarketTokenUpdate",
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


  // ----------------------------------------------
  // EVENT MARKET-DATA UPDATE
  // ----------------------------------------------

  await timedPoolQuery(
    "sqlMarketEventUpdate",
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


// ==================================================
// 11E. GRADUATION UPDATE
// ==================================================

async function markTokenGraduated(
  tokenAddress,
  graduatedAt
) {
  if (!tokenAddress) return;

  await timedPoolQuery(
    "sqlGraduationUpdate",
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
// through the existing pre-grad ingestion pipeline.
//
// Performance diagnostics:
// • Measure total processing time
// • Measure the database-write phase
// • Record timing on success and failure
// • Preserve existing ingestion behavior
// ==================================================


// ==================================================
// 13A. PROCESS QUEUED SIGNATURE
// ==================================================

async function processQueuedSignature(item) {
  if (!item?.signature) {
    return;
  }

  const signature = item.signature;

  queuedSignatures.delete(signature);

  if (
    seenSignatures.has(signature) ||
    inFlightSignatures.has(signature)
  ) {
    stats.droppedDuplicate += 1;
    return;
  }

  if (
    Date.now() - item.enqueuedAt >
    SIGNATURE_MAX_AGE_MS
  ) {
    stats.droppedStale += 1;
    return;
  }

  if (!isPregradEnabled()) {
    stats.droppedDuringPause += 1;
    return;
  }

  inFlightSignatures.add(signature);
  stats.dequeued += 1;

  // Begin timing only after the signature is accepted
  // for processing. Queue waiting time is excluded.
  const processingStartedAt = performanceNow();

  let permanentlySeen = false;

  try {
    // ----------------------------------------------
    // FETCH HYDRATED TRANSACTION
    //
    // fetchFullTransaction() already records its
    // own RPC timing in Section 9.
    // ----------------------------------------------

    const tx = await fetchFullTransaction(
      signature
    );

    if (!tx) {
      stats.skippedEmptyTx += 1;

      // Preserve existing behavior:
      // Do not mark a temporary null RPC response
      // as permanently seen.
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

        type: "helius_ws_pregrad_tx",

        payload: tx,
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
    "unsupported_pump_instruction"
  ) {
    stats.skippedUnsupportedPumpInstruction += 1;
  }

  else if (
    classified.reason ===
    "unresolved_token_mint"
  ) {
    stats.skippedUnresolvedMint += 1;

    recordUnresolvedMintDiagnostics(
      tx,
      signature
    );
  }

  permanentlySeen = true;
  return;
}

    const event = classified.event;
    const token = classified.tokenUpsert;

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

      const solAmount = Number(
        event.sol_amount
      );

      if (
        !Number.isFinite(solAmount) ||
        solAmount < minSolAmount
      ) {
        stats.skippedSmallSolAmount += 1;
        permanentlySeen = true;
        return;
      }
    }

    // ----------------------------------------------
    // DATABASE WRITE PERFORMANCE
    //
    // Measures the complete primary write phase:
    // • Token upsert
    // • Event insert
    // • Market-data update, when applicable
    // • Graduation update, when applicable
    //
    // The inner finally records timing even if
    // a database operation throws or returns early.
    // ----------------------------------------------

    const dbWriteStartedAt = performanceNow();

    let inserted = false;

    try {
      // --------------------------------------------
      // PRIMARY TOKEN WRITE
      // --------------------------------------------

      await upsertLaunchpadToken(token);

      // --------------------------------------------
      // PRIMARY EVENT WRITE
      // --------------------------------------------

      inserted = await insertLaunchpadEvent(
        event
      );

      permanentlySeen = true;

      if (!inserted) {
        return;
      }

      // --------------------------------------------
      // LIVE MARKET DATA
      // --------------------------------------------

      if (
        ["buy", "sell"].includes(
          event.event_type
        )
      ) {
        await updateLaunchpadMarketDataFromEvent(
          event
        );
      }

      // --------------------------------------------
      // GRADUATION
      // --------------------------------------------

      if (
        event.event_type === "migrate"
      ) {
        await markTokenGraduated(
          event.token_address,
          event.block_time
        );
      }
    } finally {
      recordPerformanceTiming(
        "dbWrite",
        performanceNow() - dbWriteStartedAt
      );
    }

    // ----------------------------------------------
    // ASYNC HOLDER ENRICHMENT
    //
    // Only creates and buys initiate holder scans.
    // This call is intentionally never awaited.
    // ----------------------------------------------

    if (
      event.event_type === "create" ||
      event.event_type === "buy"
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
    // Preserve the existing error counter and
    // error-handling behavior.
    stats.txFetchErrors += 1;

    logError(
      "Failed processing signature",
      {
        signature,

        error: String(
          error?.message || error
        ),
      }
    );
  } finally {
    // ----------------------------------------------
    // TOTAL PROCESSING PERFORMANCE
    //
    // Includes:
    // • RPC fetch and retries
    // • Optional raw storage
    // • Classification
    // • Database writes
    //
    // Excludes:
    // • Time spent waiting in the queue
    // • Async holder-enrichment execution
    // ----------------------------------------------

    recordPerformanceTiming(
      "processing",
      performanceNow() - processingStartedAt
    );

    inFlightSignatures.delete(signature);

    if (permanentlySeen) {
      addSeenSignature(signature);
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
// Produces one compact scanner-health record per
// logging interval.
//
// Existing diagnostics:
// • WebSocket health
// • Queue depth and oldest signature age
// • In-flight transaction count
// • Incoming, drain, insert and processing rates
// • Cumulative scanner counters
//
// Additional performance diagnostics:
// • RPC fetch latency
// • Database-write latency
// • Total signature-processing latency
// • Completed and ongoing intake-pause duration
// • PostgreSQL connection-pool pressure
// • Effective runtime configuration
//
// Timing summaries are cumulative since startup.
// Rate measurements cover the current log interval.
//
// Observation only: does not modify ingestion.
// ==================================================

function startQueueLogger() {
  if (queueLogTimer) {
    return;
  }

  // Prevent overlapping async logger executions.
  let loggerRunning = false;

  queueLogTimer = setInterval(
    async () => {
      if (loggerRunning) {
        return;
      }

      loggerRunning = true;

      try {
        // ------------------------------------------
        // SHARED CONTROL REFRESH
        // ------------------------------------------

        await getPregradControl();

        const now = Date.now();

        const seconds = Math.max(
          (now - previousLogAt) / 1000,
          1
        );

        const oldest = signatureQueue[0];

        // ------------------------------------------
        // INGESTION RATES
        //
        // These measure the interval since the
        // previous successful stats log.
        // ------------------------------------------

        const incomingPerSecond = Number(
          (
            (stats.queued - previousStats.queued) /
            seconds
          ).toFixed(2)
        );

        const drainedPerSecond = Number(
          (
            (stats.dequeued - previousStats.dequeued) /
            seconds
          ).toFixed(2)
        );

        const insertedPerSecond = Number(
          (
            (
              stats.insertedEvents -
              previousStats.insertedEvents
            ) / seconds
          ).toFixed(2)
        );

        const processedPerSecond = Number(
          (
            (
              stats.processed -
              previousStats.processed
            ) / seconds
          ).toFixed(2)
        );

        // ------------------------------------------
        // PERFORMANCE SUMMARIES
        //
        // Cumulative since scanner startup.
        // Includes samples, average and maximum.
        // ------------------------------------------

        const rpcFetchPerformance =
          getPerformanceSummary("rpcFetch");

        const dbWritePerformance =
          getPerformanceSummary("dbWrite");

        const processingPerformance =
          getPerformanceSummary("processing");

        const intakePausePerformance =
          getPerformanceSummary("intakePause");

        // ------------------------------------------
        // CURRENT INTAKE PAUSE
        //
        // Completed pauses are recorded in stats.
        // An ongoing pause must be measured here.
        // ------------------------------------------

        const currentPauseMs =
          intakePaused &&
          intakePausedAt !== null
            ? Math.max(
                Math.round(
                  performanceNow() - intakePausedAt
                ),
                0
              )
            : 0;

        // ------------------------------------------
        // POSTGRESQL CONNECTION POOL
        //
        // These are synchronous pool properties.
        // No additional database query is required.
        // ------------------------------------------

        const postgresPool = {
          totalConnections:
            pool.totalCount,

          idleConnections:
            pool.idleCount,

          waitingRequests:
            pool.waitingCount,

          configuredMaxConnections:
            pool.options.max,
        };

        // ------------------------------------------
        // EFFECTIVE RUNTIME CONFIGURATION
        //
        // Queue limit and minimum SOL threshold may
        // be overridden by pregrad_system_control.
        // ------------------------------------------

        const effectiveConfiguration = {
          maxQueueSize:
            effectiveMaxQueueSize(),

          resumeQueueSize:
            RESUME_QUEUE_SIZE,

          signatureMaxAgeMs:
            SIGNATURE_MAX_AGE_MS,

          workerConcurrency:
            WORKER_CONCURRENCY,

          maxTxPerSecond:
            MAX_TX_PER_SECOND,

          minSolAmount:
            effectiveMinSolAmount(),

          storeRawEvents:
            STORE_RAW_EVENTS,
        };

        // ------------------------------------------
        // EMIT SCANNER HEALTH RECORD
        // ------------------------------------------

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
              ws?.readyState ?? null,

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
                    now - oldest.enqueuedAt,
                    0
                  )
                : 0,

            // Interval rates
            incomingPerSecond,
            drainedPerSecond,
            insertedPerSecond,
            processedPerSecond,

            // Cumulative latency summaries
            rpcFetchPerformance,
            dbWritePerformance,
            processingPerformance,
            intakePausePerformance,

            // Ongoing pause duration
            currentPauseMs,

            // Database connection pressure
            postgresPool,

            // Actual active settings
            effectiveConfiguration,

            // Preserve every existing counter
            ...stats,
          }
        );

        // ------------------------------------------
        // UPDATE INTERVAL BASELINE
        //
        // Only advance after successful logging.
        // ------------------------------------------

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

        previousLogAt = now;
      } catch (error) {
        logError(
          "Scanner stats logging failed",
          {
            error: String(
              error?.message || error
            ),
          }
        );
      } finally {
        loggerRunning = false;
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
