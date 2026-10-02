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

// --------------------------------------------------
// SOLANA PROGRAM IDS
// --------------------------------------------------

const PUMP_LAUNCHPAD_PROGRAM_ID =
  process.env.PUMP_LAUNCHPAD_PROGRAM_ID ||
  "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

const SPL_TOKEN_PROGRAM_ID =
  "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

const TOKEN_2022_PROGRAM_ID =
  "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

// --------------------------------------------------
// HELIUS ENDPOINTS
// --------------------------------------------------

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
// ==================================================

const WORKER_CONCURRENCY = Number(
  process.env.WORKER_CONCURRENCY || 8
);


// ==================================================
// 2C-1. GLOBAL HELIUS RPC TOKEN BUCKET
//
// Limits aggregate Helius RPC request STARTS across:
//
// • getTransaction
// • getTokenSupply
// • getTokenLargestAccounts
//
// Tokens refill continuously at the configured
// long-run rate.
//
// Unused capacity may accumulate up to the configured
// burst capacity so short traffic spikes can be
// absorbed without permanently increasing the
// long-run RPC rate.
//
// V1.2:
// • Refill rate: 55 starts/sec
// • Burst capacity: 15 starts
// ==================================================

const HELIUS_RPC_MAX_STARTS_PER_SECOND =
  Number(
    process.env
      .HELIUS_RPC_MAX_STARTS_PER_SECOND ||
      55
  );

const HELIUS_RPC_BURST_CAPACITY =
  Number(
    process.env
      .HELIUS_RPC_BURST_CAPACITY ||
      15
  );

// ==================================================
// 2C-2. DATABASE WRITE DISPATCHER
//
// Signature workers fetch/classify transactions and
// hand accepted writes to this independent dispatcher.
//
// Different tokens may write concurrently.
// The same token is dispatched strictly FIFO.
//
// The existing per-token serializer remains enabled
// inside the DB write function as a safety net.
// ==================================================

const DB_WRITE_CONCURRENCY = Number(
  process.env.DB_WRITE_CONCURRENCY || 12
);

const MAX_DB_WRITE_QUEUE_SIZE = Number(
  process.env.MAX_DB_WRITE_QUEUE_SIZE || 10000
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
// 2E-1. POSTGRES BASELINE RTT DIAGNOSTIC
//
// Measures end-to-end latency of a minimal:
//
//   SELECT 1
//
// against the same PostgreSQL pool used by ingestion.
//
// Diagnostic only:
// • No writes
// • No locks
// • No schema changes
// • Runs independently of transaction writes
// ==================================================

const DB_RTT_PROBE_INTERVAL_MS = Number(
  process.env.DB_RTT_PROBE_INTERVAL_MS || 10000
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
// ==================================================
// DATABASE WRITE DISPATCHER STATE
// ==================================================

const dbWriteQueue = [];

// Tokens that currently have a DB job executing.
const activeDbWriteTokens = new Set();

let dbWritesInFlight = 0;
let dbWriteDispatcherScheduled = false;

const tokenSafetyEnrichmentInFlight = new Map();
const tokenLastHolderEnrichedAt = new Map();

// ==================================================
// GLOBAL HELIUS RPC TOKEN BUCKET STATE
// ==================================================

let heliusRpcTokens =
  Math.max(
    0,
    HELIUS_RPC_BURST_CAPACITY
  );

let heliusRpcLastRefillAt =
  performanceNow();

let heliusRpcPacerTail =
  Promise.resolve();

// ==================================================
// ACTIVE TOKEN DATABASE WRITES
//
// Diagnostic-only tracker for concurrent writes
// targeting the same token.
// ==================================================

const activeTokenDbWrites =
  new Map();

// ==================================================
// REAL PER-TOKEN DATABASE SERIALIZER
//
// When enabled:
//
// • Writes for DIFFERENT tokens remain concurrent.
// • Writes for the SAME token execute one at a time.
// • Serialization happens BEFORE pool.connect().
// • No additional PostgreSQL queries are added.
//
// Controlled through:
//
// TOKEN_DB_SERIALIZATION_ENABLED=true
// ==================================================

const TOKEN_DB_SERIALIZATION_ENABLED =
  String(
    process.env
      .TOKEN_DB_SERIALIZATION_ENABLED ||
      "false"
  ) === "true";

const tokenDbSerializationLanes =
  new Map();

// ==================================================
// SHADOW PER-TOKEN SERIALIZER
//
// Diagnostic-only simulation.
//
// Models what would happen if combined database writes
// for the SAME token were serialized.
//
// IMPORTANT:
//
// • Does NOT delay real writes.
// • Does NOT change worker behavior.
// • Does NOT acquire additional DB connections.
// • Does NOT change SQL.
// • Does NOT affect ingestion ordering.
//
// Each token stores the hypothetical time at which its
// serialized write lane becomes available.
// ==================================================

const shadowTokenSerializer =
  new Map();

const SEEN_SIGNATURE_LIMIT = Number(
  process.env.SEEN_SIGNATURE_LIMIT || 100000
);

let queueLogTimer = null;
let staleDrainTimer = null;
let dbRttProbeTimer = null;
let dbRttProbeRunning = false;
let intakeCompletenessTimer = null;

// ==================================================
// POSTGRES RTT DISTRIBUTION / RECENT HISTORY
// ==================================================

const DB_RTT_RECENT_LIMIT = 10;

const dbRttProbeBuckets = {
  under10ms: 0,
  ms10To25: 0,
  ms25To50: 0,
  ms50To100: 0,
  ms100To150: 0,
  ms150To250: 0,
  ms250To500: 0,
  ms500Plus: 0,
};

const dbRttRecentProbes = [];

// ==================================================
// POSTGRES RTT BACKEND IDENTITY DIAGNOSTIC
//
// Tracks RTT behavior by PostgreSQL backend PID.
//
// This lets us determine whether:
//
// • individual pooled connections remain FAST / SLOW
// • or the SAME connection switches between regimes
//
// Diagnostic only:
// • No additional PostgreSQL queries
// • Uses node-postgres client.processID
// ==================================================

const dbRttBackendDiagnostics =
  new Map();


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
  resolvedMultipleCandidateRule5: 0,
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
// GLOBAL HELIUS RPC PACER
// ==========================================

heliusRpcPacerSamples: 0,
heliusRpcPacerImmediate: 0,
heliusRpcPacerWaited: 0,

heliusRpcPacerWaitTotalMs: 0,
heliusRpcPacerWaitMaxMs: 0,


  // ==========================================
// TOKEN SERIALIZER WORKER WAIT DIAGNOSTICS
//
// Measures how many scanner workers are
// simultaneously parked waiting for a
// same-token serialization lane.
//
// Observation only:
// • No database queries
// • No scheduling changes
// • No ingestion behavior changes
// ==========================================

tokenSerializerWorkersWaitingCurrent: 0,
tokenSerializerWorkersWaitingMax: 0,

tokenSerializerWorkerWaitSamples: 0,

tokenSerializerWorkerWaitDepth1: 0,
tokenSerializerWorkerWaitDepth2: 0,
tokenSerializerWorkerWaitDepth3To5: 0,
tokenSerializerWorkerWaitDepth6To10: 0,
tokenSerializerWorkerWaitDepth11To20: 0,
tokenSerializerWorkerWaitDepth21Plus: 0,

tokenSerializerWorkerSaturationSamples: 0,

// ==========================================
// ERRORS
// ==========================================

txFetchErrors: 0,
workerErrors: 0,

rpcRetries: 0,
rpcNullRetries: 0,
rpcRateLimitedRetries: 0,
rpcOtherRetries: 0,

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
  rpcAttemptSamples: 0,
rpcAttemptTotalMs: 0,
rpcAttemptMaxMs: 0,

  // ==========================================
  // DATABASE WRITE PERFORMANCE
  // ==========================================

  dbWriteSamples: 0,
  dbWriteTotalMs: 0,
  dbWriteMaxMs: 0,

  // ==========================================
// DATABASE CONNECTION / QUERY DIAGNOSTICS
// ==========================================

// Time spent waiting for pool.connect()
dbPoolAcquireSamples: 0,
dbPoolAcquireTotalMs: 0,
dbPoolAcquireMaxMs: 0,

// Time spent executing SQL after a client
// has already been acquired.
dbQueryExecutionSamples: 0,
dbQueryExecutionTotalMs: 0,
dbQueryExecutionMaxMs: 0,

// Slow-query buckets.
dbQueriesOver250ms: 0,
dbQueriesOver500ms: 0,
dbQueriesOver1000ms: 0,
dbQueriesOver5000ms: 0,

  dbBeginSamples: 0,
dbBeginTotalMs: 0,
dbBeginMaxMs: 0,

dbTokenUpsertSamples: 0,
dbTokenUpsertTotalMs: 0,
dbTokenUpsertMaxMs: 0,

dbEventInsertSamples: 0,
dbEventInsertTotalMs: 0,
dbEventInsertMaxMs: 0,

dbMarketUpdateSamples: 0,
dbMarketUpdateTotalMs: 0,
dbMarketUpdateMaxMs: 0,

dbCommitSamples: 0,
dbCommitTotalMs: 0,
dbCommitMaxMs: 0,

  // ==========================================
// DATABASE WRITE DISPATCHER
// ==========================================

dbWriteJobsQueued: 0,
dbWriteJobsStarted: 0,
dbWriteJobsCompleted: 0,
dbWriteJobsFailed: 0,

dbWriteQueueDepthMax: 0,

dbWriteInFlightCurrent: 0,
dbWriteInFlightMax: 0,

dbWriteDispatchBlockedSameToken: 0,
dbWriteDispatchBackpressure: 0,

dbWriteQueueWaitSamples: 0,
dbWriteQueueWaitTotalMs: 0,
dbWriteQueueWaitMaxMs: 0,

  // ==========================================
// POSTGRES BASELINE RTT DIAGNOSTIC
// ==========================================

dbRttProbeSamples: 0,
dbRttProbeErrors: 0,

dbRttProbeTotalMs: 0,
dbRttProbeLatestMs: 0,
dbRttProbeMaxMs: 0,

  // ==========================================
// POSTGRES RTT COMPONENT DIAGNOSTIC
// ==========================================

// Time spent acquiring a client specifically
// for the baseline RTT probe.
dbRttAcquireSamples: 0,
dbRttAcquireTotalMs: 0,
dbRttAcquireMaxMs: 0,

// Time spent executing SELECT 1 after the
// probe already owns a PostgreSQL client.
dbRttQuerySamples: 0,
dbRttQueryTotalMs: 0,
dbRttQueryMaxMs: 0,
  // ==========================================
// REAL TOKEN SERIALIZATION
// ==========================================

tokenSerializerSamples: 0,
tokenSerializerImmediate: 0,
tokenSerializerWaited: 0,

tokenSerializerWaitTotalMs: 0,
tokenSerializerWaitMaxMs: 0,

tokenSerializerQueueDepthTotal: 0,
tokenSerializerQueueDepthMax: 0,

  // ==========================================
// SLOW TRANSACTION FORENSICS
//
// For transactions >= 500ms, identify which
// measured database stage consumed the most time.
// ==========================================

slowTxnForensicSamples: 0,

slowTxnTokenUpsertDominant: 0,
slowTxnCommitDominant: 0,
slowTxnEventInsertDominant: 0,
slowTxnMarketUpdateDominant: 0,
slowTxnBeginDominant: 0,

slowTxnUnclassified: 0,

slowTxnDominantStageTotalMs: 0,
slowTxnDominantStageMaxMs: 0,

  // ==========================================
// SLOW TRANSACTION CONTENTION FORENSICS
//
// For ALL transactions >= 500ms:
//
// • Did another write for the same token
//   already exist when this transaction began?
//
// • Was the transaction COMMIT dominant?
//
// Observation only.
// ==========================================

slowTxnWithSameTokenContention: 0,
slowTxnWithoutSameTokenContention: 0,

slowTxnContentionDepthTotal: 0,
slowTxnContentionDepthMax: 0,

slowTxnWithDepth1: 0,
slowTxnWithDepth2: 0,
slowTxnWithDepth3Plus: 0,

// COMMIT-dominant subset

slowCommitDominantWithSameTokenContention: 0,
slowCommitDominantWithoutSameTokenContention: 0,

slowCommitDominantContentionDepthTotal: 0,
slowCommitDominantContentionDepthMax: 0,

  // ==========================================
// SHADOW TOKEN SERIALIZATION
//
// Simulates per-token DB serialization without
// changing production behavior.
// ==========================================

shadowSerializerSamples: 0,

shadowSerializerImmediateSamples: 0,
shadowSerializerWouldWaitSamples: 0,

shadowSerializerWaitTotalMs: 0,
shadowSerializerWaitMaxMs: 0,

shadowSerializerDepth1: 0,
shadowSerializerDepth2: 0,
shadowSerializerDepth3Plus: 0,
shadowSerializerQueueDepthMax: 0,
  shadowSerializerResolvedSamples: 0,
shadowSerializerCancelledSamples: 0,

// Number of real writes that began while another
// real write for the same token was active.
allTokenWritesWithSameTokenContention: 0,
allTokenWritesWithoutSameTokenContention: 0,

// Total observed DB transaction execution time fed
// into the shadow model.
shadowSerializerObservedServiceTotalMs: 0,
shadowSerializerObservedServiceMaxMs: 0,

  // ==========================================
// SLOW TOKEN UPSERT CONTENTION FORENSICS
//
// For token UPSERTs >= 500ms, determine whether
// another database write for the same token was
// already in flight when this transaction began.
//
// Observation only:
// • No additional database queries
// • No per-transaction logging
// • No ingestion behavior changes
// ==========================================

slowTokenUpsertSamples: 0,

slowTokenUpsertWithSameTokenContention: 0,
slowTokenUpsertWithoutSameTokenContention: 0,

slowTokenUpsertContentionDepthTotal: 0,
slowTokenUpsertContentionDepthMax: 0,

slowTokenUpsertWithDepth1: 0,
slowTokenUpsertWithDepth2: 0,
slowTokenUpsertWithDepth3Plus: 0,

slowTokenUpsertDurationTotalMs: 0,
slowTokenUpsertDurationMaxMs: 0,

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
// CONTENTION DEPTH PERFORMANCE DIAGNOSTICS
//
// Observation only. No additional database queries.
// ==================================================

const CONTENTION_DEPTH_BUCKETS = [
  "0",
  "1",
  "2",
  "3-5",
  "6-10",
  "11+",
];

function makeDepthBucket() {
  return {
    samples: 0,
    totalMs: 0,
    maxMs: 0,
    over250ms: 0,
    over500ms: 0,
    over1000ms: 0,
  };
}

function makeDepthBuckets() {
  return Object.fromEntries(
    CONTENTION_DEPTH_BUCKETS.map(
      (name) => [name, makeDepthBucket()]
    )
  );
}

const contentionDepthDiagnostics = {
  upsertAtArrival: makeDepthBuckets(),
  upsertAtExecution: makeDepthBuckets(),
  transactionAtArrival: makeDepthBuckets(),
  transactionAtExecution: makeDepthBuckets(),
};

function getContentionDepthBucket(depth) {
  if (!Number.isFinite(depth) || depth < 0) {
    return null;
  }

  const value = Math.floor(depth);

  if (value === 0) return "0";
  if (value === 1) return "1";
  if (value === 2) return "2";
  if (value <= 5) return "3-5";
  if (value <= 10) return "6-10";

  return "11+";
}

function recordContentionDepthTiming(
  category,
  depth,
  durationMs
) {
  const bucketName =
    getContentionDepthBucket(depth);

  const bucket =
    contentionDepthDiagnostics[category]?.[
      bucketName
    ];

  if (
    !bucket ||
    !Number.isFinite(durationMs) ||
    durationMs < 0
  ) {
    return;
  }

  bucket.samples += 1;
  bucket.totalMs += durationMs;

  bucket.maxMs = Math.max(
    bucket.maxMs,
    durationMs
  );

  if (durationMs >= 250) {
    bucket.over250ms += 1;
  }

  if (durationMs >= 500) {
    bucket.over500ms += 1;
  }

  if (durationMs >= 1000) {
    bucket.over1000ms += 1;
  }
}

function summarizeContentionDepth() {
  return Object.fromEntries(
    Object.entries(
      contentionDepthDiagnostics
    ).map(([category, buckets]) => [
      category,

      Object.fromEntries(
        Object.entries(buckets).map(
          ([depth, bucket]) => [
            depth,
            {
              samples: bucket.samples,

              avgMs:
                bucket.samples > 0
                  ? Number(
                      (
                        bucket.totalMs /
                        bucket.samples
                      ).toFixed(2)
                    )
                  : null,

              maxMs:
                bucket.samples > 0
                  ? Number(
                      bucket.maxMs.toFixed(2)
                    )
                  : null,

              slow500Pct:
                bucket.samples > 0
                  ? Number(
                      (
                        100 *
                        bucket.over500ms /
                        bucket.samples
                      ).toFixed(2)
                    )
                  : null,

              over250ms: bucket.over250ms,
              over500ms: bucket.over500ms,
              over1000ms: bucket.over1000ms,
            },
          ]
        )
      ),
    ])
  );
}
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
// Core pipeline:
// • Helius transaction fetch
// • Individual Helius RPC attempts
// • Complete database-write phase
// • Complete signature processing
// • Intake pauses
//
// Existing SQL diagnostics:
// • Token upsert
// • Event insert / combined transaction
// • Market token update
// • Market event update
// • Graduation update
//
// Database diagnostics:
// • PostgreSQL pool acquisition
// • Complete transaction execution
// • Slow transaction thresholds
//
// Combined-write stage diagnostics:
// • BEGIN
// • Token UPSERT
// • Event INSERT
// • Market UPDATE
// • COMMIT
//
// Uses counters defined in const stats.
// ==================================================

function performanceNow() {
  return Number(
    process.hrtime.bigint()
  ) / 1e6;
}


// ==================================================
// CORE PERFORMANCE COUNTER MAP
// ==================================================

function getPerformanceCounterMap() {
  return {
    rpcFetch: {
      samples: "rpcFetchSamples",
      total: "rpcFetchTotalMs",
      max: "rpcFetchMaxMs",
    },

    rpcAttempt: {
      samples: "rpcAttemptSamples",
      total: "rpcAttemptTotalMs",
      max: "rpcAttemptMaxMs",
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


// ==================================================
// CORE PERFORMANCE TIMING RECORDER
// ==================================================

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


// ==================================================
// CORE PERFORMANCE SUMMARY
// ==================================================

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
// DATABASE DIAGNOSTIC TIMING
//
// Measures:
//
// • poolAcquire
//     Time waiting for pool.connect()
//
// • queryExecution
//     Complete combined-write transaction after
//     a PostgreSQL client has been acquired.
// ==================================================

function recordDbDiagnosticTiming(
  category,
  durationMs
) {
  if (
    !Number.isFinite(durationMs) ||
    durationMs < 0
  ) {
    return;
  }

  let samplesKey;
  let totalKey;
  let maxKey;

  if (category === "poolAcquire") {
    samplesKey =
      "dbPoolAcquireSamples";

    totalKey =
      "dbPoolAcquireTotalMs";

    maxKey =
      "dbPoolAcquireMaxMs";

  } else if (
    category === "queryExecution"
  ) {
    samplesKey =
      "dbQueryExecutionSamples";

    totalKey =
      "dbQueryExecutionTotalMs";

    maxKey =
      "dbQueryExecutionMaxMs";

  } else {
    return;
  }

  stats[samplesKey] += 1;

  stats[totalKey] +=
    durationMs;

  stats[maxKey] = Math.max(
    stats[maxKey],
    durationMs
  );
}


// ==================================================
// COMBINED-WRITE STAGE DIAGNOSTICS
//
// Measures individual stages inside 11C:
//
// • begin
// • tokenUpsert
// • eventInsert
// • marketUpdate
// • commit
//
// These counters are observational only.
// ==================================================

function getDbStageCounterMap() {
  return {
    begin: {
      samples: "dbBeginSamples",
      total: "dbBeginTotalMs",
      max: "dbBeginMaxMs",
    },

    tokenUpsert: {
      samples: "dbTokenUpsertSamples",
      total: "dbTokenUpsertTotalMs",
      max: "dbTokenUpsertMaxMs",
    },

    eventInsert: {
      samples: "dbEventInsertSamples",
      total: "dbEventInsertTotalMs",
      max: "dbEventInsertMaxMs",
    },

    marketUpdate: {
      samples: "dbMarketUpdateSamples",
      total: "dbMarketUpdateTotalMs",
      max: "dbMarketUpdateMaxMs",
    },

    commit: {
      samples: "dbCommitSamples",
      total: "dbCommitTotalMs",
      max: "dbCommitMaxMs",
    },
  };
}

// ==================================================
// SLOW TRANSACTION FORENSICS
//
// Observation only.
//
// For transactions >= 500ms:
// • Determine which measured DB stage was slowest.
// • Increment one aggregate dominance counter.
// • Do not log individual transactions.
// • Do not add database work.
// ==================================================

function recordSlowTransactionForensic(
  totalDurationMs,
  stageDurations,
  sameTokenContentionDepth = 0
) {
  if (
    !Number.isFinite(totalDurationMs) ||
    totalDurationMs < 500
  ) {
    return;
  }

  stats.slowTxnForensicSamples += 1;

  // ================================================
  // SAME-TOKEN CONTENTION CLASSIFICATION
  // ================================================

  const contentionDepth =
    Number.isFinite(
      sameTokenContentionDepth
    )
      ? Math.max(
          0,
          sameTokenContentionDepth
        )
      : 0;

  if (contentionDepth > 0) {
    stats.slowTxnWithSameTokenContention +=
      1;

    stats.slowTxnContentionDepthTotal +=
      contentionDepth;

    stats.slowTxnContentionDepthMax =
      Math.max(
        stats.slowTxnContentionDepthMax,
        contentionDepth
      );

    if (contentionDepth === 1) {
      stats.slowTxnWithDepth1 += 1;

    } else if (contentionDepth === 2) {
      stats.slowTxnWithDepth2 += 1;

    } else {
      stats.slowTxnWithDepth3Plus += 1;
    }

  } else {
    stats.slowTxnWithoutSameTokenContention +=
      1;
  }


  // ================================================
  // DOMINANT-STAGE CLASSIFICATION
  // ================================================

  const stages = [
    [
      "begin",
      stageDurations?.begin,
    ],
    [
      "tokenUpsert",
      stageDurations?.tokenUpsert,
    ],
    [
      "eventInsert",
      stageDurations?.eventInsert,
    ],
    [
      "marketUpdate",
      stageDurations?.marketUpdate,
    ],
    [
      "commit",
      stageDurations?.commit,
    ],
  ].filter(
    ([, durationMs]) =>
      Number.isFinite(durationMs) &&
      durationMs >= 0
  );

  if (!stages.length) {
    stats.slowTxnUnclassified += 1;
    return;
  }

  let dominantStage =
    stages[0];

  for (
    const stage of stages.slice(1)
  ) {
    if (
      stage[1] >
      dominantStage[1]
    ) {
      dominantStage = stage;
    }
  }

  const [
    dominantStageName,
    dominantStageDurationMs,
  ] = dominantStage;


  // ================================================
  // EXISTING DOMINANT-STAGE COUNTERS
  // ================================================

  switch (dominantStageName) {
    case "begin":
      stats.slowTxnBeginDominant += 1;
      break;

    case "tokenUpsert":
      stats.slowTxnTokenUpsertDominant +=
        1;
      break;

    case "eventInsert":
      stats.slowTxnEventInsertDominant +=
        1;
      break;

    case "marketUpdate":
      stats.slowTxnMarketUpdateDominant +=
        1;
      break;

    case "commit":
      stats.slowTxnCommitDominant += 1;
      break;

    default:
      stats.slowTxnUnclassified += 1;
      return;
  }


  // ================================================
  // COMMIT-DOMINANT CONTENTION FORENSICS
  // ================================================

  if (
    dominantStageName === "commit"
  ) {
    if (contentionDepth > 0) {
      stats.slowCommitDominantWithSameTokenContention +=
        1;

      stats.slowCommitDominantContentionDepthTotal +=
        contentionDepth;

      stats.slowCommitDominantContentionDepthMax =
        Math.max(
          stats.slowCommitDominantContentionDepthMax,
          contentionDepth
        );

    } else {
      stats.slowCommitDominantWithoutSameTokenContention +=
        1;
    }
  }


  // ================================================
  // EXISTING DOMINANT-STAGE DURATION STATS
  // ================================================

  stats.slowTxnDominantStageTotalMs +=
    dominantStageDurationMs;

  stats.slowTxnDominantStageMaxMs =
    Math.max(
      stats.slowTxnDominantStageMaxMs,
      dominantStageDurationMs
    );
}

function recordDbStageTiming(
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
    getDbStageCounterMap()[category];

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


function getDbStageSummary(
  category
) {
  const counters =
    getDbStageCounterMap()[category];

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
// REAL PER-TOKEN DATABASE SERIALIZATION
// ==================================================

async function acquireTokenDbSerializationLane(
  tokenAddress
) {
  if (
    !TOKEN_DB_SERIALIZATION_ENABLED ||
    !tokenAddress
  ) {
    return null;
  }

  let lane =
    tokenDbSerializationLanes.get(
      tokenAddress
    );

  if (!lane) {
    lane = {
      tail: Promise.resolve(),
      pending: 0,
    };

    tokenDbSerializationLanes.set(
      tokenAddress,
      lane
    );
  }

  lane.pending += 1;

    const queueDepth =
    Math.max(
      0,
      lane.pending - 1
    );

  stats.tokenSerializerSamples += 1;

  stats.tokenSerializerQueueDepthTotal +=
    queueDepth;

  stats.tokenSerializerQueueDepthMax =
    Math.max(
      stats.tokenSerializerQueueDepthMax,
      queueDepth
    );

  const previousTail =
    lane.tail;

  let releaseCurrent;

  const currentGate =
    new Promise((resolve) => {
      releaseCurrent = resolve;
    });

  lane.tail =
    previousTail.then(
      () => currentGate
    );

  const waitStartedAt =
  performanceNow();

// ================================================
// WORKER WAIT DIAGNOSTIC
//
// queueDepth > 0 means another same-token write
// is already ahead of this write.
//
// Because the caller awaits this function,
// this scanner worker is now parked until its
// token lane becomes available.
// ================================================

let countedAsWaitingWorker = false;

if (queueDepth > 0) {
  countedAsWaitingWorker = true;

  stats.tokenSerializerWorkersWaitingCurrent += 1;

  stats.tokenSerializerWorkerWaitSamples += 1;

  stats.tokenSerializerWorkersWaitingMax =
    Math.max(
      stats.tokenSerializerWorkersWaitingMax,
      stats.tokenSerializerWorkersWaitingCurrent
    );

  const workersWaiting =
    stats.tokenSerializerWorkersWaitingCurrent;

  if (workersWaiting === 1) {
    stats.tokenSerializerWorkerWaitDepth1 += 1;

  } else if (workersWaiting === 2) {
    stats.tokenSerializerWorkerWaitDepth2 += 1;

  } else if (workersWaiting <= 5) {
    stats.tokenSerializerWorkerWaitDepth3To5 += 1;

  } else if (workersWaiting <= 10) {
    stats.tokenSerializerWorkerWaitDepth6To10 += 1;

  } else if (workersWaiting <= 20) {
    stats.tokenSerializerWorkerWaitDepth11To20 += 1;

  } else {
    stats.tokenSerializerWorkerWaitDepth21Plus += 1;
  }

  if (
    workersWaiting >=
    WORKER_CONCURRENCY
  ) {
    stats.tokenSerializerWorkerSaturationSamples += 1;
  }
}

try {
  await previousTail;

} finally {
  if (countedAsWaitingWorker) {
    stats.tokenSerializerWorkersWaitingCurrent =
      Math.max(
        0,
        stats.tokenSerializerWorkersWaitingCurrent - 1
      );
  }
}

const waitDurationMs =
  performanceNow() -
  waitStartedAt;

    if (waitDurationMs >= 1) {
    stats.tokenSerializerWaited += 1;

    stats.tokenSerializerWaitTotalMs +=
      waitDurationMs;

    stats.tokenSerializerWaitMaxMs =
      Math.max(
        stats.tokenSerializerWaitMaxMs,
        waitDurationMs
      );

  } else {
    stats.tokenSerializerImmediate += 1;
  }

  return {
    tokenAddress,
    lane,
    releaseCurrent,
    waitDurationMs,
    released: false,
  };
}


function releaseTokenDbSerializationLane(
  reservation
) {
  if (
    !reservation ||
    reservation.released
  ) {
    return;
  }

  reservation.released = true;

  const {
    tokenAddress,
    lane,
    releaseCurrent,
  } = reservation;

  lane.pending = Math.max(
    0,
    lane.pending - 1
  );

  // Unblock exactly the next same-token write.
  releaseCurrent();

  // Delete only when this token has no current
  // or queued serialized writes remaining.
  if (
    lane.pending === 0 &&
    tokenDbSerializationLanes.get(
      tokenAddress
    ) === lane
  ) {
    tokenDbSerializationLanes.delete(
      tokenAddress
    );
  }
}

// ==================================================
// TOKEN DATABASE WRITE CONTENTION TRACKING
// ==================================================

function beginTokenDbWrite(
  tokenAddress
) {
  if (!tokenAddress) {
    return 0;
  }

  const existingDepth =
    activeTokenDbWrites.get(
      tokenAddress
    ) || 0;

  activeTokenDbWrites.set(
    tokenAddress,
    existingDepth + 1
  );

  // Return the number of OTHER writes that were
  // already active for this token.
  return existingDepth;
}

// ==================================================
// BEGIN SHADOW TOKEN SERIALIZATION
//
// Creates a hypothetical reservation for this token.
//
// No waiting occurs.
//
// Returns the hypothetical queue state that existed
// when this real write began.
// ==================================================

// ==================================================
// BEGIN SHADOW TOKEN SERIALIZATION
//
// Diagnostic-only FIFO simulation.
//
// Creates an ordered shadow reservation for this
// token without delaying the real database write.
//
// IMPORTANT:
//
// • Does NOT wait.
// • Does NOT change worker behavior.
// • Does NOT change database behavior.
// • Does NOT change event ordering.
// • Does NOT predict duration before it is known.
//
// The reservation is completed later by
// finishShadowTokenSerialization(), which supplies
// the observed DB transaction execution duration.
// ==================================================

function beginShadowTokenSerialization(
  tokenAddress,
  sameTokenContentionDepth = 0
) {
  if (!tokenAddress) {
    return null;
  }

  const realStartedAt =
    performanceNow();

  // ----------------------------------------------
  // GET / CREATE TOKEN SHADOW LANE
  // ----------------------------------------------

  let lane =
    shadowTokenSerializer.get(
      tokenAddress
    );

  if (!lane) {
    lane = {
      nextSequence: 1,

      // Last hypothetical serialized finish time
      // that has been fully resolved.
      resolvedAvailableAt: null,

      // Reservations waiting to be resolved in
      // original arrival order.
      writes: [],
    };

    shadowTokenSerializer.set(
      tokenAddress,
      lane
    );
  }

  // ----------------------------------------------
  // CREATE FIFO RESERVATION
  // ----------------------------------------------

  const sequence =
    lane.nextSequence;

  lane.nextSequence += 1;

  const queueDepthAtArrival =
    lane.writes.length;

  const reservation = {
    tokenAddress,
    sequence,

    realStartedAt,

    sameTokenContentionDepth:
      Number.isFinite(
        sameTokenContentionDepth
      )
        ? Math.max(
            0,
            sameTokenContentionDepth
          )
        : 0,

    queueDepthAtArrival,

    // Filled by completion helper.
    observedServiceMs: null,
    completed: false,
  };

  lane.writes.push(
    reservation
  );

  // ----------------------------------------------
  // BASE POPULATION COUNTERS
  // ----------------------------------------------

  stats.shadowSerializerSamples += 1;

  if (
    reservation.sameTokenContentionDepth > 0
  ) {
    stats.allTokenWritesWithSameTokenContention +=
      1;
  } else {
    stats.allTokenWritesWithoutSameTokenContention +=
      1;
  }

  // ----------------------------------------------
  // NOTE:
  //
  // Do NOT classify this reservation yet as:
  //
  // • immediate
  // • would wait
  // • depth 1 / 2 / 3+
  // • hypothetical wait time
  //
  // Those values cannot be known correctly until
  // predecessor service durations are available.
  //
  // finishShadowTokenSerialization() resolves them
  // later in FIFO order.
  // ----------------------------------------------

  return reservation;
}

// ==================================================
// FINISH SHADOW TOKEN SERIALIZATION
//
// Extends the hypothetical reservation using the
// REAL observed transaction execution duration.
//
// This is intentionally a workload simulation:
//
// observed duration != predicted serialized duration.
//
// Contention may inflate the observed duration, so
// shadow wait estimates should be interpreted as
// conservative queue-pressure estimates.
// ==================================================

// ==================================================
// FINISH SHADOW TOKEN SERIALIZATION
//
// Diagnostic-only FIFO simulation.
//
// Marks this reservation complete using the REAL
// observed DB transaction execution duration.
//
// Then resolves as many completed reservations as
// possible from the HEAD of this token's FIFO.
//
// IMPORTANT:
//
// • Does NOT delay real writes.
// • Does NOT change database behavior.
// • Does NOT change worker behavior.
// • Resolves strictly in shadow arrival order.
// • Uses observed transaction duration only after
//   that duration is actually known.
// ==================================================

function finishShadowTokenSerialization(
  reservation,
  observedServiceMs
) {
  if (
    !reservation?.tokenAddress ||
    !Number.isFinite(observedServiceMs) ||
    observedServiceMs < 0
  ) {
    return;
  }

  const tokenAddress =
    reservation.tokenAddress;

  const lane =
    shadowTokenSerializer.get(
      tokenAddress
    );

  if (!lane) {
    return;
  }

  // ----------------------------------------------
  // MARK THIS RESERVATION COMPLETE
  // ----------------------------------------------

  reservation.observedServiceMs =
    observedServiceMs;

  reservation.completed = true;

  stats.shadowSerializerObservedServiceTotalMs +=
    observedServiceMs;

  stats.shadowSerializerObservedServiceMaxMs =
    Math.max(
      stats.shadowSerializerObservedServiceMaxMs,
      observedServiceMs
    );

  // ----------------------------------------------
  // RESOLVE COMPLETED FIFO HEADS
  //
  // A later transaction may finish before an
  // earlier transaction.
  //
  // We therefore resolve only while the HEAD of
  // the queue has completed.
  // ----------------------------------------------

  while (
    lane.writes.length > 0 &&
    lane.writes[0].completed === true
  ) {
    const current =
      lane.writes.shift();

    stats.shadowSerializerResolvedSamples += 1;

    // --------------------------------------------
    // HYPOTHETICAL SERIALIZED START
    //
    // First write:
    //   starts when it really arrived.
    //
    // Later write:
    //   starts at the later of:
    //
    //   • its real arrival time
    //   • previous serialized finish time
    // --------------------------------------------

    const previousAvailableAt =
      Number.isFinite(
        lane.resolvedAvailableAt
      )
        ? lane.resolvedAvailableAt
        : current.realStartedAt;

    const hypotheticalStartAt =
      Math.max(
        current.realStartedAt,
        previousAvailableAt
      );

    const hypotheticalWaitMs =
      Math.max(
        0,
        hypotheticalStartAt -
          current.realStartedAt
      );

    const hypotheticalFinishAt =
      hypotheticalStartAt +
      current.observedServiceMs;

    // --------------------------------------------
    // CLASSIFY SHADOW RESULT
    // --------------------------------------------

    if (hypotheticalWaitMs > 0) {
      stats.shadowSerializerWouldWaitSamples +=
        1;

      stats.shadowSerializerWaitTotalMs +=
        hypotheticalWaitMs;

      stats.shadowSerializerWaitMaxMs =
        Math.max(
          stats.shadowSerializerWaitMaxMs,
          hypotheticalWaitMs
        );

      // queueDepthAtArrival represents how many
      // unresolved shadow writes were already ahead
      // of this write when it entered the lane.
      const shadowDepth =
        Math.max(
          1,
          current.queueDepthAtArrival || 0
        );

      stats.shadowSerializerQueueDepthMax =
        Math.max(
          stats.shadowSerializerQueueDepthMax,
          shadowDepth
        );

      if (shadowDepth === 1) {
        stats.shadowSerializerDepth1 += 1;

      } else if (shadowDepth === 2) {
        stats.shadowSerializerDepth2 += 1;

      } else {
        stats.shadowSerializerDepth3Plus += 1;
      }

    } else {
      stats.shadowSerializerImmediateSamples +=
        1;
    }

    // --------------------------------------------
    // ADVANCE HYPOTHETICAL SERIALIZED LANE
    // --------------------------------------------

    lane.resolvedAvailableAt =
      hypotheticalFinishAt;
  }

  // ----------------------------------------------
  // CLEAN UP IDLE TOKEN LANES
  //
  // Once every reservation for this token has been
  // resolved, no queue state needs to remain.
  //
  // Deleting it prevents this diagnostic Map from
  // growing indefinitely.
  // ----------------------------------------------

  if (lane.writes.length === 0) {
    shadowTokenSerializer.delete(
      tokenAddress
    );
  }
}

function endTokenDbWrite(
  tokenAddress
) {
  if (!tokenAddress) {
    return;
  }

  const currentDepth =
    activeTokenDbWrites.get(
      tokenAddress
    ) || 0;

  if (currentDepth <= 1) {
    activeTokenDbWrites.delete(
      tokenAddress
    );

    return;
  }

  activeTokenDbWrites.set(
    tokenAddress,
    currentDepth - 1
  );
}

// ==================================================
// CANCEL SHADOW TOKEN SERIALIZATION
//
// Diagnostic-only FIFO cleanup.
//
// Used when a shadow reservation was created but the
// real database transaction never actually began.
//
// Primary example:
//
// • pool.connect() fails before writeStartedAt exists.
//
// The cancelled reservation contributes NO observed
// service time and NO hypothetical wait classification.
//
// Once cancelled, it no longer blocks later completed
// reservations in the same shadow FIFO.
// ==================================================

function cancelShadowTokenSerialization(
  reservation
) {
  if (
    !reservation?.tokenAddress
  ) {
    return;
  }

  const tokenAddress =
    reservation.tokenAddress;

  const lane =
    shadowTokenSerializer.get(
      tokenAddress
    );

  if (!lane) {
    return;
  }

  // ----------------------------------------------
  // MARK RESERVATION CANCELLED
  //
  // A cancelled reservation represents work that
  // never entered the measured DB transaction.
  //
  // It therefore has zero shadow service time and
  // should not be classified as immediate/waiting.
  // ----------------------------------------------

  reservation.cancelled = true;
  reservation.completed = true;
  reservation.observedServiceMs = null;
  stats.shadowSerializerCancelledSamples += 1;

  // ----------------------------------------------
  // DRAIN RESOLVABLE FIFO HEADS
  //
  // Cancellation may unblock later reservations
  // that already completed in the real system.
  // ----------------------------------------------

  while (
    lane.writes.length > 0 &&
    lane.writes[0].completed === true
  ) {
    const current =
      lane.writes.shift();

    // --------------------------------------------
    // CANCELLED RESERVATION
    //
    // Remove it from the FIFO without advancing
    // the hypothetical serialized clock.
    // --------------------------------------------

    if (current.cancelled === true) {
      continue;
    }

    // --------------------------------------------
    // COMPLETED REAL TRANSACTION
    //
    // This is the same FIFO-resolution logic used
    // by finishShadowTokenSerialization().
    // --------------------------------------------

    if (
      !Number.isFinite(
        current.observedServiceMs
      ) ||
      current.observedServiceMs < 0
    ) {
      continue;
    }

    const previousAvailableAt =
      Number.isFinite(
        lane.resolvedAvailableAt
      )
        ? lane.resolvedAvailableAt
        : current.realStartedAt;

    const hypotheticalStartAt =
      Math.max(
        current.realStartedAt,
        previousAvailableAt
      );

    const hypotheticalWaitMs =
      Math.max(
        0,
        hypotheticalStartAt -
          current.realStartedAt
      );

    const hypotheticalFinishAt =
      hypotheticalStartAt +
      current.observedServiceMs;

    // --------------------------------------------
    // CLASSIFY SHADOW RESULT
    // --------------------------------------------

    if (hypotheticalWaitMs > 0) {
      stats.shadowSerializerWouldWaitSamples +=
        1;

      stats.shadowSerializerWaitTotalMs +=
        hypotheticalWaitMs;

      stats.shadowSerializerWaitMaxMs =
        Math.max(
          stats.shadowSerializerWaitMaxMs,
          hypotheticalWaitMs
        );

      const shadowDepth =
        Math.max(
          1,
          current.queueDepthAtArrival || 0
        );

      stats.shadowSerializerQueueDepthMax =
        Math.max(
          stats.shadowSerializerQueueDepthMax,
          shadowDepth
        );

      if (shadowDepth === 1) {
        stats.shadowSerializerDepth1 += 1;

      } else if (shadowDepth === 2) {
        stats.shadowSerializerDepth2 += 1;

      } else {
        stats.shadowSerializerDepth3Plus += 1;
      }

    } else {
      stats.shadowSerializerImmediateSamples +=
        1;
    }

    // --------------------------------------------
    // ADVANCE HYPOTHETICAL SERIALIZED LANE
    // --------------------------------------------

    lane.resolvedAvailableAt =
      hypotheticalFinishAt;
  }

  // ----------------------------------------------
  // CLEAN UP EMPTY TOKEN LANE
  // ----------------------------------------------

  if (lane.writes.length === 0) {
    shadowTokenSerializer.delete(
      tokenAddress
    );
  }
}


// ==================================================
// SLOW TOKEN UPSERT CONTENTION FORENSICS
//
// Records only token UPSERTs >= 500ms.
//
// sameTokenContentionDepth means:
//
//   0 = no other write for this token was active
//       when this transaction began
//
//   1 = one other write was already active
//
//   2 = two other writes were already active
//
//   3+ = three or more other writes were active
//
// No database work is added.
// ==================================================

function recordSlowTokenUpsertForensic(
  tokenUpsertDurationMs,
  sameTokenContentionDepth
) {
  if (
    !Number.isFinite(
      tokenUpsertDurationMs
    ) ||
    tokenUpsertDurationMs < 500
  ) {
    return;
  }

  const contentionDepth =
    Number.isFinite(
      sameTokenContentionDepth
    )
      ? Math.max(
          0,
          sameTokenContentionDepth
        )
      : 0;

  stats.slowTokenUpsertSamples += 1;

  stats.slowTokenUpsertDurationTotalMs +=
    tokenUpsertDurationMs;

  stats.slowTokenUpsertDurationMaxMs =
    Math.max(
      stats.slowTokenUpsertDurationMaxMs,
      tokenUpsertDurationMs
    );

  if (contentionDepth > 0) {
    stats.slowTokenUpsertWithSameTokenContention +=
      1;

    stats.slowTokenUpsertContentionDepthTotal +=
      contentionDepth;

    stats.slowTokenUpsertContentionDepthMax =
      Math.max(
        stats.slowTokenUpsertContentionDepthMax,
        contentionDepth
      );

    if (contentionDepth === 1) {
      stats.slowTokenUpsertWithDepth1 += 1;
    } else if (contentionDepth === 2) {
      stats.slowTokenUpsertWithDepth2 += 1;
    } else {
      stats.slowTokenUpsertWithDepth3Plus += 1;
    }

    return;
  }

  stats.slowTokenUpsertWithoutSameTokenContention +=
    1;
}


// ==================================================
// SLOW DATABASE TRANSACTION DIAGNOSTICS
//
// Thresholds are cumulative:
//
// >500ms is also counted in >250ms.
// >1000ms is also counted in >500ms and >250ms.
// ==================================================

function recordSlowDbQuery(
  durationMs
) {
  if (
    !Number.isFinite(durationMs) ||
    durationMs < 0
  ) {
    return;
  }

  if (durationMs >= 250) {
    stats.dbQueriesOver250ms += 1;
  }

  if (durationMs >= 500) {
    stats.dbQueriesOver500ms += 1;
  }

  if (durationMs >= 1000) {
    stats.dbQueriesOver1000ms += 1;
  }

  if (durationMs >= 5000) {
    stats.dbQueriesOver5000ms += 1;
  }
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
  const totalStartedAt =
    performanceNow();

  const acquireStartedAt =
    performanceNow();

  let client = null;

  try {
    client = await pool.connect();

    const acquireDurationMs =
      performanceNow() -
      acquireStartedAt;

    recordDbDiagnosticTiming(
      "poolAcquire",
      acquireDurationMs
    );

    const queryStartedAt =
      performanceNow();

    try {
      return await client.query(
        sql,
        params
      );
    } finally {
      const queryDurationMs =
        performanceNow() -
        queryStartedAt;

      recordDbDiagnosticTiming(
        "queryExecution",
        queryDurationMs
      );

      recordSlowDbQuery(
        queryDurationMs
      );
    }
  } finally {
    if (client) {
      client.release();
    }

    recordPerformanceTiming(
      category,
      performanceNow() -
        totalStartedAt
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
// 8A-1. EXACT OLDEST SIGNATURE AGE
//
// Returns the age of the oldest signature currently
// waiting in the signature queue.
//
// Diagnostic only.
// No queue behavior is changed.
// ==================================================

function getOldestSignatureAgeMs() {
  const oldestSignature =
    signatureQueue[0];

  if (
    !oldestSignature ||
    !Number.isFinite(oldestSignature.enqueuedAt)
  ) {
    return 0;
  }

  return Math.max(
    Date.now() - oldestSignature.enqueuedAt,
    0
  );
}


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
    const transitionAt = new Date().toISOString();
    const transitionPerformanceAt = performanceNow();

    // Capture exact state at the instant the pause fires.
    const transitionQueueSize = signatureQueue.length;
    const transitionOldestSignatureAgeMs =
      getOldestSignatureAgeMs();

    intakePaused = true;

    // Begin measuring this pause.
    intakePausedAt = transitionPerformanceAt;

    stats.intakePausedCount += 1;

    logInfo("Intake paused", {
      transitionType: "PAUSE",
      transitionAt,

      queueSizeExact: transitionQueueSize,
      maxQueueSize,

      oldestSignatureAgeMs:
        transitionOldestSignatureAgeMs === null
          ? null
          : Number(
              transitionOldestSignatureAgeMs.toFixed(2)
            ),

      dbWriteQueueSize:
        dbWriteQueue.length,

      dbWritesInFlight,

      inFlightSignatures:
  inFlightSignatures.size,

      heliusAvailableTokens:
        Number.isFinite(heliusRpcTokens)
          ? Number(heliusRpcTokens.toFixed(2))
          : null,

      queuedTotal:
        stats.queued ?? null,

      dequeuedTotal:
        stats.dequeued ?? null,
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
    const transitionAt = new Date().toISOString();
    const transitionPerformanceAt = performanceNow();

    // Capture exact state BEFORE changing pause state.
    const transitionQueueSize = signatureQueue.length;
    const transitionOldestSignatureAgeMs =
      getOldestSignatureAgeMs();

    // Capture duration before clearing pause state.
    const pauseDurationMs =
      intakePausedAt === null
        ? null
        : transitionPerformanceAt - intakePausedAt;

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
      transitionType: "RESUME",
      transitionAt,

      queueSizeExact: transitionQueueSize,
      resumeQueueSize: RESUME_QUEUE_SIZE,

      oldestSignatureAgeMs:
        transitionOldestSignatureAgeMs === null
          ? null
          : Number(
              transitionOldestSignatureAgeMs.toFixed(2)
            ),

      pauseDurationMs:
        pauseDurationMs === null
          ? null
          : Number(
              pauseDurationMs.toFixed(2)
            ),

      dbWriteQueueSize:
        dbWriteQueue.length,

      dbWritesInFlight,

      inFlightSignatures:
  inFlightSignatures.size,

      heliusAvailableTokens:
        Number.isFinite(heliusRpcTokens)
          ? Number(heliusRpcTokens.toFixed(2))
          : null,

      queuedTotal:
        stats.queued ?? null,

      dequeuedTotal:
        stats.dequeued ?? null,
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
// GLOBAL HELIUS RPC PACER
//
// Serializes only RPC START permission.
//
// It does NOT serialize the actual HTTP requests.
//
// Example at 60 starts/sec:
//
// request A starts at 0ms
// request B starts at ~16.7ms
// request C starts at ~33.3ms
//
// A may still be running when B and C begin.
//
// This preserves concurrency while preventing bursts.
// ==================================================

async function acquireHeliusRpcStartSlot() {
  if (
    !Number.isFinite(
      HELIUS_RPC_MAX_STARTS_PER_SECOND
    ) ||
    HELIUS_RPC_MAX_STARTS_PER_SECOND <= 0
  ) {
    return;
  }

  const refillRatePerMs =
    HELIUS_RPC_MAX_STARTS_PER_SECOND /
    1000;

  const capacity =
    Math.max(
      1,
      HELIUS_RPC_BURST_CAPACITY
    );

  const waitStartedAt =
    performanceNow();

  let releaseGate;

  const previousGate =
    heliusRpcPacerTail;

  heliusRpcPacerTail =
    new Promise((resolve) => {
      releaseGate = resolve;
    });

  await previousGate;

  try {
    while (true) {
      const now =
        performanceNow();

      const elapsedMs =
        Math.max(
          0,
          now - heliusRpcLastRefillAt
        );

      if (elapsedMs > 0) {
        heliusRpcTokens =
          Math.min(
            capacity,
            heliusRpcTokens +
              elapsedMs *
                refillRatePerMs
          );

        heliusRpcLastRefillAt =
          now;
      }

      if (heliusRpcTokens >= 1) {
        heliusRpcTokens -= 1;
        break;
      }

      const tokensNeeded =
        1 - heliusRpcTokens;

      const waitMs =
        tokensNeeded /
        refillRatePerMs;

      await sleep(
        Math.max(
          1,
          Math.ceil(waitMs)
        )
      );
    }

  } finally {
    releaseGate();
  }

  const totalWaitMs =
    performanceNow() -
    waitStartedAt;

  stats.heliusRpcPacerSamples += 1;

  if (totalWaitMs >= 1) {
    stats.heliusRpcPacerWaited += 1;

    stats.heliusRpcPacerWaitTotalMs +=
      totalWaitMs;

    stats.heliusRpcPacerWaitMaxMs =
      Math.max(
        stats.heliusRpcPacerWaitMaxMs,
        totalWaitMs
      );

  } else {
    stats.heliusRpcPacerImmediate += 1;
  }
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
  // ----------------------------------------------
  // GLOBAL HELIUS RPC PACER
  //
  // Every Helius HTTP request must acquire a start
  // slot before it is allowed to begin.
  //
  // This limits aggregate request STARTS across all
  // callers without serializing the requests
  // themselves.
  // ----------------------------------------------

  await acquireHeliusRpcStartSlot();

  // ----------------------------------------------
  // SEND RPC REQUEST
  // ----------------------------------------------

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

  // ----------------------------------------------
  // HTTP ERROR
  //
  // Preserve status on the error so the existing
  // retry logic can specifically identify 429s.
  // ----------------------------------------------

  if (!response.ok) {
    const error = new Error(
      `RPC HTTP error ${response.status}`
    );

    error.status = response.status;

    throw error;
  }

  // ----------------------------------------------
  // PARSE JSON-RPC RESPONSE
  // ----------------------------------------------

  const json =
    await response.json();

  // ----------------------------------------------
  // JSON-RPC ERROR
  // ----------------------------------------------

  if (json.error) {
    throw new Error(
      `RPC error: ${JSON.stringify(
        json.error
      )}`
    );
  }

  // ----------------------------------------------
  // SUCCESS
  // ----------------------------------------------

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
        // ------------------------------------------
        // PURE RPC ATTEMPT TIMING
        //
        // Measures only the individual Helius
        // getTransaction request.
        //
        // Does NOT include retry sleep/backoff.
        // ------------------------------------------

        const attemptStartedAt =
          performanceNow();

        let transaction;

        try {
          transaction = await heliusRpc(
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
        } finally {
          recordPerformanceTiming(
            "rpcAttempt",
            performanceNow() -
              attemptStartedAt
          );
        }

        // ------------------------------------------
        // SUCCESS
        // ------------------------------------------

        if (transaction) {
          return transaction;
        }

        // ------------------------------------------
        // NULL RESPONSE RETRY
        //
        // Helius returned successfully, but the
        // transaction was not available yet.
        // ------------------------------------------

        if (attempt < RPC_RETRY_COUNT) {
          stats.rpcRetries += 1;
          stats.rpcNullRetries += 1;

          await sleep(
            RPC_RETRY_DELAY_MS *
              (attempt + 1)
          );
        }
      } catch (error) {
        lastError = error;

        // ------------------------------------------
        // CLASSIFY RPC ERROR
        // ------------------------------------------

        const wasRateLimited =
          error?.status === 429 ||
          String(
            error?.message || ""
          ).includes("429");

        // ------------------------------------------
        // RPC ERROR RETRY
        //
        // Preserve the existing retry behavior while
        // separately counting:
        //
        // • Rate-limit retries
        // • Other RPC-error retries
        // ------------------------------------------

        if (attempt < RPC_RETRY_COUNT) {
          stats.rpcRetries += 1;

          if (wasRateLimited) {
            stats.rpcRateLimitedRetries += 1;
          } else {
            stats.rpcOtherRetries += 1;
          }

          await sleep(
            wasRateLimited
              ? backoffDelay(
                  attempt,
                  true
                )
              : RPC_RETRY_DELAY_MS *
                  (attempt + 1)
          );
        }
      }
    }

    // ----------------------------------------------
    // ALL ATTEMPTS EXHAUSTED
    // ----------------------------------------------

    if (lastError) {
      throw lastError;
    }

    return null;
  } finally {
    // ----------------------------------------------
    // COMPLETE FETCH TIMING
    //
    // Includes:
    // • Helius request time
    // • Null-response retries
    // • RPC-error retries
    // • Retry/backoff delays
    //
    // Compare this against rpcAttemptPerformance
    // to isolate retry/backoff overhead.
    //
    // Diagnostic invariant:
    //
    // rpcRetries should equal:
    //
    //   rpcNullRetries
    // + rpcRateLimitedRetries
    // + rpcOtherRetries
    //
    // ----------------------------------------------

    recordPerformanceTiming(
      "rpcFetch",
      performanceNow() -
        fetchStartedAt
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
// UNRESOLVED CREATE MINT DIAGNOSTIC STATE
// ============================================================

const UNRESOLVED_CREATE_SAMPLE_LIMIT = 50;

const unresolvedCreateMintSamples = [];

let unresolvedCreateMintDiagnosticComplete = false;


// ============================================================
// RULE 5 SHADOW DIAGNOSTIC STATE
// ============================================================

const RULE5_SHADOW_SAMPLE_LIMIT = 50;

const rule5ShadowSamples = [];

let rule5ShadowDiagnosticComplete = false;

// ============================================================
// RULE 5 FAILURE FORENSIC DIAGNOSTIC STATE
// ============================================================
//
// Capture detailed transaction structure ONLY when the
// existing Rule 5 shadow hypothesis cannot resolve a Create.
//
// Diagnostic only.
// Does NOT modify production mint resolution.
// ============================================================

const RULE5_FAILURE_SAMPLE_LIMIT = 25;

const rule5FailureSamples = [];

let rule5FailureDiagnosticComplete = false;

// ============================================================
// UNRESOLVED CREATE MINT DIAGNOSTIC
//
// Capture detailed structure for unresolved Create events.
//
// Diagnostic only.
// Does NOT modify production mint resolution.
// ============================================================

function recordUnresolvedCreateMintSample(
  tx,
  signature = null
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
  // ONLY CREATE EVENTS
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

  // ----------------------------------------------
  // COLLECT PARSED TOKEN INSTRUCTIONS
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
  // OUTER TOKEN INSTRUCTIONS
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
  // INNER TOKEN INSTRUCTIONS
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
  // RAW SUPPORTING STRUCTURE
  // ----------------------------------------------

  const preTokenBalances =
    tx?.meta?.preTokenBalances ?? [];

  const postTokenBalances =
    tx?.meta?.postTokenBalances ?? [];

  const accountKeys =
    Array.isArray(
      tx?.transaction?.message?.accountKeys
    )
      ? tx.transaction.message.accountKeys
      : [];

  // ----------------------------------------------
  // RELEVANT LOGS
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

  const sample = {
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
  };

  unresolvedCreateMintSamples.push(
    sample
  );

  // ----------------------------------------------
  // LOG EACH SAMPLE
  // ----------------------------------------------

  logInfo(
    "Unresolved create mint diagnostic sample",
    {
      sampleNumber:
        unresolvedCreateMintSamples.length,

      sample,
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

    const candidateCountDistribution = {};

    const pumpAccountCountDistribution = {};

    const tokenInstructionTypeCounts = {};

    for (
      const row
      of unresolvedCreateMintSamples
    ) {
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

      for (
        const ix
        of row.pumpInstructions
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
        of row.tokenInstructions
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


// ============================================================
// RULE 5 SHADOW DIAGNOSTIC
//
// Test whether unresolved genuine Create transactions can be
// safely resolved using agreement between:
//
//   Pump CreateV2 account[0]
//              +
//   SPL Token InitializeMint / InitializeMint2 mint
//
// MintTo is collected as an additional independent signal.
//
// IMPORTANT:
//
// This is SHADOW ONLY.
// It does NOT modify production mint resolution.
// ============================================================

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
  // FIND PUMP CREATEV2 INSTRUCTION CANDIDATES
  //
  // Current observed CreateV2 schema:
  //
  // accountCount = 20
  // expected mint = account[0]
  //
  // This remains diagnostic-only until validated.
  // ----------------------------------------------

  const pumpCreateInstructions = [];

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
  // TOKEN INITIALIZATION / MINTTO SIGNALS
  // ----------------------------------------------

  const initializeMintInstructions = [];

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

    // --------------------------------------------
    // INITIALIZE MINT
    // --------------------------------------------

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

    // --------------------------------------------
    // MINT TO
    // --------------------------------------------

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
  // OUTER TOKEN INSTRUCTIONS
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
  // INNER TOKEN INSTRUCTIONS
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
  // DERIVE UNIQUE VALUES
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

  // ----------------------------------------------
  // CANDIDATE VALIDATION
  // ----------------------------------------------

  const createMintIsCandidate =
    typeof createMint === "string" &&
    candidateSet.has(createMint);

  const initializeMintIsCandidate =
    typeof initializeMint === "string" &&
    candidateSet.has(initializeMint);

  // ----------------------------------------------
  // SIGNAL AGREEMENT
  // ----------------------------------------------

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
  // Requirements:
  //
  // 1. Exactly one CreateV2 account[0] hypothesis
  // 2. Exactly one InitializeMint hypothesis
  // 3. Both mints are token-balance candidates
  // 4. Both independently identify the same mint
  //
  // MintTo is supporting evidence for now and is
  // NOT required for shadow resolution.
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
    signature:
      signature || null,

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
// FAILURE-ONLY FORENSIC DIAGNOSTIC
// ----------------------------------------------

if (!resolvable) {
  recordRule5FailureDiagnostic(
    tx,
    signature,
    sample
  );
}

  // ----------------------------------------------
  // LOG EACH SAMPLE
  // ----------------------------------------------

  logInfo(
    "Rule 5 shadow diagnostic sample",
    {
      sampleNumber:
        rule5ShadowSamples.length,

      signature:
        signature || null,

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
      // SIGNAL DISAGREEMENTS
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

    // --------------------------------------------
    // FINAL RULE 5 SHADOW REPORT
    // --------------------------------------------

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

// ============================================================
// RULE 5 FAILURE FORENSIC DIAGNOSTIC
//
// Purpose:
//
// Study Create transactions that remain unresolved by the
// current Rule 5 shadow hypothesis.
//
// We specifically want to determine:
//
// • What outer instruction owns InitializeMint / MintTo?
// • Is that parent instruction a Pump instruction?
// • What is its account count?
// • Which candidate mints does it reference?
// • Is there another Pump Create schema that our current
//   accountCount === 20 hypothesis does not recognize?
//
// IMPORTANT:
//
// • Diagnostic only.
// • Does NOT modify production mint resolution.
// • Runs only after the existing Rule 5 shadow test fails.
// ============================================================

function recordRule5FailureDiagnostic(
  tx,
  signature,
  rule5Sample
) {
  // ----------------------------------------------
  // STOP AFTER SAMPLE LIMIT
  // ----------------------------------------------

  if (
    rule5FailureDiagnosticComplete ||
    rule5FailureSamples.length >=
      RULE5_FAILURE_SAMPLE_LIMIT
  ) {
    return;
  }

  // ----------------------------------------------
  // ONLY FAILED RULE 5 CREATE SAMPLES
  // ----------------------------------------------

  if (
    !rule5Sample ||
    rule5Sample.eventType !== "create" ||
    rule5Sample.resolvable === true
  ) {
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
  // TRANSACTION STRUCTURE
  // ----------------------------------------------

  const outerInstructions =
    getInstructions(tx) || [];

  const innerGroups =
    getInnerInstructions(tx) || [];

  // ----------------------------------------------
  // NORMALIZE ONE INSTRUCTION
  // ----------------------------------------------

  function summarizeInstruction(
    ix,
    location,
    outerIndex,
    innerIndex
  ) {
    const accounts =
      Array.isArray(ix?.accounts)
        ? ix.accounts
        : [];

    const parsed =
      ix?.parsed ?? null;

    return {
      location,

      outerIndex,

      innerIndex,

      program:
        ix?.program ?? null,

      programId:
        ix?.programId ?? null,

      isPumpProgram:
        ix?.programId ===
        PUMP_LAUNCHPAD_PROGRAM_ID,

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

      candidateAccounts:
        accounts
          .map(
            (account, index) => ({
              index,
              account,
            })
          )
          .filter(row =>
            candidateSet.has(row.account)
          ),

      data:
        ix?.data ?? null,

      parsedType:
        parsed?.type ?? null,

      parsedInfo:
        parsed?.info ?? null,
    };
  }

  // ----------------------------------------------
  // ALL OUTER INSTRUCTIONS
  // ----------------------------------------------

  const outerInstructionSummaries = [];

  for (
    let outerIndex = 0;
    outerIndex < outerInstructions.length;
    outerIndex += 1
  ) {
    outerInstructionSummaries.push(
      summarizeInstruction(
        outerInstructions[outerIndex],
        "outer",
        outerIndex,
        null
      )
    );
  }

  // ----------------------------------------------
  // ALL PUMP INSTRUCTIONS
  //
  // Unlike the existing Rule 5 diagnostic, DO NOT
  // filter by accountCount === 20.
  // ----------------------------------------------

  const allPumpInstructions = [];

  for (
    let outerIndex = 0;
    outerIndex < outerInstructions.length;
    outerIndex += 1
  ) {
    const ix =
      outerInstructions[outerIndex];

    if (
      ix?.programId ===
      PUMP_LAUNCHPAD_PROGRAM_ID
    ) {
      allPumpInstructions.push(
        summarizeInstruction(
          ix,
          "outer",
          outerIndex,
          null
        )
      );
    }
  }

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
        ix?.programId ===
        PUMP_LAUNCHPAD_PROGRAM_ID
      ) {
        allPumpInstructions.push(
          summarizeInstruction(
            ix,
            "inner",
            group?.index ?? null,
            innerIndex
          )
        );
      }
    }
  }

  // ----------------------------------------------
  // TOKEN CREATE SIGNALS
  // ----------------------------------------------

  const tokenCreateSignals = [];

  function inspectTokenSignal(
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

    if (
      type !== "initializeMint" &&
      type !== "initializeMint2" &&
      type !== "mintTo" &&
      type !== "mintToChecked"
    ) {
      return;
    }

    const mint =
      parsed?.info?.mint ?? null;

    tokenCreateSignals.push({
      location,
      outerIndex,
      innerIndex,

      program:
        ix?.program ?? null,

      programId:
        ix?.programId ?? null,

      type,

      mint,

      mintIsCandidate:
        typeof mint === "string" &&
        candidateSet.has(mint),
    });
  }

  // Outer token instructions.

  for (
    let outerIndex = 0;
    outerIndex < outerInstructions.length;
    outerIndex += 1
  ) {
    inspectTokenSignal(
      outerInstructions[outerIndex],
      "outer",
      outerIndex,
      null
    );
  }

  // Inner token instructions.

  for (const group of innerGroups) {
    const instructions =
      group?.instructions || [];

    for (
      let innerIndex = 0;
      innerIndex < instructions.length;
      innerIndex += 1
    ) {
      inspectTokenSignal(
        instructions[innerIndex],
        "inner",
        group?.index ?? null,
        innerIndex
      );
    }
  }

  // ----------------------------------------------
  // PARENT OUTER INSTRUCTIONS
  //
  // For every InitializeMint / MintTo found inside
  // an inner-instruction group, capture the outer
  // instruction that invoked that group.
  // ----------------------------------------------

  const parentOuterIndexes =
    new Set(
      tokenCreateSignals
        .filter(
          signal =>
            signal.location === "inner" &&
            Number.isInteger(
              signal.outerIndex
            )
        )
        .map(
          signal =>
            signal.outerIndex
        )
    );

  const parentOuterInstructions =
    Array.from(parentOuterIndexes)
      .sort((a, b) => a - b)
      .map(outerIndex => {
        const ix =
          outerInstructions[outerIndex];

        if (!ix) {
          return {
            outerIndex,
            missing: true,
          };
        }

        return summarizeInstruction(
          ix,
          "outer",
          outerIndex,
          null
        );
      });

  // ----------------------------------------------
  // RELEVANT LOGS
  // ----------------------------------------------

  const logs =
    getLogMessages(tx);

  const relevantLogs =
    logs.filter(line => {
      if (typeof line !== "string") {
        return false;
      }

      const lower =
        line.toLowerCase();

      return (
        lower.includes("instruction:") ||
        lower.includes("initialize") ||
        lower.includes("mint") ||
        lower.includes("create") ||
        lower.includes(
          PUMP_LAUNCHPAD_PROGRAM_ID.toLowerCase()
        )
      );
    });

  // ----------------------------------------------
  // RECORD FAILURE SAMPLE
  // ----------------------------------------------

  const failureSample = {
    signature:
      signature || null,

    eventType:
      rule5Sample.eventType,

    failureReason: {
      pumpCreateInstructionCount:
        rule5Sample.pumpCreateInstructionCount,

      initializeMintInstructionCount:
        rule5Sample.initializeMintInstructionCount,

      mintToInstructionCount:
        rule5Sample.mintToInstructionCount,

      createMint:
        rule5Sample.createMint,

      initializeMint:
        rule5Sample.initializeMint,

      mintToMint:
        rule5Sample.mintToMint,

      createMintIsCandidate:
        rule5Sample.createMintIsCandidate,

      initializeMintIsCandidate:
        rule5Sample.initializeMintIsCandidate,

      createVsInitializeMatch:
        rule5Sample.createVsInitializeMatch,

      initializeVsMintToMatch:
        rule5Sample.initializeVsMintToMatch,
    },

    candidateCount:
      candidates.length,

    candidates,

    outerInstructionCount:
      outerInstructions.length,

    outerInstructions:
      outerInstructionSummaries,

    allPumpInstructionCount:
      allPumpInstructions.length,

    allPumpInstructions,

    tokenCreateSignalCount:
      tokenCreateSignals.length,

    tokenCreateSignals,

    parentOuterInstructionCount:
      parentOuterInstructions.length,

    parentOuterInstructions,

    relevantLogs,
  };

  rule5FailureSamples.push(
    failureSample
  );

  // ----------------------------------------------
  // LOG EACH FAILURE
  // ----------------------------------------------

  logInfo(
    "Rule 5 failure forensic sample",
    {
      sampleNumber:
        rule5FailureSamples.length,

      signature:
        signature || null,

      candidateCount:
        candidates.length,

      allPumpInstructionCount:
        allPumpInstructions.length,

      tokenCreateSignalCount:
        tokenCreateSignals.length,

      parentOuterInstructionCount:
        parentOuterInstructions.length,

      failureSample,
    }
  );

  // ----------------------------------------------
  // FINAL SUMMARY
  // ----------------------------------------------

  if (
    rule5FailureSamples.length >=
    RULE5_FAILURE_SAMPLE_LIMIT
  ) {
    rule5FailureDiagnosticComplete =
      true;

    const parentProgramCounts = {};

    const parentAccountCountDistribution = {};

    const pumpAccountCountDistribution = {};

    let parentIsPumpCount = 0;

    let parentNotPumpCount = 0;

    for (
      const row
      of rule5FailureSamples
    ) {
      for (
        const parent
        of row.parentOuterInstructions
      ) {
        const programKey =
          parent.programId ||
          parent.program ||
          "unknown";

        parentProgramCounts[
          programKey
        ] =
          (
            parentProgramCounts[
              programKey
            ] || 0
          ) + 1;

        const accountKey =
          String(
            parent.accountCount ?? "unknown"
          );

        parentAccountCountDistribution[
          accountKey
        ] =
          (
            parentAccountCountDistribution[
              accountKey
            ] || 0
          ) + 1;

        if (parent.isPumpProgram) {
          parentIsPumpCount += 1;
        } else {
          parentNotPumpCount += 1;
        }
      }

      for (
        const pumpIx
        of row.allPumpInstructions
      ) {
        const accountKey =
          String(pumpIx.accountCount);

        pumpAccountCountDistribution[
          accountKey
        ] =
          (
            pumpAccountCountDistribution[
              accountKey
            ] || 0
          ) + 1;
      }
    }

    logInfo(
      "Rule 5 failure forensic diagnostic complete",
      {
        examined:
          rule5FailureSamples.length,

        parentIsPumpCount,

        parentNotPumpCount,

        parentProgramCounts,

        parentAccountCountDistribution,

        pumpAccountCountDistribution,

        samples:
          rule5FailureSamples,
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

  const eventType =
    inferEventTypeFromLogs(tx);

  const candidateSet =
    new Set(candidates);

  // ----------------------------------------------
  // COLLECT PUMP-CONFIRMED CANDIDATES
  // AND PUMP INSTRUCTIONS
  // ----------------------------------------------

  const pumpConfirmedCandidates =
    new Set();

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
  // ONE CANDIDATE + PUMP CONFIRMATION
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
  // MULTIPLE CANDIDATES,
  // EXACTLY ONE PUMP-CONFIRMED
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
  // VALIDATED AMBIGUOUS BUY / SELL SCHEMAS
  //
  // BUY:
  //   27 / 28 accounts -> mint at account[1]
  //
  // SELL:
  //   26 / 27 accounts -> mint at account[1]
  // ----------------------------------------------

  if (
    candidates.length > 1 &&
    confirmedCandidates.length > 1 &&
    (
      eventType === "buy" ||
      eventType === "sell"
    )
  ) {
    const rule4ExpectedMints =
      new Set();

    let qualifyingInstructionCount = 0;

    for (
      const pumpIx
      of pumpInstructions
    ) {
      const accounts =
        pumpIx.accounts || [];

      const accountCount =
        accounts.length;

      let schemaMatches = false;

      if (
        eventType === "buy" &&
        (
          accountCount === 27 ||
          accountCount === 28
        )
      ) {
        schemaMatches = true;
      } else if (
        eventType === "sell" &&
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

      const expectedMint =
        accounts[1] || null;

      if (
        typeof expectedMint !== "string" ||
        !candidateSet.has(expectedMint)
      ) {
        return null;
      }

      rule4ExpectedMints.add(
        expectedMint
      );
    }

    if (
      qualifyingInstructionCount === 0
    ) {
      return null;
    }

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

    return null;
  }

  // ----------------------------------------------
  // RULE 5:
  // VALIDATED AMBIGUOUS CREATE RESOLUTION
  //
  // Required:
  //
  // Pump Create:
  //   20 accounts
  //   account[0] = X
  //
  // SPL Token / Token-2022:
  //   InitializeMint / InitializeMint2
  //   mint = X
  //
  // Optional corroboration:
  //   MintTo / MintToChecked
  //   if present, must also identify X.
  //
  // Any ambiguity or disagreement fails closed.
  // ----------------------------------------------

  if (
    eventType === "create" &&
    candidates.length > 1 &&
    confirmedCandidates.length > 1
  ) {
    const rule5CreateExpectedMints =
      new Set();

    const rule5InitializeMints =
      new Set();

    const rule5MintToMints =
      new Set();

    let qualifyingCreateInstructionCount = 0;

    // --------------------------------------------
    // PUMP CREATE STRUCTURE
    // --------------------------------------------

    for (
      const pumpIx
      of pumpInstructions
    ) {
      const accounts =
        pumpIx.accounts || [];

      if (
        accounts.length !== 20
      ) {
        continue;
      }

      qualifyingCreateInstructionCount += 1;

      const expectedMint =
        accounts[0] || null;

      if (
        typeof expectedMint !== "string" ||
        !candidateSet.has(expectedMint)
      ) {
        return null;
      }

      rule5CreateExpectedMints.add(
        expectedMint
      );
    }

    if (
      qualifyingCreateInstructionCount === 0 ||
      rule5CreateExpectedMints.size !== 1
    ) {
      return null;
    }

    // --------------------------------------------
    // TOKEN-PROGRAM CREATION SIGNALS
    //
    // Whitelist:
    //
    // SPL Token
    // Token-2022
    // --------------------------------------------

    function inspectRule5TokenInstruction(ix) {
      const isSupportedTokenProgram =
        ix?.programId ===
          SPL_TOKEN_PROGRAM_ID ||
        ix?.programId ===
          TOKEN_2022_PROGRAM_ID;

      if (!isSupportedTokenProgram) {
        return;
      }

      const parsed =
        ix?.parsed ?? null;

      if (!parsed) {
        return;
      }

      const type =
        parsed?.type ?? null;

      const mint =
        parsed?.info?.mint ?? null;

      // InitializeMint is required evidence.

      if (
        type === "initializeMint" ||
        type === "initializeMint2"
      ) {
        if (
          typeof mint === "string"
        ) {
          rule5InitializeMints.add(
            mint
          );
        }

        return;
      }

      // MintTo is optional corroborating evidence.

      if (
        type === "mintTo" ||
        type === "mintToChecked"
      ) {
        if (
          typeof mint === "string"
        ) {
          rule5MintToMints.add(
            mint
          );
        }
      }
    }

    // --------------------------------------------
    // SCAN OUTER TOKEN INSTRUCTIONS
    // --------------------------------------------

    for (
      const ix
      of outerInstructions
    ) {
      inspectRule5TokenInstruction(ix);
    }

    // --------------------------------------------
    // SCAN INNER TOKEN INSTRUCTIONS
    // --------------------------------------------

    for (
      const group
      of innerGroups
    ) {
      const instructions =
        group?.instructions || [];

      for (
        const ix
        of instructions
      ) {
        inspectRule5TokenInstruction(ix);
      }
    }

    // --------------------------------------------
    // REQUIRE ONE UNIQUE INITIALIZED MINT
    // --------------------------------------------

    if (
      rule5InitializeMints.size !== 1
    ) {
      return null;
    }

    const createMint =
      Array.from(
        rule5CreateExpectedMints
      )[0];

    const initializeMint =
      Array.from(
        rule5InitializeMints
      )[0];

    // --------------------------------------------
    // CREATE + INITIALIZEMINT MUST AGREE
    // --------------------------------------------

    if (
      !candidateSet.has(createMint) ||
      !candidateSet.has(initializeMint) ||
      createMint !== initializeMint
    ) {
      return null;
    }

    // --------------------------------------------
    // MINTTO CONSISTENCY CHECK
    //
    // MintTo is optional.
    // If present, it must uniquely agree.
    // --------------------------------------------

    if (
      rule5MintToMints.size > 0
    ) {
      if (
        rule5MintToMints.size !== 1
      ) {
        return null;
      }

      const mintToMint =
        Array.from(
          rule5MintToMints
        )[0];

      if (
        !candidateSet.has(mintToMint) ||
        mintToMint !== createMint
      ) {
        return null;
      }
    }

    // --------------------------------------------
    // RULE 5 RESOLVED
    // --------------------------------------------

    stats.resolvedMultipleCandidateRule5 += 1;

    return createMint;
  }

  // ----------------------------------------------
  // NO SAFE RESOLUTION
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
// 11. DATABASE WRITE PATH
//
// Purpose:
//
// Preserve strict write ordering and data integrity:
//
//   1. Upsert token
//   2. Insert event
//   3. Update market state only if event was inserted
//
// All primary writes use ONE checked-out PostgreSQL
// client and ONE transaction.
//
// This avoids:
// • Event-before-token foreign-key risk
// • Updating the same row twice in writable CTEs
// • Duplicate events advancing token market state
//
// Graduation remains separate because migrate events
// are rare and require explicit state promotion.
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
// 11B. MARKET DATA CALCULATION
// ==================================================

function calculateEventMarketData(event) {
  if (
    !["buy", "sell"].includes(
      event?.event_type
    )
  ) {
    return null;
  }

  const priceSol = Number(
    event?.price_per_token || 0
  );

  if (
    !Number.isFinite(priceSol) ||
    priceSol <= 0
  ) {
    return null;
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

  return {
    priceSol,
    marketCapSol,
    latestPriceUsd,
    marketCapUsd,

    solPriceUsd:
      hasUsd
        ? SOL_PRICE_USD
        : null,
  };
}


// ==================================================
// 11C. COMBINED TOKEN + EVENT WRITE TRANSACTION
//
// IMPORTANT:
//
// Strict ordering:
//
//   acquire PostgreSQL client
//        ↓
//   BEGIN
//        ↓
//   token UPSERT
//        ↓
//   event INSERT
//        ↓
//   optional market UPDATE
//        ↓
//   COMMIT
//
// Duplicate signatures:
// • Event insert returns no row.
// • Market state does NOT advance.
// • Transaction still commits token metadata safely.
//
// One PostgreSQL client is held for the complete write.
//
// Performance diagnostics:
//
// • Pool acquisition measured separately.
// • Complete transaction measured separately.
// • Individual transaction stages measured:
//     - BEGIN
//     - token UPSERT
//     - event INSERT
//     - market UPDATE
//     - COMMIT
//
// No additional database queries.
// No changes to ingestion behavior.
// ==================================================
async function writeLaunchpadTokenAndEvent(
  token,
  event
) {
  if (
    !token?.token_address ||
    !event?.token_address ||
    !event?.signature
  ) {
    return false;
  }

  const market =
    calculateEventMarketData(event);

  const tokenAddress =
    token.token_address;

  // ================================================
  // REAL PER-TOKEN SERIALIZATION
  //
  // Acquire BEFORE:
  // • beginTokenDbWrite()
  // • shadow diagnostics
  // • pool.connect()
  //
  // Same-token writes therefore never compete for
  // PostgreSQL connections or row locks.
  // ================================================

  let tokenSerializationReservation =
    null;

  if (TOKEN_DB_SERIALIZATION_ENABLED) {
    tokenSerializationReservation =
      await acquireTokenDbSerializationLane(
        tokenAddress
      );
  }

  // ================================================
  // SAME-TOKEN CONTENTION DIAGNOSTIC
  // ================================================

  const sameTokenContentionDepth =
    beginTokenDbWrite(
      tokenAddress
    );

  // ================================================
  // SHADOW PER-TOKEN SERIALIZER
  // ================================================

  const shadowReservation =
    beginShadowTokenSerialization(
      tokenAddress,
      sameTokenContentionDepth
    );

  let shadowReservationFinished = false;

  // ================================================
  // DATABASE CONNECTION ACQUISITION
  // ================================================

  const acquireStartedAt =
    performanceNow();

  let client = null;

  try {
    client =
      await pool.connect();

    const acquireDurationMs =
      performanceNow() -
      acquireStartedAt;

    recordDbDiagnosticTiming(
      "poolAcquire",
      acquireDurationMs
    );

    // ================================================
    // COMPLETE TRANSACTION TIMER
    // ================================================

    const writeStartedAt =
      performanceNow();

    let tokenWritten = false;
    let inserted = false;
    let marketUpdated = false;

    const stageDurations = {
      begin: null,
      tokenUpsert: null,

      // eventInsert now represents the combined
      // event INSERT + conditional market UPDATE
      // statement.
      eventInsert: null,

      // Market UPDATE no longer has its own network
      // round trip. Keep this field for compatibility
      // with existing diagnostics.
      marketUpdate: null,

      commit: null,
    };

    // ================================================
    // UPSERT EXECUTION CONTENTION DEPTH
    // ================================================

    let upsertExecutionDepth = null;

    try {

      // ================================================
      // BEGIN
      // ================================================

      const beginStartedAt =
        performanceNow();

      await client.query("BEGIN");

      stageDurations.begin =
        performanceNow() -
        beginStartedAt;

      recordDbStageTiming(
        "begin",
        stageDurations.begin
      );

      // ================================================
      // STEP 1 — TOKEN UPSERT
      // ================================================

      upsertExecutionDepth =
        Math.max(
          0,
          (
            activeTokenDbWrites.get(
              tokenAddress
            ) || 1
          ) - 1
        );

      const tokenUpsertStartedAt =
        performanceNow();

      const tokenResult =
        await client.query(
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
            $8,$9,$10,$11,$12,$13,
            NOW()
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

      stageDurations.tokenUpsert =
        performanceNow() -
        tokenUpsertStartedAt;

      recordDbStageTiming(
        "tokenUpsert",
        stageDurations.tokenUpsert
      );

      // ================================================
      // CONTENTION DEPTH PERFORMANCE
      // ================================================

      recordContentionDepthTiming(
        "upsertAtArrival",
        sameTokenContentionDepth,
        stageDurations.tokenUpsert
      );

      recordContentionDepthTiming(
        "upsertAtExecution",
        upsertExecutionDepth,
        stageDurations.tokenUpsert
      );

      recordSlowTokenUpsertForensic(
        stageDurations.tokenUpsert,
        sameTokenContentionDepth
      );

      tokenWritten =
        tokenResult.rowCount > 0;

      // ================================================
      // STEP 2 — EVENT INSERT + CONDITIONAL MARKET UPDATE
      //
      // This replaces TWO PostgreSQL round trips:
      //
      //   event INSERT
      //   market UPDATE
      //
      // with ONE statement.
      //
      // Critical invariant:
      //
      // market_update can only see rows returned by
      // inserted_event.
      //
      // Therefore a duplicate signature produces zero
      // inserted_event rows and CANNOT advance market
      // state.
      // ================================================

      const eventWriteStartedAt =
        performanceNow();

      const eventWriteResult =
        await client.query(
          `
          WITH inserted_event AS (
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
              market_cap_sol,
              market_cap_usd,
              sol_price_usd,
              raw_json
            )

            VALUES (
              $1,$2,$3,$4,$5,$6,$7,
              $8,$9,$10,$11,$12,$13
            )

            ON CONFLICT (signature)
            DO NOTHING

            RETURNING id
          ),

          market_update AS (
            UPDATE pump_launchpad_tokens

            SET
              latest_price_sol = $14,
              market_cap_sol = $15,

              latest_price = COALESCE(
                $16,
                latest_price
              ),

              market_cap_usd = COALESCE(
                $17,
                market_cap_usd
              ),

              fdv_usd = COALESCE(
                $17,
                fdv_usd
              ),

              ath_market_cap_sol =
                GREATEST(
                  COALESCE(
                    ath_market_cap_sol,
                    0
                  ),
                  $15
                ),

              atl_market_cap_sol =
                CASE
                  WHEN
                    atl_market_cap_sol IS NULL
                    OR atl_market_cap_sol = 0
                  THEN $15

                  ELSE LEAST(
                    atl_market_cap_sol,
                    $15
                  )
                END,

              ath_market_cap_usd =
                CASE
                  WHEN $17 IS NULL
                  THEN ath_market_cap_usd

                  ELSE GREATEST(
                    COALESCE(
                      ath_market_cap_usd,
                      0
                    ),
                    $17
                  )
                END,

              atl_market_cap_usd =
                CASE
                  WHEN $17 IS NULL
                  THEN atl_market_cap_usd

                  WHEN
                    atl_market_cap_usd IS NULL
                    OR atl_market_cap_usd = 0
                  THEN $17

                  ELSE LEAST(
                    atl_market_cap_usd,
                    $17
                  )
                END,

              updated_market_data_at = NOW(),
              updated_at = NOW()

            WHERE
              token_address = $1

              AND EXISTS (
                SELECT 1
                FROM inserted_event
              )

              AND $18::boolean = TRUE

            RETURNING token_address
          )

          SELECT
            EXISTS (
              SELECT 1
              FROM inserted_event
            ) AS inserted,

            EXISTS (
              SELECT 1
              FROM market_update
            ) AS market_updated
          `,
          [
            // ------------------------------------------
            // EVENT INSERT
            // $1 - $13
            // ------------------------------------------

            event.token_address,
            event.signature,
            event.slot,
            event.block_time,
            event.event_type,
            event.wallet_address,
            event.sol_amount,
            event.token_amount,

            market?.priceSol ??
              event.price_per_token ??
              null,

            market?.marketCapSol ?? null,
            market?.marketCapUsd ?? null,
            market?.solPriceUsd ?? null,

            STORE_RAW_EVENTS
              ? event.raw_json
              : null,

            // ------------------------------------------
            // MARKET UPDATE
            // $14 - $18
            // ------------------------------------------

            market?.priceSol ?? null,
            market?.marketCapSol ?? null,
            market?.latestPriceUsd ?? null,
            market?.marketCapUsd ?? null,

            Boolean(
              market &&
              ["buy", "sell"].includes(
                event.event_type
              )
            ),
          ]
        );

      stageDurations.eventInsert =
        performanceNow() -
        eventWriteStartedAt;

      recordDbStageTiming(
        "eventInsert",
        stageDurations.eventInsert
      );

      const resultRow =
        eventWriteResult.rows?.[0] || {};

      inserted =
        resultRow.inserted === true;

      marketUpdated =
        resultRow.market_updated === true;

      // ================================================
      // COMMIT
      // ================================================

      const commitStartedAt =
        performanceNow();

      await client.query("COMMIT");

      stageDurations.commit =
        performanceNow() -
        commitStartedAt;

      recordDbStageTiming(
        "commit",
        stageDurations.commit
      );

      // ================================================
      // STATS
      //
      // Count only committed work.
      // ================================================

      if (tokenWritten) {
        stats.insertedTokens += 1;
      }

      if (inserted) {
        stats.insertedEvents += 1;

        if (
          ["buy", "sell"].includes(
            event.event_type
          )
        ) {
          if (marketUpdated) {
            stats.updatedMarketData += 1;
          } else if (!market) {
            stats.skippedMarketDataUpdate += 1;
          }
        }
      }

      return inserted;

    } catch (error) {

      try {
        await client.query("ROLLBACK");
      } catch (rollbackError) {
        logError(
          "Database rollback failed",
          {
            error:
              rollbackError?.message ||
              String(rollbackError),
          }
        );
      }

      throw error;

    } finally {

      // ================================================
      // CONNECTION-SAFE TRANSACTION FINALIZATION
      //
      // Diagnostics must NEVER prevent the PostgreSQL
      // client from returning to the pool.
      // ================================================

      try {

        const queryDurationMs =
          performanceNow() -
          writeStartedAt;

        // ================================================
        // COMPLETE TRANSACTION PERFORMANCE
        // ================================================

        recordContentionDepthTiming(
          "transactionAtArrival",
          sameTokenContentionDepth,
          queryDurationMs
        );

        recordContentionDepthTiming(
          "transactionAtExecution",
          upsertExecutionDepth,
          queryDurationMs
        );

        recordPerformanceTiming(
          "sqlEventInsert",
          queryDurationMs
        );

        recordDbDiagnosticTiming(
          "queryExecution",
          queryDurationMs
        );

        recordSlowDbQuery(
          queryDurationMs
        );

        recordSlowTransactionForensic(
          queryDurationMs,
          stageDurations,
          sameTokenContentionDepth
        );

        // ================================================
        // SHADOW SERIALIZER COMPLETION
        // ================================================

        finishShadowTokenSerialization(
          shadowReservation,
          queryDurationMs
        );

        shadowReservationFinished = true;

      } finally {

        // ================================================
        // DATABASE CONNECTION RELEASE
        //
        // MUST execute even if diagnostics throw.
        // ================================================

        client.release();
      }
    }

  } finally {

    // ================================================
    // SHADOW RESERVATION FAIL-SAFE
    // ================================================

    if (
      shadowReservation &&
      !shadowReservationFinished
    ) {
      cancelShadowTokenSerialization(
        shadowReservation
      );
    }

    // ================================================
    // SAME-TOKEN CONTENTION CLEANUP
    // ================================================

    endTokenDbWrite(
      tokenAddress
    );

    // ================================================
    // REAL PER-TOKEN SERIALIZER RELEASE
    //
    // MUST remain last.
    // ================================================

    releaseTokenDbSerializationLane(
      tokenSerializationReservation
    );
  }
}
// ==================================================
// 11D. GRADUATION UPDATE
//
// Migrate events are rare, so graduation remains a
// separate explicit update.
//
// The normal token upsert protects JUST_GRADUATED
// and POST_GRAD from being overwritten afterward.
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
// DATABASE WRITE DISPATCHER
//
//
// ==================================================


// ==================================================
// ENQUEUE DATABASE WRITE JOB
// ==================================================

function enqueueDbWriteJob(job) {
  if (
    !job?.signature ||
    !job?.token?.token_address ||
    !job?.event?.token_address
  ) {
    return false;
  }

  // ----------------------------------------------
  // SAFETY BACKPRESSURE
  //
  // Never allow the independent DB queue to grow
  // without bound.
  //
  // If the dispatcher queue reaches its configured
  // maximum, refuse the handoff.
  //
  // processQueuedSignature() will then fall back to
  // the existing synchronous DB-write path rather
  // than silently dropping an accepted event.
  // ----------------------------------------------

  if (
    dbWriteQueue.length >=
    MAX_DB_WRITE_QUEUE_SIZE
  ) {
    stats.dbWriteDispatchBackpressure += 1;

    return false;
  }

  job.enqueuedAt =
    performanceNow();

  dbWriteQueue.push(job);

  stats.dbWriteJobsQueued += 1;

  stats.dbWriteQueueDepthMax =
    Math.max(
      stats.dbWriteQueueDepthMax,
      dbWriteQueue.length
    );

  scheduleDbWriteDispatcher();

  return true;
}


// ==================================================
// SCHEDULE DATABASE DISPATCHER
//
// Multiple enqueue/completion events may request a
// dispatch pass during the same event-loop turn.
//
// Collapse those requests into one setImmediate()
// callback.
// ==================================================

function scheduleDbWriteDispatcher() {
  if (dbWriteDispatcherScheduled) {
    return;
  }

  dbWriteDispatcherScheduled = true;

  setImmediate(() => {
    dbWriteDispatcherScheduled = false;

    dispatchDbWriteJobs();
  });
}


// ==================================================
// FIND NEXT DISPATCHABLE DATABASE JOB
//
// The DB queue contains jobs from many tokens.
//
// If the first queued job belongs to a token that is
// already executing, continue scanning for work from
// another token.
//
// This prevents a hot token from causing global
// head-of-line blocking.
//
// FIFO ordering is still preserved WITHIN each token
// because a later job for the same token cannot start
// while that token is active.
// ==================================================

function findNextDispatchableDbJobIndex() {
  for (
    let index = 0;
    index < dbWriteQueue.length;
    index += 1
  ) {
    const job =
      dbWriteQueue[index];

    const tokenAddress =
      job?.token?.token_address;

    // Invalid jobs should normally never reach this
    // queue because enqueueDbWriteJob() validates them.
    //
    // Returning the index allows the dispatcher to
    // remove the malformed job rather than allowing it
    // to block the queue indefinitely.
    if (!tokenAddress) {
      return index;
    }

    if (
      !activeDbWriteTokens.has(
        tokenAddress
      )
    ) {
      return index;
    }

    stats.dbWriteDispatchBlockedSameToken += 1;
  }

  return -1;
}


// ==================================================
// EXECUTE ONE DATABASE WRITE JOB
//
// This function owns the accepted signature AFTER
// processQueuedSignature() successfully hands it to
// the DB dispatcher.
//
// It is responsible for:
//
// • Primary token/event DB write
// • Graduation update
// • Async enrichment dispatch
// • Success counters
// • Seen-signature finalization
// • inFlightSignatures cleanup
// • Final end-to-end processing timing
//
// IMPORTANT:
//
// This function does NOT release the dispatcher's
// token lane or global DB slot.
//
// dispatchDbWriteJobs() owns those resources and
// releases them in its Promise.finally() handler.
// ==================================================

async function executeDbWriteJob(job) {
  const {
    signature,
    token,
    event,
    processingStartedAt,
  } = job;

  let permanentlySeen = false;

  const dbWriteStartedAt =
    performanceNow();

  try {
    // --------------------------------------------
    // PRIMARY TOKEN / EVENT WRITE
    //
    // The existing per-token DB serializer remains
    // inside this function's downstream write path
    // as a safety invariant.
    // --------------------------------------------

    const inserted =
      await writeLaunchpadTokenAndEvent(
        token,
        event
      );

    // Once the primary database path has completed
    // normally, preserve the existing signature
    // lifecycle.
    //
    // Duplicate / already-present events are also
    // considered permanently handled.
    permanentlySeen = true;

    // --------------------------------------------
    // DUPLICATE / ALREADY-PRESENT EVENT
    //
    // No additional downstream work is required.
    //
    // The dispatcher job completed successfully even
    // though no new event row was inserted.
    // --------------------------------------------

    if (!inserted) {
      stats.dbWriteJobsCompleted += 1;

      return;
    }

    // --------------------------------------------
    // GRADUATION
    //
    // This remains part of the dispatcher-owned job.
    //
    // Do not mark the dispatcher job completed until
    // this awaited work succeeds.
    // --------------------------------------------

    if (
      event.event_type ===
      "migrate"
    ) {
      await markTokenGraduated(
        event.token_address,
        event.block_time
      );
    }

    // --------------------------------------------
    // ASYNC HOLDER / SAFETY ENRICHMENT
    //
    // Intentionally NOT awaited.
    //
    // Enrichment remains outside the primary
    // ingestion / DB-write critical path and does
    // not determine dispatcher completion.
    // --------------------------------------------

    if (
      event.event_type === "create" ||
      event.event_type === "buy"
    ) {
      dispatchTokenSafetyEnrichment(
        event.token_address
      );
    }

    // --------------------------------------------
    // INSERTED-EVENT SUCCESS STATS
    //
    // These counters intentionally represent only
    // newly inserted / accepted events.
    // --------------------------------------------

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

    // --------------------------------------------
    // SUCCESSFUL DISPATCHER COMPLETION
    //
    // Reaching this point means all awaited work
    // owned by this dispatcher job completed
    // successfully.
    //
    // Duplicate jobs are counted in the early-return
    // path above. Newly inserted jobs are counted
    // here only after any required graduation work
    // has also succeeded.
    //
    // This keeps dispatcher accounting mutually
    // exclusive:
    //
    // started
    //   ≈ completed + failed + currently in flight
    // --------------------------------------------

    stats.dbWriteJobsCompleted += 1;

  } finally {
    // --------------------------------------------
    // DB WRITE PERFORMANCE
    //
    // For dispatched jobs this measures execution
    // time after the dispatcher actually starts the
    // job.
    //
    // Dispatcher queue wait is measured separately.
    // --------------------------------------------

    recordPerformanceTiming(
      "dbWrite",
      performanceNow() -
        dbWriteStartedAt
    );

    // --------------------------------------------
    // SIGNATURE LIFECYCLE COMPLETES HERE
    //
    // processQueuedSignature() deliberately leaves
    // the signature in inFlightSignatures after a
    // successful dispatcher handoff.
    //
    // Ownership therefore ends here regardless of
    // success or failure.
    // --------------------------------------------

    inFlightSignatures.delete(
      signature
    );

    if (permanentlySeen) {
      addSeenSignature(
        signature
      );
    }

    // --------------------------------------------
    // END-TO-END PROCESSING PERFORMANCE
    //
    // For dispatched accepted events this measures:
    //
    // accepted by worker
    //        ↓
    // RPC / classification
    //        ↓
    // DB dispatcher queue
    //        ↓
    // database work
    //        ↓
    // dispatcher completion
    //
    // Signature-queue waiting remains excluded.
    // --------------------------------------------

    if (
      Number.isFinite(
        processingStartedAt
      )
    ) {
      recordPerformanceTiming(
        "processing",
        performanceNow() -
          processingStartedAt
      );
    }
  }
}

// ==================================================
// DISPATCH DATABASE WRITE JOBS
//
// Starts as many jobs as possible while respecting:
//
// 1. Global DB_WRITE_CONCURRENCY
// 2. Maximum one active write per token
//
// Different tokens remain fully concurrent.
//
// Same-token jobs remain queued until the currently
// active write for that token completes.
// ==================================================

function dispatchDbWriteJobs() {
  while (
    dbWritesInFlight <
      DB_WRITE_CONCURRENCY &&
    dbWriteQueue.length > 0
  ) {
    const jobIndex =
      findNextDispatchableDbJobIndex();

    // Every queued job currently belongs to a token
    // that already has a DB write executing.
    //
    // A completion callback will schedule another
    // dispatcher pass when one of those lanes opens.
    if (jobIndex < 0) {
      return;
    }

    const [job] =
      dbWriteQueue.splice(
        jobIndex,
        1
      );

    const tokenAddress =
      job?.token?.token_address;

    // --------------------------------------------
    // DEFENSIVE INVALID-JOB HANDLING
    //
    // enqueueDbWriteJob() already validates jobs, so
    // this should never normally fire.
    //
    // Do not allow malformed work to consume a
    // dispatcher slot or poison the queue.
    // --------------------------------------------

    if (
      !job?.signature ||
      !tokenAddress ||
      !job?.event?.token_address
    ) {
      stats.dbWriteJobsFailed += 1;

      logError(
        "Invalid database write job",
        {
          signature:
            job?.signature ||
            null,

          tokenAddress:
            tokenAddress ||
            null,
        }
      );

      if (job?.signature) {
        inFlightSignatures.delete(
          job.signature
        );
      }

      continue;
    }

    // --------------------------------------------
    // RESERVE TOKEN LANE
    // --------------------------------------------

    activeDbWriteTokens.add(
      tokenAddress
    );

    // --------------------------------------------
    // RESERVE GLOBAL DB SLOT
    // --------------------------------------------

    dbWritesInFlight += 1;

    stats.dbWriteJobsStarted += 1;

    stats.dbWriteInFlightCurrent =
      dbWritesInFlight;

    stats.dbWriteInFlightMax =
      Math.max(
        stats.dbWriteInFlightMax,
        dbWritesInFlight
      );

    // --------------------------------------------
    // DISPATCH QUEUE WAIT
    //
    // Measures time between:
    //
    // enqueueDbWriteJob()
    //        ↓
    // actual DB execution start
    // --------------------------------------------

    const queueWaitMs =
      Number.isFinite(
        job.enqueuedAt
      )
        ? Math.max(
            0,
            performanceNow() -
              job.enqueuedAt
          )
        : 0;

    stats.dbWriteQueueWaitSamples += 1;

    stats.dbWriteQueueWaitTotalMs +=
      queueWaitMs;

    stats.dbWriteQueueWaitMaxMs =
      Math.max(
        stats.dbWriteQueueWaitMaxMs,
        queueWaitMs
      );

    // --------------------------------------------
    // EXECUTE ASYNCHRONOUSLY
    //
    // IMPORTANT:
    //
    // We deliberately do NOT await this Promise.
    //
    // The dispatcher may immediately start another
    // job for a DIFFERENT token while capacity
    // remains available.
    // --------------------------------------------

    executeDbWriteJob(job)
      .catch((error) => {
        stats.dbWriteJobsFailed += 1;

        logError(
          "Database write job failed",
          {
            signature:
              job.signature,

            tokenAddress,

            error:
              String(
                error?.message ||
                error
              ),
          }
        );
      })
      .finally(() => {
        // ----------------------------------------
        // RELEASE TOKEN LANE
        // ----------------------------------------

        activeDbWriteTokens.delete(
          tokenAddress
        );

        // ----------------------------------------
        // RELEASE GLOBAL DB SLOT
        // ----------------------------------------

        dbWritesInFlight =
          Math.max(
            0,
            dbWritesInFlight - 1
          );

        stats.dbWriteInFlightCurrent =
          dbWritesInFlight;

        // ----------------------------------------
        // RESUME DISPATCH
        //
        // Completion may have:
        //
        // • opened a global concurrency slot
        // • made another same-token job eligible
        //
        // Schedule another dispatcher pass.
        // ----------------------------------------

        scheduleDbWriteDispatcher();
      });
  }
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

  inFlightSignatures.add(
    signature
  );

  stats.dequeued += 1;

  // Begin timing only after the signature is accepted
  // for processing. Signature-queue waiting is excluded.
  const processingStartedAt =
    performanceNow();

  let permanentlySeen = false;

  // When true, ownership of:
  //
  // • inFlightSignatures cleanup
  // • seen-signature finalization
  // • final processing timing
  //
  // has moved to executeDbWriteJob().
  let handedOffToDbDispatcher =
    false;

  try {
    // ----------------------------------------------
    // FETCH HYDRATED TRANSACTION
    //
    // fetchFullTransaction() already records its
    // own RPC timing in Section 9.
    // ----------------------------------------------

    const tx =
      await fetchFullTransaction(
        signature
      );

    if (!tx) {
      stats.skippedEmptyTx += 1;

      // Preserve existing behavior:
      //
      // A temporary null RPC response is NOT marked
      // permanently seen so it remains retryable.
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
        "unsupported_pump_instruction"
      ) {
        stats.skippedUnsupportedPumpInstruction +=
          1;
      }

      else if (
        classified.reason ===
        "unresolved_token_mint"
      ) {
        stats.skippedUnresolvedMint +=
          1;

        recordUnresolvedMintDiagnostics(
          tx,
          signature
        );
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
    //
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
        solAmount < minSolAmount
      ) {
        stats.skippedSmallSolAmount +=
          1;

        permanentlySeen = true;
        return;
      }
    }

    // ==============================================
    // DATABASE WRITE HANDOFF
    //
    // The signature worker has now completed:
    //
    // • RPC fetch
    // • validation
    // • classification
    // • mint resolution
    // • trade-size filtering
    //
    // From this point forward, ALL accepted database
    // work must pass through the DB dispatcher.
    //
    // IMPORTANT:
    //
    // There is NO synchronous database fallback.
    //
    // Allowing a worker to bypass the dispatcher
    // during backpressure could violate same-token
    // FIFO ordering.
    // ==============================================

    const dbJob = {
      signature,
      token,
      event,
      processingStartedAt,
    };

    // ----------------------------------------------
    // ORDER-PRESERVING DISPATCHER BACKPRESSURE
    //
    // Normally this loop executes exactly once.
    //
    // If the DB queue reaches its configured maximum,
    // wait briefly for dispatcher capacity and retry.
    //
    // This may temporarily occupy a signature worker
    // during genuine DB saturation, but it preserves:
    //
    // • accepted events
    // • dispatcher ownership
    // • same-token FIFO ordering
    //
    // and prevents direct DB writes from jumping ahead
    // of already-queued work for the same token.
    // ----------------------------------------------

    while (true) {
      const handedOff =
        enqueueDbWriteJob(
          dbJob
        );

      if (handedOff) {
        handedOffToDbDispatcher = true;

        // IMPORTANT:
        //
        // Do NOT:
        //
        // • remove signature from inFlightSignatures
        // • add signature to seenSignatures
        // • record final processing timing
        //
        // executeDbWriteJob() now owns the remainder
        // of this signature's lifecycle.
        return;
      }

      // --------------------------------------------
      // WAIT FOR DISPATCHER CAPACITY
      //
      // enqueueDbWriteJob() returning false means the
      // bounded dispatcher queue could not accept the
      // job immediately.
      //
      // Do not bypass the dispatcher.
      // Do not drop the accepted event.
      // --------------------------------------------

      await sleep(25);
    }

  } catch (error) {
    // Preserve the existing error counter and
    // error-handling behavior for work still owned
    // by the signature-processing worker.
    //
    // Once dispatcher handoff succeeds, failures are
    // handled by the dispatcher instead.

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
    // ==============================================
    // SIGNATURE LIFECYCLE OWNERSHIP
    //
    // If the DB dispatcher accepted the job,
    // executeDbWriteJob() owns final cleanup.
    //
    // Otherwise this function retains the original
    // lifecycle responsibilities.
    // ==============================================

    if (!handedOffToDbDispatcher) {
      // --------------------------------------------
      // TOTAL PROCESSING PERFORMANCE
      //
      // For signatures that do NOT enter the
      // dispatcher, preserve the original meaning.
      // --------------------------------------------

      recordPerformanceTiming(
        "processing",
        performanceNow() -
          processingStartedAt
      );

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
  while (workerRunning) {
    drainStaleQueueItems();

    const item =
      signatureQueue.shift();

    if (!item) {
      maybeResumeIntake();

      // Keep a small idle sleep so empty workers do not
      // spin the CPU while waiting for new signatures.
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
          error:
            error?.message ||
            String(error),
        }
      );
    }

    maybeResumeIntake();

       // ================================================
    // V1.2 RPC TOKEN-BUCKET THROTTLE
    //
    // Intentionally no fixed post-signature sleep.
    //
    // Worker throughput is governed by:
    //
    // • Helius global RPC token bucket
    // • Actual RPC latency
    // • Signature queue pressure
    // • Downstream DB backpressure
    //
    // The token bucket preserves the configured
    // long-run Helius RPC start rate while allowing
    // unused capacity to accumulate for short bursts.
    //
    // V1.2 controlled experiment:
    //
    //   Refill rate:
    //     HELIUS_RPC_MAX_STARTS_PER_SECOND
    //
    //   Burst capacity:
    //     HELIUS_RPC_BURST_CAPACITY
    //
    // There is no fixed sleep after each processed
    // signature.
    //
    // DB dispatcher, per-token FIFO, serializer,
    // PostgreSQL concurrency, queue configuration,
    // and RPC retry behavior remain unchanged.
    // ================================================
  }
}

function startQueueWorkers() {
  if (workerRunning) {
    return;
  }

  workerRunning = true;

  for (
    let workerId = 0;
    workerId < WORKER_CONCURRENCY;
    workerId += 1
  ) {
    const workerPromise =
      queueWorkerLoop(workerId);

    workerPromises.push(
      workerPromise
    );
  }

  logInfo(
    "Queue workers started",
    {
      workerConcurrency:
        WORKER_CONCURRENCY,

      heliusRpcMode:
        "token_bucket",

      heliusRpcMaxStartsPerSecond:
        HELIUS_RPC_MAX_STARTS_PER_SECOND,

      heliusRpcBurstCapacity:
        HELIUS_RPC_BURST_CAPACITY,
    }
  );
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
// 16B-1. INTAKE COMPLETENESS MINUTE COLLECTOR
//
// Purpose:
// • Persist one auditable scanner-health row per FULL minute.
// • Use deltas of cumulative counters for minute-local counts.
// • Sample queue pressure from the existing health logger.
// • Keep the scoring layer provisional / shadow-only.
//
// IMPORTANT:
// • The first partial minute after boot is intentionally skipped.
// • No ingestion behavior is changed.
// • One INSERT ... ON CONFLICT UPDATE is issued per full minute.
// ==================================================

const INTAKE_COMPLETENESS_FLUSH_GRACE_MS = 250;

let intakeCompletenessReady = false;
let intakeCompletenessMinuteAt = null;
let intakeCompletenessBaseline = null;

let intakeCompletenessSamples = {
  samples: 0,
  queueSizeTotal: 0,
  queueSizeMax: 0,
  oldestSignatureAgeTotalMs: 0,
  oldestSignatureAgeMaxMs: 0,
  dbQueueSizeTotal: 0,
  dbQueueSizeMax: 0,
  enabledSamples: 0,
  socketHealthySamples: 0,
};

function resetIntakeCompletenessSamples() {
  intakeCompletenessSamples = {
    samples: 0,
    queueSizeTotal: 0,
    queueSizeMax: 0,
    oldestSignatureAgeTotalMs: 0,
    oldestSignatureAgeMaxMs: 0,
    dbQueueSizeTotal: 0,
    dbQueueSizeMax: 0,
    enabledSamples: 0,
    socketHealthySamples: 0,
  };
}

function currentCumulativePauseMs() {
  const activePauseMs =
    intakePaused && intakePausedAt !== null
      ? Math.max(
          performanceNow() - intakePausedAt,
          0
        )
      : 0;

  return (
    stats.intakePauseTotalMs +
    activePauseMs
  );
}

function captureIntakeCompletenessCounters() {
  return {
    capturedAt: Date.now(),

    queued: stats.queued,
    dequeued: stats.dequeued,
    processed: stats.processed,
    insertedEvents: stats.insertedEvents,

    intakePausedCount: stats.intakePausedCount,
    cumulativePauseMs: currentCumulativePauseMs(),
    droppedDuringPause: stats.droppedDuringPause,
    droppedQueueFull: stats.droppedQueueFull,
    droppedStale: stats.droppedStale,

    rpcAttemptSamples: stats.rpcAttemptSamples,
    rpcAttemptTotalMs: stats.rpcAttemptTotalMs,
    rpcFetchSamples: stats.rpcFetchSamples,
    rpcFetchTotalMs: stats.rpcFetchTotalMs,
    rpcRetries: stats.rpcRetries,
    rpcRateLimitedRetries: stats.rpcRateLimitedRetries,
    rpcNullRetries: stats.rpcNullRetries,
    heliusRpcPacerWaited: stats.heliusRpcPacerWaited,
    heliusRpcPacerWaitTotalMs: stats.heliusRpcPacerWaitTotalMs,

    dbWriteJobsQueued: stats.dbWriteJobsQueued,
    dbWriteJobsCompleted: stats.dbWriteJobsCompleted,
    dbWriteJobsFailed: stats.dbWriteJobsFailed,
    dbWriteQueueWaitSamples: stats.dbWriteQueueWaitSamples,
    dbWriteQueueWaitTotalMs: stats.dbWriteQueueWaitTotalMs,
    dbWriteSamples: stats.dbWriteSamples,
    dbWriteTotalMs: stats.dbWriteTotalMs,

    workerErrors: stats.workerErrors,
    txFetchErrors: stats.txFetchErrors,
  };
}

function nonNegativeDelta(current, previous) {
  const a = Number(current);
  const b = Number(previous);

  if (!Number.isFinite(a) || !Number.isFinite(b)) {
    return 0;
  }

  return Math.max(a - b, 0);
}

function deltaAverage(
  currentSamples,
  previousSamples,
  currentTotal,
  previousTotal
) {
  const samples =
    nonNegativeDelta(
      currentSamples,
      previousSamples
    );

  if (samples <= 0) {
    return null;
  }

  const total =
    nonNegativeDelta(
      currentTotal,
      previousTotal
    );

  return Number(
    (total / samples).toFixed(2)
  );
}

function calculateIntakeCompletenessScore(m) {
  let score = 100;

  // Known loss is intentionally punished heavily.
  if (m.dropped_during_pause > 0) score -= 60;
  if (m.dropped_queue_full > 0) score -= 60;
  if (m.dropped_stale > 0) score -= 50;

  if (m.intake_paused_ms > 0) {
    score -= Math.min(
      40,
      (m.intake_paused_ms / 60000) * 40
    );
  }

  if (m.oldest_signature_age_ms_max > 60000) score -= 35;
  else if (m.oldest_signature_age_ms_max > 30000) score -= 20;
  else if (m.oldest_signature_age_ms_max > 15000) score -= 10;
  else if (m.oldest_signature_age_ms_max > 5000) score -= 3;

  const maxQueue = Math.max(
    effectiveMaxQueueSize(),
    1
  );

  const queuePressure =
    m.queue_size_max / maxQueue;

  if (queuePressure >= 0.96) score -= 20;
  else if (queuePressure >= 0.80) score -= 10;
  else if (queuePressure >= 0.40) score -= 5;

  const retryRate =
    m.rpc_attempt_count > 0
      ? m.rpc_retry_count /
        m.rpc_attempt_count
      : 0;

  if (retryRate > 0.05) score -= 15;
  else if (retryRate > 0.02) score -= 8;
  else if (retryRate > 0.01) score -= 3;

  if (m.db_jobs_failed > 0) score -= 30;
  if (m.worker_errors > 0) score -= 20;
  if (m.tx_fetch_errors > 0) score -= 20;

  return Math.max(
    0,
    Math.min(
      100,
      Math.round(score)
    )
  );
}

function classifyIntakeCompleteness(
  score,
  knownLoss
) {
  if (knownLoss) return "BAD";
  if (score >= 95) return "ELITE";
  if (score >= 85) return "GOOD";
  if (score >= 70) return "DEGRADED";
  return "BAD";
}

function recordIntakeCompletenessHealthSample(
  now = Date.now()
) {
  if (!intakeCompletenessReady) {
    return;
  }

  const sampleMinuteAt =
    Math.floor(now / 60000) * 60000;

  // Only sample the minute currently being collected.
  if (sampleMinuteAt !== intakeCompletenessMinuteAt) {
    return;
  }

  const oldestSignature = signatureQueue[0];

  const oldestSignatureAgeMs =
    oldestSignature
      ? Math.max(
          now - oldestSignature.enqueuedAt,
          0
        )
      : 0;

  const queueSize = signatureQueue.length;
  const dbQueueSize = dbWriteQueue.length;

  intakeCompletenessSamples.samples += 1;
  intakeCompletenessSamples.queueSizeTotal += queueSize;
  intakeCompletenessSamples.queueSizeMax = Math.max(
    intakeCompletenessSamples.queueSizeMax,
    queueSize
  );

  intakeCompletenessSamples.oldestSignatureAgeTotalMs +=
    oldestSignatureAgeMs;

  intakeCompletenessSamples.oldestSignatureAgeMaxMs =
    Math.max(
      intakeCompletenessSamples.oldestSignatureAgeMaxMs,
      oldestSignatureAgeMs
    );

  intakeCompletenessSamples.dbQueueSizeTotal +=
    dbQueueSize;

  intakeCompletenessSamples.dbQueueSizeMax = Math.max(
    intakeCompletenessSamples.dbQueueSizeMax,
    dbQueueSize
  );

  if (isPregradEnabled()) {
    intakeCompletenessSamples.enabledSamples += 1;
  }

  if (socketAlive) {
    intakeCompletenessSamples.socketHealthySamples += 1;
  }
}

async function flushIntakeCompletenessMinute(
  minuteAt,
  baseline,
  current,
  samples
) {
  if (!baseline || !current) {
    return;
  }

  const elapsedSeconds = Math.max(
    (current.capturedAt - baseline.capturedAt) / 1000,
    1
  );

  const sampleCount = samples.samples;

  const row = {
    minute_at: new Date(minuteAt).toISOString(),

    incoming_count: nonNegativeDelta(current.queued, baseline.queued),
    dequeued_count: nonNegativeDelta(current.dequeued, baseline.dequeued),
    processed_count: nonNegativeDelta(current.processed, baseline.processed),
    inserted_event_count: nonNegativeDelta(current.insertedEvents, baseline.insertedEvents),

    incoming_per_second: null,
    drained_per_second: null,
    processed_per_second: null,

    queue_size_avg:
      sampleCount > 0
        ? Number(
            (
              samples.queueSizeTotal /
              sampleCount
            ).toFixed(2)
          )
        : null,

    queue_size_max: samples.queueSizeMax,

    oldest_signature_age_ms_avg:
      sampleCount > 0
        ? Number(
            (
              samples.oldestSignatureAgeTotalMs /
              sampleCount
            ).toFixed(2)
          )
        : null,

    oldest_signature_age_ms_max:
      samples.oldestSignatureAgeMaxMs,

    intake_pause_count:
      nonNegativeDelta(
        current.intakePausedCount,
        baseline.intakePausedCount
      ),

    intake_paused_ms:
      Math.round(
        nonNegativeDelta(
          current.cumulativePauseMs,
          baseline.cumulativePauseMs
        )
      ),

    dropped_during_pause:
      nonNegativeDelta(
        current.droppedDuringPause,
        baseline.droppedDuringPause
      ),

    dropped_queue_full:
      nonNegativeDelta(
        current.droppedQueueFull,
        baseline.droppedQueueFull
      ),

    dropped_stale:
      nonNegativeDelta(
        current.droppedStale,
        baseline.droppedStale
      ),

    rpc_attempt_count:
      nonNegativeDelta(
        current.rpcAttemptSamples,
        baseline.rpcAttemptSamples
      ),

    rpc_retry_count:
      nonNegativeDelta(
        current.rpcRetries,
        baseline.rpcRetries
      ),

    rpc_rate_limited_retry_count:
      nonNegativeDelta(
        current.rpcRateLimitedRetries,
        baseline.rpcRateLimitedRetries
      ),

    rpc_null_retry_count:
      nonNegativeDelta(
        current.rpcNullRetries,
        baseline.rpcNullRetries
      ),

    rpc_fetch_avg_ms:
      deltaAverage(
        current.rpcFetchSamples,
        baseline.rpcFetchSamples,
        current.rpcFetchTotalMs,
        baseline.rpcFetchTotalMs
      ),

    rpc_attempt_avg_ms:
      deltaAverage(
        current.rpcAttemptSamples,
        baseline.rpcAttemptSamples,
        current.rpcAttemptTotalMs,
        baseline.rpcAttemptTotalMs
      ),

    rpc_pacer_wait_avg_ms:
      deltaAverage(
        current.heliusRpcPacerWaited,
        baseline.heliusRpcPacerWaited,
        current.heliusRpcPacerWaitTotalMs,
        baseline.heliusRpcPacerWaitTotalMs
      ),

    db_jobs_queued:
      nonNegativeDelta(
        current.dbWriteJobsQueued,
        baseline.dbWriteJobsQueued
      ),

    db_jobs_completed:
      nonNegativeDelta(
        current.dbWriteJobsCompleted,
        baseline.dbWriteJobsCompleted
      ),

    db_jobs_failed:
      nonNegativeDelta(
        current.dbWriteJobsFailed,
        baseline.dbWriteJobsFailed
      ),

    db_queue_size_avg:
      sampleCount > 0
        ? Number(
            (
              samples.dbQueueSizeTotal /
              sampleCount
            ).toFixed(2)
          )
        : null,

    db_queue_size_max:
      samples.dbQueueSizeMax,

    db_queue_wait_avg_ms:
      deltaAverage(
        current.dbWriteQueueWaitSamples,
        baseline.dbWriteQueueWaitSamples,
        current.dbWriteQueueWaitTotalMs,
        baseline.dbWriteQueueWaitTotalMs
      ),

    db_write_avg_ms:
      deltaAverage(
        current.dbWriteSamples,
        baseline.dbWriteSamples,
        current.dbWriteTotalMs,
        baseline.dbWriteTotalMs
      ),

    worker_errors:
      nonNegativeDelta(
        current.workerErrors,
        baseline.workerErrors
      ),

    tx_fetch_errors:
      nonNegativeDelta(
        current.txFetchErrors,
        baseline.txFetchErrors
      ),
  };

  row.incoming_per_second = Number(
    (row.incoming_count / elapsedSeconds).toFixed(2)
  );

  row.drained_per_second = Number(
    (row.dequeued_count / elapsedSeconds).toFixed(2)
  );

  row.processed_per_second = Number(
    (row.processed_count / elapsedSeconds).toFixed(2)
  );

  const expectedHealthSamples =
    Math.max(
      1,
      Math.floor(
        60000 /
        Math.max(QUEUE_LOG_EVERY_MS, 1)
      ) - 1
    );

  const healthSamplingComplete =
    sampleCount >= expectedHealthSamples;

  const systemHealthyForAllSamples =
    sampleCount > 0 &&
    samples.enabledSamples === sampleCount &&
    samples.socketHealthySamples === sampleCount;

  const knownLoss =
    row.dropped_during_pause > 0 ||
    row.dropped_queue_full > 0 ||
    row.dropped_stale > 0 ||
    row.db_jobs_failed > 0 ||
    row.tx_fetch_errors > 0;

  row.completeness_score =
    calculateIntakeCompletenessScore(row);

  // Missing health samples or scanner/socket downtime
  // makes the minute unsafe for completeness-sensitive
  // research even if no explicit drop counter moved.
  if (!healthSamplingComplete) {
    row.completeness_score = Math.min(
      row.completeness_score,
      69
    );
  }

  if (!systemHealthyForAllSamples) {
    row.completeness_score = Math.min(
      row.completeness_score,
      49
    );
  }

  row.completeness_state =
    classifyIntakeCompleteness(
      row.completeness_score,
      knownLoss
    );

  row.coverage_eligible =
    !knownLoss &&
    healthSamplingComplete &&
    systemHealthyForAllSamples &&
    row.intake_paused_ms === 0 &&
    row.oldest_signature_age_ms_max < 30000;

  await pool.query(
    `
      INSERT INTO northstar_intake_completeness_minutes (
        minute_at,
        incoming_count,
        dequeued_count,
        processed_count,
        inserted_event_count,
        incoming_per_second,
        drained_per_second,
        processed_per_second,
        queue_size_avg,
        queue_size_max,
        oldest_signature_age_ms_avg,
        oldest_signature_age_ms_max,
        intake_pause_count,
        intake_paused_ms,
        dropped_during_pause,
        dropped_queue_full,
        dropped_stale,
        rpc_attempt_count,
        rpc_retry_count,
        rpc_rate_limited_retry_count,
        rpc_null_retry_count,
        rpc_fetch_avg_ms,
        rpc_attempt_avg_ms,
        rpc_pacer_wait_avg_ms,
        db_jobs_queued,
        db_jobs_completed,
        db_jobs_failed,
        db_queue_size_avg,
        db_queue_size_max,
        db_queue_wait_avg_ms,
        db_write_avg_ms,
        worker_errors,
        tx_fetch_errors,
        completeness_score,
        completeness_state,
        coverage_eligible
      )
      VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
        $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
        $21,$22,$23,$24,$25,$26,$27,$28,$29,$30,
        $31,$32,$33,$34,$35,$36
      )
      ON CONFLICT (minute_at)
      DO UPDATE SET
        incoming_count = EXCLUDED.incoming_count,
        dequeued_count = EXCLUDED.dequeued_count,
        processed_count = EXCLUDED.processed_count,
        inserted_event_count = EXCLUDED.inserted_event_count,
        incoming_per_second = EXCLUDED.incoming_per_second,
        drained_per_second = EXCLUDED.drained_per_second,
        processed_per_second = EXCLUDED.processed_per_second,
        queue_size_avg = EXCLUDED.queue_size_avg,
        queue_size_max = EXCLUDED.queue_size_max,
        oldest_signature_age_ms_avg = EXCLUDED.oldest_signature_age_ms_avg,
        oldest_signature_age_ms_max = EXCLUDED.oldest_signature_age_ms_max,
        intake_pause_count = EXCLUDED.intake_pause_count,
        intake_paused_ms = EXCLUDED.intake_paused_ms,
        dropped_during_pause = EXCLUDED.dropped_during_pause,
        dropped_queue_full = EXCLUDED.dropped_queue_full,
        dropped_stale = EXCLUDED.dropped_stale,
        rpc_attempt_count = EXCLUDED.rpc_attempt_count,
        rpc_retry_count = EXCLUDED.rpc_retry_count,
        rpc_rate_limited_retry_count = EXCLUDED.rpc_rate_limited_retry_count,
        rpc_null_retry_count = EXCLUDED.rpc_null_retry_count,
        rpc_fetch_avg_ms = EXCLUDED.rpc_fetch_avg_ms,
        rpc_attempt_avg_ms = EXCLUDED.rpc_attempt_avg_ms,
        rpc_pacer_wait_avg_ms = EXCLUDED.rpc_pacer_wait_avg_ms,
        db_jobs_queued = EXCLUDED.db_jobs_queued,
        db_jobs_completed = EXCLUDED.db_jobs_completed,
        db_jobs_failed = EXCLUDED.db_jobs_failed,
        db_queue_size_avg = EXCLUDED.db_queue_size_avg,
        db_queue_size_max = EXCLUDED.db_queue_size_max,
        db_queue_wait_avg_ms = EXCLUDED.db_queue_wait_avg_ms,
        db_write_avg_ms = EXCLUDED.db_write_avg_ms,
        worker_errors = EXCLUDED.worker_errors,
        tx_fetch_errors = EXCLUDED.tx_fetch_errors,
        completeness_score = EXCLUDED.completeness_score,
        completeness_state = EXCLUDED.completeness_state,
        coverage_eligible = EXCLUDED.coverage_eligible
    `,
    [
      row.minute_at,
      row.incoming_count,
      row.dequeued_count,
      row.processed_count,
      row.inserted_event_count,
      row.incoming_per_second,
      row.drained_per_second,
      row.processed_per_second,
      row.queue_size_avg,
      row.queue_size_max,
      row.oldest_signature_age_ms_avg,
      row.oldest_signature_age_ms_max,
      row.intake_pause_count,
      row.intake_paused_ms,
      row.dropped_during_pause,
      row.dropped_queue_full,
      row.dropped_stale,
      row.rpc_attempt_count,
      row.rpc_retry_count,
      row.rpc_rate_limited_retry_count,
      row.rpc_null_retry_count,
      row.rpc_fetch_avg_ms,
      row.rpc_attempt_avg_ms,
      row.rpc_pacer_wait_avg_ms,
      row.db_jobs_queued,
      row.db_jobs_completed,
      row.db_jobs_failed,
      row.db_queue_size_avg,
      row.db_queue_size_max,
      row.db_queue_wait_avg_ms,
      row.db_write_avg_ms,
      row.worker_errors,
      row.tx_fetch_errors,
      row.completeness_score,
      row.completeness_state,
      row.coverage_eligible,
    ]
  );

  logInfo(
    "Intake completeness minute written",
    {
      minuteAt: row.minute_at,
      completenessScore: row.completeness_score,
      completenessState: row.completeness_state,
      coverageEligible: row.coverage_eligible,
      queueSizeMax: row.queue_size_max,
      oldestSignatureAgeMsMax:
        row.oldest_signature_age_ms_max,
      droppedDuringPause:
        row.dropped_during_pause,
      droppedQueueFull:
        row.dropped_queue_full,
      droppedStale:
        row.dropped_stale,
    }
  );
}

async function rollIntakeCompletenessMinute() {
  const now = Date.now();
  const currentMinuteAt =
    Math.floor(now / 60000) * 60000;

  // First boundary after boot establishes a clean
  // baseline. The boot partial-minute is skipped.
  if (!intakeCompletenessReady) {
    intakeCompletenessReady = true;
    intakeCompletenessMinuteAt = currentMinuteAt;
    intakeCompletenessBaseline =
      captureIntakeCompletenessCounters();
    resetIntakeCompletenessSamples();
    return;
  }

  const minuteToFlush =
    intakeCompletenessMinuteAt;

  const baseline =
    intakeCompletenessBaseline;

  const samples = {
    ...intakeCompletenessSamples,
  };

  const current =
    captureIntakeCompletenessCounters();

  // Advance state BEFORE the DB write so a slow
  // completeness INSERT cannot corrupt the next minute.
  intakeCompletenessMinuteAt = currentMinuteAt;
  intakeCompletenessBaseline = current;
  resetIntakeCompletenessSamples();

  try {
    await flushIntakeCompletenessMinute(
      minuteToFlush,
      baseline,
      current,
      samples
    );
  } catch (error) {
    logError(
      "Intake completeness minute write failed",
      {
        minuteAt:
          new Date(minuteToFlush).toISOString(),
        error:
          String(error?.message || error),
      }
    );
  }
}

function scheduleNextIntakeCompletenessBoundary() {
  if (intentionalShutdown) {
    return;
  }

  const now = Date.now();
  const nextMinuteAt =
    Math.floor(now / 60000) * 60000 +
    60000;

  const delayMs = Math.max(
    nextMinuteAt - now +
      INTAKE_COMPLETENESS_FLUSH_GRACE_MS,
    50
  );

  intakeCompletenessTimer = setTimeout(
    async () => {
      intakeCompletenessTimer = null;

      await rollIntakeCompletenessMinute();

      scheduleNextIntakeCompletenessBoundary();
    },
    delayMs
  );
}

function startIntakeCompletenessCollector() {
  if (intakeCompletenessTimer) {
    return;
  }

  // Intentionally wait for the next UTC minute boundary
  // before establishing the first full-minute baseline.
  scheduleNextIntakeCompletenessBoundary();

  logInfo(
    "Intake completeness collector started",
    {
      table:
        "northstar_intake_completeness_minutes",
      firstPartialMinuteSkipped: true,
    }
  );
}

// ==================================================
// 16C. SCANNER STATS LOGGER
//
// Produces one compact scanner-health record per
// logging interval.
//
// INGESTION:
// • WebSocket health
// • Signature queue depth / age
// • In-flight signature count
// • Incoming / drain / insert / processing rates
//
// DB WRITE DISPATCHER:
// • DB-write queue depth / age
// • Active DB-write concurrency
// • Active token lanes
// • DB dispatcher queue-wait latency
// • Dispatcher throughput
// • Dispatcher backpressure
//
// TOKEN SERIALIZER:
// • Worker waits
// • Maximum simultaneous waiters
// • Token-lane queue depth
// • Same-token contention diagnostics
//
// DATABASE:
// • Complete DB-write latency
// • Pool-acquisition latency
// • PostgreSQL transaction latency
// • BEGIN
// • token UPSERT
// • event INSERT
// • market UPDATE
// • COMMIT
// • Slow transaction diagnostics
//
// RPC:
// • Full RPC fetch lifecycle
// • Pure RPC attempt latency
//
// COVERAGE:
// • Intake pauses
// • Pause duration
// • Queue / stale / pause drops
//
// Timing summaries are cumulative since startup.
// Rate measurements cover the current log interval.
//
// Observation only: does not modify ingestion.
// ==================================================

// ==================================================
// 16C. SCANNER STATS LOGGER
//
// Produces one scanner-health record per interval.
//
// Tracks:
//
// INGESTION
// • Signature queue depth / age
// • In-flight signatures
// • Incoming / drain / insert / processing rates
//
// DB WRITE DISPATCHER
// • Queue depth / oldest job
// • Jobs queued / started / completed
// • Queue wait
// • Active DB writes / token lanes
// • Backpressure
//
// TOKEN SERIALIZER
// • Real serializer waits
// • Worker wait pressure
//
// DATABASE
// • DB-write latency
// • Pool acquisition
// • Transaction execution
// • Individual transaction stages
// • Slow transaction distribution
// • Contention-depth diagnostics
//
// RPC
// • Global Helius RPC pacing
// • Full fetch lifecycle
// • Individual RPC attempts
//
// COVERAGE / HEALTH
// • Intake pauses
// • PostgreSQL pool pressure
// • Effective runtime configuration
//
// Timing summaries are cumulative since startup.
// Rates cover only the current logging interval.
//
// Observation only.
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
        // ==========================================
        // SHARED CONTROL REFRESH
        // ==========================================

        await getPregradControl();

        const now = Date.now();

        // Feed queue-pressure samples into the current
        // full-minute completeness bucket.
        recordIntakeCompletenessHealthSample(now);

        const seconds = Math.max(
          (now - previousLogAt) / 1000,
          1
        );

        const oldestSignature =
          signatureQueue[0];

        const oldestDbWriteJob =
          dbWriteQueue[0];

        // ==========================================
        // INTERVAL INGESTION RATES
        // ==========================================

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

        // ==========================================
        // DB DISPATCHER INTERVAL RATES
        // ==========================================

        const dbJobsQueuedPerSecond =
          Number(
            (
              (
                stats.dbWriteJobsQueued -
                (
                  previousStats
                    .dbWriteJobsQueued ||
                  0
                )
              ) /
              seconds
            ).toFixed(2)
          );

        const dbJobsStartedPerSecond =
          Number(
            (
              (
                stats.dbWriteJobsStarted -
                (
                  previousStats
                    .dbWriteJobsStarted ||
                  0
                )
              ) /
              seconds
            ).toFixed(2)
          );

        const dbJobsCompletedPerSecond =
          Number(
            (
              (
                stats.dbWriteJobsCompleted -
                (
                  previousStats
                    .dbWriteJobsCompleted ||
                  0
                )
              ) /
              seconds
            ).toFixed(2)
          );

        // ==========================================
        // CORE PERFORMANCE SUMMARIES
        // ==========================================

        const rpcFetchPerformance =
          getPerformanceSummary(
            "rpcFetch"
          );

        const rpcAttemptPerformance =
          getPerformanceSummary(
            "rpcAttempt"
          );

        const dbWritePerformance =
          getPerformanceSummary(
            "dbWrite"
          );

        const processingPerformance =
          getPerformanceSummary(
            "processing"
          );

        const intakePausePerformance =
          getPerformanceSummary(
            "intakePause"
          );

        // ==========================================
        // GLOBAL HELIUS RPC PACER
        //
        // Measures time callers spend waiting for
        // permission to START a Helius RPC request.
        //
        // This does not measure HTTP request latency.
        // rpcAttemptPerformance handles that.
        // ==========================================

  const heliusRpcPacerHealth = {
  mode:
    "token_bucket",

  maxStartsPerSecond:
    HELIUS_RPC_MAX_STARTS_PER_SECOND,

  burstCapacity:
    HELIUS_RPC_BURST_CAPACITY,

  availableTokens:
    Number(
      heliusRpcTokens.toFixed(2)
    ),

  samples:
    stats.heliusRpcPacerSamples,

  immediate:
    stats.heliusRpcPacerImmediate,

  waited:
    stats.heliusRpcPacerWaited,

  waitPct:
    stats.heliusRpcPacerSamples > 0
      ? Number(
          (
            (
              stats.heliusRpcPacerWaited /
              stats.heliusRpcPacerSamples
            ) *
            100
          ).toFixed(2)
        )
      : null,

  avgWaitMs:
    stats.heliusRpcPacerWaited > 0
      ? Number(
          (
            stats.heliusRpcPacerWaitTotalMs /
            stats.heliusRpcPacerWaited
          ).toFixed(2)
        )
      : null,

  maxWaitMs:
    stats.heliusRpcPacerWaited > 0
      ? Number(
          stats.heliusRpcPacerWaitMaxMs
            .toFixed(2)
        )
      : null,
};

        // ==========================================
        // DATABASE DIAGNOSTIC SUMMARIES
        //
        // These diagnostics use their dedicated
        // counters rather than
        // getPerformanceSummary().
        // ==========================================

        const dbPoolAcquirePerformance = {
          samples:
            stats.dbPoolAcquireSamples,

          avgMs:
            stats.dbPoolAcquireSamples > 0
              ? Number(
                  (
                    stats.dbPoolAcquireTotalMs /
                    stats.dbPoolAcquireSamples
                  ).toFixed(2)
                )
              : null,

          maxMs:
            stats.dbPoolAcquireSamples > 0
              ? Number(
                  stats.dbPoolAcquireMaxMs.toFixed(
                    2
                  )
                )
              : null,
        };

        const dbQueryExecutionPerformance = {
          samples:
            stats.dbQueryExecutionSamples,

          avgMs:
            stats.dbQueryExecutionSamples > 0
              ? Number(
                  (
                    stats.dbQueryExecutionTotalMs /
                    stats.dbQueryExecutionSamples
                  ).toFixed(2)
                )
              : null,

          maxMs:
            stats.dbQueryExecutionSamples > 0
              ? Number(
                  stats.dbQueryExecutionMaxMs.toFixed(
                    2
                  )
                )
              : null,
        };

        // ==========================================
        // COMBINED-WRITE STAGE SUMMARIES
        // ==========================================

        const dbBeginPerformance =
          getDbStageSummary(
            "begin"
          );

        const dbTokenUpsertPerformance =
          getDbStageSummary(
            "tokenUpsert"
          );

        const dbEventInsertPerformance =
          getDbStageSummary(
            "eventInsert"
          );

        const dbMarketUpdatePerformance =
          getDbStageSummary(
            "marketUpdate"
          );

        const dbCommitPerformance =
          getDbStageSummary(
            "commit"
          );

        // ==========================================
        // CONTENTION DEPTH PERFORMANCE
        // ==========================================

        const contentionDepthPerformance =
          summarizeContentionDepth();

        // ==========================================
        // SLOW DATABASE TRANSACTION DISTRIBUTION
        //
        // Threshold counters are cumulative.
        // ==========================================

        const slowDbQueries = {
          over250ms:
            stats.dbQueriesOver250ms,

          over500ms:
            stats.dbQueriesOver500ms,

          over1000ms:
            stats.dbQueriesOver1000ms,

          over5000ms:
            stats.dbQueriesOver5000ms,
        };

        // ==========================================
        // SIGNATURE QUEUE HEALTH
        // ==========================================

        const oldestSignatureAgeMs =
          oldestSignature
            ? Math.max(
                now -
                  oldestSignature.enqueuedAt,
                0
              )
            : 0;

        // ==========================================
        // DATABASE WRITE DISPATCHER HEALTH
        // ==========================================

        const oldestDbWriteJobAgeMs =
          oldestDbWriteJob &&
          Number.isFinite(
            oldestDbWriteJob.enqueuedAt
          )
            ? Math.max(
                performanceNow() -
                  oldestDbWriteJob.enqueuedAt,
                0
              )
            : 0;

        const avgDbWriteQueueWaitMs =
          stats.dbWriteQueueWaitSamples > 0
            ? Number(
                (
                  stats.dbWriteQueueWaitTotalMs /
                  stats.dbWriteQueueWaitSamples
                ).toFixed(2)
              )
            : null;

        const dbWriteDispatcher = {
          queueSize:
            dbWriteQueue.length,

          inFlight:
            dbWritesInFlight,

          activeTokens:
            activeDbWriteTokens.size,

          concurrency:
            DB_WRITE_CONCURRENCY,

          maxQueueSize:
            MAX_DB_WRITE_QUEUE_SIZE,

          oldestJobAgeMs:
            Number(
              oldestDbWriteJobAgeMs.toFixed(
                2
              )
            ),

          avgQueueWaitMs:
            avgDbWriteQueueWaitMs,

          maxQueueWaitMs:
            stats.dbWriteQueueWaitSamples > 0
              ? Number(
                  stats.dbWriteQueueWaitMaxMs.toFixed(
                    2
                  )
                )
              : null,

          jobsQueuedPerSecond:
            dbJobsQueuedPerSecond,

          jobsStartedPerSecond:
            dbJobsStartedPerSecond,

          jobsCompletedPerSecond:
            dbJobsCompletedPerSecond,

          queueDepthMax:
            stats.dbWriteQueueDepthMax,

          inFlightMax:
            stats.dbWriteInFlightMax,

          // NOTE:
          // This is a dispatcher scan counter.
          // It may count the same blocked queued job
          // more than once across dispatch passes.
          blockedSameTokenScans:
            stats.dbWriteDispatchBlockedSameToken,

          backpressureEvents:
            stats.dbWriteDispatchBackpressure,

          jobsQueued:
            stats.dbWriteJobsQueued,

          jobsStarted:
            stats.dbWriteJobsStarted,

          jobsCompleted:
            stats.dbWriteJobsCompleted,

          jobsFailed:
            stats.dbWriteJobsFailed,
        };

        // ==========================================
        // REAL TOKEN SERIALIZER HEALTH
        // ==========================================

        const avgTokenSerializerWaitMs =
          stats.tokenSerializerWaited > 0
            ? Number(
                (
                  stats.tokenSerializerWaitTotalMs /
                  stats.tokenSerializerWaited
                ).toFixed(2)
              )
            : null;

        const avgTokenSerializerQueueDepth =
          stats.tokenSerializerSamples > 0
            ? Number(
                (
                  stats.tokenSerializerQueueDepthTotal /
                  stats.tokenSerializerSamples
                ).toFixed(2)
              )
            : null;

        const tokenSerializerHealth = {
          samples:
            stats.tokenSerializerSamples,

          immediate:
            stats.tokenSerializerImmediate,

          waited:
            stats.tokenSerializerWaited,

          avgWaitMs:
            avgTokenSerializerWaitMs,

          maxWaitMs:
            stats.tokenSerializerWaited > 0
              ? Number(
                  stats.tokenSerializerWaitMaxMs.toFixed(
                    2
                  )
                )
              : null,

          avgQueueDepth:
            avgTokenSerializerQueueDepth,

          maxQueueDepth:
            stats.tokenSerializerQueueDepthMax,
        };

        // ==========================================
        // TOKEN SERIALIZER WORKER PRESSURE
        //
        // With the DB dispatcher working correctly,
        // these should remain near zero because the
        // dispatcher should prevent same-token jobs
        // from entering the serializer concurrently.
        // ==========================================

        const tokenSerializerWorkerHealth = {
          waitingCurrent:
            stats.tokenSerializerWorkersWaitingCurrent,

          waitingMax:
            stats.tokenSerializerWorkersWaitingMax,

          waitSamples:
            stats.tokenSerializerWorkerWaitSamples,

          waitDepth1:
            stats.tokenSerializerWorkerWaitDepth1,

          waitDepth2:
            stats.tokenSerializerWorkerWaitDepth2,

          waitDepth3To5:
            stats.tokenSerializerWorkerWaitDepth3To5,

          waitDepth6To10:
            stats.tokenSerializerWorkerWaitDepth6To10,

          waitDepth11To20:
            stats.tokenSerializerWorkerWaitDepth11To20,

          waitDepth21Plus:
            stats.tokenSerializerWorkerWaitDepth21Plus,

          saturationSamples:
            stats.tokenSerializerWorkerSaturationSamples,
        };

        // ==========================================
        // CURRENT INTAKE PAUSE
        //
        // Completed pauses are already represented
        // by intakePausePerformance.
        // ==========================================

        const currentPauseMs =
          intakePaused &&
          intakePausedAt !== null
            ? Math.max(
                Math.round(
                  performanceNow() -
                    intakePausedAt
                ),
                0
              )
            : 0;

        // ==========================================
        // POSTGRESQL CONNECTION POOL
        // ==========================================

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
// ==========================================
// EFFECTIVE RUNTIME CONFIGURATION
//
// Queue limit and minimum SOL threshold may
// be overridden by pregrad_system_control.
// ==========================================

const effectiveConfiguration = {
  maxQueueSize:
    effectiveMaxQueueSize(),

  resumeQueueSize:
    RESUME_QUEUE_SIZE,

  signatureMaxAgeMs:
    SIGNATURE_MAX_AGE_MS,

  workerConcurrency:
    WORKER_CONCURRENCY,

  heliusRpcMode:
    "token_bucket",

  heliusRpcMaxStartsPerSecond:
    HELIUS_RPC_MAX_STARTS_PER_SECOND,

  heliusRpcBurstCapacity:
    HELIUS_RPC_BURST_CAPACITY,

  heliusRpcAvailableTokens:
    Number.isFinite(heliusRpcTokens)
      ? Number(
          heliusRpcTokens.toFixed(2)
        )
      : null,

          dbWriteConcurrency:
            DB_WRITE_CONCURRENCY,

          maxDbWriteQueueSize:
            MAX_DB_WRITE_QUEUE_SIZE,

          minSolAmount:
            effectiveMinSolAmount(),

          storeRawEvents:
            STORE_RAW_EVENTS,

          tokenDbSerializationEnabled:
            TOKEN_DB_SERIALIZATION_ENABLED,
        };

        const dbRttBackendHealth =
  Array.from(
    dbRttBackendDiagnostics.values()
  )
    .map((backend) => ({
      backendPid:
        backend.backendPid,

      samples:
        backend.samples,

      avgQueryMs:
        backend.samples > 0
          ? Number(
              (
                backend.totalQueryMs /
                backend.samples
              ).toFixed(2)
            )
          : null,

      minQueryMs:
        backend.minQueryMs !== null
          ? Number(
              backend.minQueryMs
                .toFixed(2)
            )
          : null,

      maxQueryMs:
        backend.samples > 0
          ? Number(
              backend.maxQueryMs
                .toFixed(2)
            )
          : null,

      fastUnder10ms:
        backend.fastUnder10ms,

      middle10To100ms:
        backend.middle10To100ms,

      slow100msPlus:
        backend.slow100msPlus,

      firstSeenAt:
        backend.firstSeenAt,

      lastSeenAt:
        backend.lastSeenAt,
    }))
    .sort(
      (a, b) =>
        b.samples - a.samples
    );

const dbRttProbeHealth = {
  samples:
    stats.dbRttProbeSamples,

  errors:
    stats.dbRttProbeErrors,

  latestMs:
    stats.dbRttProbeSamples > 0
      ? Number(
          stats.dbRttProbeLatestMs
            .toFixed(2)
        )
      : null,

  avgMs:
    stats.dbRttProbeSamples > 0
      ? Number(
          (
            stats.dbRttProbeTotalMs /
            stats.dbRttProbeSamples
          ).toFixed(2)
        )
      : null,

  maxMs:
    stats.dbRttProbeSamples > 0
      ? Number(
          stats.dbRttProbeMaxMs
            .toFixed(2)
        )
      : null,

  // ==========================================
  // POOL ACQUISITION COMPONENT
  // ==========================================

  acquire: {
    samples:
      stats.dbRttAcquireSamples,

    avgMs:
      stats.dbRttAcquireSamples > 0
        ? Number(
            (
              stats.dbRttAcquireTotalMs /
              stats.dbRttAcquireSamples
            ).toFixed(2)
          )
        : null,

    maxMs:
      stats.dbRttAcquireSamples > 0
        ? Number(
            stats.dbRttAcquireMaxMs
              .toFixed(2)
          )
        : null,
  },

  // ==========================================
  // SELECT 1 EXECUTION COMPONENT
  // ==========================================

  query: {
    samples:
      stats.dbRttQuerySamples,

    avgMs:
      stats.dbRttQuerySamples > 0
        ? Number(
            (
              stats.dbRttQueryTotalMs /
              stats.dbRttQuerySamples
            ).toFixed(2)
          )
        : null,

    maxMs:
      stats.dbRttQuerySamples > 0
        ? Number(
            stats.dbRttQueryMaxMs
              .toFixed(2)
          )
        : null,
  },

  // ==========================================
  // TOTAL RTT DISTRIBUTION
  // ==========================================

  distribution: {
    under10ms:
      dbRttProbeBuckets.under10ms,

    ms10To25:
      dbRttProbeBuckets.ms10To25,

    ms25To50:
      dbRttProbeBuckets.ms25To50,

    ms50To100:
      dbRttProbeBuckets.ms50To100,

    ms100To150:
      dbRttProbeBuckets.ms100To150,

    ms150To250:
      dbRttProbeBuckets.ms150To250,

    ms250To500:
      dbRttProbeBuckets.ms250To500,

    ms500Plus:
      dbRttProbeBuckets.ms500Plus,
  },

  // ==========================================
  // POSTGRES BACKEND IDENTITY
  // ==========================================

  backends:
    Array.from(
      dbRttBackendDiagnostics.values()
    )
      .map((backend) => ({
        backendPid:
          backend.backendPid,

        samples:
          backend.samples,

        avgQueryMs:
          backend.samples > 0
            ? Number(
                (
                  backend.totalQueryMs /
                  backend.samples
                ).toFixed(2)
              )
            : null,

        minQueryMs:
          backend.minQueryMs !== null
            ? Number(
                backend.minQueryMs
                  .toFixed(2)
              )
            : null,

        maxQueryMs:
          backend.samples > 0
            ? Number(
                backend.maxQueryMs
                  .toFixed(2)
              )
            : null,

        fastUnder10ms:
          backend.fastUnder10ms,

        middle10To100ms:
          backend.middle10To100ms,

        slow100msPlus:
          backend.slow100msPlus,

        firstSeenAt:
          backend.firstSeenAt,

        lastSeenAt:
          backend.lastSeenAt,
      }))
      .sort(
        (a, b) =>
          b.samples - a.samples
      ),

  // ==========================================
  // RECENT PROBE HISTORY
  // ==========================================

  recent:
    dbRttRecentProbes.map(
      (probe) => ({
        ...probe,
      })
    ),
};

        // ==========================================
        // EMIT SCANNER HEALTH RECORD
        // ==========================================

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

            // --------------------------------------
            // SIGNATURE PIPELINE
            // --------------------------------------

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

            oldestSignatureAgeMs,

            // --------------------------------------
            // INTERVAL INGESTION RATES
            // --------------------------------------

            incomingPerSecond,
            drainedPerSecond,
            insertedPerSecond,
            processedPerSecond,

            // --------------------------------------
            // DB WRITE DISPATCHER
            // --------------------------------------

            dbWriteDispatcher,

            // --------------------------------------
            // TOKEN SERIALIZATION
            // --------------------------------------

            tokenSerializerHealth,
            tokenSerializerWorkerHealth,

            // --------------------------------------
            // RPC PACING / PERFORMANCE
            // --------------------------------------

            heliusRpcPacerHealth,
            rpcFetchPerformance,
            rpcAttemptPerformance,

// --------------------------------------
// DATABASE PERFORMANCE
// --------------------------------------

dbWritePerformance,
dbPoolAcquirePerformance,
dbQueryExecutionPerformance,

dbRttProbeHealth,

dbBeginPerformance,
dbTokenUpsertPerformance,
dbEventInsertPerformance,
dbMarketUpdatePerformance,
dbCommitPerformance,

contentionDepthPerformance,

slowDbQueries,



            // --------------------------------------
            // END-TO-END PROCESSING
            // --------------------------------------

            processingPerformance,

            // --------------------------------------
            // COVERAGE / PAUSE HEALTH
            // --------------------------------------

            intakePausePerformance,
            currentPauseMs,

            // --------------------------------------
            // POSTGRES / CONFIGURATION
            // --------------------------------------

            postgresPool,
            effectiveConfiguration,

            // --------------------------------------
            // PRESERVE ALL RAW CUMULATIVE COUNTERS
            // --------------------------------------

            ...stats,
          }
        );

        // ==========================================
        // UPDATE INTERVAL BASELINE
        //
        // Only advance after successful logging.
        // ==========================================

        previousStats = {
          queued:
            stats.queued,

          dequeued:
            stats.dequeued,

          insertedEvents:
            stats.insertedEvents,

          processed:
            stats.processed,

          dbWriteJobsQueued:
            stats.dbWriteJobsQueued,

          dbWriteJobsStarted:
            stats.dbWriteJobsStarted,

          dbWriteJobsCompleted:
            stats.dbWriteJobsCompleted,
        };

        previousLogAt = now;

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

function recordDbRttBucket(
  durationMs
) {
  if (
    !Number.isFinite(durationMs) ||
    durationMs < 0
  ) {
    return;
  }

  if (durationMs < 10) {
    dbRttProbeBuckets.under10ms += 1;

  } else if (durationMs < 25) {
    dbRttProbeBuckets.ms10To25 += 1;

  } else if (durationMs < 50) {
    dbRttProbeBuckets.ms25To50 += 1;

  } else if (durationMs < 100) {
    dbRttProbeBuckets.ms50To100 += 1;

  } else if (durationMs < 150) {
    dbRttProbeBuckets.ms100To150 += 1;

  } else if (durationMs < 250) {
    dbRttProbeBuckets.ms150To250 += 1;

  } else if (durationMs < 500) {
    dbRttProbeBuckets.ms250To500 += 1;

  } else {
    dbRttProbeBuckets.ms500Plus += 1;
  }
}


function recordRecentDbRttProbe(
  durationMs,
  acquireMs,
  queryMs,
  backendPid
) {
  dbRttRecentProbes.push({
    time:
      nowIso(),

    backendPid:
      Number.isFinite(backendPid)
        ? backendPid
        : null,

    rttMs:
      Number(
        durationMs.toFixed(2)
      ),

    acquireMs:
      Number(
        acquireMs.toFixed(2)
      ),

    queryMs:
      Number(
        queryMs.toFixed(2)
      ),

    poolTotal:
      pool.totalCount,

    poolIdle:
      pool.idleCount,

    poolWaiting:
      pool.waitingCount,

    dbQueueSize:
      dbWriteQueue.length,

    dbWritesInFlight,

    activeTokens:
      activeDbWriteTokens.size,

    signatureQueueSize:
      signatureQueue.length,
  });

  while (
    dbRttRecentProbes.length >
    DB_RTT_RECENT_LIMIT
  ) {
    dbRttRecentProbes.shift();
  }
}


function recordDbRttBackendProbe(
  backendPid,
  queryMs
) {
  if (
    !Number.isFinite(backendPid) ||
    !Number.isFinite(queryMs) ||
    queryMs < 0
  ) {
    return;
  }

  let backend =
    dbRttBackendDiagnostics.get(
      backendPid
    );

  if (!backend) {
    backend = {
      backendPid,

      samples: 0,

      totalQueryMs: 0,

      minQueryMs: null,

      maxQueryMs: 0,

      fastUnder10ms: 0,

      middle10To100ms: 0,

      slow100msPlus: 0,

      firstSeenAt: null,

      lastSeenAt: null,
    };

    dbRttBackendDiagnostics.set(
      backendPid,
      backend
    );
  }

  const timestamp =
    nowIso();

  backend.samples += 1;

  backend.totalQueryMs +=
    queryMs;

  backend.minQueryMs =
    backend.minQueryMs === null
      ? queryMs
      : Math.min(
          backend.minQueryMs,
          queryMs
        );

  backend.maxQueryMs =
    Math.max(
      backend.maxQueryMs,
      queryMs
    );

  if (queryMs < 10) {
    backend.fastUnder10ms += 1;

  } else if (queryMs < 100) {
    backend.middle10To100ms += 1;

  } else {
    backend.slow100msPlus += 1;
  }

  if (!backend.firstSeenAt) {
    backend.firstSeenAt =
      timestamp;
  }

  backend.lastSeenAt =
    timestamp;
}
// ==================================================
// 16C. POSTGRES BASELINE RTT DIAGNOSTIC
//
// Periodically executes:
//
//   SELECT 1
//
// through the normal PostgreSQL pool.
//
// This measures the baseline application → PostgreSQL
// round-trip independently of the ingestion write
// transaction.
//
// Observation only.
// ==================================================

async function runDbRttProbe() {
  if (dbRttProbeRunning) {
    return;
  }

  dbRttProbeRunning = true;

  const totalStartedAt =
    performanceNow();

  let client = null;

  try {
    // ==============================================
    // PHASE 1 — POOL ACQUISITION
    // ==============================================

    const acquireStartedAt =
      performanceNow();

    client =
      await pool.connect();
    const backendPid =
  Number.isFinite(client.processID)
    ? client.processID
    : null;

    const acquireMs =
      performanceNow() -
      acquireStartedAt;

    stats.dbRttAcquireSamples += 1;

    stats.dbRttAcquireTotalMs +=
      acquireMs;

    stats.dbRttAcquireMaxMs =
      Math.max(
        stats.dbRttAcquireMaxMs,
        acquireMs
      );

    // ==============================================
    // PHASE 2 — SELECT 1 ON ACQUIRED CLIENT
    // ==============================================

    const queryStartedAt =
      performanceNow();

    await client.query(
      "SELECT 1"
    );

    const queryMs =
      performanceNow() -
      queryStartedAt;
    recordDbRttBackendProbe(
  backendPid,
  queryMs
);

    stats.dbRttQuerySamples += 1;

    stats.dbRttQueryTotalMs +=
      queryMs;

    stats.dbRttQueryMaxMs =
      Math.max(
        stats.dbRttQueryMaxMs,
        queryMs
      );

    // ==============================================
    // TOTAL PROBE
    // ==============================================

    const durationMs =
      performanceNow() -
      totalStartedAt;

    stats.dbRttProbeSamples += 1;

    stats.dbRttProbeTotalMs +=
      durationMs;

    stats.dbRttProbeLatestMs =
      durationMs;

    stats.dbRttProbeMaxMs =
      Math.max(
        stats.dbRttProbeMaxMs,
        durationMs
      );

    recordDbRttBucket(
      durationMs
    );

    recordRecentDbRttProbe(
  durationMs,
  acquireMs,
  queryMs,
  backendPid
);

  } catch (error) {
    stats.dbRttProbeErrors += 1;

    logError(
      "Postgres RTT probe failed",
      {
        error:
          String(
            error?.message ||
            error
          ),
      }
    );

  } finally {
    if (client) {
      client.release();
    }

    dbRttProbeRunning = false;
  }
}
   

function startDbRttProbe() {
  if (dbRttProbeTimer) {
    return;
  }

  // Get one baseline measurement immediately.
  runDbRttProbe().catch(() => {});

  dbRttProbeTimer =
    setInterval(
      () => {
        runDbRttProbe().catch(() => {});
      },
      DB_RTT_PROBE_INTERVAL_MS
    );

  logInfo(
    "Postgres RTT probe started",
    {
      intervalMs:
        DB_RTT_PROBE_INTERVAL_MS,
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

  if (dbRttProbeTimer) {
    clearInterval(
      dbRttProbeTimer
    );

    dbRttProbeTimer =
      null;
  }

  if (intakeCompletenessTimer) {
    clearTimeout(
      intakeCompletenessTimer
    );

    intakeCompletenessTimer =
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
// 5. Wait for active signature workers to settle
// 6. Drain queued / active DB dispatcher work
// 7. Close the PostgreSQL pool
//
// IMPORTANT:
//
// Signature workers may hand accepted events to the
// independent DB-write dispatcher and return before
// PostgreSQL work finishes.
//
// Shutdown therefore MUST wait for BOTH:
//
// • dbWriteQueue.length === 0
// • dbWritesInFlight === 0
//
// before closing the PostgreSQL pool.
//
// Raw-event retention is intentionally excluded from
// this service.
// ==================================================


// ==================================================
// 18A. SHUTDOWN CONTROLS
// ==================================================
//
// The drain timeout prevents shutdown from hanging
// forever if PostgreSQL becomes unavailable.
//
// Polling is intentionally lightweight and does not
// create any database work.
// ==================================================

const SHUTDOWN_DB_DRAIN_TIMEOUT_MS =
  Number(
    process.env
      .SHUTDOWN_DB_DRAIN_TIMEOUT_MS ||
    30000
  );

const SHUTDOWN_DB_DRAIN_POLL_MS =
  Number(
    process.env
      .SHUTDOWN_DB_DRAIN_POLL_MS ||
    100
  );


// ==================================================
// 18B. WAIT FOR DB DISPATCHER DRAIN
//
// Wait until:
//
// • No queued DB jobs remain.
// • No DB jobs are currently executing.
//
// Returns:
//
// true  = dispatcher fully drained
// false = shutdown timeout reached
//
// IMPORTANT:
//
// Do NOT disable the dispatcher while draining.
//
// Active jobs may finish and make later same-token
// FIFO jobs dispatchable. The normal dispatcher must
// therefore remain operational until the queue is
// completely empty.
// ==================================================

async function waitForDbWriteDispatcherDrain() {
  const startedAt =
    Date.now();

  while (true) {
    const queueSize =
      dbWriteQueue.length;

    const inFlight =
      dbWritesInFlight;

    if (
      queueSize === 0 &&
      inFlight === 0
    ) {
      return true;
    }

    const elapsedMs =
      Date.now() -
      startedAt;

    if (
      elapsedMs >=
      SHUTDOWN_DB_DRAIN_TIMEOUT_MS
    ) {
      logError(
        "Database write dispatcher drain timed out",
        {
          elapsedMs,

          queueSize,

          inFlight,

          activeTokens:
            activeDbWriteTokens.size,

          maxDrainMs:
            SHUTDOWN_DB_DRAIN_TIMEOUT_MS,
        }
      );

      return false;
    }

    await sleep(
      SHUTDOWN_DB_DRAIN_POLL_MS
    );
  }
}


// ==================================================
// 18C. BOOT
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
startDbRttProbe();
startIntakeCompletenessCollector();


    // ----------------------------------------------
    // START HELIUS INTAKE
    //
    // Connect only after the database, control cache,
    // workers, and maintenance timers are ready.
    // ----------------------------------------------

    connect();


 // ----------------------------------------------
// BOOT SUMMARY
// ----------------------------------------------

logInfo(
  "PreGrad scanner boot completed",
  {
    workerConcurrency:
      WORKER_CONCURRENCY,

    // ------------------------------------------
    // HELIUS RPC TOKEN BUCKET
    // ------------------------------------------

    heliusRpcMode:
      "token_bucket",

    heliusRpcMaxStartsPerSecond:
      HELIUS_RPC_MAX_STARTS_PER_SECOND,

    heliusRpcBurstCapacity:
      HELIUS_RPC_BURST_CAPACITY,

    // ------------------------------------------
    // SIGNATURE QUEUE
    // ------------------------------------------

    maxQueueSize:
      effectiveMaxQueueSize(),

    resumeQueueSize:
      RESUME_QUEUE_SIZE,

    // ------------------------------------------
    // DATABASE WRITE PIPELINE
    // ------------------------------------------

    dbWriteConcurrency:
      DB_WRITE_CONCURRENCY,

    maxDbWriteQueueSize:
      MAX_DB_WRITE_QUEUE_SIZE,

    tokenDbSerializationEnabled:
      TOKEN_DB_SERIALIZATION_ENABLED,

    // ------------------------------------------
    // OPTIONAL PIPELINES
    // ------------------------------------------

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
// 18D. SHUTDOWN
// ==================================================

async function shutdown(
  signal = "unknown"
) {
  if (intentionalShutdown) {
    return;
  }

  intentionalShutdown = true;


  // ----------------------------------------------
  // INITIAL SHUTDOWN SNAPSHOT
  // ----------------------------------------------

  logInfo(
    "Shutting down PreGrad scanner",
    {
      signal,

      signatureQueueSize:
        signatureQueue.length,

      signatureInFlight:
        inFlightSignatures.size,

      dbWriteQueueSize:
        dbWriteQueue.length,

      dbWritesInFlight,

      activeDbWriteTokens:
        activeDbWriteTokens.size,
    }
  );


  // ----------------------------------------------
  // STOP NEW INTAKE
  //
  // Existing accepted work is allowed to finish.
  // ----------------------------------------------

  intakePaused = true;

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
  //
  // No new signatures should enter the scanner after
  // this point.
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
  // STOP SIGNATURE WORKERS
  //
  // Workers currently processing a signature are
  // allowed to finish that iteration.
  //
  // This is important because a worker may still be
  // preparing a DB dispatcher handoff.
  // ----------------------------------------------

  workerRunning = false;


  // ----------------------------------------------
  // WAIT FOR SIGNATURE WORKERS
  //
  // Once this completes, no worker can enqueue a new
  // DB dispatcher job.
  // ----------------------------------------------

  try {
    await Promise.allSettled(
      workerPromises
    );

    logInfo(
      "Signature workers stopped",
      {
        remainingSignatureQueue:
          signatureQueue.length,

        signatureInFlight:
          inFlightSignatures.size,

        dbWriteQueueSize:
          dbWriteQueue.length,

        dbWritesInFlight,
      }
    );

  } catch (error) {
    logError(
      "Signature worker shutdown wait failed",
      {
        error:
          String(
            error?.message ||
            error
          ),
      }
    );
  }


  // ----------------------------------------------
  // DRAIN DB WRITE DISPATCHER
  //
  // Signature workers are now stopped, so the DB
  // dispatcher owns the final accepted work.
  //
  // Keep PostgreSQL OPEN while this drains.
  // ----------------------------------------------

  let dispatcherDrained =
    false;

  try {
    dispatcherDrained =
      await waitForDbWriteDispatcherDrain();

  } catch (error) {
    logError(
      "Database write dispatcher drain failed",
      {
        error:
          String(
            error?.message ||
            error
          ),
      }
    );
  }


  // ----------------------------------------------
  // FINAL DISPATCHER SNAPSHOT
  // ----------------------------------------------

  logInfo(
    "Database write dispatcher shutdown state",
    {
      drained:
        dispatcherDrained,

      queueSize:
        dbWriteQueue.length,

      inFlight:
        dbWritesInFlight,

      activeTokens:
        activeDbWriteTokens.size,

      jobsQueued:
        stats.dbWriteJobsQueued,

      jobsStarted:
        stats.dbWriteJobsStarted,

      jobsCompleted:
        stats.dbWriteJobsCompleted,

      jobsFailed:
        stats.dbWriteJobsFailed,
    }
  );


  // ----------------------------------------------
  // CLOSE DATABASE POOL
  //
  // Under normal operation the dispatcher is now
  // completely empty.
  //
  // If the drain timed out, pool.end() is still
  // attempted so the process can terminate cleanly.
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


  // ----------------------------------------------
  // FINAL SHUTDOWN SUMMARY
  // ----------------------------------------------

  logInfo(
    "PreGrad scanner shutdown completed",
    {
      signal,

      dispatcherDrained,

      remainingSignatureQueue:
        signatureQueue.length,

      remainingInFlightSignatures:
        inFlightSignatures.size,

      remainingDbWriteQueue:
        dbWriteQueue.length,

      remainingDbWritesInFlight:
        dbWritesInFlight,
    }
  );

  process.exit(0);
}


// ==================================================
// 18E. PROCESS SIGNALS
// ==================================================

process.once(
  "SIGINT",
  () => {
    void shutdown(
      "SIGINT"
    );
  }
);

process.once(
  "SIGTERM",
  () => {
    void shutdown(
      "SIGTERM"
    );
  }
);


// ==================================================
// 18F. START SERVICE
// ==================================================

boot();
