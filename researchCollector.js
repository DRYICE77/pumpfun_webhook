"use strict";

// North Star Research Collector V1.2 — performance diagnostics — durable batch and continuous RPC worker.
// Does not subscribe to WebSockets or modify production tables.
require("dotenv").config();
const crypto = require("node:crypto");
const { performance } = require("node:perf_hooks");
const { Pool } = require("pg");


const rawRunId = process.env.NORTHSTAR_RESEARCH_RUN_ID;
const RUN_ID = rawRunId?.trim();

const uuidPattern =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

console.log("[RESEARCH ENV DIAGNOSTIC]", {
  runIdPresent: rawRunId !== undefined,
  runIdLength: rawRunId?.length ?? null,
  trimmedLength: RUN_ID?.length ?? null,
  validUUID: uuidPattern.test(RUN_ID || ""),
  separatorCodePoints: RUN_ID
    ? [...RUN_ID]
        .filter(c => !/[0-9a-f]/i.test(c))
        .map(c => c.codePointAt(0).toString(16))
    : []
});

const DATABASE_URL = process.env.DATABASE_URL;
const HELIUS_API_KEY = process.env.HELIUS_API_KEY;
const RPC_URL = process.env.HELIUS_RPC_URL ||
  (HELIUS_API_KEY ? `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}` : null);

function positiveInt(name, fallback, min, max) {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isSafeInteger(value) || value < min || value > max) {
    throw new Error(`${name} must be an integer between ${min} and ${max}`);
  }
  return value;
}

const WORKERS = positiveInt("RESEARCH_WORKERS", 2, 1, 4);
const MAX_ATTEMPTS = positiveInt("RESEARCH_MAX_ATTEMPTS", 12, 1, 100);
const RPC_SPACING_MS = positiveInt("RESEARCH_RPC_SPACING_MS", 250, 0, 60000);
const POLL_MS = positiveInt("RESEARCH_POLL_MS", 2000, 100, 60000);
const FETCH_TIMEOUT_MS = positiveInt("RESEARCH_FETCH_TIMEOUT_MS", 25000, 1000, 120000);
const LEASE_SECONDS = positiveInt("RESEARCH_LEASE_SECONDS", 90, 30, 600);
const PERF_REPORT_MS = positiveInt("RESEARCH_PERF_REPORT_MS", 30000, 5000, 300000);
const PERF_SAMPLE_CAP = positiveInt("RESEARCH_PERF_SAMPLE_CAP", 10000, 100, 100000);
if (LEASE_SECONDS * 1000 <= FETCH_TIMEOUT_MS + 10000) {
  throw new Error("RESEARCH_LEASE_SECONDS must exceed fetch timeout by more than 10 seconds");
}


if (!uuidPattern.test(RUN_ID || "")) {
  throw new Error(
    "Set a valid NORTHSTAR_RESEARCH_RUN_ID"
  );
}

if (!DATABASE_URL || !RPC_URL) {
  throw new Error("Set DATABASE_URL and HELIUS_API_KEY (or HELIUS_RPC_URL)");
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: process.env.PG_SSL === "false" ? false : { rejectUnauthorized: false },
  max: WORKERS + 2,
  connectionTimeoutMillis: 10000,
});
pool.on("error", (err) => console.error("Idle PostgreSQL connection error:", err.message));

let stopping = false;
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const shortError = (err) => String(err?.message || err).slice(0, 1500);
const log = (data) => console.log(JSON.stringify({ at: new Date().toISOString(), ...data }));

// Process-local, rolling timing metrics. No SQL writes or transaction payloads.
// All durations are milliseconds, measured with a monotonic clock.
const perfMetrics = new Map();
let perfStartedAt = performance.now();
let perfReportTimer = null;
function recordPerf(name, durationMs) {
  if (!Number.isFinite(durationMs) || durationMs < 0) return;
  let entry = perfMetrics.get(name);
  if (!entry) {
    entry = { count: 0, sum: 0, min: Infinity, max: 0, samples: [] };
    perfMetrics.set(name, entry);
  }
  entry.count++;
  entry.sum += durationMs;
  entry.min = Math.min(entry.min, durationMs);
  entry.max = Math.max(entry.max, durationMs);
  // Keep a bounded, evenly spread sample for approximate p95.
  if (entry.samples.length < PERF_SAMPLE_CAP) entry.samples.push(durationMs);
  else entry.samples[entry.count % PERF_SAMPLE_CAP] = durationMs;
}
const perfCounts = { claimed: 0, archived: 0, retried: 0, failed: 0, superseded: 0, idlePolls: 0 };
function reportPerf() {
  const elapsedSec = (performance.now() - perfStartedAt) / 1000;
  const timingsMs = {};
  for (const [name, entry] of perfMetrics) {
    const sorted = [...entry.samples].sort((a, b) => a - b);
    const p95 = sorted[Math.max(0, Math.ceil(sorted.length * 0.95) - 1)];
    timingsMs[name] = {
      n: entry.count,
      avg: Number((entry.sum / entry.count).toFixed(1)),
      p95: Number(p95.toFixed(1)),
      max: Number(entry.max.toFixed(1)),
    };
  }
  log({ event: "research_collector_perf", windowSeconds: Number(elapsedSec.toFixed(1)),
    workers: WORKERS, rpcSpacingMs: RPC_SPACING_MS, ...perfCounts,
    archivedPerMinute: elapsedSec > 0 ? Number((perfCounts.archived * 60 / elapsedSec).toFixed(1)) : 0,
    timingsMs });
  perfMetrics.clear();
  for (const key of Object.keys(perfCounts)) perfCounts[key] = 0;
  perfStartedAt = performance.now();
}


// Hash a deterministic representation of the RPC response, not original HTTP bytes.
function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort().map((k) => `${JSON.stringify(k)}:${canonicalJson(value[k])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

async function assertRunExists() {
  const { rows } = await pool.query(
    "SELECT status FROM public.northstar_research_runs WHERE run_id=$1", [RUN_ID]
  );
  if (!rows.length) throw new Error(`Research run not found: ${RUN_ID}`);
  if (!["created", "running"].includes(rows[0].status)) {
    throw new Error(`Research run is ${rows[0].status}; refusing to process`);
  }
}

function validSignature(sig) {
  return typeof sig === "string" && /^[1-9A-HJ-NP-Za-km-z]{64,88}$/.test(sig);
}

async function enqueueSignatures(signatures) {
  let count = 0;
  for (const signature of new Set(signatures)) {
    if (!validSignature(signature)) throw new Error(`Invalid base58 signature: ${signature}`);
    await pool.query(`
      INSERT INTO public.northstar_research_signatures
        (signature, first_run_id, first_observed_at, last_observed_at, status)
      VALUES ($1, $2, NOW(), NOW(), 'pending')
      ON CONFLICT (signature) DO UPDATE
      SET last_observed_at=NOW(), updated_at=NOW()
    `, [signature, RUN_ID]);
    count++;
  }
  return count;
}

// Finalize expired leases that have already used their last attempt.
// Otherwise they would remain stuck as 'fetching' forever after a crash.
async function finalizeExhaustedLeases() {
  const { rowCount } = await pool.query(`
    UPDATE public.northstar_research_signatures
    SET status='failed', lease_expires_at=NULL, next_retry_at=NULL,
        last_error=COALESCE(last_error, 'Worker lease expired after final permitted attempt'),
        updated_at=NOW()
    WHERE first_run_id=$1 AND status='fetching'
      AND fetch_attempts >= $2 AND lease_expires_at <= NOW()
  `, [RUN_ID, MAX_ATTEMPTS]);
  if (rowCount) log({ event: "exhausted_leases_finalized", count: rowCount });
}

// PostgreSQL row lock makes claiming safe across multiple processes.
// Attempt count acts as a fencing generation for reclaimed jobs.
async function claimOne() {
  const startedAt = performance.now();
  try {
  const { rows } = await pool.query(`
    WITH candidate AS (
      SELECT signature
      FROM public.northstar_research_signatures
      WHERE first_run_id=$1 AND fetch_attempts < $2
        AND (
          status='pending'
          OR (status='retry' AND (next_retry_at IS NULL OR next_retry_at <= NOW()))
          OR (status='fetching' AND lease_expires_at <= NOW())
        )
      ORDER BY first_observed_at, signature
      FOR UPDATE SKIP LOCKED
      LIMIT 1
    )
    UPDATE public.northstar_research_signatures s
    SET status='fetching', fetch_attempts=s.fetch_attempts+1,
        last_attempt_at=NOW(),
        lease_expires_at=NOW()+($3 * INTERVAL '1 second'),
        next_retry_at=NULL, last_error=NULL, updated_at=NOW()
    FROM candidate c
    WHERE s.signature=c.signature
    RETURNING s.signature, s.fetch_attempts, s.first_observed_at
  `, [RUN_ID, MAX_ATTEMPTS, LEASE_SECONDS]);
  return rows[0] || null;
  } finally { recordPerf("dbClaim", performance.now() - startedAt); }
}

// Serialize RPC starts across workers, preserving a minimum spacing.
let rpcTail = Promise.resolve();
let nextRpcStartAt = 0;
async function paceRpc() {
  const startedAt = performance.now();
  const turn = rpcTail.then(async () => {
    const wait = Math.max(0, nextRpcStartAt - Date.now());
    if (wait) await delay(wait);
    nextRpcStartAt = Date.now() + RPC_SPACING_MS;
  });
  rpcTail = turn.catch(() => {});
  try { await turn; }
  finally { recordPerf("rpcPaceWait", performance.now() - startedAt); }
}

async function getTransaction(signature) {
  await paceRpc();
  const fetchStartedAt = performance.now();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  try {
    const response = await fetch(RPC_URL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        jsonrpc: "2.0", id: 1, method: "getTransaction",
        params: [signature, {
  encoding: "jsonParsed",
  commitment: "confirmed",
  maxSupportedTransactionVersion: 1,
}],
      }),
      signal: controller.signal,
    });
    if (!response.ok) throw new Error(`Helius HTTP ${response.status}`);
    const body = await response.json();
    if (body.error) throw new Error(`Helius RPC ${JSON.stringify(body.error)}`);
    if (!body.result) throw new Error("Helius returned null transaction (not available yet)");
    const signatures = body.result.transaction?.signatures;
    if (!Array.isArray(signatures) || signatures[0] !== signature) {
      throw new Error("RPC transaction signature mismatch");
    }
    return body.result;
  } finally {
    recordPerf("rpcFetch", performance.now() - fetchStartedAt);
    clearTimeout(timer);
  }
}

async function archiveTransaction(job, payload) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    throw new Error("Unexpected transaction payload shape");
  }
  if (!Number.isSafeInteger(payload.slot)) throw new Error("Missing or invalid slot");
  if (!payload.meta || typeof payload.meta !== "object") {
    throw new Error("Missing transaction metadata");
  }
  const serializationStartedAt = performance.now();
  const serialized = JSON.stringify(payload);
  const hash = crypto.createHash("sha256").update(canonicalJson(payload)).digest("hex");
  recordPerf("jsonSerializeHash", performance.now() - serializationStartedAt);
  const success = payload.meta.err == null;
  const blockTime = Number.isFinite(payload.blockTime)
    ? new Date(payload.blockTime * 1000) : null;
  const version = payload.version == null ? null : String(payload.version);
  const poolWaitStartedAt = performance.now();
  const client = await pool.connect();
  recordPerf("dbPoolWait", performance.now() - poolWaitStartedAt);
  const archiveStartedAt = performance.now();
  try {
    await client.query("BEGIN");
    // Lock the inbox row before archive insertion so a superseded worker cannot write.
    const lease = await client.query(`
      SELECT signature FROM public.northstar_research_signatures
      WHERE signature=$1 AND first_run_id=$2 AND status='fetching'
        AND fetch_attempts=$3 AND lease_expires_at > NOW()
      FOR UPDATE
    `, [job.signature, RUN_ID, job.fetch_attempts]);
    if (!lease.rowCount) throw new Error("Job lease expired or was superseded");

    const inserted = await client.query(`
      INSERT INTO public.northstar_research_raw_transactions
        (signature, run_id, slot, block_time, first_observed_at,
         fetched_at, source, rpc_provider, transaction_version,
         transaction_success, payload, payload_sha256)
      VALUES ($1,$2,$3,$4,$5,NOW(),'batch_rpc','helius',$6,$7,$8::jsonb,$9)
      ON CONFLICT (signature) DO NOTHING
      RETURNING signature
    `, [job.signature, RUN_ID, payload.slot, blockTime, job.first_observed_at,
      version, success, serialized, hash]);

    if (!inserted.rowCount) {
      const existing = await client.query(`
        SELECT payload_sha256 FROM public.northstar_research_raw_transactions
        WHERE signature=$1
      `, [job.signature]);
      if (!existing.rows.length || existing.rows[0].payload_sha256 !== hash) {
        throw new Error("Existing archived signature has a different payload hash");
      }
    }

    const updated = await client.query(`
      UPDATE public.northstar_research_signatures
      SET status='archived', archived_at=NOW(), lease_expires_at=NULL,
          next_retry_at=NULL, last_error=NULL, updated_at=NOW()
      WHERE signature=$1 AND first_run_id=$2 AND status='fetching'
        AND fetch_attempts=$3 AND lease_expires_at > NOW()
      RETURNING signature
    `, [job.signature, RUN_ID, job.fetch_attempts]);
    if (!updated.rowCount) throw new Error("Lease expired before archival commit");
    await client.query("COMMIT");
    return { inserted: Boolean(inserted.rowCount), success };
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    recordPerf("dbArchiveTransaction", performance.now() - archiveStartedAt);
    client.release();
  }
}

async function markFailure(job, err) {
  const terminal = job.fetch_attempts >= MAX_ATTEMPTS;
  const backoffSeconds = Math.min(3600, 5 * (2 ** Math.min(job.fetch_attempts - 1, 9)));
  const { rowCount } = await pool.query(`
    UPDATE public.northstar_research_signatures
    SET status=$4,
        next_retry_at=CASE WHEN $4='retry'
          THEN NOW()+($5 * INTERVAL '1 second') ELSE NULL END,
        lease_expires_at=NULL, last_error=$6, updated_at=NOW()
    WHERE signature=$1 AND first_run_id=$2 AND status='fetching'
      AND fetch_attempts=$3
  `, [job.signature, RUN_ID, job.fetch_attempts,
    terminal ? "failed" : "retry", backoffSeconds, shortError(err)]);
  return { terminal, updated: Boolean(rowCount) };
}

async function worker(workerId, continuous) {
  let attempted = 0;
  while (!stopping) {
    const claimStartedAt = performance.now();
    const job = await claimOne();
    if (!job) {
      perfCounts.idlePolls++;
      if (!continuous) break;
      await delay(POLL_MS);
      continue;
    }
    perfCounts.claimed++;
    const jobStartedAt = claimStartedAt;
    try {
      const payload = await getTransaction(job.signature);
      const result = await archiveTransaction(job, payload);
      perfCounts.archived++;
      log({ workerId, signature: job.signature, status: "archived",
        inserted: result.inserted, transactionSuccess: result.success });
    } catch (err) {
      const failureStartedAt = performance.now();
      const failure = await markFailure(job, err);
      recordPerf("dbMarkFailure", performance.now() - failureStartedAt);
      const status = failure.updated ? (failure.terminal ? "failed" : "retry") : "superseded";
      if (status === "retry") perfCounts.retried++;
      else if (status === "failed") perfCounts.failed++;
      else perfCounts.superseded++;
      log({ workerId, signature: job.signature,
        status, attempt: job.fetch_attempts, error: shortError(err) });
    } finally {
      recordPerf("jobEndToEnd", performance.now() - jobStartedAt);
    }
    attempted++;
  }
  return attempted;
}

async function recoverLoop() {
  while (!stopping) {
    await finalizeExhaustedLeases();
    // Run frequently enough to make expired final attempts visible.
    for (let i = 0; i < 15 && !stopping; i++) await delay(1000);
  }
}

async function main() {
  const args = process.argv.slice(2);
  const command = args.shift();
  if (!["enqueue", "work", "enqueue-and-work", "run"].includes(command)) {
    console.log("Usage: node researchCollector.js enqueue <signature...>\n" +
      "       node researchCollector.js work\n" +
      "       node researchCollector.js enqueue-and-work <signature...>\n" +
      "       node researchCollector.js run");
    return;
  }
  await assertRunExists();
  if (command === "enqueue" || command === "enqueue-and-work") {
    if (!args.length) throw new Error("Supply at least one transaction signature");
    const count = await enqueueSignatures(args);
    log({ event: "enqueued", count });
  }
  if (command === "enqueue") return;
  await finalizeExhaustedLeases();
  const continuous = command === "run";
  if (continuous) {
    process.once("SIGTERM", () => { stopping = true; log({ event: "shutdown_requested", signal: "SIGTERM" }); });
    process.once("SIGINT", () => { stopping = true; log({ event: "shutdown_requested", signal: "SIGINT" }); });
    log({ event: "continuous_worker_started", runId: RUN_ID, workers: WORKERS, pollMs: POLL_MS, rpcSpacingMs: RPC_SPACING_MS, perfReportMs: PERF_REPORT_MS });
    perfReportTimer = setInterval(reportPerf, PERF_REPORT_MS);
  }
  const workers = Array.from({ length: WORKERS }, (_, i) => worker(i + 1, continuous));
  if (continuous) {
    await Promise.all([...workers, recoverLoop()]);
  } else {
    const results = await Promise.all(workers);
    reportPerf();
    log({ event: "batch_finished", attempted: results.reduce((a, b) => a + b, 0) });
  }
  if (perfReportTimer) { clearInterval(perfReportTimer); perfReportTimer = null; reportPerf(); }
}

main().catch((err) => {
  console.error("Research collector fatal error:", shortError(err));
  process.exitCode = 1;
}).finally(async () => { await pool.end(); });
