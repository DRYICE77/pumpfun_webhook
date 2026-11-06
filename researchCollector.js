"use strict";

// North Star Research Collector V1 — controlled batch / durable recovery worker.
// This is NOT a live WebSocket collector yet. No production tables are modified.
require("dotenv").config();
const crypto = require("node:crypto");
const { Pool } = require("pg");

const RUN_ID = process.env.NORTHSTAR_RESEARCH_RUN_ID;
const DATABASE_URL = process.env.DATABASE_URL;
const HELIUS_API_KEY = process.env.HELIUS_API_KEY;
const RPC_URL = process.env.HELIUS_RPC_URL ||
  (HELIUS_API_KEY ? `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}` : null);
const WORKERS = Math.max(1, Math.min(4, Number(process.env.RESEARCH_WORKERS || 2)));
const MAX_ATTEMPTS = Math.max(1, Number(process.env.RESEARCH_MAX_ATTEMPTS || 12));
const LEASE_SECONDS = 90;
const FETCH_TIMEOUT_MS = 25000;
const RPC_SPACING_MS = Math.max(0, Number(process.env.RESEARCH_RPC_SPACING_MS || 250));

if (!RUN_ID || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(RUN_ID)) {
  throw new Error("Set a valid NORTHSTAR_RESEARCH_RUN_ID");
}
if (!DATABASE_URL || !RPC_URL) throw new Error("Set DATABASE_URL and HELIUS_API_KEY (or HELIUS_RPC_URL)");

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: process.env.PG_SSL === "false" ? false : { rejectUnauthorized: false },
  max: WORKERS + 2,
  connectionTimeoutMillis: 10000,
});

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const shortError = (err) => String(err?.message || err).slice(0, 1500);

// Stable hash of JSONB-equivalent JSON (objects have sorted keys).
// Hash represents canonicalized content, not original HTTP response bytes.
function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort().map(k => `${JSON.stringify(k)}:${canonicalJson(value[k])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

async function assertRunExists() {
  const { rows } = await pool.query(
    "SELECT run_id, status FROM public.northstar_research_runs WHERE run_id=$1", [RUN_ID]
  );
  if (!rows.length) throw new Error(`Research run not found: ${RUN_ID}`);
  if (["completed", "failed"].includes(rows[0].status)) {
    throw new Error(`Research run is ${rows[0].status}; refusing to process`);
  }
}

function validSignature(sig) {
  return typeof sig === "string" && /^[1-9A-HJ-NP-Za-km-z]{64,88}$/.test(sig);
}

async function enqueueSignatures(signatures) {
  let count = 0;
  for (const signature of [...new Set(signatures)]) {
    if (!validSignature(signature)) throw new Error(`Invalid base58 signature: ${signature}`);
    await pool.query(`
      INSERT INTO public.northstar_research_signatures
        (signature, first_run_id, first_observed_at, last_observed_at, status)
      VALUES ($1, $2, NOW(), NOW(), 'pending')
      ON CONFLICT (signature) DO UPDATE
      SET last_observed_at = NOW(), updated_at = NOW()
    `, [signature, RUN_ID]);
    count++;
  }
  return count;
}

// Claim one eligible job. Expired leases are recoverable after a crash.
async function claimOne() {
  const { rows } = await pool.query(`
    WITH candidate AS (
      SELECT signature
      FROM public.northstar_research_signatures
      WHERE first_run_id = $1
        AND fetch_attempts < $2
        AND (
          status = 'pending'
          OR (status = 'retry' AND (next_retry_at IS NULL OR next_retry_at <= NOW()))
          OR (status = 'fetching' AND lease_expires_at <= NOW())
        )
      ORDER BY first_observed_at, signature
      FOR UPDATE SKIP LOCKED
      LIMIT 1
    )
    UPDATE public.northstar_research_signatures s
    SET status='fetching',
        fetch_attempts=s.fetch_attempts+1,
        last_attempt_at=NOW(),
        lease_expires_at=NOW()+($3 * INTERVAL '1 second'),
        next_retry_at=NULL,
        last_error=NULL,
        updated_at=NOW()
    FROM candidate c
    WHERE s.signature=c.signature
    RETURNING s.signature, s.fetch_attempts, s.first_observed_at
  `, [RUN_ID, MAX_ATTEMPTS, LEASE_SECONDS]);
  return rows[0] || null;
}

let rpcTail = Promise.resolve();
async function paceRpc() {
  const next = rpcTail.then(() => delay(RPC_SPACING_MS));
  rpcTail = next.catch(() => {});
  await next;
}

async function getTransaction(signature) {
  await paceRpc();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  try {
    const response = await fetch(RPC_URL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        jsonrpc: "2.0", id: 1, method: "getTransaction",
        params: [signature, { encoding: "jsonParsed", commitment: "confirmed", maxSupportedTransactionVersion: 0 }]
      }),
      signal: controller.signal,
    });
    if (!response.ok) throw new Error(`Helius HTTP ${response.status}`);
    const body = await response.json();
    if (body.error) throw new Error(`Helius RPC ${JSON.stringify(body.error)}`);
    if (!body.result) throw new Error("Helius returned null transaction (not available yet)");
    return body.result;
  } finally {
    clearTimeout(timer);
  }
}

async function archiveTransaction(job, payload) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    throw new Error("Unexpected transaction payload shape");
  }
  if (!Number.isSafeInteger(payload.slot)) throw new Error("Missing or invalid slot");
  const serialized = JSON.stringify(payload);
  const hash = crypto.createHash("sha256").update(canonicalJson(payload)).digest("hex");
  const success = payload.meta?.err == null;
  const blockTime = Number.isFinite(payload.blockTime)
    ? new Date(payload.blockTime * 1000) : null;
  const version = payload.version == null ? null : String(payload.version);
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
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
      WHERE signature=$1 AND status='fetching' AND fetch_attempts=$2
      RETURNING signature
    `, [job.signature, job.fetch_attempts]);
    if (!updated.rowCount) throw new Error("Job lease was superseded before archival");
    await client.query("COMMIT");
    return { inserted: Boolean(inserted.rowCount), success };
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

async function markFailure(job, err) {
  const terminal = job.fetch_attempts >= MAX_ATTEMPTS;
  const backoffSeconds = Math.min(3600, 5 * (2 ** Math.min(job.fetch_attempts - 1, 9)));
  await pool.query(`
    UPDATE public.northstar_research_signatures
    SET status=$3,
        next_retry_at=CASE WHEN $3='retry' THEN NOW()+($4 * INTERVAL '1 second') ELSE NULL END,
        lease_expires_at=NULL, last_error=$5, updated_at=NOW()
    WHERE signature=$1 AND status='fetching' AND fetch_attempts=$2
  `, [job.signature, job.fetch_attempts, terminal ? "failed" : "retry",
      backoffSeconds, shortError(err)]);
}

async function worker(workerId) {
  let processed = 0;
  for (;;) {
    const job = await claimOne();
    if (!job) break; // One batch pass: retry jobs become eligible in a future invocation.
    try {
      const payload = await getTransaction(job.signature);
      const result = await archiveTransaction(job, payload);
      console.log(JSON.stringify({ workerId, signature: job.signature,
        status: "archived", inserted: result.inserted, transactionSuccess: result.success }));
    } catch (err) {
      await markFailure(job, err);
      console.error(JSON.stringify({ workerId, signature: job.signature,
        status: "retry_or_failed", attempt: job.fetch_attempts, error: shortError(err) }));
    }
    processed++;
  }
  return processed;
}

async function main() {
  const args = process.argv.slice(2);
  const command = args.shift();
  if (!["enqueue", "work", "enqueue-and-work"].includes(command)) {
    console.log("Usage: node researchCollector.js enqueue <signature...>\n" +
      "       node researchCollector.js work\n" +
      "       node researchCollector.js enqueue-and-work <signature...>");
    return;
  }
  await assertRunExists();
  if (command !== "work") {
    if (!args.length) throw new Error("Supply at least one transaction signature");
    const n = await enqueueSignatures(args);
    console.log(`Persisted ${n} signature(s) to durable inbox`);
  }
  if (command !== "enqueue") {
    const results = await Promise.all(Array.from({ length: WORKERS }, (_, i) => worker(i + 1)));
    console.log(`Batch finished. Attempted ${results.reduce((a, b) => a + b, 0)} signature(s).`);
  }
}

main().catch((err) => { console.error(err); process.exitCode = 1; })
  .finally(() => pool.end());
