"use strict";

// North Star Research WebSocket V2 — PumpSwap-only intake.
// Durable PostgreSQL inbox, batched writes, bounded in-memory buffer.
// IMPORTANT: WebSocket notifications cannot be replayed. This listener is not
// lossless across disconnects, process crashes, or database outages; reconcile
// coverage gaps separately using RPC and durable coverage records.
require("dotenv").config();
const WebSocket = require("ws");
const { Pool } = require("pg");

const RUN_ID = (process.env.NORTHSTAR_RESEARCH_RUN_ID || "").trim();
const DATABASE_URL = process.env.DATABASE_URL;
const API_KEY = process.env.HELIUS_API_KEY;
const WSS_URL = process.env.HELIUS_WSS_URL ||
  (API_KEY ? `wss://mainnet.helius-rpc.com/?api-key=${encodeURIComponent(API_KEY)}` : "");
const PROGRAM_ID = process.env.RESEARCH_PUMP_AMM_PROGRAM_ID ||
  "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const positiveInt = (value, fallback, min, max) => {
  const n = Number(value);
  return Number.isSafeInteger(n) && n >= min && n <= max ? n : fallback;
};
const BATCH_SIZE = positiveInt(process.env.RESEARCH_WS_BATCH_SIZE, 250, 1, 1000);
const FLUSH_MS = positiveInt(process.env.RESEARCH_WS_FLUSH_MS, 100, 10, 10000);
const MAX_PENDING = positiveInt(process.env.RESEARCH_WS_MAX_PENDING, 10000, BATCH_SIZE, 100000);
const RETRY_MS = positiveInt(process.env.RESEARCH_WS_DB_RETRY_MS, 3000, 100, 60000);
const RECONNECT_MS = positiveInt(process.env.RESEARCH_WS_RECONNECT_MS, 5000, 1000, 120000);
const HEARTBEAT_MS = 30000;
const HEALTH_MS = 30000;

if (!/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(RUN_ID)) {
  throw new Error("Invalid NORTHSTAR_RESEARCH_RUN_ID");
}
if (!DATABASE_URL || !WSS_URL) throw new Error("Set DATABASE_URL and HELIUS_API_KEY (or HELIUS_WSS_URL)");
if (!/^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(PROGRAM_ID)) throw new Error("Invalid program ID");
if (MAX_PENDING < BATCH_SIZE) throw new Error("RESEARCH_WS_MAX_PENDING must be >= RESEARCH_WS_BATCH_SIZE");

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: process.env.PG_SSL === "false" ? false : { rejectUnauthorized: false },
  max: 3,
  connectionTimeoutMillis: 10000,
  statement_timeout: 15000,
});
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const log = (event, extra = {}) => console.log(JSON.stringify({ at: new Date().toISOString(), event, ...extra }));
const safeError = err => String(err?.message || err).slice(0, 300);

let socket = null;
let stopping = false;
let connecting = false;
let subscriptionId = null;
let reconnectTimer = null;
let heartbeatTimer = null;
let healthTimer = null;
let flushTimer = null;
let draining = false;
let queue = [];
const pendingSet = new Set();
let seen = 0;
let persisted = 0;
let batches = 0;
let duplicateNotifications = 0;
let dbFailures = 0;
let overflowEvents = 0;
let lastSlot = null;
let connectionStartedAt = null;
let lastDisconnectAt = null;
let connectionNumber = 0;
let previousHealth = { at: Date.now(), seen: 0, persisted: 0 };

async function checkRun() {
  const result = await pool.query(
    "SELECT status FROM public.northstar_research_runs WHERE run_id=$1", [RUN_ID]
  );
  if (!result.rows.length || !["created", "running"].includes(result.rows[0].status)) {
    throw new Error("Research run not found or not active (created/running)");
  }
}

function scheduleReconnect() {
  if (stopping || reconnectTimer) return;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    if (queue.length || draining) {
      scheduleReconnect(); // Do not reopen intake before persistence recovers.
      return;
    }
    connect();
  }, RECONNECT_MS);
}

function closeIntake(reason) {
  if (!socket || socket.readyState === WebSocket.CLOSED) return;
  log("intake_interrupted", { reason, pending: queue.length, lastSlot });
  socket.terminate();
}

// One query per batch, not one query per signature. UNNEST preserves one row per
// array index; duplicate signatures are prevented by pendingSet and DB PK.
async function persistBatch(items) {
  const signatures = items.map(x => x.signature);
  const observed = items.map(x => x.observedAt);
  const slots = items.map(x => x.slot);
  await pool.query(`
    INSERT INTO public.northstar_research_signatures
      (signature, first_run_id, first_observed_at, last_observed_at, observed_slot, status)
    SELECT x.signature, $1::uuid, x.observed_at, x.observed_at, x.slot, 'pending'
    FROM UNNEST($2::text[], $3::timestamptz[], $4::bigint[])
      AS x(signature, observed_at, slot)
    ON CONFLICT (signature) DO UPDATE SET
      last_observed_at = EXCLUDED.last_observed_at,
      observed_slot = COALESCE(public.northstar_research_signatures.observed_slot, EXCLUDED.observed_slot),
      updated_at = NOW()
  `, [RUN_ID, signatures, observed, slots]);
}

async function drain() {
  if (draining) return;
  draining = true;
  try {
    while (queue.length) {
      const items = queue.slice(0, BATCH_SIZE);
      const started = Date.now();
      try {
        await persistBatch(items);
        // Remove only after the database confirms success.
        queue.splice(0, items.length);
        for (const item of items) pendingSet.delete(item.signature);
        persisted += items.length;
        batches++;
        if (batches <= 3 || batches % 50 === 0) {
          log("db_batch_persisted", {
            count: items.length, durationMs: Date.now() - started,
            pending: queue.length, batches, persisted,
          });
        }
      } catch (err) {
        dbFailures++;
        log("db_persist_failed", {
          error: safeError(err), batchSize: items.length,
          pending: queue.length, dbFailures,
        });
        closeIntake("database_persistence_failure");
        await sleep(RETRY_MS); // Retain the same batch for retry.
      }
    }
  } finally {
    draining = false;
  }
}

function handleMessage(raw, ws) {
  if (ws !== socket || stopping) return;
  let msg;
  try { msg = JSON.parse(raw.toString()); }
  catch (err) { log("invalid_ws_json", { error: safeError(err) }); return; }

  if (msg.id === 1) {
    if (msg.error) {
      log("subscription_rejected", { error: msg.error });
      closeIntake("subscription_rejected");
    } else {
      subscriptionId = msg.result;
      log("subscribed", { program: PROGRAM_ID, subscriptionId });
    }
    return;
  }
  if (msg.method !== "logsNotification") return;
  if (subscriptionId == null || msg.params?.subscription !== subscriptionId) return;

  const signature = msg.params?.result?.value?.signature;
  const slot = msg.params?.result?.context?.slot;
  if (typeof signature !== "string" || !/^[1-9A-HJ-NP-Za-km-z]{64,88}$/.test(signature)) {
    log("invalid_notification_signature");
    return;
  }
  seen++;
  lastSlot = Number.isSafeInteger(slot) ? slot : lastSlot;
  if (pendingSet.has(signature)) {
    duplicateNotifications++;
    return;
  }
  if (queue.length >= MAX_PENDING) {
    overflowEvents++;
    log("coverage_gap_queue_overflow", {
      signature, pending: queue.length, slot, lastSlot, overflowEvents,
    });
    closeIntake("queue_overflow");
    return;
  }
  queue.push({ signature, slot: Number.isSafeInteger(slot) ? slot : null, observedAt: new Date() });
  pendingSet.add(signature);
  if (queue.length >= BATCH_SIZE) void drain();
}

function connect() {
  if (stopping || connecting || socket?.readyState === WebSocket.OPEN) return;
  connecting = true;
  connectionNumber++;
  const connection = connectionNumber;
  subscriptionId = null;
  let alive = true;
  const ws = new WebSocket(WSS_URL, { handshakeTimeout: 15000 });
  socket = ws;
  ws.on("open", () => {
    connecting = false;
    connectionStartedAt = new Date().toISOString();
    log("ws_connected", { connection, previousDisconnectAt: lastDisconnectAt });
    ws.send(JSON.stringify({
      jsonrpc: "2.0", id: 1, method: "logsSubscribe",
      params: [{ mentions: [PROGRAM_ID] }, { commitment: "confirmed" }],
    }));
    if (heartbeatTimer) clearInterval(heartbeatTimer);
    heartbeatTimer = setInterval(() => {
      if (!alive) {
        closeIntake("heartbeat_timeout");
        return;
      }
      alive = false;
      if (ws.readyState === WebSocket.OPEN) ws.ping();
    }, HEARTBEAT_MS);
  });
  ws.on("pong", () => { alive = true; });
  ws.on("message", raw => handleMessage(raw, ws));
  ws.on("error", err => log("ws_error", { connection, error: safeError(err) }));
  ws.on("close", (code, reason) => {
    connecting = false;
    if (heartbeatTimer) clearInterval(heartbeatTimer);
    heartbeatTimer = null;
    lastDisconnectAt = new Date().toISOString();
    log("ws_disconnected_gap_possible", {
      connection, code, reason: reason.toString().slice(0, 100),
      connectedAt: connectionStartedAt, disconnectedAt: lastDisconnectAt,
      lastSlot, pending: queue.length,
    });
    if (socket === ws) socket = null;
    scheduleReconnect();
  });
}

async function shutdown(signal) {
  if (stopping) return;
  stopping = true;
  log("shutdown_requested", { signal, pending: queue.length });
  clearTimeout(reconnectTimer);
  clearInterval(heartbeatTimer);
  clearInterval(healthTimer);
  clearInterval(flushTimer);
  if (socket) socket.terminate();
  // Attempt final flush. Process may be force-killed before completion.
  void drain();
  const deadline = Date.now() + 15000;
  while ((draining || queue.length) && Date.now() < deadline) await sleep(100);
  if (queue.length) {
    log("coverage_gap_shutdown_unpersisted", { count: queue.length, lastSlot });
    // Avoid hanging on a blocked database request during shutdown.
    process.exit(1);
  }
  await pool.end();
  process.exit(0);
}
process.on("SIGTERM", () => { void shutdown("SIGTERM"); });
process.on("SIGINT", () => { void shutdown("SIGINT"); });

async function main() {
  await checkRun();
  log("research_ws_starting", {
    program: PROGRAM_ID, maxPending: MAX_PENDING,
    batchSize: BATCH_SIZE, flushMs: FLUSH_MS,
  });
  flushTimer = setInterval(() => { if (queue.length) void drain(); }, FLUSH_MS);
  healthTimer = setInterval(() => {
    const now = Date.now();
    const minutes = Math.max((now - previousHealth.at) / 60000, 0.001);
    log("research_ws_health", {
      connected: socket?.readyState === WebSocket.OPEN,
      subscribed: subscriptionId != null,
      seen, persisted, duplicateNotifications, dbFailures, overflowEvents,
      pending: queue.length, batches, lastSlot,
      seenPerMinute: Math.round((seen - previousHealth.seen) / minutes),
      persistedPerMinute: Math.round((persisted - previousHealth.persisted) / minutes),
    });
    previousHealth = { at: now, seen, persisted };
  }, HEALTH_MS);
  connect();
}
main().catch(async err => {
  log("fatal", { error: safeError(err) });
  await pool.end();
  process.exitCode = 1;
});
