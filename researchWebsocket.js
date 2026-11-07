"use stricTt";

// North Star Research WebSocket V1 — PumpSwap-only pilot.
// Discovers signatures; the separate researchCollector.js worker fetches raw transactions.
// WebSocket delivery is NOT replayable. Disconnects/DB failures require later reconciliation.
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
const MAX_PENDING = Math.max(10, Number(process.env.RESEARCH_WS_MAX_PENDING || 1000));
const RETRY_MS = Math.max(1000, Number(process.env.RESEARCH_WS_DB_RETRY_MS || 3000));
const RECONNECT_MS = Math.max(1000, Number(process.env.RESEARCH_WS_RECONNECT_MS || 5000));
const HEARTBEAT_MS = 30000;
const HEALTH_MS = 30000;

if (!/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(RUN_ID)) {
  throw new Error("Invalid NORTHSTAR_RESEARCH_RUN_ID");
}
if (!DATABASE_URL || !WSS_URL) throw new Error("Set DATABASE_URL and HELIUS_API_KEY (or HELIUS_WSS_URL)");
if (!/^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(PROGRAM_ID)) throw new Error("Invalid program ID");

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: process.env.PG_SSL === "false" ? false : { rejectUnauthorized: false },
  max: 3,
  connectionTimeoutMillis: 10000,
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
let draining = false;
let queue = [];
let seen = 0;
let persisted = 0;
let duplicateNotifications = 0;
let dbFailures = 0;
let lastSlot = null;
let connectionStartedAt = null;
let lastDisconnectAt = null;
let connectionNumber = 0;

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
      scheduleReconnect(); // Don't reopen intake while DB backlog remains.
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

async function drain() {
  if (draining) return;
  draining = true;
  try {
    while (queue.length) {
      const item = queue[0];
      try {
        await pool.query(`
          INSERT INTO public.northstar_research_signatures
            (signature, first_run_id, first_observed_at, last_observed_at, observed_slot, status)
          VALUES ($1, $2, $3, $3, $4, 'pending')
          ON CONFLICT (signature) DO UPDATE SET
            last_observed_at = EXCLUDED.last_observed_at,
            observed_slot = COALESCE(public.northstar_research_signatures.observed_slot, EXCLUDED.observed_slot),
            updated_at = NOW()
        `, [item.signature, RUN_ID, item.observedAt, item.slot]);
        persisted++;
        queue.shift();
      } catch (err) {
        dbFailures++;
        log("db_persist_failed", { error: safeError(err), pending: queue.length });
        closeIntake("database_persistence_failure");
        await sleep(RETRY_MS);
      }
    }
  } finally {
    draining = false;
  }
}

function handleMessage(raw) {
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

  // Only dedupe while awaiting persistence; database UNIQUE constraint handles all others.
  if (queue.some(item => item.signature === signature)) {
    duplicateNotifications++;
    return;
  }
  if (queue.length >= MAX_PENDING) {
    // The triggering signature cannot be safely retained. Explicitly record a coverage gap.
    log("coverage_gap_queue_overflow", { signature, pending: queue.length, slot, lastSlot });
    closeIntake("queue_overflow");
    return;
  }
  queue.push({ signature, slot: Number.isSafeInteger(slot) ? slot : null, observedAt: new Date() });
  void drain();
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
  ws.on("message", handleMessage);
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
  if (socket) socket.terminate();
  // Railway may force-terminate us; log any unpersisted in-memory notifications.
  const deadline = Date.now() + 15000;
  while ((draining || queue.length) && Date.now() < deadline) await sleep(100);
  if (queue.length) log("coverage_gap_shutdown_unpersisted", { count: queue.length, lastSlot });
  await pool.end();
  process.exit(queue.length ? 1 : 0);
}
process.on("SIGTERM", () => { void shutdown("SIGTERM"); });
process.on("SIGINT", () => { void shutdown("SIGINT"); });

async function main() {
  await checkRun();
  log("research_ws_starting", { program: PROGRAM_ID, maxPending: MAX_PENDING });
  healthTimer = setInterval(() => {
    log("research_ws_health", {
      connected: socket?.readyState === WebSocket.OPEN,
      subscribed: subscriptionId != null,
      seen, persisted, duplicateNotifications, dbFailures,
      pending: queue.length, lastSlot,
    });
  }, HEALTH_MS);
  connect();
}
main().catch(async err => {
  log("fatal", { error: safeError(err) });
  await pool.end();
  process.exitCode = 1;
});
