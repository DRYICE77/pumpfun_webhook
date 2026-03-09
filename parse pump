require("dotenv").config();

const http = require("http");
const WebSocket = require("ws");
const { Pool } = require("pg");

const HELIUS_API_KEY = process.env.HELIUS_API_KEY;
const DATABASE_URL = process.env.DATABASE_URL;
const PORT = Number(process.env.PORT || 8080);

const CHAIN = "solana";
const STORE_RAW_EVENTS = String(process.env.STORE_RAW_EVENTS || "true") === "true";
const RAW_RETENTION_COUNT = Number(process.env.RAW_RETENTION_COUNT || 5000);

const PUMP_AMM_PROGRAM_ID =
  process.env.PUMP_AMM_PROGRAM_ID ||
  "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";

const WSS_URL = `wss://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;
const RPC_URL = `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;

if (!HELIUS_API_KEY) {
  console.error("Missing HELIUS_API_KEY");
  process.exit(1);
}

if (!DATABASE_URL) {
  console.error("Missing DATABASE_URL");
  process.exit(1);
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

let ws = null;
let pingInterval = null;
let reconnectTimeout = null;
let retryCount = 0;

const seenSignatures = new Set();
const SEEN_SIGNATURE_LIMIT = 20000;

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

function backoffDelay(attempt) {
  return Math.min(1000 * 2 ** attempt, 30000);
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
  return heliusRpc("getTransaction", [
    signature,
    {
      encoding: "jsonParsed",
      maxSupportedTransactionVersion: 0,
      commitment: "confirmed",
    },
  ]);
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

function txTouchesPumpAmm(tx) {
  const logs = getLogMessages(tx);
  if (logs.some((l) => l.includes(PUMP_AMM_PROGRAM_ID))) return true;

  const keys = getAccountKeys(tx);
  if (keys.includes(PUMP_AMM_PROGRAM_ID)) return true;

  const instructions = tx?.transaction?.message?.instructions || [];
  for (const ix of instructions) {
    if (ix.programId === PUMP_AMM_PROGRAM_ID) return true;
  }

  const inner = tx?.meta?.innerInstructions || [];
  for (const group of inner) {
    for (const ix of group.instructions || []) {
      if (ix.programId === PUMP_AMM_PROGRAM_ID) return true;
    }
  }

  return false;
}

function getSideFromLogs(tx) {
  const logs = getLogMessages(tx);

  if (logs.some((l) => l.includes("Instruction: Buy"))) return "buy";
  if (logs.some((l) => l.includes("Instruction: Sell"))) return "sell";

  return null;
}

function getSignerWallet(tx) {
  const keys = tx?.transaction?.message?.accountKeys || [];
  for (const key of keys) {
    if (typeof key === "string") continue;
    if (key.signer) return key.pubkey;
  }
  return null;
}

function getWalletSolDelta(tx, walletAddress) {
  const keys = tx?.transaction?.message?.accountKeys || [];
  const preBalances = tx?.meta?.preBalances || [];
  const postBalances = tx?.meta?.postBalances || [];

  for (let i = 0; i < keys.length; i++) {
    const pubkey = typeof keys[i] === "string" ? keys[i] : keys[i].pubkey;
    if (pubkey !== walletAddress) continue;

    const pre = preBalances[i] || 0;
    const post = postBalances[i] || 0;
    return (post - pre) / 1e9;
  }

  return 0;
}

function parseTokenBalances(arr) {
  return (arr || []).map((row) => {
    const owner = row.owner || null;
    const mint = row.mint || null;
    const accountIndex = row.accountIndex;
    const decimals = row.uiTokenAmount?.decimals ?? 0;
    const rawAmount = row.uiTokenAmount?.amount || "0";
    const amount = Number(rawAmount) / 10 ** decimals;

    return {
      owner,
      mint,
      accountIndex,
      decimals,
      amount,
    };
  });
}

function buildTokenMap(rows) {
  const map = new Map();
  for (const row of rows) {
    const key = `${row.owner}::${row.mint}::${row.accountIndex}`;
    map.set(key, row);
  }
  return map;
}

function findWalletTokenDelta(tx, walletAddress) {
  const preRows = parseTokenBalances(tx?.meta?.preTokenBalances || []);
  const postRows = parseTokenBalances(tx?.meta?.postTokenBalances || []);

  const preMap = buildTokenMap(preRows);
  const postMap = buildTokenMap(postRows);

  const keys = new Set([...preMap.keys(), ...postMap.keys()]);
  let best = null;

  for (const key of keys) {
    const pre = preMap.get(key);
    const post = postMap.get(key);

    const owner = post?.owner || pre?.owner || null;
    const mint = post?.mint || pre?.mint || null;
    const preAmt = pre?.amount || 0;
    const postAmt = post?.amount || 0;
    const delta = postAmt - preAmt;

    if (!owner || !mint) continue;
    if (owner !== walletAddress) continue;
    if (Math.abs(delta) === 0) continue;

    if (!best || Math.abs(delta) > Math.abs(best.delta)) {
      best = { mint, delta };
    }
  }

  return best;
}

function getFeeSol(tx) {
  return (tx?.meta?.fee || 0) / 1e9;
}

function getTs(tx) {
  return tx?.blockTime ? new Date(tx.blockTime * 1000) : new Date();
}

function parsePumpTrade(tx, signature) {
  if (!tx || !tx.meta || !tx.transaction) {
    return { ok: false, reason: "missing tx fields" };
  }

  if (tx.meta.err) {
    return { ok: false, reason: "tx failed" };
  }

  if (!txTouchesPumpAmm(tx)) {
    return { ok: false, reason: "not pump amm" };
  }

  const sideFromLogs = getSideFromLogs(tx);
  if (!sideFromLogs) {
    return { ok: false, reason: "no buy/sell log" };
  }

  const walletAddress = getSignerWallet(tx);
  if (!walletAddress) {
    return { ok: false, reason: "no signer wallet" };
  }

  const tokenDelta = findWalletTokenDelta(tx, walletAddress);
  if (!tokenDelta) {
    return { ok: false, reason: "no wallet token delta" };
  }

  const walletSolDelta = getWalletSolDelta(tx, walletAddress);
  const solAmount = Math.abs(walletSolDelta);
  const tokenAmount = Math.abs(tokenDelta.delta);

  if (solAmount <= 0) {
    return { ok: false, reason: "zero sol amount" };
  }

  if (tokenAmount <= 0) {
    return { ok: false, reason: "zero token amount" };
  }

  const inferredSide =
    tokenDelta.delta > 0 && walletSolDelta < 0
      ? "buy"
      : tokenDelta.delta < 0 && walletSolDelta > 0
      ? "sell"
      : null;

  if (!inferredSide) {
    return { ok: false, reason: "could not infer side" };
  }

  if (sideFromLogs !== inferredSide) {
    return {
      ok: false,
      reason: `log side ${sideFromLogs} != inferred side ${inferredSide}`,
    };
  }

  const ts = getTs(tx);

  return {
    ok: true,
    trade: {
      wallet_address: walletAddress,
      token_id: tokenDelta.mint,
      side: inferredSide,
      sol_amount: solAmount,
      token_amount: tokenAmount,
      fee_sol: getFeeSol(tx),
      bonding_progress: 0,
      sol_in_curve: 0,
      ts,
      signature,
      slot: tx.slot || null,
      raw: null,
    },
  };
}

async function createTables() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS raw_webhook_events (
      id BIGSERIAL PRIMARY KEY,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      signature TEXT,
      slot BIGINT,
      timestamp BIGINT,
      type TEXT,
      payload JSONB
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS wallet_trades (
      id BIGSERIAL PRIMARY KEY,
      wallet_address TEXT NOT NULL,
      token_id TEXT NOT NULL,
      side TEXT NOT NULL,
      sol_amount NUMERIC NOT NULL,
      token_amount NUMERIC NOT NULL,
      fee_sol NUMERIC NOT NULL DEFAULT 0,
      bonding_progress NUMERIC NOT NULL DEFAULT 0,
      sol_in_curve NUMERIC NOT NULL DEFAULT 0,
      ts TIMESTAMPTZ NOT NULL,
      signature TEXT NOT NULL,
      slot BIGINT,
      raw JSONB,
      chain TEXT NOT NULL DEFAULT 'solana'
    );
  `);

  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS raw_webhook_events_signature_idx
    ON raw_webhook_events (signature);
  `);

  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS wallet_trades_signature_wallet_side_idx
    ON wallet_trades (signature, wallet_address, side);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS wallet_trades_ts_idx
    ON wallet_trades (ts);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS wallet_trades_token_ts_idx
    ON wallet_trades (token_id, ts);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS wallet_trades_token_side_ts_idx
    ON wallet_trades (token_id, side, ts);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS wallet_trades_wallet_idx
    ON wallet_trades (wallet_address);
  `);
}

async function insertRawWebhookEvent({ signature, slot, timestamp, type, payload }) {
  if (!STORE_RAW_EVENTS) return;

  await pool.query(
    `
    INSERT INTO raw_webhook_events (signature, slot, timestamp, type, payload)
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
    DELETE FROM raw_webhook_events
    WHERE id IN (
      SELECT id
      FROM raw_webhook_events
      ORDER BY created_at DESC
      OFFSET $1
    )
    `,
    [RAW_RETENTION_COUNT]
  );
}

async function insertWalletTrade(trade) {
  await pool.query(
    `
    INSERT INTO wallet_trades (
      wallet_address,
      token_id,
      side,
      sol_amount,
      token_amount,
      fee_sol,
      bonding_progress,
      sol_in_curve,
      ts,
      signature,
      slot,
      raw,
      chain
    )
    VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13
    )
    ON CONFLICT (signature, wallet_address, side) DO NOTHING
    `,
    [
      trade.wallet_address,
      trade.token_id,
      trade.side,
      trade.sol_amount,
      trade.token_amount,
      trade.fee_sol,
      trade.bonding_progress,
      trade.sol_in_curve,
      trade.ts,
      trade.signature,
      trade.slot,
      trade.raw,
      CHAIN,
    ]
  );
}

async function processSignature(signature, fallbackSlot = null, fallbackBlockTime = null) {
  if (!signature || alreadySeen(signature)) return;
  addSeenSignature(signature);

  try {
    const tx = await fetchFullTransaction(signature);
    if (!tx) {
      console.log(`No tx yet for ${signature}`);
      return;
    }

    await insertRawWebhookEvent({
      signature,
      slot: tx.slot || fallbackSlot,
      timestamp: tx.blockTime || fallbackBlockTime,
      type: "helius_ws_tx",
      payload: tx,
    });

    const parsed = parsePumpTrade(tx, signature);
    if (!parsed.ok) {
      console.log(`Skipped ${signature}: ${parsed.reason}`);
      return;
    }

    await insertWalletTrade(parsed.trade);

    console.log(
      `Inserted ${parsed.trade.side} | wallet=${parsed.trade.wallet_address} | token=${parsed.trade.token_id} | sol=${parsed.trade.sol_amount.toFixed(
        4
      )}`
    );
  } catch (err) {
    console.error(`Failed processing ${signature}:`, err.message);
  }
}

function subscribe() {
  const request = {
    jsonrpc: "2.0",
    id: 1,
    method: "logsSubscribe",
    params: [
      {
        mentions: [PUMP_AMM_PROGRAM_ID],
      },
      {
        commitment: "confirmed",
      },
    ],
  };

  ws.send(JSON.stringify(request));
  console.log("Sent logsSubscribe");
}

function startPing() {
  stopPing();
  pingInterval = setInterval(() => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.ping();
    }
  }, 30000);
}

function stopPing() {
  if (pingInterval) {
    clearInterval(pingInterval);
    pingInterval = null;
  }
}

function reconnect() {
  if (reconnectTimeout) return;

  const delay = backoffDelay(retryCount);
  console.log(`Reconnecting in ${delay}ms`);

  reconnectTimeout = setTimeout(() => {
    reconnectTimeout = null;
    retryCount += 1;
    connect();
  }, delay);
}

function connect() {
  console.log(`Connecting to ${WSS_URL}`);
  ws = new WebSocket(WSS_URL);

  ws.on("open", () => {
    console.log("WebSocket opened");
    retryCount = 0;
    subscribe();
    startPing();
  });

  ws.on("message", async (data) => {
    try {
      const msg = JSON.parse(data.toString());

      if (typeof msg.result === "number" && msg.id === 1) {
        console.log(`Subscribed successfully: ${msg.result}`);
        return;
      }

      const value = msg?.params?.result?.value;
      if (!value) return;
      if (value.err) return;

      await processSignature(
        value.signature,
        msg?.params?.result?.context?.slot || null,
        value?.blockTime || null
      );
    } catch (err) {
      console.error("WS message parse error:", err.message);
    }
  });

  ws.on("error", (err) => {
    console.error("WebSocket error:", err.message);
  });

  ws.on("close", () => {
    console.log("WebSocket closed");
    stopPing();
    reconnect();
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
          })
        );
      } catch (err) {
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: false, error: err.message }));
      }
      return;
    }

    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("pumpfun scanner running");
  })
  .listen(PORT, () => {
    console.log(`HTTP server listening on ${PORT}`);
  });

async function boot() {
  try {
    const test = await pool.query("SELECT now()");
    console.log("DB connected:", test.rows[0].now);

    await createTables();
    console.log("Tables ready");

    if (STORE_RAW_EVENTS) {
      setInterval(() => {
        trimRawEvents().catch((err) =>
          console.error("Raw trim error:", err.message)
        );
      }, 5 * 60 * 1000);
    }

    connect();
  } catch (err) {
    console.error("Boot failed:", err.message);
    process.exit(1);
  }
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

async function shutdown() {
  console.log("Shutting down...");
  stopPing();

  if (reconnectTimeout) {
    clearTimeout(reconnectTimeout);
    reconnectTimeout = null;
  }

  if (ws) {
    try {
      ws.close();
    } catch (_) {}
  }

  try {
    await pool.end();
  } catch (_) {}

  process.exit(0);
}

boot();
