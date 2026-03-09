require("dotenv").config();

const http = require("http");
const WebSocket = require("ws");
const { Pool } = require("pg");

const HELIUS_API_KEY = process.env.HELIUS_API_KEY;
const DATABASE_URL = process.env.DATABASE_URL;
const PORT = process.env.PORT || 8080;
const CHAIN = process.env.CHAIN || "solana";
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
  ssl:
    DATABASE_URL.includes("localhost") || DATABASE_URL.includes("127.0.0.1")
      ? false
      : { rejectUnauthorized: false },
});

let ws = null;
let pingInterval = null;
let reconnectTimeout = null;
let retryCount = 0;

const seenSignatures = new Set();
const SEEN_SIGNATURE_LIMIT = 10000;

function addSeenSignature(sig) {
  seenSignatures.add(sig);
  if (seenSignatures.size > SEEN_SIGNATURE_LIMIT) {
    const first = seenSignatures.values().next().value;
    seenSignatures.delete(first);
  }
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

function extractAccountKeys(tx) {
  return (
    tx?.transaction?.message?.accountKeys?.map((k) =>
      typeof k === "string" ? k : k.pubkey
    ) || []
  );
}

function txTouchesPumpAmm(tx) {
  const accountKeys = extractAccountKeys(tx);
  if (accountKeys.includes(PUMP_AMM_PROGRAM_ID)) return true;

  const instructions = tx?.transaction?.message?.instructions || [];
  for (const ix of instructions) {
    if (ix.programId === PUMP_AMM_PROGRAM_ID) return true;
    if (ix.program === "unknown" && ix.programId === PUMP_AMM_PROGRAM_ID) {
      return true;
    }
  }

  const inner = tx?.meta?.innerInstructions || [];
  for (const group of inner) {
    for (const ix of group.instructions || []) {
      if (ix.programId === PUMP_AMM_PROGRAM_ID) return true;
    }
  }

  const logs = tx?.meta?.logMessages || [];
  return logs.some((l) => l.includes(PUMP_AMM_PROGRAM_ID));
}

function getSignerWallet(tx) {
  const keys = tx?.transaction?.message?.accountKeys || [];
  for (const k of keys) {
    if (typeof k === "string") continue;
    if (k.signer) return k.pubkey;
  }
  return null;
}

function getFeeSol(tx) {
  const lamports = tx?.meta?.fee || 0;
  return lamports / 1e9;
}

function getBlockTimeTs(tx) {
  if (!tx?.blockTime) return null;
  return new Date(tx.blockTime * 1000);
}

function parseTokenBalanceMap(arr) {
  const out = new Map();

  for (const row of arr || []) {
    const owner = row.owner || null;
    const mint = row.mint || null;
    const accountIndex = row.accountIndex;
    const amountRaw = row.uiTokenAmount?.amount || "0";
    const decimals = row.uiTokenAmount?.decimals ?? 0;
    const amount = Number(amountRaw) / 10 ** decimals;

    const key = `${owner}::${mint}::${accountIndex}`;
    out.set(key, {
      owner,
      mint,
      accountIndex,
      amount,
      decimals,
    });
  }

  return out;
}

function findLargestOwnerTokenDelta(tx, walletAddress) {
  const pre = tx?.meta?.preTokenBalances || [];
  const post = tx?.meta?.postTokenBalances || [];

  const preMap = parseTokenBalanceMap(pre);
  const postMap = parseTokenBalanceMap(post);

  const keys = new Set([...preMap.keys(), ...postMap.keys()]);
  let best = null;

  for (const key of keys) {
    const preRow = preMap.get(key);
    const postRow = postMap.get(key);

    const owner = postRow?.owner || preRow?.owner || null;
    const mint = postRow?.mint || preRow?.mint || null;
    const decimals = postRow?.decimals ?? preRow?.decimals ?? 0;
    const preAmt = preRow?.amount || 0;
    const postAmt = postRow?.amount || 0;
    const delta = postAmt - preAmt;

    if (!owner || !mint) continue;
    if (owner !== walletAddress) continue;
    if (Math.abs(delta) <= 0) continue;

    if (!best || Math.abs(delta) > Math.abs(best.delta)) {
      best = {
        mint,
        delta,
        decimals,
      };
    }
  }

  return best;
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

function parsePumpTrade(tx, signature) {
  if (!tx || !tx.meta || !tx.transaction) return null;
  if (tx.meta.err) return null;
  if (!txTouchesPumpAmm(tx)) return null;

  const walletAddress = getSignerWallet(tx);
  if (!walletAddress) return null;

  const tokenDelta = findLargestOwnerTokenDelta(tx, walletAddress);
  if (!tokenDelta) return null;

  const walletSolDelta = getWalletSolDelta(tx, walletAddress);

  let side = null;
  if (tokenDelta.delta > 0 && walletSolDelta < 0) side = "buy";
  if (tokenDelta.delta < 0 && walletSolDelta > 0) side = "sell";
  if (!side) return null;

  const solAmount = Math.abs(walletSolDelta);
  const tokenAmount = Math.abs(tokenDelta.delta);

  if (solAmount <= 0 || tokenAmount <= 0) return null;

  const priceSol = solAmount / tokenAmount;

  return {
    chain: CHAIN,
    wallet_address: walletAddress,
    token_address: tokenDelta.mint,
    side,
    sol_amount: solAmount,
    token_amount: tokenAmount,
    fee_sol: getFeeSol(tx),
    price_sol: priceSol,
    block_time: getBlockTimeTs(tx),
    ts: getBlockTimeTs(tx),
    signature,
    slot: tx.slot || null,
    raw: JSON.stringify(tx),
  };
}

async function insertRawWebhookEvent({
  signature,
  slot,
  timestamp,
  type,
  payload,
}) {
  const sql = `
    INSERT INTO raw_webhook_events (signature, slot, timestamp, type, payload)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT DO NOTHING
  `;

  await pool.query(sql, [
    signature,
    slot,
    timestamp,
    type,
    payload,
  ]);
}

async function getOrCreateTokenId(client, tokenAddress) {
  const existing = await client.query(
    `SELECT id FROM tokens WHERE token_address = $1 LIMIT 1`,
    [tokenAddress]
  );

  if (existing.rows.length) return existing.rows[0].id;

  const inserted = await client.query(
    `
    INSERT INTO tokens (chain, token_address, source)
    VALUES ($1, $2, $3)
    RETURNING id
    `,
    [CHAIN, tokenAddress, "helius_ws"]
  );

  return inserted.rows[0].id;
}

async function insertWalletTrade(trade) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const tokenId = await getOrCreateTokenId(client, trade.token_address);

    const sql = `
      INSERT INTO wallet_trades (
        block_time,
        bonding_progress,
        chain,
        fee_sol,
        minutes_from_launch,
        price_sol,
        raw,
        side,
        signature,
        slot,
        sol_amount,
        sol_in_curve,
        source,
        token_amount,
        token_id,
        ts,
        wallet_address
      )
      VALUES (
        $1,     -- block_time
        $2,     -- bonding_progress
        $3,     -- chain
        $4,     -- fee_sol
        $5,     -- minutes_from_launch
        $6,     -- price_sol
        $7,     -- raw
        $8,     -- side
        $9,     -- signature
        $10,    -- slot
        $11,    -- sol_amount
        $12,    -- sol_in_curve
        $13,    -- source
        $14,    -- token_amount
        $15,    -- token_id
        $16,    -- ts
        $17     -- wallet_address
      )
      ON CONFLICT DO NOTHING
    `;

    await client.query(sql, [
      trade.block_time,
      null,              // bonding_progress
      trade.chain,
      trade.fee_sol,
      null,              // minutes_from_launch
      trade.price_sol,
      trade.raw,
      trade.side,
      trade.signature,
      trade.slot,
      trade.sol_amount,
      null,              // sol_in_curve
      "helius_ws",
      trade.token_amount,
      tokenId,
      trade.ts,
      trade.wallet_address,
    ]);

    await client.query("COMMIT");
    return { ok: true, tokenId };
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

async function processSignature(signature, slotFromLog = null, blockTimeFromLog = null) {
  if (!signature || seenSignatures.has(signature)) return;
  addSeenSignature(signature);

  try {
    const tx = await fetchFullTransaction(signature);
    if (!tx) {
      console.log(`No tx returned yet for ${signature}`);
      return;
    }

    await insertRawWebhookEvent({
      signature,
      slot: tx.slot || slotFromLog,
      timestamp: tx.blockTime || blockTimeFromLog,
      type: "helius_ws_tx",
      payload: tx,
    });

    const trade = parsePumpTrade(tx, signature);
    if (!trade) {
      console.log(`Not a parseable pump trade: ${signature}`);
      return;
    }

    const result = await insertWalletTrade(trade);

    console.log(
      `Inserted trade ${trade.side} | wallet=${trade.wallet_address} | token=${trade.token_address} | sig=${signature} | tokenId=${result.tokenId}`
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
  console.log("Sent logsSubscribe request");
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
  console.log(`Reconnecting in ${delay}ms...`);

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

      const signature = value.signature;
      const err = value.err;
      if (err) return;

      await processSignature(
        signature,
        msg?.params?.result?.context?.slot || null,
        value?.blockTime || null
      );
    } catch (err) {
      console.error("Message parse error:", err.message);
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
        await pool.query("SELECT 1");
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(
          JSON.stringify({
            ok: true,
            db: true,
            websocketState: ws ? ws.readyState : null,
            retryCount,
          })
        );
      } catch (err) {
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(
          JSON.stringify({
            ok: false,
            db: false,
            error: err.message,
          })
        );
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
    await pool.query("SELECT 1");
    console.log("Database connected");
    connect();
  } catch (err) {
    console.error("Failed to connect to DB:", err.message);
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
