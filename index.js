import express from "express";
import pkg from "pg";

const { Pool } = pkg;

const app = express();
app.use(express.json({ limit: "2mb" }));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

// --------------------
// Helpers
// --------------------
function requireHeliusAuth(req, res) {
  const expected = process.env.HELIUS_WEBHOOK_SECRET;
  const auth = req.headers.authorization || "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7) : null;

  if (!expected || token !== expected) {
    res.status(401).send("unauthorized");
    return false;
  }
  return true;
}

function toISOFromUnixSeconds(ts) {
  if (!ts) return null;
  // Helius enhanced uses seconds
  const n = Number(ts);
  if (!Number.isFinite(n)) return null;
  return new Date(n * 1000).toISOString();
}

function safeInt(v) {
  const n = Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : null;
}

function safeBigIntString(v) {
  // For huge integers (raw token amounts), store as TEXT in DB if you need it later.
  if (v === null || v === undefined) return null;
  if (typeof v === "bigint") return v.toString();
  if (typeof v === "number") return Number.isFinite(v) ? Math.trunc(v).toString() : null;
  if (typeof v === "string") return v;
  return null;
}

function normalizeMint(mint) {
  return typeof mint === "string" ? mint.trim() : null;
}

function isLikelyPumpFunMint(mint) {
  // Many pump.fun token mints end with "pump". This is a heuristic; keep it loose.
  return typeof mint === "string" && mint.endsWith("pump");
}

// Upsert token by token_address and return token_id
async function upsertTokenAndGetId({
  token_address,
  chain = "solana",
  source = "pumpfun",
  launch_time = null,
  name = null,
  symbol = null,
  creator_wallet = null,
}) {
  if (!token_address) return null;

  // IMPORTANT:
  // This assumes your Tokens table is named exactly "Tokens"
  // and has columns: token_address, chain, source, launch_time, name, symbol, creator_wallet
  // and token_address has a UNIQUE constraint.
  //
  // If your table name is lowercase "tokens", change "Tokens" to "tokens" below.

  const q = `
    INSERT INTO "Tokens" (token_address, chain, source, launch_time, name, symbol, creator_wallet)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (token_address)
    DO UPDATE SET
      chain = COALESCE(EXCLUDED.chain, "Tokens".chain),
      source = COALESCE(EXCLUDED.source, "Tokens".source),
      launch_time = COALESCE(EXCLUDED.launch_time, "Tokens".launch_time),
      name = COALESCE(EXCLUDED.name, "Tokens".name),
      symbol = COALESCE(EXCLUDED.symbol, "Tokens".symbol),
      creator_wallet = COALESCE(EXCLUDED.creator_wallet, "Tokens".creator_wallet)
    RETURNING id
  `;

  const params = [token_address, chain, source, launch_time, name, symbol, creator_wallet];
  const { rows } = await pool.query(q, params);
  return rows?.[0]?.id ?? null;
}

// --------------------
// Health routes
// --------------------
app.get("/", (req, res) => res.status(200).send("ok"));
app.post("/", (req, res) => res.status(200).send("ok"));
app.get("/health", (req, res) => res.status(200).send("server alive"));

// --------------------
// Webhook
// --------------------
app.post("/webhooks/helius", async (req, res) => {
  try {
    if (!requireHeliusAuth(req, res)) return;

    // Helius enhanced webhooks usually send an array
    const events = Array.isArray(req.body) ? req.body : [req.body];
    console.log(`Received ${events.length} Helius event(s)`);

    // 1) Persist raw events (source of truth)
    for (const e of events) {
      const signature = e?.signature;
      if (!signature) continue;

      // NOTE:
      // This assumes raw_webhook_events has:
      // signature TEXT UNIQUE, timestamp BIGINT, slot BIGINT, type TEXT, payload JSONB, created_at timestamptz
      await pool.query(
        `
        INSERT INTO raw_webhook_events
          (signature, timestamp, slot, type, payload, created_at)
        VALUES
          ($1, $2, $3, $4, $5, NOW())
        ON CONFLICT (signature) DO NOTHING
        `,
        [
          signature,
          e?.timestamp ?? null, // seconds
          e?.slot ?? null,
          e?.type ?? null,
          e, // JSONB column recommended
        ]
      );
    }

    // 2) Parse SWAPs for pump.fun-ish tokens and write wallet_trades
    //
    // IMPORTANT: This writes to wallet_trades using token_id (NOT token_address),
    // which avoids your schema error.
    //
    // Assumptions about wallet_trades columns (based on your screenshot):
    // - signature (text)
    // - slot (bigint/int8 recommended)
    // - block_time (timestamptz)
    // - wallet_address (text)
    // - token_id (int)  <-- required
    // - side (text)     e.g. "buy" | "sell"
    // - sol_amount (numeric)   <-- store SOL as NUMERIC
    // - token_amount (numeric) <-- store token units as NUMERIC
    // - fee_sol (numeric)      <-- store SOL fee as NUMERIC
    // - source (text)
    // - chain (text)
    //
    // If your wallet_trades has different names, tweak the INSERT below.

    for (const e of events) {
      if (e?.type !== "SWAP") continue;

      const signature = e?.signature;
      const slot = e?.slot ?? null;
      const blockTime = toISOFromUnixSeconds(e?.timestamp);
      const wallet = e?.feePayer ?? null; // usually the user wallet
      const chain = "solana";
      const source = e?.source ?? "helius";

      // Pull token transfers and find a likely pump.fun mint
      const tokenTransfers = Array.isArray(e?.tokenTransfers) ? e.tokenTransfers : [];
      const candidate = tokenTransfers.find((t) => isLikelyPumpFunMint(t?.mint));

      if (!candidate) continue;

      const mint = normalizeMint(candidate.mint);
      if (!mint) continue;

      // Determine if buy or sell relative to feePayer
      // In your sample, feePayer received the pump token => buy
      const toUser = candidate.toUserAccount ?? null;
      const fromUser = candidate.fromUserAccount ?? null;

      let side = null;
      if (wallet && toUser === wallet) side = "buy";
      else if (wallet && fromUser === wallet) side = "sell";
      else side = "unknown";

      // Amounts
      const tokenAmount = Number(candidate.tokenAmount ?? 0) || 0;

      // SOL moved: try to infer via nativeTransfers involving feePayer
      // This is heuristic; you can refine later.
      const nativeTransfers = Array.isArray(e?.nativeTransfers) ? e.nativeTransfers : [];
      let solNet = 0;
      for (const nt of nativeTransfers) {
        const amtLamports = Number(nt?.amount ?? 0) || 0;
        const from = nt?.fromUserAccount ?? null;
        const to = nt?.toUserAccount ?? null;
        if (!wallet) continue;

        if (from === wallet) solNet -= amtLamports;
        if (to === wallet) solNet += amtLamports;
      }
      const solAmount = solNet / 1e9;

      // fee is in lamports
      const feeSol = (Number(e?.fee ?? 0) || 0) / 1e9;

      // Upsert token and get token_id
      const tokenId = await upsertTokenAndGetId({
        token_address: mint,
        chain,
        source: "pumpfun",
        launch_time: null,
      });

      if (!tokenId) continue;

      // Insert wallet_trades
      // Ensure wallet_trades.signature UNIQUE (or change ON CONFLICT target)
      await pool.query(
        `
        INSERT INTO wallet_trades
          (signature, slot, block_time, wallet_address, token_id, side, sol_amount, token_amount, fee_sol, source, chain, raw)
        VALUES
          ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT (signature) DO NOTHING
        `,
        [
          signature,
          slot,
          blockTime,
          wallet,
          tokenId,
          side,
          solAmount,
          tokenAmount,
          feeSol,
          source,
          chain,
          e, // store raw on the trade row too (optional)
        ]
      );
    }

    return res.status(200).json({ success: true });
  } catch (err) {
    console.error("Webhook error:", err);
    return res.status(500).send("server error");
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("Server running on port", PORT));
