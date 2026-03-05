import express from "express";
import pkg from "pg";

const { Pool } = pkg;

const app = express();
app.use(express.json({ limit: "2mb" }));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  // Railway Postgres commonly requires SSL. If yours doesn't, set PGSSLMODE=disable.
  ssl: process.env.PGSSLMODE === "disable" ? false : { rejectUnauthorized: false },
});

// ----------------------------
// Helpers
// ----------------------------
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

function normalizeEpochMs(ts) {
  if (ts == null) return null;
  const n = Number(ts);
  if (!Number.isFinite(n)) return null;
  // if it's already in ms (>= ~2001-09-09 in ms), keep it
  if (n >= 1e12) return Math.trunc(n);
  // otherwise assume seconds -> ms
  return Math.trunc(n * 1000);
}

async function tableExists(tableName) {
  const { rows } = await pool.query(
    `
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = $1
    LIMIT 1
    `,
    [tableName]
  );
  return rows.length > 0;
}

async function quotedCaseTableExists(exactName) {
  // If your table was created as "Tokens" (quoted), information_schema stores it exactly as Tokens
  const { rows } = await pool.query(
    `
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = $1
    LIMIT 1
    `,
    [exactName]
  );
  return rows.length > 0;
}

async function ensureRawWebhookEventsConstraint() {
  // Your INSERT uses ON CONFLICT(signature) which requires a UNIQUE constraint/index on signature.
  // If you already have it, this is harmless; if not, it avoids future errors.
  try {
    await pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS raw_webhook_events_signature_uidx ON raw_webhook_events(signature)`
    );
  } catch (e) {
    // If permissions/UI-managed schema, ignore.
    console.log("Could not ensure unique index on raw_webhook_events.signature (ok):", e?.message);
  }
}

function extractTokenCandidatesFromEvent(e) {
  // Grab any non-SOL mints seen in tokenTransfers
  const transfers = Array.isArray(e?.tokenTransfers) ? e.tokenTransfers : [];
  const mints = new Set();
  for (const t of transfers) {
    const mint = t?.mint;
    if (!mint) continue;
    if (mint === "So11111111111111111111111111111111111111112") continue;
    mints.add(mint);
  }
  return [...mints];
}

async function upsertTokenAndGetId({
  tokenAddress,
  chain = "solana",
  source = "pumpfun",
  launchTimeMs,
  name = null,
  symbol = null,
  creatorWallet = null,
}) {
  if (!tokenAddress) return null;

  // IMPORTANT: Your table is named "Tokens" (capital T), so we must quote it.
  // If you later rename to lowercase tokens, update this.
  const tokensTableName = (await quotedCaseTableExists("Tokens")) ? `"Tokens"` : "tokens";

  // If neither exists, skip
  const exists =
    tokensTableName === `"Tokens"` ? await quotedCaseTableExists("Tokens") : await tableExists("tokens");
  if (!exists) {
    console.log("Tokens table not found (neither Tokens nor tokens). Skipping token upsert.");
    return null;
  }

  // Ensure token_address unique is ideal. If you don’t have it, ON CONFLICT will fail.
  // You can add it in your DB UI: unique index on token_address.
  const launchTime = launchTimeMs ? new Date(launchTimeMs) : null;

  try {
    const { rows } = await pool.query(
      `
      INSERT INTO ${tokensTableName}
        (token_address, name, symbol, launch_time, source, creator_wallet, chain)
      VALUES
        ($1, $2, $3, $4, $5, $6, $7)
      ON CONFLICT (token_address) DO UPDATE SET
        name = COALESCE(EXCLUDED.name, ${tokensTableName}.name),
        symbol = COALESCE(EXCLUDED.symbol, ${tokensTableName}.symbol),
        launch_time = COALESCE(EXCLUDED.launch_time, ${tokensTableName}.launch_time),
        source = COALESCE(EXCLUDED.source, ${tokensTableName}.source),
        creator_wallet = COALESCE(EXCLUDED.creator_wallet, ${tokensTableName}.creator_wallet),
        chain = COALESCE(EXCLUDED.chain, ${tokensTableName}.chain)
      RETURNING id
      `,
      [tokenAddress, name, symbol, launchTime, source, creatorWallet, chain]
    );
    return rows?.[0]?.id ?? null;
  } catch (err) {
    // If ON CONFLICT fails due to missing unique constraint, fall back to SELECT/INSERT naive
    console.log("Token upsert failed (likely missing unique constraint). Falling back. Error:", err?.message);

    // Try select
    try {
      const { rows: sel } = await pool.query(
        `SELECT id FROM ${tokensTableName} WHERE token_address = $1 LIMIT 1`,
        [tokenAddress]
      );
      if (sel.length) return sel[0].id;
    } catch {}

    // Try insert without conflict handling
    try {
      const { rows: ins } = await pool.query(
        `
        INSERT INTO ${tokensTableName}
          (token_address, name, symbol, launch_time, source, creator_wallet, chain)
        VALUES
          ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id
        `,
        [tokenAddress, name, symbol, launchTime, source, creatorWallet, chain]
      );
      return ins?.[0]?.id ?? null;
    } catch (e2) {
      console.log("Token insert fallback failed:", e2?.message);
      return null;
    }
  }
}

// ----------------------------
// Health routes
// ----------------------------
app.get("/", (req, res) => res.status(200).send("ok"));
app.post("/", (req, res) => res.status(200).send("ok"));
app.get("/health", (req, res) => res.status(200).send("server alive"));

// ----------------------------
// Webhook
// ----------------------------
app.post("/webhooks/helius", async (req, res) => {
  try {
    if (!requireHeliusAuth(req, res)) return;

    const events = Array.isArray(req.body) ? req.body : [req.body];
    console.log(`Received ${events.length} Helius event(s)`);

    await ensureRawWebhookEventsConstraint();

    for (const e of events) {
      const signature = e?.signature;
      if (!signature) continue;

      const slot = e?.slot ?? null;
      const type = e?.type ?? null;
      const tsMs = normalizeEpochMs(e?.timestamp);

      // 1) Store raw payload (source of truth)
      await pool.query(
        `
        INSERT INTO raw_webhook_events
          (signature, "timestamp", slot, type, payload, created_at)
        VALUES
          ($1, $2, $3, $4, $5, NOW())
        ON CONFLICT (signature) DO NOTHING
        `,
        [
          signature,
          // If your raw_webhook_events.timestamp is INT4, this must be seconds not ms.
          // BUT you already fixed overflow issues; ideally set it to BIGINT.
          // Here we store seconds to be safe if column is int4.
          tsMs ? Math.floor(tsMs / 1000) : null,
          slot,
          type,
          e,
        ]
      );

      // 2) Token upsert: capture any non-SOL mints touched by this event
      const mints = extractTokenCandidatesFromEvent(e);

      for (const mint of mints) {
        await upsertTokenAndGetId({
          tokenAddress: mint,
          chain: "solana",
          source: "pumpfun",
          launchTimeMs: tsMs,
          // if Helius provides token metadata later, you can fill name/symbol
          name: null,
          symbol: null,
          creatorWallet: null,
        });
      }

      // 3) (Optional) wallet_trades parsing goes here later
      // Keep it off until schema is 100% aligned to avoid breaking ingestion.
    }

    return res.status(200).json({ success: true });
  } catch (err) {
    console.error("Webhook error:", err);
    return res.status(500).send("server error");
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("Server running on port", PORT));
