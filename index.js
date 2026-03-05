import express from "express";
import pkg from "pg";

const { Pool } = pkg;

const app = express();
app.use(express.json({ limit: "2mb" }));

// --------------------
// DB
// --------------------
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

// --------------------
// Auth
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

// --------------------
// Trade parsing helpers
// --------------------
function findAccountData(event, wallet) {
  return (event.accountData || []).find((a) => a.account === wallet) || null;
}

function findPumpTokenDeltaForWallet(event, wallet) {
  // Best source: accountData.tokenBalanceChanges (raw units + decimals)
  for (const ad of event.accountData || []) {
    for (const tbc of ad.tokenBalanceChanges || []) {
      if (
        tbc?.userAccount === wallet &&
        typeof tbc?.mint === "string" &&
        tbc.mint.endsWith("pump")
      ) {
        const rawStr = tbc?.rawTokenAmount?.tokenAmount;
        const decimals = tbc?.rawTokenAmount?.decimals ?? null;
        if (rawStr == null) continue;

        // rawStr can be negative, BigInt handles it
        const raw = BigInt(rawStr);
        return { mint: tbc.mint, raw, decimals };
      }
    }
  }

  // Fallback: tokenTransfers (human float) + direction
  const tt = (event.tokenTransfers || []).find(
    (x) =>
      typeof x?.mint === "string" &&
      x.mint.endsWith("pump") &&
      (x.toUserAccount === wallet || x.fromUserAccount === wallet)
  );

  if (tt) {
    return {
      mint: tt.mint,
      raw: null,
      decimals: null,
      tokenAmountHuman: tt.tokenAmount,
      to: tt.toUserAccount,
      from: tt.fromUserAccount,
    };
  }

  return null;
}

async function getOrCreateToken(pool, mint, tsSeconds) {
  // NOTE: your table name appears as "Tokens" in Retool (capitalized)
  const found = await pool.query(
    `SELECT id, launch_time FROM "Tokens" WHERE token_address = $1 LIMIT 1`,
    [mint]
  );
  if (found.rows.length) return found.rows[0];

  const inserted = await pool.query(
    `
    INSERT INTO "Tokens"
      (chain, creator_wallet, launch_time, name, symbol, source, token_address)
    VALUES
      ($1, $2, to_timestamp($3), $4, $5, $6, $7)
    RETURNING id, launch_time
    `,
    ["solana", null, tsSeconds, null, null, "pumpfun", mint]
  );

  return inserted.rows[0];
}

function safeAbsBigIntToString(bi) {
  // store as string if your DB columns are bigint/text-safe
  // if your columns are integer, you can Number() but it may overflow later
  const abs = bi < 0n ? -bi : bi;
  return abs.toString();
}

// --------------------
// Health
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

    const events = Array.isArray(req.body) ? req.body : [req.body];
    console.log(`Received ${events.length} Helius event(s)`);

    for (const e of events) {
      const signature = e?.signature;
      if (!signature) continue;

      // 1) Always persist raw (source of truth)
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
          e?.timestamp ?? null,
          e?.slot ?? null,
          e?.type ?? null,
          e, // if payload column is TEXT, change to JSON.stringify(e)
        ]
      );

      // 2) Parse pump.fun trades -> wallet_trades
      // Skip obvious non-trade noise quickly
      const typeUpper = String(e?.type || "").toUpperCase();
      const sourceUpper = String(e?.source || "").toUpperCase();
      if (typeUpper.includes("NFT") || typeUpper.includes("COMPRESSED")) continue;
      if (sourceUpper.includes("BUBBLEGUM")) continue;

      const wallet = e?.feePayer;
      const tsSeconds = e?.timestamp;
      if (!wallet || !tsSeconds) continue;

      const pumpDelta = findPumpTokenDeltaForWallet(e, wallet);
      if (!pumpDelta) continue; // not a pump.fun token movement for this wallet

      const walletAD = findAccountData(e, wallet);
      const nativeChangeLamports = walletAD?.nativeBalanceChange; // can be negative
      const feeLamports = e?.fee ?? null;
      const slot = e?.slot ?? null;

      // Determine BUY/SELL
      let side = null;
      if (pumpDelta.raw != null && nativeChangeLamports != null) {
        if (pumpDelta.raw > 0n && nativeChangeLamports < 0) side = "BUY";
        if (pumpDelta.raw < 0n && nativeChangeLamports > 0) side = "SELL";
      } else {
        // fallback direction
        if (pumpDelta.to === wallet) side = "BUY";
        if (pumpDelta.from === wallet) side = "SELL";
      }
      if (!side) continue;

      // Upsert token
      const tokenRow = await getOrCreateToken(pool, pumpDelta.mint, tsSeconds);
      const tokenId = tokenRow.id;

      // minutes_from_launch (best-effort)
      let minutesFromLaunch = null;
      if (tokenRow.launch_time) {
        const launchMs = new Date(tokenRow.launch_time).getTime();
        const tradeMs = tsSeconds * 1000;
        minutesFromLaunch = Math.floor((tradeMs - launchMs) / 60000);
        if (minutesFromLaunch < 0) minutesFromLaunch = 0;
      }

      // Amounts
      // IMPORTANT: storing as lamports/raw units is best.
      // If your DB columns are bigint: pass strings (safe).
      // If your DB columns are int: convert to Number() (risk overflow later).
      const solAmountLamportsAbs =
        nativeChangeLamports != null
          ? Math.abs(nativeChangeLamports).toString()
          : null;

      const tokenAmountRawAbs =
        pumpDelta.raw != null ? safeAbsBigIntToString(pumpDelta.raw) : null;

      // Insert into wallet_trades
      // NOTE: this assumes wallet_trades.signature is UNIQUE for ON CONFLICT to work.
      await pool.query(
        `
        INSERT INTO wallet_trades
          (ts, chain, wallet_address, token_id, side, signature, slot, block_time,
           sol_amount, token_amount, fee_sol, minutes_from_launch, source, raw)
        VALUES
          (to_timestamp($1), $2, $3, $4, $5, $6, $7, to_timestamp($1),
           $8, $9, $10, $11, $12, $13)
        ON CONFLICT (signature) DO NOTHING
        `,
        [
          tsSeconds,
          "solana",
          wallet,
          tokenId,
          side,
          signature,
          slot,
          solAmountLamportsAbs, // lamports (string)
          tokenAmountRawAbs,    // raw token units (string)
          feeLamports != null ? feeLamports.toString() : null, // lamports (string)
          minutesFromLaunch,
          "pumpfun",
          JSON.stringify(e),
        ]
      );

      console.log(`wallet_trades inserted: ${side} mint=${pumpDelta.mint} sig=${signature}`);
    }

    return res.status(200).json({ success: true });
  } catch (err) {
    console.error("Webhook error:", err);
    return res.status(500).send("server error");
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("Server running on port", PORT));
