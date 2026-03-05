import express from "express";
import pkg from "pg";

const { Pool } = pkg;

const app = express();
app.use(express.json({ limit: "2mb" }));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  // If you ever get SSL errors on Railway, uncomment:
  // ssl: { rejectUnauthorized: false },
});

// --------------------
// Helpers
// --------------------
function getBearerToken(req) {
  const auth = req.headers.authorization || "";
  if (!auth.startsWith("Bearer ")) return null;
  return auth.slice(7).trim();
}

function requireHeliusAuth(req, res) {
  const expected = process.env.HELIUS_WEBHOOK_SECRET;
  const token = getBearerToken(req);

  // If secret isn't set, fail closed
  if (!expected || !token || token !== expected) {
    res.status(401).send("unauthorized");
    return false;
  }
  return true;
}

function toBigIntish(value) {
  // pg can cast strings -> BIGINT safely. We avoid JS Number overflow.
  if (value === null || value === undefined) return null;
  if (typeof value === "bigint") return value.toString();
  if (typeof value === "number") return Math.trunc(value).toString();
  if (typeof value === "string") return value;
  return null;
}

function unixToTimestamptzSeconds(ts) {
  // Helius enhanced uses seconds timestamps (often). If ms sneaks in, handle it.
  if (!ts) return null;
  const n = Number(ts);
  if (!Number.isFinite(n)) return null;
  const ms = n > 1e12 ? n : n * 1000;
  return new Date(ms).toISOString();
}

const SOL_MINT = "So11111111111111111111111111111111111111112";

// Net token delta for a wallet on a given mint from tokenTransfers
function getNetTokenDelta(e, wallet, mint) {
  const transfers = Array.isArray(e?.tokenTransfers) ? e.tokenTransfers : [];
  let net = 0;

  for (const t of transfers) {
    if (t?.mint !== mint) continue;
    const amt = Number(t?.tokenAmount ?? 0);
    if (!Number.isFinite(amt) || amt === 0) continue;

    if (t?.toUserAccount === wallet) net += amt;
    if (t?.fromUserAccount === wallet) net -= amt;
  }
  return net;
}

// Net SOL delta (in SOL) for feePayer using accountData nativeBalanceChange (lamports)
function getNetSolDelta(e, wallet) {
  const accs = Array.isArray(e?.accountData) ? e.accountData : [];
  const row = accs.find((a) => a?.account === wallet);
  const lamports = row?.nativeBalanceChange;
  if (lamports === null || lamports === undefined) return null;
  const n = Number(lamports);
  if (!Number.isFinite(n)) return null;
  return n / 1e9; // lamports -> SOL (can be negative)
}

// pump.fun mints frequently end with "pump" (as in your example).
function looksLikePumpfunMint(mint) {
  return typeof mint === "string" && mint.endsWith("pump") && mint.length > 20;
}

function extractInterestingMints(e) {
  const transfers = Array.isArray(e?.tokenTransfers) ? e.tokenTransfers : [];
  const mints = new Set();

  for (const t of transfers) {
    const mint = t?.mint;
    if (!mint || mint === SOL_MINT) continue;
    // Keep pumpfun-looking mints, but you can loosen this later if you want all SPLs
    if (looksLikePumpfunMint(mint)) mints.add(mint);
  }
  return [...mints];
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
  const startedAt = Date.now();

  try {
    if (!requireHeliusAuth(req, res)) return;

    const events = Array.isArray(req.body) ? req.body : [req.body];
    console.log(`Received ${events.length} Helius event(s)`);

    // Respond fast so Helius is happy (optional). We still await writes here for now.
    // If you want ultra-low latency, you can res.json(...) immediately and process async.
    // For now: keep it simple and reliable.

    const client = await pool.connect();
    try {
      // You can wrap in a transaction if you want atomicity, but it’s not required
      // await client.query("BEGIN");

      for (const e of events) {
        const signature = e?.signature;
        if (!signature) continue;

        // 1) Store raw event (source of truth)
        await client.query(
          `
          INSERT INTO raw_webhook_events
            (signature, timestamp, slot, type, payload, created_at)
          VALUES
            ($1, $2::BIGINT, $3::BIGINT, $4, $5::jsonb, NOW())
          ON CONFLICT (signature) DO NOTHING
          `,
          [
            signature,
            toBigIntish(e?.timestamp),
            toBigIntish(e?.slot),
            e?.type ?? null,
            JSON.stringify(e),
          ]
        );

        // 2) Upsert tokens (discovered from transfers)
        const mints = extractInterestingMints(e);
        for (const mint of mints) {
          // launch_time is "best effort" here (webhook time, not true creation time)
          const launchTime = unixToTimestamptzSeconds(e?.timestamp);

          await client.query(
            `
            INSERT INTO tokens
              (token_address, chain, source, launch_time)
            VALUES
              ($1, $2, $3, $4)
            ON CONFLICT (token_address) DO UPDATE
              SET source = COALESCE(tokens.source, EXCLUDED.source),
                  chain = COALESCE(tokens.chain, EXCLUDED.chain),
                  launch_time = COALESCE(tokens.launch_time, EXCLUDED.launch_time)
            `,
            [mint, "solana", "pumpfun", launchTime]
          );
        }

        // 3) Parse SWAPs into wallet_trades (best-effort)
        // NOTE: this assumes wallet_trades has *at least* these columns:
        // signature (text), wallet_address (text), token_address (text),
        // side (text), sol_amount (numeric), token_amount (numeric),
        // price_sol (numeric), slot (bigint), ts (timestamptz), raw (jsonb)
        //
        // If your schema differs, this will log a warning but won’t break ingestion.
        if (e?.type === "SWAP") {
          const wallet = e?.feePayer;
          if (wallet) {
            const pumpMints = extractInterestingMints(e);

            // If multiple, we’ll insert one row per mint (usually just one)
            for (const mint of pumpMints) {
              const netToken = getNetTokenDelta(e, wallet, mint);
              const netSol = getNetSolDelta(e, wallet); // can be negative or positive

              if (!netSol || !netToken) continue;

              const side = netToken > 0 ? "buy" : "sell";
              const tokenAmount = Math.abs(netToken);
              const solAmount = Math.abs(netSol);

              if (tokenAmount === 0 || solAmount === 0) continue;

              const priceSolPerToken = solAmount / tokenAmount;
              const tsIso = unixToTimestamptzSeconds(e?.timestamp);

              try {
                await client.query(
                  `
                  INSERT INTO wallet_trades
                    (signature, wallet_address, token_address, side,
                     sol_amount, token_amount, price_sol, slot, ts, raw)
                  VALUES
                    ($1, $2, $3, $4,
                     $5::numeric, $6::numeric, $7::numeric, $8::BIGINT, $9, $10::jsonb)
                  ON CONFLICT DO NOTHING
                  `,
                  [
                    signature,
                    wallet,
                    mint,
                    side,
                    solAmount,
                    tokenAmount,
                    priceSolPerToken,
                    toBigIntish(e?.slot),
                    tsIso,
                    JSON.stringify(e),
                  ]
                );
              } catch (err) {
                // Don’t break ingestion if schema mismatch
                console.warn(
                  "wallet_trades insert skipped (schema mismatch or missing constraint). Error:",
                  err?.message || err
                );
              }
            }
          }
        }
      }

      // await client.query("COMMIT");
    } catch (err) {
      // await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }

    const ms = Date.now() - startedAt;
    return res.status(200).json({ success: true, received: events.length, ms });
  } catch (err) {
    console.error("Webhook error:", err);
    return res.status(500).send("server error");
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("Server running on port", PORT));
