import express from "express";
import pkg from "pg";

const { Pool } = pkg;

const app = express();
app.use(express.json({ limit: "2mb" }));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  // Optional: avoids some SSL headaches on managed DBs
  ssl: process.env.PGSSLMODE ? undefined : { rejectUnauthorized: false },
});

// --------------------
// Configurable table names (helps if you have "Tokens" capitalized)
// --------------------
const TABLE_RAW = process.env.TABLE_RAW_WEBHOOK_EVENTS || "raw_webhook_events";
const TABLE_TOKENS = process.env.TABLE_TOKENS || "Tokens"; // try "tokens" if yours is lowercase
const TABLE_WALLET_TRADES = process.env.TABLE_WALLET_TRADES || "wallet_trades";
const TABLE_WALLET_POSITIONS =
  process.env.TABLE_WALLET_TOKEN_POSITIONS || "wallet_token_positions";

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
// Health
// --------------------
app.get("/", (req, res) => res.status(200).send("ok"));
app.post("/", (req, res) => res.status(200).send("ok"));
app.get("/health", (req, res) => res.status(200).send("server alive"));

// --------------------
// Schema introspection (prevents “column does not exist”)
// --------------------
const columnsCache = new Map();

/**
 * Returns a Set of columns for a table in the current schema.
 * Works for mixed-case tables too IF they exist with that exact name.
 */
async function getTableColumns(tableName) {
  if (columnsCache.has(tableName)) return columnsCache.get(tableName);

  // Try as-is, and also try lowercase fallback.
  // NOTE: information_schema stores table_name case-sensitive as created.
  const candidates = [tableName, tableName.toLowerCase()];

  for (const candidate of candidates) {
    const { rows } = await pool.query(
      `
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = $1
      `,
      [candidate]
    );

    if (rows?.length) {
      const set = new Set(rows.map((r) => r.column_name));
      columnsCache.set(tableName, set);
      // Also cache the actual found name so we can use it later
      columnsCache.set(`__resolved__:${tableName}`, candidate);
      return set;
    }
  }

  // Cache empty so we don't spam queries
  const empty = new Set();
  columnsCache.set(tableName, empty);
  return empty;
}

async function resolveTableName(tableName) {
  await getTableColumns(tableName);
  return columnsCache.get(`__resolved__:${tableName}`) || tableName;
}

function hasAll(colsSet, neededCols = []) {
  return neededCols.every((c) => colsSet.has(c));
}

// --------------------
// Helpers: safe inserts based on existing columns
// --------------------
async function insertRawWebhookEvent(e) {
  const signature = e?.signature;
  if (!signature) return;

  const rawCols = await getTableColumns(TABLE_RAW);
  const rawTable = await resolveTableName(TABLE_RAW);

  // Your schema from earlier: (signature, timestamp, slot, type, payload, created_at)
  if (!hasAll(rawCols, ["signature", "payload", "created_at"])) {
    console.log(`[raw] table ${rawTable} missing expected columns, skipping raw insert`);
    return;
  }

  // timestamp/slot/type are optional
  const tsVal = e?.timestamp ?? null;
  const slotVal = e?.slot ?? null;
  const typeVal = e?.type ?? null;

  await pool.query(
    `
    INSERT INTO "${rawTable}"
      (signature, timestamp, slot, type, payload, created_at)
    VALUES
      ($1, $2, $3, $4, $5, NOW())
    ON CONFLICT (signature) DO NOTHING
    `,
    [signature, tsVal, slotVal, typeVal, e]
  );
}

/**
 * Upsert token row and return token id if the table supports it.
 * Supports either:
 * - Tokens(id, token_address, chain, source, launch_time, name, symbol, creator_wallet)
 */
async function upsertTokenAndGetId({ tokenAddress, chain, source, launchTime }) {
  if (!tokenAddress) return null;

  const tokenCols = await getTableColumns(TABLE_TOKENS);
  const tokenTable = await resolveTableName(TABLE_TOKENS);

  // Must have token_address; id is optional but needed to return a token_id
  if (!tokenCols.has("token_address")) {
    console.log(`[tokens] table ${tokenTable} missing token_address, skipping upsert`);
    return null;
  }

  // Build dynamic insert/upsert based on columns you actually have
  const insertCols = ["token_address"];
  const insertVals = ["$1"];
  const updates = [];
  const args = [tokenAddress];
  let argi = 2;

  function addCol(col, value) {
    if (!tokenCols.has(col)) return;
    insertCols.push(col);
    insertVals.push(`$${argi}`);
    args.push(value);
    // on conflict update: set col = excluded.col (for non-nullable metadata)
    updates.push(`${col} = EXCLUDED.${col}`);
    argi++;
  }

  addCol("chain", chain ?? "solana");
  addCol("source", source ?? "pumpfun");
  addCol("launch_time", launchTime ?? null);

  // If you have name/symbol later, you can add them too
  // addCol("name", name ?? null);
  // addCol("symbol", symbol ?? null);
  // addCol("creator_wallet", creatorWallet ?? null);

  // If table doesn't have id, we can still upsert but can't return id
  const canReturnId = tokenCols.has("id");

  const sql = `
    INSERT INTO "${tokenTable}" (${insertCols.join(", ")})
    VALUES (${insertVals.join(", ")})
    ON CONFLICT (token_address) DO UPDATE
      SET ${updates.length ? updates.join(", ") : "token_address = EXCLUDED.token_address"}
    ${canReturnId ? "RETURNING id" : ""}
  `;

  const result = await pool.query(sql, args);
  if (canReturnId) return result.rows?.[0]?.id ?? null;
  return null;
}

/**
 * Extract a pump.fun-like trade from a Helius Enhanced SWAP event.
 * Heuristics:
 * - trader wallet = feePayer (or account)
 * - token mint picked from tokenTransfers where wallet receives or sends
 * - SOL delta from nativeTransfers affecting the trader
 */
function parseTradeFromHeliusEvent(e) {
  if (!e) return null;
  if (e.type !== "SWAP") return null;

  const wallet =
    e.feePayer ||
    e.feePayerAccount ||
    e?.transaction?.feePayer ||
    e?.accountData?.[0]?.account ||
    null;

  if (!wallet) return null;

  const tokenTransfers = Array.isArray(e.tokenTransfers) ? e.tokenTransfers : [];
  const nativeTransfers = Array.isArray(e.nativeTransfers) ? e.nativeTransfers : [];

  // SOL delta for wallet (lamports)
  let lamportsIn = 0;
  let lamportsOut = 0;

  for (const t of nativeTransfers) {
    const from = t?.fromUserAccount || t?.fromAccount;
    const to = t?.toUserAccount || t?.toAccount;
    const amt = Number(t?.amount ?? 0);
    if (!amt) continue;
    if (to === wallet) lamportsIn += amt;
    if (from === wallet) lamportsOut += amt;
  }

  const solDelta = (lamportsIn - lamportsOut) / 1e9; // + means received SOL

  // Find token delta for wallet:
  // If wallet receives tokens => buy
  // If wallet sends tokens => sell
  let tokenMint = null;
  let tokenAmount = 0;

  // prefer transfers where wallet is involved
  const involved = tokenTransfers.filter((t) => {
    const from = t?.fromUserAccount || t?.fromAccount;
    const to = t?.toUserAccount || t?.toAccount;
    return from === wallet || to === wallet;
  });

  // Pick the biggest absolute token transfer involving wallet
  for (const t of involved) {
    const mint = t?.mint;
    if (!mint) continue;

    const rawAmt =
      typeof t?.tokenAmount === "number"
        ? t.tokenAmount
        : Number(t?.tokenAmount ?? t?.rawTokenAmount?.tokenAmount ?? 0);

    if (!rawAmt) continue;

    const from = t?.fromUserAccount || t?.fromAccount;
    const to = t?.toUserAccount || t?.toAccount;

    // wallet receives => + ; wallet sends => -
    const signed = to === wallet ? Math.abs(rawAmt) : -Math.abs(rawAmt);

    if (Math.abs(signed) > Math.abs(tokenAmount)) {
      tokenMint = mint;
      tokenAmount = signed;
    }
  }

  if (!tokenMint || tokenAmount === 0) return null;

  // Determine side and solSpent/solReceived magnitude
  // For buys: wallet spends SOL (solDelta negative) and receives tokens (tokenAmount positive)
  // For sells: wallet receives SOL (solDelta positive) and sends tokens (tokenAmount negative)
  const side =
    tokenAmount > 0 && solDelta < 0 ? "BUY" :
    tokenAmount < 0 && solDelta > 0 ? "SELL" :
    // fallback heuristic: token direction
    tokenAmount > 0 ? "BUY" : "SELL";

  const solAmountAbs = Math.abs(solDelta);
  const tokenAmountAbs = Math.abs(tokenAmount);

  const priceSolPerToken =
    tokenAmountAbs > 0 ? solAmountAbs / tokenAmountAbs : null;

  return {
    wallet_address: wallet,
    token_address: tokenMint,
    side,
    sol_amount: solAmountAbs,
    token_amount: tokenAmountAbs,
    price_sol_per_token: priceSolPerToken,
  };
}

async function insertWalletTrade({ tokenId, trade, signature, slot, timestamp, source }) {
  const cols = await getTableColumns(TABLE_WALLET_TRADES);
  const table = await resolveTableName(TABLE_WALLET_TRADES);

  // We support either schema:
  // A) token_id + wallet_address ...
  // B) token_address + wallet_address ...
  const hasTokenId = cols.has("token_id");
  const hasTokenAddress = cols.has("token_address");

  if (!cols.has("wallet_address")) {
    console.log(`[wallet_trades] missing wallet_address, skipping`);
    return;
  }

  // Basic required fields we try to write if present
  const insertCols = [];
  const insertVals = [];
  const args = [];
  let i = 1;

  function add(col, val) {
    if (!cols.has(col)) return;
    insertCols.push(col);
    insertVals.push(`$${i}`);
    args.push(val);
    i++;
  }

  add("wallet_address", trade.wallet_address);

  if (hasTokenId) add("token_id", tokenId);
  else if (hasTokenAddress) add("token_address", trade.token_address);
  else {
    console.log(`[wallet_trades] no token_id or token_address column, skipping`);
    return;
  }

  add("side", trade.side);
  add("sol_amount", trade.sol_amount);
  add("token_amount", trade.token_amount);
  add("price_sol_per_token", trade.price_sol_per_token);

  add("signature", signature);
  add("slot", slot ?? null);
  add("ts", timestamp ? new Date(timestamp * 1000) : null);
  add("source", source ?? "pumpfun");

  // Choose a conflict target if your table has one
  // (If none exists, inserts will still work but may duplicate)
  let conflictClause = "";
  if (cols.has("signature") && cols.has("wallet_address")) {
    // Best if you make a UNIQUE(signature, wallet_address) or UNIQUE(signature, wallet_address, token_id)
    conflictClause = "ON CONFLICT DO NOTHING";
  }

  const sql = `
    INSERT INTO "${table}" (${insertCols.join(", ")})
    VALUES (${insertVals.join(", ")})
    ${conflictClause}
  `;

  await pool.query(sql, args);
}

async function upsertWalletPosition({ tokenId, trade }) {
  const cols = await getTableColumns(TABLE_WALLET_POSITIONS);
  const table = await resolveTableName(TABLE_WALLET_POSITIONS);

  if (!cols.has("wallet_address")) return;
  if (!cols.has("token_id")) return;
  if (!cols.has("qty")) {
    // If you named it something else, we can adapt later
    return;
  }

  // qty delta: + on buy, - on sell
  const qtyDelta = trade.side === "BUY" ? trade.token_amount : -trade.token_amount;

  // Basic upsert: increments qty, updates last_seen_ts if present
  const hasLast = cols.has("last_seen_ts");
  const hasUpdatedAt = cols.has("updated_at");

  const sql = `
    INSERT INTO "${table}" (wallet_address, token_id, qty${hasLast ? ", last_seen_ts" : ""}${hasUpdatedAt ? ", updated_at" : ""})
    VALUES ($1, $2, $3${hasLast ? ", NOW()" : ""}${hasUpdatedAt ? ", NOW()" : ""})
    ON CONFLICT (wallet_address, token_id) DO UPDATE
      SET qty = "${table}".qty + EXCLUDED.qty
      ${hasLast ? ", last_seen_ts = NOW()" : ""}
      ${hasUpdatedAt ? ", updated_at = NOW()" : ""}
  `;

  await pool.query(sql, [trade.wallet_address, tokenId, qtyDelta]);
}

// --------------------
// Webhook handler
// --------------------
app.post("/webhooks/helius", async (req, res) => {
  try {
    if (!requireHeliusAuth(req, res)) return;

    const events = Array.isArray(req.body) ? req.body : [req.body];
    console.log(`Received ${events.length} Helius event(s)`);

    // Always store raw first
    for (const e of events) {
      await insertRawWebhookEvent(e);
    }

    // Then parse + insert derived data
    for (const e of events) {
      const signature = e?.signature;
      if (!signature) continue;

      const trade = parseTradeFromHeliusEvent(e);
      if (!trade) continue;

      // Upsert token
      const tokenId = await upsertTokenAndGetId({
        tokenAddress: trade.token_address,
        chain: "solana",
        source: "pumpfun",
        launchTime: e?.timestamp ? new Date(e.timestamp * 1000) : null,
      });

      // Insert trade (supports either token_id or token_address schemas)
      await insertWalletTrade({
        tokenId,
        trade,
        signature,
        slot: e?.slot ?? null,
        timestamp: e?.timestamp ?? null,
        source: e?.source ?? "pumpfun",
      });

      // Update positions if your schema supports it (wallet_address, token_id, qty)
      if (tokenId) {
        await upsertWalletPosition({ tokenId, trade });
      }
    }

    return res.status(200).json({ success: true });
  } catch (err) {
    console.error("Webhook error:", err);
    return res.status(500).send("server error");
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("Server running on port", PORT));
