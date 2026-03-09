require("dotenv").config();

const { Pool } = require("pg");

const DATABASE_URL = process.env.DATABASE_URL;
const CHAIN = process.env.CHAIN || "solana";
const PUMP_AMM_PROGRAM_ID =
  process.env.PUMP_AMM_PROGRAM_ID ||
  "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";

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

function getLogMessages(payload) {
  return payload?.meta?.logMessages || [];
}

function getInstructions(payload) {
  return payload?.transaction?.message?.instructions || [];
}

function getInnerInstructions(payload) {
  return payload?.meta?.innerInstructions || [];
}

function touchesPumpAmm(payload) {
  const logs = getLogMessages(payload);
  if (logs.some((l) => l.includes(PUMP_AMM_PROGRAM_ID))) return true;

  const instructions = getInstructions(payload);
  for (const ix of instructions) {
    if (ix.programId === PUMP_AMM_PROGRAM_ID) return true;
  }

  const inner = getInnerInstructions(payload);
  for (const group of inner) {
    for (const ix of group.instructions || []) {
      if (ix.programId === PUMP_AMM_PROGRAM_ID) return true;
    }
  }

  return false;
}

function getSideFromLogs(payload) {
  const logs = getLogMessages(payload);

  if (logs.some((l) => l.includes("Instruction: Buy"))) return "buy";
  if (logs.some((l) => l.includes("Instruction: Sell"))) return "sell";

  return null;
}

function getSignerWallet(payload) {
  const keys = payload?.transaction?.message?.accountKeys || [];

  for (const key of keys) {
    if (typeof key === "string") continue;
    if (key.signer) return key.pubkey;
  }

  return null;
}

function getBlockTime(payload) {
  if (!payload?.blockTime) return null;
  return new Date(payload.blockTime * 1000);
}

function getFeeSol(payload) {
  return (payload?.meta?.fee || 0) / 1e9;
}

function parseTokenBalances(arr) {
  const rows = [];

  for (const row of arr || []) {
    const owner = row.owner || null;
    const mint = row.mint || null;
    const accountIndex = row.accountIndex;
    const decimals = row.uiTokenAmount?.decimals ?? 0;
    const rawAmount = row.uiTokenAmount?.amount || "0";
    const amount = Number(rawAmount) / 10 ** decimals;

    rows.push({
      owner,
      mint,
      accountIndex,
      decimals,
      amount,
    });
  }

  return rows;
}

function buildTokenBalanceMap(rows) {
  const map = new Map();

  for (const row of rows) {
    const key = `${row.owner}::${row.mint}::${row.accountIndex}`;
    map.set(key, row);
  }

  return map;
}

function findWalletTokenDelta(payload, walletAddress) {
  const preRows = parseTokenBalances(payload?.meta?.preTokenBalances || []);
  const postRows = parseTokenBalances(payload?.meta?.postTokenBalances || []);

  const preMap = buildTokenBalanceMap(preRows);
  const postMap = buildTokenBalanceMap(postRows);

  const keys = new Set([...preMap.keys(), ...postMap.keys()]);

  let best = null;

  for (const key of keys) {
    const pre = preMap.get(key);
    const post = postMap.get(key);

    const owner = post?.owner || pre?.owner || null;
    const mint = post?.mint || pre?.mint || null;
    const decimals = post?.decimals ?? pre?.decimals ?? 0;
    const preAmt = pre?.amount || 0;
    const postAmt = post?.amount || 0;
    const delta = postAmt - preAmt;

    if (!owner || !mint) continue;
    if (owner !== walletAddress) continue;
    if (Math.abs(delta) === 0) continue;

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

function getWalletSolDelta(payload, walletAddress) {
  const keys = payload?.transaction?.message?.accountKeys || [];
  const preBalances = payload?.meta?.preBalances || [];
  const postBalances = payload?.meta?.postBalances || [];

  for (let i = 0; i < keys.length; i++) {
    const pubkey = typeof keys[i] === "string" ? keys[i] : keys[i].pubkey;
    if (pubkey !== walletAddress) continue;

    return ((postBalances[i] || 0) - (preBalances[i] || 0)) / 1e9;
  }

  return 0;
}

function parsePumpTradeFromPayload(row) {
  const payload = row.payload;

  if (!payload) {
    return { ok: false, reason: "missing payload" };
  }

  if (payload?.meta?.err) {
    return { ok: false, reason: "transaction failed" };
  }

  if (!touchesPumpAmm(payload)) {
    return { ok: false, reason: "not pump amm" };
  }

  const side = getSideFromLogs(payload);
  if (!side) {
    return { ok: false, reason: "no buy/sell instruction found" };
  }

  const walletAddress = getSignerWallet(payload);
  if (!walletAddress) {
    return { ok: false, reason: "no signer wallet found" };
  }

  const tokenDelta = findWalletTokenDelta(payload, walletAddress);
  if (!tokenDelta) {
    return { ok: false, reason: "no token delta found for signer" };
  }

  const walletSolDelta = getWalletSolDelta(payload, walletAddress);
  const solAmount = Math.abs(walletSolDelta);
  const tokenAmount = Math.abs(tokenDelta.delta);

  if (solAmount <= 0) {
    return { ok: false, reason: "sol amount is zero" };
  }

  if (tokenAmount <= 0) {
    return { ok: false, reason: "token amount is zero" };
  }

  const inferredSide =
    tokenDelta.delta > 0 && walletSolDelta < 0
      ? "buy"
      : tokenDelta.delta < 0 && walletSolDelta > 0
      ? "sell"
      : null;

  if (!inferredSide) {
    return {
      ok: false,
      reason: "token/sol deltas do not align to buy or sell",
    };
  }

  if (side !== inferredSide) {
    return {
      ok: false,
      reason: `log side ${side} does not match inferred side ${inferredSide}`,
    };
  }

  return {
    ok: true,
    trade: {
      chain: CHAIN,
      wallet_address: walletAddress,
      token_address: tokenDelta.mint,
      side,
      sol_amount: solAmount,
      token_amount: tokenAmount,
      fee_sol: getFeeSol(payload),
      price_sol: solAmount / tokenAmount,
      block_time: getBlockTime(payload),
      ts: getBlockTime(payload),
      signature: row.signature,
      slot: row.slot,
      raw: JSON.stringify(payload),
    },
  };
}

async function getOrCreateTokenId(client, tokenAddress) {
  const existing = await client.query(
    `SELECT id FROM tokens WHERE token_address = $1 LIMIT 1`,
    [tokenAddress]
  );

  if (existing.rows.length) {
    return existing.rows[0].id;
  }

  const inserted = await client.query(
    `
      INSERT INTO tokens (chain, token_address, source)
      VALUES ($1, $2, $3)
      RETURNING id
    `,
    [CHAIN, tokenAddress, "helius_parser"]
  );

  return inserted.rows[0].id;
}

async function insertTradeAndMarkParsed(rawRow, trade) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const tokenId = await getOrCreateTokenId(client, trade.token_address);

    await client.query(
      `
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
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17
      )
      ON CONFLICT DO NOTHING
      `,
      [
        trade.block_time,
        null,
        trade.chain,
        trade.fee_sol,
        null,
        trade.price_sol,
        trade.raw,
        trade.side,
        trade.signature,
        trade.slot,
        trade.sol_amount,
        null,
        "helius_parser",
        trade.token_amount,
        tokenId,
        trade.ts,
        trade.wallet_address,
      ]
    );

    await client.query(
      `
      UPDATE raw_webhook_events
      SET parsed_at = now(),
          parse_status = 'parsed',
          parse_error = null
      WHERE id = $1
      `,
      [rawRow.id]
    );

    await client.query("COMMIT");
    return { ok: true, tokenId };
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

async function markRawRow(rawId, status, errorText = null) {
  await pool.query(
    `
    UPDATE raw_webhook_events
    SET parsed_at = now(),
        parse_status = $2,
        parse_error = $3
    WHERE id = $1
    `,
    [rawId, status, errorText]
  );
}

async function processBatch(limit = 100) {
  const res = await pool.query(
    `
    SELECT id, signature, slot, timestamp, payload
    FROM raw_webhook_events
    WHERE parsed_at IS NULL
      AND payload::text LIKE '%pAMMBay6oce%'
    ORDER BY id ASC
    LIMIT $1
    `,
    [limit]
  );

  if (!res.rows.length) {
    console.log("No unparsed pump rows found");
    return 0;
  }

  let parsedCount = 0;

  for (const row of res.rows) {
    try {
      const result = parsePumpTradeFromPayload(row);

      if (!result.ok) {
        await markRawRow(row.id, "skipped", result.reason);
        console.log(`Skipped raw row ${row.id}: ${result.reason}`);
        continue;
      }

      const inserted = await insertTradeAndMarkParsed(row, result.trade);
      parsedCount += 1;

      console.log(
        `Parsed row ${row.id} | ${result.trade.side} | wallet=${result.trade.wallet_address} | token=${result.trade.token_address} | tokenId=${inserted.tokenId}`
      );
    } catch (err) {
      await markRawRow(row.id, "error", err.message);
      console.error(`Error parsing raw row ${row.id}: ${err.message}`);
    }
  }

  return parsedCount;
}

async function main() {
  try {
    await pool.query("SELECT 1");
    console.log("Parser DB connected");

    const processed = await processBatch(250);
    console.log(`Done. Parsed ${processed} rows.`);
  } catch (err) {
    console.error(err);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

main();
