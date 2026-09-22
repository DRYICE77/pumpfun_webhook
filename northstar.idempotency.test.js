/**
 * NorthStar minute-worker idempotency test.
 *
 * Runs the real minute SQL twice inside one transaction.
 * Always rolls back. Does NOT touch worker checkpoints,
 * worker runs, or the commit test gate.
 *
 * Run only while the live minute worker is stopped.
 */

const fs = require("fs");
const path = require("path");
const { Client } = require("pg");

const WINDOW_START = "2026-09-20T23:42:00.000Z";
const WINDOW_END = "2026-09-20T23:43:00.000Z";

const minuteSql = fs.readFileSync(
  path.join(__dirname, "northstar.minute.sql"),
  "utf8"
);

const client = new Client({
  connectionString: process.env.DATABASE_URL,
  connectionTimeoutMillis: 10000,
});

async function getMinuteRows() {
  const result = await client.query(
    `
    SELECT
      token_address,
      minute_bucket,
      buy_volume_sol,
      sell_volume_sol,
      total_volume_sol,
      buy_count,
      sell_count,
      trade_count,
      unique_buyers,
      unique_sellers,
      unique_wallets,
      latest_price_per_token,
      first_trade_at,
      last_trade_at,
      updated_at
    FROM public.northstar_token_volume_minutes
    WHERE minute_bucket >= $1::timestamptz
      AND minute_bucket < $2::timestamptz
    ORDER BY token_address, minute_bucket
    `,
    [WINDOW_START, WINDOW_END]
  );

  return result.rows;
}

function metricFingerprint(rows) {
  return JSON.stringify(
    rows.map(({ updated_at, ...metrics }) => metrics)
  );
}

async function main() {
  if (!process.env.DATABASE_URL) {
    throw new Error("DATABASE_URL is missing");
  }

  await client.connect();

  let transactionStarted = false;

  try {
    await client.query("BEGIN");
    transactionStarted = true;

    await client.query(
      "SET LOCAL statement_timeout = '30000ms'"
    );

    const before = await getMinuteRows();

    console.log("BEFORE", {
      rows: before.length,
    });

    const first = await client.query(minuteSql, [
      WINDOW_START,
      WINDOW_END,
    ]);

    const afterFirst = await getMinuteRows();

    console.log("FIRST_REPLAY", first.rows[0]);

    const second = await client.query(minuteSql, [
      WINDOW_START,
      WINDOW_END,
    ]);

    const afterSecond = await getMinuteRows();

    console.log("SECOND_REPLAY", second.rows[0]);

    const sameMetrics =
      metricFingerprint(afterFirst) ===
      metricFingerprint(afterSecond);

    const sameUpdatedAt =
      afterFirst.every((row, index) => {
        const next = afterSecond[index];

        return (
          next &&
          row.token_address === next.token_address &&
          new Date(row.updated_at).getTime() ===
            new Date(next.updated_at).getTime()
        );
      });

    const secondWroteZero =
      Number(second.rows[0]?.minutes_written) === 0;

    const uniqueKeys =
      new Set(
        afterSecond.map(
          (row) =>
            `${row.token_address}|${row.minute_bucket}`
        )
      ).size === afterSecond.length;

    const hasEvents =
      Number(first.rows[0]?.qualifying_events) > 0;

    const passed =
      hasEvents &&
      sameMetrics &&
      sameUpdatedAt &&
      secondWroteZero &&
      uniqueKeys;

    console.log("TEST_RESULT", {
      passed,
      hasEvents,
      sameMetrics,
      sameUpdatedAt,
      secondWroteZero,
      uniqueKeys,
      rowsBefore: before.length,
      rowsAfterFirst: afterFirst.length,
      rowsAfterSecond: afterSecond.length,
    });

    if (!passed) {
      process.exitCode = 1;
    }
  } finally {
    if (transactionStarted) {
      await client.query("ROLLBACK");
      console.log("ROLLBACK_COMPLETE");
    }

    await client.end();
  }
}

main().catch((error) => {
  console.error("TEST_ERROR", error);
  process.exitCode = 1;
});
