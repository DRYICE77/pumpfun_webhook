"use strict";

// ==================================================
// NORTH STAR PIT OBSERVATION VERIFICATION
// northstar.pit.test.js
//
// Purpose:
//
// Verify that northstar.minute.sql produces
// point-in-time-safe minute observations.
//
// Tests:
//
// 1. Source boundary isolation
// 2. Deterministic replay
// 3. Future-event exclusion
// 4. Historical observation stability
//
// Safety:
//
// • Uses one PostgreSQL transaction
// • Inserts only synthetic test data
// • Always ROLLBACK
// • Does not advance NorthStar checkpoints
// • Does not consume worker test-gate commits
// • Does not modify collection status
//
// IMPORTANT:
//
// This test uses the REAL northstar.minute.sql.
// ==================================================

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { Pool } = require("pg");


// ==================================================
// 1. CONFIG
// ==================================================

const DATABASE_URL =
  process.env.DATABASE_URL;

if (!DATABASE_URL) {
  throw new Error(
    "DATABASE_URL is required"
  );
}

const SQL_PATH = path.join(
  __dirname,
  "northstar.minute.sql"
);

if (!fs.existsSync(SQL_PATH)) {
  throw new Error(
    `Missing SQL file: ${SQL_PATH}`
  );
}

const minuteSql =
  fs.readFileSync(
    SQL_PATH,
    "utf8"
  );

const pool = new Pool({
  connectionString: DATABASE_URL,

  ssl:
    process.env.PGSSLMODE === "disable"
      ? false
      : {
          rejectUnauthorized: false,
        },

  max: 2,

  connectionTimeoutMillis: 15000,

  statement_timeout: 120000,

  application_name:
    "northstar-pit-test",
});


// ==================================================
// 2. BASIC HELPERS
// ==================================================

function log(message, extra = null) {
  if (extra === null) {
    console.log(
      `[northstar-pit-test] ${message}`
    );

    return;
  }

  console.log(
    `[northstar-pit-test] ${message} ${JSON.stringify(
      extra
    )}`
  );
}


function fail(message, extra = null) {
  const error =
    new Error(message);

  error.testDetails = extra;

  throw error;
}


function randomSignature() {
  return (
    "NORTHSTAR_PIT_TEST_" +
    crypto.randomBytes(16).toString("hex")
  );
}


function asIso(value) {
  if (!value) {
    return null;
  }

  return new Date(value).toISOString();
}


function numericString(value) {
  if (
    value === null ||
    value === undefined
  ) {
    return null;
  }

  return String(value);
}


// ==================================================
// 3. OBSERVATION NORMALIZATION
//
// updated_at is deliberately excluded.
//
// It is write metadata, not a PIT feature.
// ==================================================

function normalizeObservation(row) {
  if (!row) {
    return null;
  }

  return {
    token_address:
      row.token_address,

    minute_bucket:
      asIso(row.minute_bucket),

    buy_volume_sol:
      numericString(
        row.buy_volume_sol
      ),

    sell_volume_sol:
      numericString(
        row.sell_volume_sol
      ),

    total_volume_sol:
      numericString(
        row.total_volume_sol
      ),

    max_buy_size_sol:
      numericString(
        row.max_buy_size_sol
      ),

    max_sell_size_sol:
      numericString(
        row.max_sell_size_sol
      ),

    net_max_buy_size_sol:
      numericString(
        row.net_max_buy_size_sol
      ),

    max_buy_to_sell_size_ratio:
      numericString(
        row.max_buy_to_sell_size_ratio
      ),

    latest_market_cap_usd:
      numericString(
        row.latest_market_cap_usd
      ),

    latest_price_per_token:
      numericString(
        row.latest_price_per_token
      ),

    latest_sol_price_usd:
      numericString(
        row.latest_sol_price_usd
      ),

    buy_count:
      Number(row.buy_count),

    sell_count:
      Number(row.sell_count),

    trade_count:
      Number(row.trade_count),

    unique_buyers:
      Number(row.unique_buyers),

    unique_sellers:
      Number(row.unique_sellers),

    unique_wallets:
      Number(row.unique_wallets),

    first_trade_at:
      asIso(row.first_trade_at),

    last_trade_at:
      asIso(row.last_trade_at),
  };
}


function observationsEqual(a, b) {
  return (
    JSON.stringify(a) ===
    JSON.stringify(b)
  );
}


// ==================================================
// 4. FIND TEST MINUTE
//
// Choose a real token/minute with:
// • multiple qualifying trades
// • positive SOL
// • valid token address
//
// We deliberately choose a recent historical
// candidate, but not the current minute.
// ==================================================

async function findCandidateMinute(
  client
) {
  const result =
    await client.query(`
      SELECT
        e.token_address,

        date_trunc(
          'minute',
          e.block_time
        ) AS minute_bucket,

        COUNT(*)::integer
          AS trade_count,

        MIN(e.block_time)
          AS first_trade_at,

        MAX(e.block_time)
          AS last_trade_at

      FROM public.pump_launchpad_events e

      WHERE
        e.event_type IN (
          'buy',
          'sell'
        )

        AND e.sol_amount > 0

        AND e.token_address
          IS NOT NULL

        AND BTRIM(
          e.token_address
        ) <> ''

        AND e.block_time <
          date_trunc(
            'minute',
            NOW()
          ) - INTERVAL '5 minutes'

        AND e.block_time >=
          NOW() - INTERVAL '24 hours'

      GROUP BY
        e.token_address,
        date_trunc(
          'minute',
          e.block_time
        )

      HAVING COUNT(*) >= 2

      ORDER BY
        minute_bucket DESC,
        trade_count DESC

      LIMIT 1
    `);

  if (result.rowCount !== 1) {
    fail(
      "Could not find a suitable real token/minute."
    );
  }

  return result.rows[0];
}


// ==================================================
// 5. READ ONE NORTHSTAR OBSERVATION
// ==================================================

async function readObservation(
  client,
  tokenAddress,
  minuteBucket
) {
  const result =
    await client.query(
      `
      SELECT
        token_address,
        minute_bucket,

        buy_volume_sol,
        sell_volume_sol,
        total_volume_sol,

        max_buy_size_sol,
        max_sell_size_sol,
        net_max_buy_size_sol,
        max_buy_to_sell_size_ratio,

        latest_market_cap_usd,
        latest_price_per_token,
        latest_sol_price_usd,

        buy_count,
        sell_count,
        trade_count,

        unique_buyers,
        unique_sellers,
        unique_wallets,

        first_trade_at,
        last_trade_at,

        updated_at

      FROM public.northstar_token_volume_minutes

      WHERE
        token_address = $1
        AND minute_bucket = $2::timestamptz
      `,
      [
        tokenAddress,
        minuteBucket,
      ]
    );

  if (result.rowCount === 0) {
    return null;
  }

  return result.rows[0];
}


// ==================================================
// 6. RUN REAL MINUTE SQL
// ==================================================

async function runMinuteSql(
  client,
  windowStart,
  windowEnd
) {
  const result =
    await client.query(
      minuteSql,
      [
        windowStart,
        windowEnd,
      ]
    );

  return result.rows[0];
}


// ==================================================
// 7. SOURCE BOUNDARY AUDIT
//
// Independently verify that the chosen minute has
// qualifying source events inside the exact
// [windowStart, windowEnd) interval.
//
// Also count events at/after windowEnd for the same
// token. Those must not affect this observation.
// ==================================================

async function auditSourceBoundary(
  client,
  tokenAddress,
  windowStart,
  windowEnd
) {
  const inside =
    await client.query(
      `
      SELECT
        COUNT(*)::integer AS count,

        MIN(block_time)
          AS first_trade_at,

        MAX(block_time)
          AS last_trade_at

      FROM public.pump_launchpad_events

      WHERE
        token_address = $1

        AND block_time >=
          $2::timestamptz

        AND block_time <
          $3::timestamptz

        AND event_type IN (
          'buy',
          'sell'
        )

        AND sol_amount > 0
      `,
      [
        tokenAddress,
        windowStart,
        windowEnd,
      ]
    );

  const future =
    await client.query(
      `
      SELECT
        COUNT(*)::integer AS count

      FROM public.pump_launchpad_events

      WHERE
        token_address = $1

        AND block_time >=
          $2::timestamptz

        AND event_type IN (
          'buy',
          'sell'
        )

        AND sol_amount > 0
      `,
      [
        tokenAddress,
        windowEnd,
      ]
    );

  return {
    inside:
      inside.rows[0],

    future:
      future.rows[0],
  };
}


// ==================================================
// 8. INSERT SYNTHETIC FUTURE EVENT
//
// Critical property:
//
// block_time is AFTER windowEnd.
//
// If PIT isolation works correctly, rerunning the
// historical window must completely ignore this row.
//
// This row exists only inside the test transaction.
// ==================================================

async function insertSyntheticFutureEvent(
  client,
  tokenAddress,
  windowEnd
) {
  const signature =
    randomSignature();

  const futureBlockTime =
    new Date(
      new Date(
        windowEnd
      ).getTime() +
      30 * 1000
    );

  const result =
    await client.query(
      `
      INSERT INTO public.pump_launchpad_events (
        token_address,
        signature,
        slot,
        block_time,
        event_type,
        wallet_address,
        sol_amount,
        token_amount,
        price_per_token,
        raw_json
      )

      VALUES (
        $1,
        $2,
        NULL,
        $3::timestamptz,
        'buy',
        $4,
        987654.321,
        123456789,
        0.123456789,
        NULL
      )

      RETURNING
        id,
        signature,
        block_time
      `,
      [
        tokenAddress,
        signature,
        futureBlockTime.toISOString(),
        "NORTHSTAR_PIT_TEST_WALLET",
      ]
    );

  return result.rows[0];
}


// ==================================================
// 9. MAIN TEST
// ==================================================

async function main() {
  const client =
    await pool.connect();

  let transactionStarted =
    false;

  try {
    log(
      "Starting PIT verification"
    );

    log(
      "Loading real northstar.minute.sql",
      {
        sqlPath:
          SQL_PATH,
      }
    );

    await client.query("BEGIN");

    transactionStarted = true;

    // Ensure no accidental commit survives.
    await client.query(
      "SET LOCAL statement_timeout = '120s'"
    );

    // ----------------------------------------------
    // FIND REAL HISTORICAL TEST MINUTE
    // ----------------------------------------------

    const candidate =
      await findCandidateMinute(
        client
      );

    const windowStart =
      new Date(
        candidate.minute_bucket
      );

    const windowEnd =
      new Date(
        windowStart.getTime() +
        60 * 1000
      );

    log(
      "Selected candidate",
      {
        tokenAddress:
          candidate.token_address,

        windowStart:
          windowStart.toISOString(),

        windowEnd:
          windowEnd.toISOString(),

        sourceTradeCount:
          candidate.trade_count,

        firstTradeAt:
          asIso(
            candidate.first_trade_at
          ),

        lastTradeAt:
          asIso(
            candidate.last_trade_at
          ),
      }
    );


    // ----------------------------------------------
    // TEST 1:
    // SOURCE BOUNDARY AUDIT
    // ----------------------------------------------

    const boundaryAudit =
      await auditSourceBoundary(
        client,
        candidate.token_address,
        windowStart.toISOString(),
        windowEnd.toISOString()
      );

    if (
      Number(
        boundaryAudit.inside.count
      ) < 1
    ) {
      fail(
        "Boundary audit found no qualifying events inside test minute.",
        boundaryAudit
      );
    }

    log(
      "PIT CUTOFF SOURCE AUDIT PASS",
      {
        insideEventCount:
          Number(
            boundaryAudit.inside.count
          ),

        existingFutureEventCount:
          Number(
            boundaryAudit.future.count
          ),
      }
    );


    // ----------------------------------------------
    // BUILD BASELINE USING REAL PRODUCTION SQL
    // ----------------------------------------------

    const firstRun =
      await runMinuteSql(
        client,
        windowStart.toISOString(),
        windowEnd.toISOString()
      );

    const baselineRow =
      await readObservation(
        client,
        candidate.token_address,
        windowStart.toISOString()
      );

    if (!baselineRow) {
      fail(
        "Production minute SQL did not produce the expected observation.",
        {
          firstRun,
        }
      );
    }

    const baseline =
      normalizeObservation(
        baselineRow
      );

    log(
      "Baseline observation created",
      {
        aggregation:
          firstRun,

        observation:
          baseline,
      }
    );


    // ----------------------------------------------
    // TEST 2:
    // DETERMINISTIC REPLAY
    //
    // Same source state + same window must produce
    // exactly the same PIT feature values.
    // ----------------------------------------------

    const secondRun =
      await runMinuteSql(
        client,
        windowStart.toISOString(),
        windowEnd.toISOString()
      );

    const replayRow =
      await readObservation(
        client,
        candidate.token_address,
        windowStart.toISOString()
      );

    const replay =
      normalizeObservation(
        replayRow
      );

    if (
      !observationsEqual(
        baseline,
        replay
      )
    ) {
      fail(
        "Deterministic replay failed.",
        {
          baseline,
          replay,
          secondRun,
        }
      );
    }

    log(
      "DETERMINISTIC REPLAY PASS",
      {
        minutesWritten:
          secondRun?.minutes_written ??
          null,
      }
    );


    // ----------------------------------------------
    // TEST 3:
    // SYNTHETIC FUTURE EVENT
    // ----------------------------------------------

    const syntheticFuture =
      await insertSyntheticFutureEvent(
        client,
        candidate.token_address,
        windowEnd.toISOString()
      );

    log(
      "Synthetic future event inserted",
      {
        signature:
          syntheticFuture.signature,

        blockTime:
          asIso(
            syntheticFuture.block_time
          ),
      }
    );


    // ----------------------------------------------
    // PROVE SYNTHETIC EVENT EXISTS AFTER CUTOFF
    // ----------------------------------------------

    const syntheticCheck =
      await client.query(
        `
        SELECT
          COUNT(*)::integer AS count

        FROM public.pump_launchpad_events

        WHERE
          signature = $1

          AND block_time >=
            $2::timestamptz
        `,
        [
          syntheticFuture.signature,
          windowEnd.toISOString(),
        ]
      );

    if (
      Number(
        syntheticCheck.rows[0].count
      ) !== 1
    ) {
      fail(
        "Synthetic future event was not positioned after the PIT cutoff."
      );
    }


    // ----------------------------------------------
    // TEST 4:
    // FUTURE-EVENT EXCLUSION
    //
    // Re-run the historical minute AFTER adding the
    // synthetic future event.
    //
    // Observation must remain identical.
    // ----------------------------------------------

    const futureRun =
      await runMinuteSql(
        client,
        windowStart.toISOString(),
        windowEnd.toISOString()
      );

    const afterFutureRow =
      await readObservation(
        client,
        candidate.token_address,
        windowStart.toISOString()
      );

    const afterFuture =
      normalizeObservation(
        afterFutureRow
      );

    if (
      !observationsEqual(
        baseline,
        afterFuture
      )
    ) {
      fail(
        "Future-event exclusion failed: a post-cutoff event changed the historical observation.",
        {
          baseline,
          afterFuture,
          syntheticFuture,
          futureRun,
        }
      );
    }

    log(
      "FUTURE EVENT EXCLUSION PASS",
      {
        syntheticFutureBlockTime:
          asIso(
            syntheticFuture.block_time
          ),

        observationLastTradeAt:
          afterFuture.last_trade_at,

        minutesWritten:
          futureRun?.minutes_written ??
          null,
      }
    );


    // ----------------------------------------------
    // TEST 5:
    // EXPLICIT BOUNDARY ASSERTION
    //
    // Historical last_trade_at must remain strictly
    // earlier than windowEnd.
    // ----------------------------------------------

    if (
      afterFuture.last_trade_at &&
      new Date(
        afterFuture.last_trade_at
      ).getTime() >=
        windowEnd.getTime()
    ) {
      fail(
        "Historical observation crossed its window_end boundary.",
        {
          lastTradeAt:
            afterFuture.last_trade_at,

          windowEnd:
            windowEnd.toISOString(),
        }
      );
    }

    log(
      "WINDOW BOUNDARY ISOLATION PASS"
    );


    // ----------------------------------------------
    // OVERALL RESULT
    // ----------------------------------------------

    log(
      "========================================"
    );

    log(
      "PIT CUTOFF ISOLATION       PASS"
    );

    log(
      "FUTURE EVENT EXCLUSION     PASS"
    );

    log(
      "DETERMINISTIC REPLAY       PASS"
    );

    log(
      "HISTORICAL STABILITY       PASS"
    );

    log(
      "DATABASE MUTATION          NONE (ROLLBACK)"
    );

    log(
      "OVERALL PIT VERIFICATION   PASS"
    );

    log(
      "========================================"
    );
  } catch (error) {
    log(
      "========================================"
    );

    log(
      "OVERALL PIT VERIFICATION   FAIL",
      {
        error:
          String(
            error?.message ||
            error
          ),

        details:
          error?.testDetails ||
          null,
      }
    );

    log(
      "========================================"
    );

    process.exitCode = 1;
  } finally {
    // ----------------------------------------------
    // ALWAYS ROLLBACK
    // ----------------------------------------------

    if (transactionStarted) {
      try {
        await client.query(
          "ROLLBACK"
        );

        log(
          "Transaction rolled back successfully"
        );
      } catch (rollbackError) {
        log(
          "ROLLBACK FAILED",
          {
            error:
              String(
                rollbackError?.message ||
                rollbackError
              ),
          }
        );

        process.exitCode = 1;
      }
    }

    client.release();

    await pool.end();
  }
}


// ==================================================
// 10. RUN
// ==================================================

main().catch((error) => {
  console.error(
    "[northstar-pit-test] Fatal error",
    error
  );

  process.exit(1);
});
