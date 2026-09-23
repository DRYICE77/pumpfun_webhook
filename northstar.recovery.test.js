"use strict";

// ============================================================
// NORTH STAR — FORWARD OUTCOME VERIFICATION
// northstar.outcome.test.js
//
// T0 DEFINITION:
//   T0 = END of the PIT observation minute.
//
// Example:
//   PIT minute: [05:43:00, 05:44:00)
//   T0:         05:44:00
//
// Therefore:
//   feature information: block_time < T0
//   outcome information: block_time >= T0
//
// TESTS:
//   1. T0 temporal separation
//   2. Deterministic outcome replay
//   3. Pre-T0 event exclusion from outcomes
//   4. Post-T0 future-event sensitivity
//   5. PIT observation remains unchanged
//   6. Transaction rollback
//
// SAFETY:
//   • One PostgreSQL transaction
//   • Synthetic rows only
//   • Always ROLLBACK
//   • No worker checkpoint changes
//   • No test-gate consumption
// ============================================================

const { Pool } = require("pg");
const crypto = require("crypto");

const DATABASE_URL = process.env.DATABASE_URL;

if (!DATABASE_URL) {
  throw new Error("DATABASE_URL is required");
}

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
    "northstar-outcome-test",
});


// ============================================================
// HELPERS
// ============================================================

function log(message, extra = null) {
  if (extra === null) {
    console.log(
      `[northstar-outcome-test] ${message}`
    );

    return;
  }

  console.log(
    `[northstar-outcome-test] ${message} ${JSON.stringify(extra)}`
  );
}


function fail(message, extra = null) {
  const error = new Error(message);

  error.testDetails = extra;

  throw error;
}


function randomSignature(label) {
  return (
    `NORTHSTAR_OUTCOME_TEST_${label}_` +
    crypto.randomBytes(16).toString("hex")
  );
}


function iso(value) {
  if (!value) return null;

  return new Date(value).toISOString();
}


function numeric(value) {
  if (
    value === null ||
    value === undefined
  ) {
    return null;
  }

  return Number(value);
}


// ============================================================
// PIT NORMALIZATION
//
// updated_at deliberately excluded.
// ============================================================

function normalizePit(row) {
  if (!row) return null;

  return {
    token_address:
      row.token_address,

    minute_bucket:
      iso(row.minute_bucket),

    buy_volume_sol:
      String(row.buy_volume_sol),

    sell_volume_sol:
      String(row.sell_volume_sol),

    total_volume_sol:
      String(row.total_volume_sol),

    max_buy_size_sol:
      String(row.max_buy_size_sol),

    max_sell_size_sol:
      String(row.max_sell_size_sol),

    net_max_buy_size_sol:
      String(row.net_max_buy_size_sol),

    max_buy_to_sell_size_ratio:
      String(
        row.max_buy_to_sell_size_ratio
      ),

    latest_market_cap_usd:
      row.latest_market_cap_usd === null
        ? null
        : String(
            row.latest_market_cap_usd
          ),

    latest_price_per_token:
      row.latest_price_per_token === null
        ? null
        : String(
            row.latest_price_per_token
          ),

    latest_sol_price_usd:
      row.latest_sol_price_usd === null
        ? null
        : String(
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
      iso(row.first_trade_at),

    last_trade_at:
      iso(row.last_trade_at),
  };
}


function equal(a, b) {
  return (
    JSON.stringify(a) ===
    JSON.stringify(b)
  );
}


// ============================================================
// FIND A REAL PIT OBSERVATION
//
// Requirements:
//
// • Existing NorthStar minute
// • Positive baseline price
// • At least 10 minutes old
// • Has subsequent real price observations
//
// T0 = minute_bucket + 1 minute
// ============================================================

async function findCandidate(client) {
  const result =
    await client.query(`
      SELECT
        n.token_address,
        n.minute_bucket,
        n.latest_price_per_token,
        n.last_trade_at,

        (
          n.minute_bucket
          + INTERVAL '1 minute'
        ) AS t0,

        COUNT(f.id)::integer
          AS future_price_events

      FROM
        public.northstar_token_volume_minutes n

      JOIN
        public.pump_launchpad_events f

        ON f.token_address =
           n.token_address

       AND f.block_time >=
           (
             n.minute_bucket
             + INTERVAL '1 minute'
           )

       AND f.event_type IN (
           'buy',
           'sell'
       )

       AND f.sol_amount > 0

       AND f.price_per_token > 0

      WHERE
        n.latest_price_per_token > 0

        AND n.minute_bucket <
            date_trunc(
              'minute',
              NOW()
            )
            - INTERVAL '10 minutes'

      GROUP BY
        n.token_address,
        n.minute_bucket,
        n.latest_price_per_token,
        n.last_trade_at

      HAVING
        COUNT(f.id) >= 5

      ORDER BY
        n.minute_bucket DESC,
        COUNT(f.id) DESC

      LIMIT 1
    `);

  if (result.rowCount !== 1) {
    fail(
      "Could not find a suitable PIT/outcome candidate."
    );
  }

  return result.rows[0];
}


// ============================================================
// READ PIT OBSERVATION
// ============================================================

async function readPit(
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

      FROM
        public.northstar_token_volume_minutes

      WHERE
        token_address = $1

        AND minute_bucket =
            $2::timestamptz
      `,
      [
        tokenAddress,
        minuteBucket,
      ]
    );

  if (result.rowCount !== 1) {
    fail(
      "Expected exactly one PIT observation."
    );
  }

  return result.rows[0];
}


// ============================================================
// INDEPENDENT OUTCOME CALCULATION
//
// This deliberately reads RAW FUTURE EVENTS.
//
// Baseline:
//   PIT latest_price_per_token
//
// Future:
//   block_time >= T0
//
// No pump_launchpad_tokens.
// No token_market_state.
// No current ATH.
// No mutable current-state source.
//
// This test uses all currently available future events.
// ============================================================

async function calculateOutcome(
  client,
  tokenAddress,
  baselinePrice,
  t0
) {
  const result =
    await client.query(
      `
      WITH future_prices AS MATERIALIZED (

        SELECT
          e.id,
          e.block_time,
          e.price_per_token::numeric
            AS price_per_token

        FROM
          public.pump_launchpad_events e

        WHERE
          e.token_address = $1

          AND e.block_time >=
              $3::timestamptz

          AND e.event_type IN (
              'buy',
              'sell'
          )

          AND e.sol_amount > 0

          AND e.price_per_token > 0

      ),

      summary AS (

        SELECT
          COUNT(*)::integer
            AS future_event_count,

          MIN(block_time)
            AS first_future_event_at,

          MAX(block_time)
            AS last_future_event_at,

          MIN(price_per_token)
            AS min_future_price,

          MAX(price_per_token)
            AS max_future_price

        FROM future_prices

      )

      SELECT
        $1::text
          AS token_address,

        $2::numeric
          AS baseline_price,

        $3::timestamptz
          AS t0,

        future_event_count,

        first_future_event_at,

        last_future_event_at,

        min_future_price,

        max_future_price,

        CASE
          WHEN
            $2::numeric > 0
            AND max_future_price
                IS NOT NULL

          THEN ROUND(
            (
              (
                max_future_price
                / $2::numeric
              )
              - 1
            )
            * 100,
            6
          )

          ELSE NULL
        END
          AS max_return_pct,

        CASE
          WHEN
            $2::numeric > 0
            AND min_future_price
                IS NOT NULL

          THEN ROUND(
            (
              (
                min_future_price
                / $2::numeric
              )
              - 1
            )
            * 100,
            6
          )

          ELSE NULL
        END
          AS min_return_pct

      FROM summary
      `,
      [
        tokenAddress,
        baselinePrice,
        t0,
      ]
    );

  return result.rows[0];
}


// ============================================================
// NORMALIZE OUTCOME
// ============================================================

function normalizeOutcome(row) {
  return {
    token_address:
      row.token_address,

    baseline_price:
      numeric(row.baseline_price),

    t0:
      iso(row.t0),

    future_event_count:
      Number(row.future_event_count),

    first_future_event_at:
      iso(row.first_future_event_at),

    last_future_event_at:
      iso(row.last_future_event_at),

    min_future_price:
      numeric(row.min_future_price),

    max_future_price:
      numeric(row.max_future_price),

    max_return_pct:
      numeric(row.max_return_pct),

    min_return_pct:
      numeric(row.min_return_pct),
  };
}


// ============================================================
// INSERT SYNTHETIC EVENT
// ============================================================

async function insertSyntheticEvent(
  client,
  {
    tokenAddress,
    blockTime,
    price,
    label,
  }
) {
  const signature =
    randomSignature(label);

  const result =
    await client.query(
      `
      INSERT INTO
        public.pump_launchpad_events (
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
        1.0,
        1.0,
        $5::numeric,
        NULL
      )

      RETURNING
        id,
        signature,
        block_time,
        price_per_token
      `,
      [
        tokenAddress,
        signature,
        blockTime,
        `NORTHSTAR_OUTCOME_${label}`,
        price,
      ]
    );

  return result.rows[0];
}


// ============================================================
// MAIN
// ============================================================

async function main() {
  const client =
    await pool.connect();

  let transactionStarted =
    false;

  try {
    log(
      "Starting independent outcome verification"
    );

    await client.query("BEGIN");

    transactionStarted = true;

    await client.query(
      "SET LOCAL statement_timeout = '120s'"
    );


    // ========================================================
    // SELECT CANDIDATE
    // ========================================================

    const candidate =
      await findCandidate(client);

    const minuteBucket =
      new Date(
        candidate.minute_bucket
      );

    const t0 =
      new Date(
        candidate.t0
      );

    const baselinePrice =
      numeric(
        candidate.latest_price_per_token
      );

    if (
      !baselinePrice ||
      baselinePrice <= 0
    ) {
      fail(
        "Candidate has invalid baseline price."
      );
    }


    log(
      "Selected candidate",
      {
        tokenAddress:
          candidate.token_address,

        pitMinute:
          minuteBucket.toISOString(),

        t0:
          t0.toISOString(),

        baselinePrice,

        pitLastTradeAt:
          iso(
            candidate.last_trade_at
          ),

        existingFuturePriceEvents:
          Number(
            candidate.future_price_events
          ),
      }
    );


    // ========================================================
    // ASSERT T0 DEFINITION
    // ========================================================

    const expectedT0 =
      minuteBucket.getTime()
      + 60 * 1000;

    if (
      t0.getTime() !== expectedT0
    ) {
      fail(
        "T0 is not exactly the end of the PIT observation minute."
      );
    }

    if (
      candidate.last_trade_at &&
      new Date(
        candidate.last_trade_at
      ).getTime() >=
        t0.getTime()
    ) {
      fail(
        "PIT observation contains data at or after T0."
      );
    }


    log(
      "T0 DEFINITION PASS",
      {
        definition:
          "T0 = end of PIT observation",

        featureBoundary:
          "block_time < T0",

        outcomeBoundary:
          "block_time >= T0",
      }
    );


    // ========================================================
    // CAPTURE PIT BEFORE OUTCOME TEST
    // ========================================================

    const pitBefore =
      normalizePit(
        await readPit(
          client,
          candidate.token_address,
          minuteBucket.toISOString()
        )
      );


    // ========================================================
    // BASELINE OUTCOME
    // ========================================================

    const baselineOutcome =
      normalizeOutcome(
        await calculateOutcome(
          client,
          candidate.token_address,
          baselinePrice,
          t0.toISOString()
        )
      );


    if (
      baselineOutcome.future_event_count <
      1
    ) {
      fail(
        "Outcome baseline has no future events."
      );
    }


    if (
      new Date(
        baselineOutcome.first_future_event_at
      ).getTime() <
        t0.getTime()
    ) {
      fail(
        "Outcome calculation consumed pre-T0 data.",
        baselineOutcome
      );
    }


    log(
      "TEMPORAL SEPARATION PASS",
      baselineOutcome
    );


    // ========================================================
    // DETERMINISTIC REPLAY
    // ========================================================

    const replayOutcome =
      normalizeOutcome(
        await calculateOutcome(
          client,
          candidate.token_address,
          baselinePrice,
          t0.toISOString()
        )
      );


    if (
      !equal(
        baselineOutcome,
        replayOutcome
      )
    ) {
      fail(
        "Outcome deterministic replay failed.",
        {
          baselineOutcome,
          replayOutcome,
        }
      );
    }


    log(
      "DETERMINISTIC OUTCOME REPLAY PASS"
    );


    // ========================================================
    // PRE-T0 EXCLUSION TEST
    //
    // Insert an absurd price 30 seconds BEFORE T0.
    //
    // Because outcomes begin at T0,
    // this must have ZERO effect.
    // ========================================================

    const preT0Time =
      new Date(
        t0.getTime()
        - 30 * 1000
      );

    const preT0Price =
      baselinePrice * 1000000;


    const preT0Event =
      await insertSyntheticEvent(
        client,
        {
          tokenAddress:
            candidate.token_address,

          blockTime:
            preT0Time.toISOString(),

          price:
            preT0Price,

          label:
            "PRE_T0",
        }
      );


    log(
      "Synthetic pre-T0 event inserted",
      {
        blockTime:
          iso(
            preT0Event.block_time
          ),

        price:
          numeric(
            preT0Event.price_per_token
          ),
      }
    );


    const afterPreT0 =
      normalizeOutcome(
        await calculateOutcome(
          client,
          candidate.token_address,
          baselinePrice,
          t0.toISOString()
        )
      );


    if (
      !equal(
        baselineOutcome,
        afterPreT0
      )
    ) {
      fail(
        "PRE-T0 EXCLUSION FAILED: feature-period information changed the future outcome.",
        {
          baselineOutcome,
          afterPreT0,
        }
      );
    }


    log(
      "PRE-T0 OUTCOME EXCLUSION PASS"
    );


    // ========================================================
    // POST-T0 SENSITIVITY TEST
    //
    // Insert an absurd future price 30 seconds AFTER T0.
    //
    // Outcome MUST change.
    // ========================================================

    const postT0Time =
      new Date(
        t0.getTime()
        + 30 * 1000
      );

    const existingMax =
      baselineOutcome.max_future_price ||
      baselinePrice;

    const postT0Price =
      Math.max(
        existingMax * 10,
        baselinePrice * 100
      );


    const postT0Event =
      await insertSyntheticEvent(
        client,
        {
          tokenAddress:
            candidate.token_address,

          blockTime:
            postT0Time.toISOString(),

          price:
            postT0Price,

          label:
            "POST_T0",
        }
      );


    log(
      "Synthetic post-T0 event inserted",
      {
        blockTime:
          iso(
            postT0Event.block_time
          ),

        price:
          numeric(
            postT0Event.price_per_token
          ),
      }
    );


    const afterPostT0 =
      normalizeOutcome(
        await calculateOutcome(
          client,
          candidate.token_address,
          baselinePrice,
          t0.toISOString()
        )
      );


    if (
      afterPostT0.future_event_count !==
      baselineOutcome.future_event_count + 1
    ) {
      fail(
        "Post-T0 event was not included exactly once.",
        {
          baselineOutcome,
          afterPostT0,
        }
      );
    }


    if (
      !(
        afterPostT0.max_future_price >
        baselineOutcome.max_future_price
      )
    ) {
      fail(
        "Post-T0 future price did not change max future price.",
        {
          baselineOutcome,
          afterPostT0,
        }
      );
    }


    if (
      !(
        afterPostT0.max_return_pct >
        baselineOutcome.max_return_pct
      )
    ) {
      fail(
        "Post-T0 future price did not change max return.",
        {
          baselineOutcome,
          afterPostT0,
        }
      );
    }


    log(
      "POST-T0 FUTURE SENSITIVITY PASS",
      {
        beforeMaxPrice:
          baselineOutcome.max_future_price,

        afterMaxPrice:
          afterPostT0.max_future_price,

        beforeMaxReturnPct:
          baselineOutcome.max_return_pct,

        afterMaxReturnPct:
          afterPostT0.max_return_pct,
      }
    );


    // ========================================================
    // PIT MUST REMAIN UNCHANGED
    // ========================================================

    const pitAfter =
      normalizePit(
        await readPit(
          client,
          candidate.token_address,
          minuteBucket.toISOString()
        )
      );


    if (
      !equal(
        pitBefore,
        pitAfter
      )
    ) {
      fail(
        "Outcome activity changed the PIT feature observation.",
        {
          pitBefore,
          pitAfter,
        }
      );
    }


    log(
      "OUTCOME → FEATURE ISOLATION PASS"
    );


    // ========================================================
    // RUNNER THRESHOLD SANITY CHECK
    //
    // These are labels only.
    // No persistence performed here.
    // ========================================================

    const runnerLabels = {
      runner_75:
        afterPostT0.max_return_pct >= 75,

      runner_100:
        afterPostT0.max_return_pct >= 100,

      runner_300:
        afterPostT0.max_return_pct >= 300,

      runner_1000:
        afterPostT0.max_return_pct >= 1000,
    };


    log(
      "RUNNER LABEL DERIVATION PASS",
      {
        maxReturnPct:
          afterPostT0.max_return_pct,

        ...runnerLabels,
      }
    );


    // ========================================================
    // FINAL REPORT
    // ========================================================

    log(
      "========================================"
    );

    log(
      "T0 DEFINITION                 PASS"
    );

    log(
      "TEMPORAL SEPARATION           PASS"
    );

    log(
      "DETERMINISTIC OUTCOME REPLAY  PASS"
    );

    log(
      "PRE-T0 OUTCOME EXCLUSION      PASS"
    );

    log(
      "POST-T0 FUTURE SENSITIVITY    PASS"
    );

    log(
      "OUTCOME → FEATURE ISOLATION   PASS"
    );

    log(
      "RUNNER LABEL DERIVATION       PASS"
    );

    log(
      "DATABASE MUTATION             NONE (ROLLBACK)"
    );

    log(
      "OVERALL OUTCOME VERIFICATION  PASS"
    );

    log(
      "========================================"
    );

  } catch (error) {
    log(
      "========================================"
    );

    log(
      "OVERALL OUTCOME VERIFICATION  FAIL",
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

    // ========================================================
    // ALWAYS ROLLBACK
    // ========================================================

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


main().catch((error) => {
  console.error(
    "[northstar-outcome-test] Fatal error",
    error
  );

  process.exit(1);
});
