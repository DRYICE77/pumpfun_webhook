-- ============================================================
-- NORTH STAR FORWARD OUTCOME SQL v0.1
-- ============================================================
--
-- PURPOSE
-- -------
-- Calculate independent forward outcomes for one PIT observation.
--
-- TEMPORAL CONTRACT
-- -----------------
-- T0 = END of the PIT observation.
--
-- Example:
--
--   PIT minute:
--       [05:43:00, 05:44:00)
--
--   T0:
--       05:44:00
--
--   PIT information:
--       block_time < T0
--
--   Outcome information:
--       block_time >= T0
--
-- Therefore:
--
--   FEATURES: event.block_time < T0
--   OUTCOMES: event.block_time >= T0
--
-- No event can belong to both sides.
--
--
-- INPUTS
-- ------
-- $1 = token_address
-- $2 = observation_minute
--      Minute bucket of the PIT observation.
--
-- $3 = outcome_cutoff
--      Exclusive upper bound for future outcome measurement.
--
-- Example:
--
--   $1 = token mint
--   $2 = 2026-09-23 05:43:00+00
--   $3 = 2026-09-23 07:44:00+00
--
--
-- BASELINE
-- --------
-- Baseline price comes ONLY from:
--
--   northstar_token_volume_minutes.latest_price_per_token
--
-- for the exact PIT observation minute.
--
-- T0 is:
--
--   observation_minute + 1 minute
--
--
-- FUTURE SOURCE
-- -------------
-- Future outcome prices come ONLY from:
--
--   public.pump_launchpad_events
--
-- where:
--
--   block_time >= T0
--   block_time < outcome_cutoff
--
--
-- PRICE UNIT
-- ----------
-- price_per_token is SOL per token.
--
-- All returns are therefore percentage changes in
-- SOL-per-token price.
--
-- No USD conversion is performed.
--
--
-- SAFETY / RESEARCH RULES
-- -----------------------
-- • No mutable current-state token table.
-- • No token_market_state ATH.
-- • No current market cap.
-- • No current token price.
-- • No future data may affect baseline.
-- • No pre-T0 event may affect outcomes.
-- • Missing outcome evidence remains NULL.
-- • Runner flags remain NULL when outcome cannot be measured.
--
-- ============================================================


WITH


-- ============================================================
-- 1. INPUT PARAMETERS
-- ============================================================

params AS MATERIALIZED (

    SELECT

        NULLIF(
            BTRIM($1::text),
            ''
        ) AS token_address,

        $2::timestamptz
            AS observation_minute,

        (
            $2::timestamptz
            + INTERVAL '1 minute'
        ) AS t0,

        $3::timestamptz
            AS outcome_cutoff

),


-- ============================================================
-- 2. VALIDATE INPUT BOUNDS
--
-- Require:
--
-- observation_minute is minute aligned
-- outcome_cutoff > T0
-- ============================================================

valid_params AS MATERIALIZED (

    SELECT
        p.*

    FROM params AS p

    WHERE

        p.token_address
            IS NOT NULL

        AND p.observation_minute =
            date_trunc(
                'minute',
                p.observation_minute
            )

        AND p.outcome_cutoff >
            p.t0

),


-- ============================================================
-- 3. PIT BASELINE
--
-- This is the ONLY source of entry/baseline price.
--
-- We deliberately do NOT look backward through raw events
-- here.
--
-- If the PIT observation does not contain a valid price,
-- the outcome is not measurable.
-- ============================================================

baseline AS MATERIALIZED (

    SELECT

        vp.token_address,

        vp.observation_minute,

        vp.t0,

        vp.outcome_cutoff,

        m.latest_price_per_token::numeric
            AS baseline_price_sol,

        m.last_trade_at
            AS baseline_last_trade_at

    FROM valid_params AS vp

    JOIN public.northstar_token_volume_minutes AS m

      ON m.token_address =
            vp.token_address

     AND m.minute_bucket =
            vp.observation_minute

    WHERE

        m.latest_price_per_token
            IS NOT NULL

        AND m.latest_price_per_token > 0

        -- Defensive temporal assertion.
        AND m.last_trade_at < vp.t0

),


-- ============================================================
-- 4. FUTURE PRICE EVENTS
--
-- STRICT TEMPORAL SEPARATION:
--
--     block_time >= T0
--
-- The upper bound is exclusive:
--
--     block_time < outcome_cutoff
--
-- Therefore the outcome interval is:
--
--     [T0, outcome_cutoff)
--
-- ============================================================

future_events AS MATERIALIZED (

    SELECT

        e.id,

        e.token_address,

        e.block_time,

        e.event_type,

        NULLIF(
            e.price_per_token::numeric,
            0
        ) AS price_per_token_sol

    FROM public.pump_launchpad_events AS e

    JOIN baseline AS b

      ON e.token_address =
            b.token_address

     AND e.block_time >=
            b.t0

     AND e.block_time <
            b.outcome_cutoff

    WHERE

        e.event_type IN (
            'buy',
            'sell'
        )

        AND e.price_per_token
            IS NOT NULL

        AND e.price_per_token > 0

),


-- ============================================================
-- 5. FUTURE PRICE SUMMARY
-- ============================================================

future_summary AS MATERIALIZED (

    SELECT

        b.token_address,

        b.observation_minute,

        b.t0,

        b.outcome_cutoff,

        b.baseline_price_sol,

        b.baseline_last_trade_at,

        COUNT(fe.id)::integer
            AS future_price_event_count,

        MIN(fe.block_time)
            AS first_future_trade_at,

        MAX(fe.block_time)
            AS last_future_trade_at,

        MAX(fe.price_per_token_sol)
            AS max_future_price_sol,

        MIN(fe.price_per_token_sol)
            AS min_future_price_sol

    FROM baseline AS b

    LEFT JOIN future_events AS fe

      ON fe.token_address =
            b.token_address

    GROUP BY

        b.token_address,

        b.observation_minute,

        b.t0,

        b.outcome_cutoff,

        b.baseline_price_sol,

        b.baseline_last_trade_at

),


-- ============================================================
-- 6. MAXIMUM FORWARD RETURN
--
-- Formula:
--
-- ((future_max / baseline) - 1) * 100
--
-- Example:
--
-- baseline = 1
-- future max = 2
--
-- max_return_pct = 100
-- ============================================================

returns AS MATERIALIZED (

    SELECT

        fs.*,

        CASE

            WHEN
                fs.future_price_event_count > 0

                AND fs.max_future_price_sol > 0

                AND fs.baseline_price_sol > 0

            THEN ROUND(

                (
                    (
                        fs.max_future_price_sol
                        /
                        fs.baseline_price_sol
                    )
                    - 1
                )
                * 100,

                6

            )

            ELSE NULL::numeric

        END AS max_return_pct,


        CASE

            WHEN
                fs.future_price_event_count > 0

                AND fs.min_future_price_sol > 0

                AND fs.baseline_price_sol > 0

            THEN ROUND(

                (
                    (
                        fs.min_future_price_sol
                        /
                        fs.baseline_price_sol
                    )
                    - 1
                )
                * 100,

                6

            )

            ELSE NULL::numeric

        END AS min_return_pct

    FROM future_summary AS fs

),


-- ============================================================
-- 7. RUNNER LABELS
--
-- IMPORTANT:
--
-- NULL outcome evidence produces NULL labels.
--
-- FALSE means:
--
--     We measured the outcome window and the token did not
--     reach the threshold.
--
-- NULL means:
--
--     We do not have sufficient future price evidence.
--
-- This distinction is essential for research integrity.
-- ============================================================

labeled AS MATERIALIZED (

    SELECT

        r.*,


        CASE

            WHEN r.max_return_pct
                IS NULL

            THEN NULL::boolean

            ELSE
                r.max_return_pct >= 75

        END AS runner_75,


        CASE

            WHEN r.max_return_pct
                IS NULL

            THEN NULL::boolean

            ELSE
                r.max_return_pct >= 100

        END AS runner_100,


        CASE

            WHEN r.max_return_pct
                IS NULL

            THEN NULL::boolean

            ELSE
                r.max_return_pct >= 300

        END AS runner_300,


        CASE

            WHEN r.max_return_pct
                IS NULL

            THEN NULL::boolean

            ELSE
                r.max_return_pct >= 1000

        END AS runner_1000

    FROM returns AS r

),


-- ============================================================
-- 8. EVENT THAT PRODUCED MAX FUTURE PRICE
--
-- Deterministic tie-breaking:
--
-- earliest block_time
-- then lowest event id
--
-- ============================================================

max_event AS MATERIALIZED (

    SELECT DISTINCT ON (
        fe.token_address
    )

        fe.token_address,

        fe.block_time
            AS max_price_at,

        fe.id
            AS max_price_event_id

    FROM future_events AS fe

    JOIN labeled AS l

      ON l.token_address =
            fe.token_address

     AND fe.price_per_token_sol =
            l.max_future_price_sol

    ORDER BY

        fe.token_address,

        fe.block_time ASC,

        fe.id ASC

),


-- ============================================================
-- 9. EVENT THAT PRODUCED MIN FUTURE PRICE
-- ============================================================

min_event AS MATERIALIZED (

    SELECT DISTINCT ON (
        fe.token_address
    )

        fe.token_address,

        fe.block_time
            AS min_price_at,

        fe.id
            AS min_price_event_id

    FROM future_events AS fe

    JOIN labeled AS l

      ON l.token_address =
            fe.token_address

     AND fe.price_per_token_sol =
            l.min_future_price_sol

    ORDER BY

        fe.token_address,

        fe.block_time ASC,

        fe.id ASC

)


-- ============================================================
-- 10. FINAL OUTCOME RECORD
-- ============================================================

SELECT

    l.token_address,

    l.observation_minute,

    l.t0,

    l.outcome_cutoff,


    -- --------------------------------------------------------
    -- BASELINE PROVENANCE
    -- --------------------------------------------------------

    l.baseline_price_sol,

    l.baseline_last_trade_at,


    -- --------------------------------------------------------
    -- FUTURE EVIDENCE
    -- --------------------------------------------------------

    l.future_price_event_count,

    l.first_future_trade_at,

    l.last_future_trade_at,


    -- --------------------------------------------------------
    -- PRICE EXTREMES
    -- --------------------------------------------------------

    l.max_future_price_sol,

    mx.max_price_at,

    mx.max_price_event_id,

    l.min_future_price_sol,

    mn.min_price_at,

    mn.min_price_event_id,


    -- --------------------------------------------------------
    -- RETURNS
    -- --------------------------------------------------------

    l.max_return_pct,

    l.min_return_pct,


    -- --------------------------------------------------------
    -- RESEARCH LABELS
    -- --------------------------------------------------------

    l.runner_75,

    l.runner_100,

    l.runner_300,

    l.runner_1000,


    -- --------------------------------------------------------
    -- AUDIT / PROVENANCE
    -- --------------------------------------------------------

    'northstar.outcome.sql:v0.1'::text
        AS outcome_calculator_version,

    clock_timestamp()
        AS calculated_at


FROM labeled AS l

LEFT JOIN max_event AS mx
  ON mx.token_address =
        l.token_address

LEFT JOIN min_event AS mn
  ON mn.token_address =
        l.token_address;
