
-- ============================================================
-- NORTH STAR MINUTE WORKER SQL v0.5
-- USD PRESERVATION + TARGETED STALE-ROW RECONCILIATION
--
-- Changes from v0.4:
-- 1. Identify token-minute keys with malformed sell events.
-- 2. Delete an existing aggregate ONLY when that key has
--    suspicious sells but zero remaining eligible events.
-- 3. Return minutes_deleted diagnostic.
-- All other aggregation and USD-preservation logic unchanged.
-- Does NOT delete arbitrary rows absent from source events.
-- IMPORTANT: Deletion permanently removes that row's USD fields.
-- Deploy only after a controlled test and explicit approval.
--
-- New rows:
--   USD fields remain NULL until populated by a
--   separately verified USD data source.
--
-- Existing rows:
--   Previously populated USD values are preserved.
--
-- Worker interface:
--   $1 = window_start (inclusive)
--   $2 = window_end   (exclusive)
--
-- No changes to event eligibility, aggregation,
-- minute boundaries, or returned diagnostics.
-- ============================================================

WITH bounds AS MATERIALIZED (
    SELECT
        $1::timestamptz AS window_start,
        $2::timestamptz AS window_end
    WHERE
        $1::timestamptz < $2::timestamptz
        AND $1::timestamptz =
            date_trunc('minute', $1::timestamptz)
        AND $2::timestamptz =
            date_trunc('minute', $2::timestamptz)
        AND $2::timestamptz - $1::timestamptz
            <= INTERVAL '10 minutes'
),

events AS MATERIALIZED (
    SELECT
        e.id,
        e.token_address,
        date_trunc('minute', e.block_time) AS minute_bucket,
        e.block_time,
        e.event_type,
        e.sol_amount::numeric AS sol_amount,
        NULLIF(BTRIM(e.wallet_address), '') AS wallet_address,
        NULLIF(e.price_per_token::numeric, 0)
            AS price_per_token_sol

    FROM public.pump_launchpad_events e

    JOIN bounds b
        ON e.block_time >= b.window_start
       AND e.block_time < b.window_end

   WHERE
    e.event_type IN ('buy', 'sell')
    AND e.sol_amount > 0
    AND e.token_address IS NOT NULL
    AND BTRIM(e.token_address) <> ''

    -- Exclude suspicious malformed sell events.
       -- Exclude suspicious malformed sell events.
    AND (
        e.event_type <> 'sell'
        OR (
            e.sol_amount::numeric = e.token_amount::numeric
            AND e.price_per_token::numeric = 1
        ) IS NOT TRUE
    )
),


-- Target only token-minute keys known to contain malformed sells.
-- Do not interpret arbitrary absence from prepared as permission
-- to delete a row: source ingestion may be incomplete.
suspicious_keys AS MATERIALIZED (
    SELECT DISTINCT
        e.token_address,
        date_trunc('minute', e.block_time) AS minute_bucket
    FROM public.pump_launchpad_events e
    JOIN bounds b
      ON e.block_time >= b.window_start
     AND e.block_time < b.window_end
    WHERE e.event_type = 'sell'
      AND e.sol_amount > 0
      AND e.token_address IS NOT NULL
      AND BTRIM(e.token_address) <> ''
      AND e.sol_amount::numeric = e.token_amount::numeric
      AND e.price_per_token::numeric = 1
),

trades AS MATERIALIZED (
    SELECT
        token_address,
        minute_bucket,

        COALESCE(
            SUM(sol_amount) FILTER (
                WHERE event_type = 'buy'
            ),
            0
        )::numeric AS buy_volume_sol,

        COALESCE(
            SUM(sol_amount) FILTER (
                WHERE event_type = 'sell'
            ),
            0
        )::numeric AS sell_volume_sol,

        SUM(sol_amount)::numeric AS total_volume_sol,

        COALESCE(
            MAX(sol_amount) FILTER (
                WHERE event_type = 'buy'
            ),
            0
        )::numeric AS max_buy_size_sol,

        COALESCE(
            MAX(sol_amount) FILTER (
                WHERE event_type = 'sell'
            ),
            0
        )::numeric AS max_sell_size_sol,

        COUNT(*) FILTER (
            WHERE event_type = 'buy'
        )::integer AS buy_count,

        COUNT(*) FILTER (
            WHERE event_type = 'sell'
        )::integer AS sell_count,

        COUNT(*)::integer AS trade_count,

        MIN(block_time) AS first_trade_at,
        MAX(block_time) AS last_trade_at,

        (
            ARRAY_AGG(
                price_per_token_sol
                ORDER BY block_time DESC, id DESC
            ) FILTER (
                WHERE price_per_token_sol > 0
            )
        )[1]::numeric AS latest_price_per_token

    FROM events

    GROUP BY
        token_address,
        minute_bucket
),

wallet_flags AS MATERIALIZED (
    SELECT
        token_address,
        minute_bucket,
        wallet_address,

        BOOL_OR(event_type = 'buy') AS bought,
        BOOL_OR(event_type = 'sell') AS sold

    FROM events

    WHERE wallet_address IS NOT NULL

    GROUP BY
        token_address,
        minute_bucket,
        wallet_address
),

wallets AS MATERIALIZED (
    SELECT
        token_address,
        minute_bucket,

        COUNT(*) FILTER (
            WHERE bought
        )::integer AS unique_buyers,

        COUNT(*) FILTER (
            WHERE sold
        )::integer AS unique_sellers,

        COUNT(*)::integer AS unique_wallets

    FROM wallet_flags

    GROUP BY
        token_address,
        minute_bucket
),

prepared AS MATERIALIZED (
    SELECT
        t.token_address,
        t.minute_bucket,

        t.buy_volume_sol,
        t.sell_volume_sol,
        t.total_volume_sol,

        t.max_buy_size_sol,
        t.max_sell_size_sol,

        (
            t.max_buy_size_sol - t.max_sell_size_sol
        )::numeric AS net_max_buy_size_sol,

        CASE
            WHEN t.max_sell_size_sol > 0
                THEN ROUND(
                    t.max_buy_size_sol
                    / t.max_sell_size_sol,
                    6
                )

            WHEN t.max_buy_size_sol > 0
                THEN 99::numeric

            ELSE 0::numeric

        END AS max_buy_to_sell_size_ratio,

        -- USD market cap is not calculated here.
        -- Existing values are preserved on conflict.
        NULL::numeric AS latest_market_cap_usd,

        -- SOL per token, NOT USD per token.
        t.latest_price_per_token,

        -- SOL/USD price is not calculated here.
        -- Existing values are preserved on conflict.
        NULL::numeric AS latest_sol_price_usd,

        t.buy_count,
        t.sell_count,
        t.trade_count,

        COALESCE(
            w.unique_buyers,
            0
        )::integer AS unique_buyers,

        COALESCE(
            w.unique_sellers,
            0
        )::integer AS unique_sellers,

        COALESCE(
            w.unique_wallets,
            0
        )::integer AS unique_wallets,

        t.first_trade_at,
        t.last_trade_at

    FROM trades t

    LEFT JOIN wallets w
        USING (
            token_address,
            minute_bucket
        )
),

upserted AS (
    INSERT INTO public.northstar_token_volume_minutes AS target (
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
    )

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

        clock_timestamp()

    FROM prepared

    ON CONFLICT (
        token_address,
        minute_bucket
    )

    DO UPDATE SET
        buy_volume_sol =
            EXCLUDED.buy_volume_sol,

        sell_volume_sol =
            EXCLUDED.sell_volume_sol,

        total_volume_sol =
            EXCLUDED.total_volume_sol,

        max_buy_size_sol =
            EXCLUDED.max_buy_size_sol,

        max_sell_size_sol =
            EXCLUDED.max_sell_size_sol,

        net_max_buy_size_sol =
            EXCLUDED.net_max_buy_size_sol,

        max_buy_to_sell_size_ratio =
            EXCLUDED.max_buy_to_sell_size_ratio,

        -- v0.4: Preserve existing USD market cap.
        latest_market_cap_usd =
            COALESCE(
                EXCLUDED.latest_market_cap_usd,
                target.latest_market_cap_usd
            ),

        latest_price_per_token =
            EXCLUDED.latest_price_per_token,

        -- v0.4: Preserve existing SOL/USD price.
        latest_sol_price_usd =
            COALESCE(
                EXCLUDED.latest_sol_price_usd,
                target.latest_sol_price_usd
            ),

        buy_count =
            EXCLUDED.buy_count,

        sell_count =
            EXCLUDED.sell_count,

        trade_count =
            EXCLUDED.trade_count,

        unique_buyers =
            EXCLUDED.unique_buyers,

        unique_sellers =
            EXCLUDED.unique_sellers,

        unique_wallets =
            EXCLUDED.unique_wallets,

        first_trade_at =
            EXCLUDED.first_trade_at,

        last_trade_at =
            EXCLUDED.last_trade_at,

        updated_at =
            clock_timestamp()

    WHERE (
        target.buy_volume_sol,
        target.sell_volume_sol,
        target.total_volume_sol,

        target.max_buy_size_sol,
        target.max_sell_size_sol,
        target.net_max_buy_size_sol,
        target.max_buy_to_sell_size_ratio,

        target.latest_market_cap_usd,
        target.latest_price_per_token,
        target.latest_sol_price_usd,

        target.buy_count,
        target.sell_count,
        target.trade_count,

        target.unique_buyers,
        target.unique_sellers,
        target.unique_wallets,

        target.first_trade_at,
        target.last_trade_at
    )

    IS DISTINCT FROM

    (
        EXCLUDED.buy_volume_sol,
        EXCLUDED.sell_volume_sol,
        EXCLUDED.total_volume_sol,

        EXCLUDED.max_buy_size_sol,
        EXCLUDED.max_sell_size_sol,
        EXCLUDED.net_max_buy_size_sol,
        EXCLUDED.max_buy_to_sell_size_ratio,

        -- v0.4: Compare against preserved USD value.
        COALESCE(
            EXCLUDED.latest_market_cap_usd,
            target.latest_market_cap_usd
        ),

        EXCLUDED.latest_price_per_token,

        -- v0.4: Compare against preserved USD value.
        COALESCE(
            EXCLUDED.latest_sol_price_usd,
            target.latest_sol_price_usd
        ),

        EXCLUDED.buy_count,
        EXCLUDED.sell_count,
        EXCLUDED.trade_count,

        EXCLUDED.unique_buyers,
        EXCLUDED.unique_sellers,
        EXCLUDED.unique_wallets,

        EXCLUDED.first_trade_at,
        EXCLUDED.last_trade_at
    )

    RETURNING
        token_address,
        minute_bucket
),

-- Reconcile ONLY a known malformed-only token-minute key.
-- Never delete a key for which at least one eligible trade exists.
-- This deliberately avoids a blanket delete of missing source keys.
reconciled_deletes AS (
    DELETE FROM public.northstar_token_volume_minutes AS target
    USING suspicious_keys AS s
    WHERE target.token_address = s.token_address
      AND target.minute_bucket = s.minute_bucket
      AND NOT EXISTS (
          SELECT 1
          FROM prepared AS p
          WHERE p.token_address = s.token_address
            AND p.minute_bucket = s.minute_bucket
      )
    RETURNING
        target.token_address,
        target.minute_bucket
)
SELECT
    (
        SELECT COUNT(*)::integer
        FROM events
    ) AS qualifying_events,

    (
        SELECT COUNT(*)::integer
        FROM prepared
    ) AS minutes_prepared,

    (
        SELECT COUNT(*)::integer
        FROM upserted
    ) AS minutes_written,

    (
        SELECT COUNT(*)::integer
        FROM reconciled_deletes
    ) AS minutes_deleted,

    (
        SELECT COUNT(DISTINCT token_address)::integer
        FROM prepared
    ) AS tokens_prepared,

    -- Counts USD market caps directly produced by this
    -- worker, not pre-existing values in the target.
    -- Always zero under the current NULL-USD policy.
    (
        SELECT COUNT(*)::integer
        FROM prepared
        WHERE latest_market_cap_usd > 0
    ) AS minutes_with_direct_usd_market_cap,

    (
        SELECT COUNT(*)::integer
        FROM prepared
        WHERE latest_price_per_token > 0
    ) AS minutes_with_sol_token_price,

    (
        SELECT MAX(last_trade_at)
        FROM prepared
    ) AS freshest_trade_at;
