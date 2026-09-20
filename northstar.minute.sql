-- NORTH STAR minute worker SQL v0.2
-- $1 inclusive window start, $2 exclusive window end; both UTC timestamptz.
-- Run ONLY through checkpointed worker, after fixing its SET LOCAL statement.
WITH bounds AS MATERIALIZED (
 SELECT $1::timestamptz AS window_start, $2::timestamptz AS window_end
 WHERE $1::timestamptz < $2::timestamptz
   AND $1::timestamptz = date_trunc('minute',$1::timestamptz)
   AND $2::timestamptz = date_trunc('minute',$2::timestamptz)
   AND $2::timestamptz - $1::timestamptz <= interval '10 minutes'
),
events AS MATERIALIZED (
 SELECT e.id, e.token_address, date_trunc('minute',e.block_time) AS minute_bucket,
        e.block_time,e.event_type,e.sol_amount::numeric AS sol_amount,
        nullif(btrim(e.wallet_address),'') AS wallet_address,
        nullif(e.market_cap_usd::numeric,0) AS direct_market_cap_usd,
        nullif(e.price_per_token::numeric,0) AS price_per_token_sol,
        nullif(e.sol_price_usd::numeric,0) AS sol_price_usd
 FROM public.pump_launchpad_events e JOIN bounds b
 ON e.block_time >= b.window_start AND e.block_time < b.window_end
 WHERE e.event_type IN ('buy','sell') AND e.sol_amount > 0
   AND e.token_address IS NOT NULL AND btrim(e.token_address) <> ''
),
trades AS MATERIALIZED (
 SELECT token_address,minute_bucket,
   coalesce(sum(sol_amount) FILTER (WHERE event_type='buy'),0)::numeric AS buy_volume_sol,
   coalesce(sum(sol_amount) FILTER (WHERE event_type='sell'),0)::numeric AS sell_volume_sol,
   sum(sol_amount)::numeric AS total_volume_sol,
   coalesce(max(sol_amount) FILTER (WHERE event_type='buy'),0)::numeric AS max_buy_size_sol,
   coalesce(max(sol_amount) FILTER (WHERE event_type='sell'),0)::numeric AS max_sell_size_sol,
   count(*) FILTER (WHERE event_type='buy')::integer AS buy_count,
   count(*) FILTER (WHERE event_type='sell')::integer AS sell_count,
   count(*)::integer AS trade_count,
   min(block_time) AS first_trade_at,max(block_time) AS last_trade_at,
   (array_agg(direct_market_cap_usd ORDER BY block_time DESC,id DESC)
      FILTER (WHERE direct_market_cap_usd > 0))[1]::numeric AS direct_market_cap_usd,
   (array_agg(price_per_token_sol ORDER BY block_time DESC,id DESC)
      FILTER (WHERE price_per_token_sol > 0))[1]::numeric AS latest_price_per_token,
   (array_agg(sol_price_usd ORDER BY block_time DESC,id DESC)
      FILTER (WHERE sol_price_usd > 0))[1]::numeric AS latest_sol_price_usd
 FROM events GROUP BY token_address,minute_bucket
),
wallet_flags AS MATERIALIZED (
 SELECT token_address,minute_bucket,wallet_address,
 bool_or(event_type='buy') AS bought,bool_or(event_type='sell') AS sold
 FROM events WHERE wallet_address IS NOT NULL
 GROUP BY token_address,minute_bucket,wallet_address
),
wallets AS MATERIALIZED (
 SELECT token_address,minute_bucket,
 count(*) FILTER (WHERE bought)::integer AS unique_buyers,
 count(*) FILTER (WHERE sold)::integer AS unique_sellers,
 count(*)::integer AS unique_wallets
 FROM wallet_flags GROUP BY token_address,minute_bucket
),
prepared AS MATERIALIZED (
 SELECT t.token_address,t.minute_bucket,t.buy_volume_sol,t.sell_volume_sol,
 t.total_volume_sol,t.max_buy_size_sol,t.max_sell_size_sol,
 (t.max_buy_size_sol-t.max_sell_size_sol)::numeric AS net_max_buy_size_sol,
 CASE WHEN t.max_sell_size_sol>0 THEN round(t.max_buy_size_sol/t.max_sell_size_sol,6)
      WHEN t.max_buy_size_sol>0 THEN 99::numeric ELSE 0::numeric END AS max_buy_to_sell_size_ratio,
 -- USD valuation ONLY from a direct positive USD-denominated source.
 -- price_per_token is SOL/token, NOT USD/token. No inferred USD conversion.
 t.direct_market_cap_usd AS latest_market_cap_usd,
 t.latest_price_per_token,t.latest_sol_price_usd,
 t.buy_count,t.sell_count,t.trade_count,
 coalesce(w.unique_buyers,0)::integer AS unique_buyers,
 coalesce(w.unique_sellers,0)::integer AS unique_sellers,
 coalesce(w.unique_wallets,0)::integer AS unique_wallets,
 t.first_trade_at,t.last_trade_at
 FROM trades t LEFT JOIN wallets w USING(token_address,minute_bucket)
),
upserted AS (
 INSERT INTO public.northstar_token_volume_minutes AS target (
 token_address,minute_bucket,buy_volume_sol,sell_volume_sol,total_volume_sol,
 max_buy_size_sol,max_sell_size_sol,net_max_buy_size_sol,max_buy_to_sell_size_ratio,
 latest_market_cap_usd,latest_price_per_token,latest_sol_price_usd,
 buy_count,sell_count,trade_count,unique_buyers,unique_sellers,unique_wallets,
 first_trade_at,last_trade_at,updated_at)
 SELECT token_address,minute_bucket,buy_volume_sol,sell_volume_sol,total_volume_sol,
 max_buy_size_sol,max_sell_size_sol,net_max_buy_size_sol,max_buy_to_sell_size_ratio,
 latest_market_cap_usd,latest_price_per_token,latest_sol_price_usd,
 buy_count,sell_count,trade_count,unique_buyers,unique_sellers,unique_wallets,
 first_trade_at,last_trade_at,clock_timestamp() FROM prepared
 ON CONFLICT(token_address,minute_bucket) DO UPDATE SET
 buy_volume_sol=excluded.buy_volume_sol,sell_volume_sol=excluded.sell_volume_sol,
 total_volume_sol=excluded.total_volume_sol,max_buy_size_sol=excluded.max_buy_size_sol,
 max_sell_size_sol=excluded.max_sell_size_sol,net_max_buy_size_sol=excluded.net_max_buy_size_sol,
 max_buy_to_sell_size_ratio=excluded.max_buy_to_sell_size_ratio,
 -- Deliberately clear prior potentially mis-denominated USD values in touched minutes.
 latest_market_cap_usd=excluded.latest_market_cap_usd,
 latest_price_per_token=excluded.latest_price_per_token,
 latest_sol_price_usd=excluded.latest_sol_price_usd,
 buy_count=excluded.buy_count,sell_count=excluded.sell_count,trade_count=excluded.trade_count,
 unique_buyers=excluded.unique_buyers,unique_sellers=excluded.unique_sellers,
 unique_wallets=excluded.unique_wallets,
 first_trade_at=excluded.first_trade_at,last_trade_at=excluded.last_trade_at,
 updated_at=clock_timestamp()
 WHERE (target.buy_volume_sol,target.sell_volume_sol,target.total_volume_sol,
 target.max_buy_size_sol,target.max_sell_size_sol,target.net_max_buy_size_sol,
 target.max_buy_to_sell_size_ratio,target.latest_market_cap_usd,
 target.latest_price_per_token,target.latest_sol_price_usd,
 target.buy_count,target.sell_count,target.trade_count,
 target.unique_buyers,target.unique_sellers,target.unique_wallets,
 target.first_trade_at,target.last_trade_at)
 IS DISTINCT FROM
 (excluded.buy_volume_sol,excluded.sell_volume_sol,excluded.total_volume_sol,
 excluded.max_buy_size_sol,excluded.max_sell_size_sol,excluded.net_max_buy_size_sol,
 excluded.max_buy_to_sell_size_ratio,excluded.latest_market_cap_usd,
 excluded.latest_price_per_token,excluded.latest_sol_price_usd,
 excluded.buy_count,excluded.sell_count,excluded.trade_count,
 excluded.unique_buyers,excluded.unique_sellers,excluded.unique_wallets,
 excluded.first_trade_at,excluded.last_trade_at)
 RETURNING token_address,minute_bucket
)
SELECT (SELECT count(*)::integer FROM events) AS qualifying_events,
       (SELECT count(*)::integer FROM prepared) AS minutes_prepared,
       (SELECT count(*)::integer FROM upserted) AS minutes_written,
       (SELECT count(DISTINCT token_address)::integer FROM prepared) AS tokens_prepared,
       (SELECT count(*)::integer FROM prepared WHERE latest_market_cap_usd > 0) AS minutes_with_direct_usd_market_cap,
       (SELECT count(*)::integer FROM prepared WHERE latest_price_per_token > 0) AS minutes_with_sol_token_price,
       (SELECT max(last_trade_at) FROM prepared) AS freshest_trade_at;
