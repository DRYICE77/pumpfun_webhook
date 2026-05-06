require("dotenv").config();
const { Pool } = require("pg");

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl:
    process.env.PGSSLMODE === "disable"
      ? false
      : { rejectUnauthorized: false },
});

const APIFY_API_TOKEN = process.env.APIFY_API_TOKEN || "";

// Example: apidojo/twitter-scraper-lite
const APIFY_ACTOR_ID =
  process.env.APIFY_ACTOR_ID || "apidojo/twitter-scraper-lite";

const TOKEN_LIMIT = Number(process.env.X_TOKEN_LIMIT || 10);
const MAX_TWEETS_PER_SEARCH = Number(process.env.X_MAX_TWEETS_PER_SEARCH || 5);
const SNAPSHOT_WINDOW_MINUTES = Number(process.env.X_SNAPSHOT_WINDOW_MINUTES || 10);
const LOOP_SLEEP_MS = Number(process.env.X_LOOP_SLEEP_MS || 60000);
const RESCRAPE_COOLDOWN_MINUTES = Number(process.env.X_RESCRAPE_COOLDOWN_MINUTES || 5);
const PER_TOKEN_SLEEP_MS = Number(process.env.X_PER_TOKEN_SLEEP_MS || 1500);

const MIN_MARKET_CAP_USD = Number(process.env.X_MIN_MARKET_CAP_USD || 8000);
const MAX_MARKET_CAP_USD = Number(process.env.X_MAX_MARKET_CAP_USD || 75000);
const MIN_BUYS_5M = Number(process.env.X_MIN_BUYS_5M || 3);
const MIN_VOLUME_5M = Number(process.env.X_MIN_VOLUME_5M || 2);

function logInfo(message, meta = {}) {
  console.log(`[pregrad-x-worker] ${message} ${JSON.stringify(meta)}`);
}

function logError(message, meta = {}) {
  console.error(`[pregrad-x-worker] ${message} ${JSON.stringify(meta)}`);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function ensureSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS x_raw_tweets (
      tweet_id TEXT PRIMARY KEY,
      author_id TEXT,
      author_username TEXT,
      text TEXT,
      created_at TIMESTAMPTZ NOT NULL,
      public_metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_row_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS pregrad_token_x_mentions (
      id BIGSERIAL PRIMARY KEY,
      token_address TEXT NOT NULL,
      tweet_id TEXT NOT NULL,
      match_value TEXT,
      match_type TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS pregrad_token_social_snapshots (
      id BIGSERIAL PRIMARY KEY,
      token_address TEXT NOT NULL,
      symbol TEXT,
      name TEXT,
      ts TIMESTAMPTZ NOT NULL,
      window_minutes INTEGER NOT NULL,
      mentions_count INTEGER NOT NULL DEFAULT 0,
      ca_mentions_count INTEGER NOT NULL DEFAULT 0,
      ticker_mentions_count INTEGER NOT NULL DEFAULT 0,
      unique_authors INTEGER NOT NULL DEFAULT 0,
      likes_total INTEGER NOT NULL DEFAULT 0,
      rts_total INTEGER NOT NULL DEFAULT 0,
      replies_total INTEGER NOT NULL DEFAULT 0,
      quotes_total INTEGER NOT NULL DEFAULT 0,
      searched BOOLEAN NOT NULL DEFAULT TRUE,
      search_terms JSONB NOT NULL DEFAULT '[]'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_pregrad_token_x_mentions_token_tweet
    ON pregrad_token_x_mentions (token_address, tweet_id);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_pregrad_social_snapshots_token_ts
    ON pregrad_token_social_snapshots (token_address, ts DESC);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_pregrad_token_x_mentions_token
    ON pregrad_token_x_mentions (token_address);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_x_raw_tweets_created_at
    ON x_raw_tweets (created_at DESC);
  `);
}

async function getPregradSocialCandidates(limit = TOKEN_LIMIT) {
  const result = await pool.query(
    `
    WITH recent_events AS (
      SELECT
        e.token_address,
        COUNT(*) FILTER (
          WHERE e.event_type ILIKE 'buy'
            AND e.block_time > NOW() - INTERVAL '5 minutes'
        ) AS buys_5m,
        COUNT(*) FILTER (
          WHERE e.event_type ILIKE 'sell'
            AND e.block_time > NOW() - INTERVAL '5 minutes'
        ) AS sells_5m,
        COUNT(DISTINCT e.wallet_address) FILTER (
          WHERE e.block_time > NOW() - INTERVAL '5 minutes'
        ) AS unique_wallets_5m,
        COUNT(*) FILTER (
          WHERE e.block_time > NOW() - INTERVAL '10 minutes'
        ) AS events_10m
      FROM pump_launchpad_events e
      WHERE e.block_time > NOW() - INTERVAL '15 minutes'
      GROUP BY e.token_address
    ),

    last_scrape AS (
      SELECT
        token_address,
        MAX(ts) AS last_snapshot_ts
      FROM pregrad_token_social_snapshots
      GROUP BY token_address
    ),

    candidates AS (
      SELECT
        t.token_address,
        t.symbol,
        t.name,
        t.creator_wallet,
        t.created_at,
        t.market_cap_usd,
        t.bonding_progress_pct,
        t.updated_market_data_at,
        COALESCE(r.buys_5m, 0) AS buys_5m,
        COALESCE(r.sells_5m, 0) AS sells_5m,
        COALESCE(r.unique_wallets_5m, 0) AS unique_wallets_5m,
        COALESCE(r.events_10m, 0) AS events_10m,
        ls.last_snapshot_ts,

        ROUND(
          LEAST(COALESCE(t.market_cap_usd, 0) / 1000.0, 35) +
          LEAST(COALESCE(r.buys_5m, 0) * 4.0, 30) +
          LEAST(COALESCE(r.unique_wallets_5m, 0) * 5.0, 25) +
          CASE
            WHEN COALESCE(r.buys_5m, 0) >= 3
             AND COALESCE(r.sells_5m, 0) = 0 THEN 10
            WHEN COALESCE(r.buys_5m, 0) > COALESCE(r.sells_5m, 0) THEN 6
            ELSE 0
          END,
          2
        ) AS social_candidate_score

      FROM pump_launchpad_tokens t
      LEFT JOIN recent_events r
        ON r.token_address = t.token_address
      LEFT JOIN last_scrape ls
        ON ls.token_address = t.token_address
      WHERE t.token_address IS NOT NULL
        AND t.token_address <> ''
        AND COALESCE(t.market_cap_usd, 0) BETWEEN $1::numeric AND $2::numeric
        AND COALESCE(r.buys_5m, 0) >= $3::int
        AND (
          ls.last_snapshot_ts IS NULL
          OR ls.last_snapshot_ts < NOW() - make_interval(mins => $4::int)
        )
    )

    SELECT *
    FROM candidates
    ORDER BY
      social_candidate_score DESC,
      buys_5m DESC,
      unique_wallets_5m DESC,
      market_cap_usd DESC NULLS LAST
    LIMIT $5::int
    `,
    [
      MIN_MARKET_CAP_USD,
      MAX_MARKET_CAP_USD,
      MIN_BUYS_5M,
      RESCRAPE_COOLDOWN_MINUTES,
      limit,
    ]
  );

  return result.rows;
}

function cleanSymbol(symbol) {
  return String(symbol || "")
    .replace(/^\$/, "")
    .replace(/[^a-zA-Z0-9_]/g, "")
    .toUpperCase()
    .trim();
}

function buildSearchTerms(token) {
  const terms = [];

  const address = String(token.token_address || "").trim();
  const symbol = cleanSymbol(token.symbol);

  // CA first: cleanest pump-fun signal
  if (address) {
    terms.push({
      term: address,
      matchType: "ca",
    });

    terms.push({
      term: `"${address}"`,
      matchType: "ca_exact",
    });
  }

  // Ticker second: useful, but noisier
  if (symbol && symbol.length >= 2 && symbol.length <= 12) {
    terms.push({
      term: `$${symbol}`,
      matchType: "ticker",
    });

    terms.push({
      term: `"$${symbol}"`,
      matchType: "ticker_exact",
    });

    terms.push({
      term: `$${symbol} CA`,
      matchType: "ticker_ca",
    });

    terms.push({
      term: `$${symbol} pump`,
      matchType: "ticker_pump",
    });
  }

  return terms;
}

async function runApifySearch(termObject, tokenAddress) {
  if (!APIFY_API_TOKEN) {
    return {
      ok: false,
      tweets: [],
      reason: "missing_apify_token",
    };
  }

  const encodedActorId = APIFY_ACTOR_ID.replace("/", "~");
  const url = `https://api.apify.com/v2/acts/${encodedActorId}/run-sync-get-dataset-items?token=${APIFY_API_TOKEN}`;

  const body = {
    searchTerms: [termObject.term],
    maxTweets: MAX_TWEETS_PER_SEARCH,
    sort: "Latest",
  };

  let res;
  try {
    res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
  } catch (err) {
    logError("Apify network error", {
      tokenAddress,
      term: termObject.term,
      error: err.message,
    });

    return {
      ok: false,
      tweets: [],
      reason: "network_error",
    };
  }

  const text = await res.text();

  let json;
  try {
    json = JSON.parse(text);
  } catch (err) {
    logError("Bad Apify JSON", {
      tokenAddress,
      status: res.status,
      term: termObject.term,
      preview: text.slice(0, 300),
    });

    return {
      ok: false,
      tweets: [],
      reason: "bad_json",
    };
  }

  if (!res.ok) {
    logError("Apify non-200 response", {
      tokenAddress,
      status: res.status,
      term: termObject.term,
      preview: JSON.stringify(json).slice(0, 300),
    });

    return {
      ok: false,
      tweets: [],
      reason: res.status === 403 ? "apify_403" : "apify_error",
    };
  }

  if (!Array.isArray(json)) {
    return {
      ok: false,
      tweets: [],
      reason: "unexpected_payload",
    };
  }

  const tweets = json.filter((tweet) => {
    if (!tweet || typeof tweet !== "object") return false;
    if (tweet.noResults === true) return false;

    return Boolean(
      tweet.id ||
      tweet.id_str ||
      tweet.tweetId ||
      tweet.postId ||
      tweet.url ||
      tweet.tweetUrl
    );
  });

  return {
    ok: true,
    tweets,
    reason: null,
  };
}

async function searchTweetsForToken(token) {
  const termObjects = buildSearchTerms(token);
  const allTweets = [];
  const usedTerms = [];

  if (termObjects.length === 0) {
    return {
      ok: false,
      tweets: [],
      reason: "no_search_terms",
      usedTerms: [],
    };
  }

  for (const termObject of termObjects) {
    const result = await runApifySearch(termObject, token.token_address);

    usedTerms.push(termObject);

    if (!result.ok) {
      if (result.reason === "apify_403") {
        return {
          ok: false,
          tweets: allTweets,
          reason: "apify_403",
          usedTerms,
        };
      }

      continue;
    }

    for (const tweet of result.tweets) {
      allTweets.push({
        tweet,
        matchValue: termObject.term,
        matchType: termObject.matchType,
      });
    }

    // If CA search finds tweets, that is enough. Avoid spending usage on noisy ticker searches.
    if (
      result.tweets.length > 0 &&
      ["ca", "ca_exact"].includes(termObject.matchType)
    ) {
      break;
    }

    await sleep(250);
  }

  return {
    ok: true,
    tweets: allTweets,
    reason: null,
    usedTerms,
  };
}

function extractTweetId(tweet) {
  const rawId =
    tweet.id ??
    tweet.id_str ??
    tweet.tweetId ??
    tweet.postId ??
    (tweet.url ? tweet.url.split("/").pop()?.split("?")[0] : null) ??
    (tweet.tweetUrl ? tweet.tweetUrl.split("/").pop()?.split("?")[0] : null);

  return rawId ? String(rawId) : null;
}

function extractAuthorId(tweet) {
  const raw =
    tweet.authorId ??
    tweet.user?.id ??
    tweet.author?.id ??
    tweet.userId ??
    null;

  return raw ? String(raw) : null;
}

function extractAuthorUsername(tweet) {
  const raw =
    tweet.author?.userName ??
    tweet.author?.username ??
    tweet.user?.userName ??
    tweet.user?.username ??
    tweet.username ??
    tweet.userName ??
    null;

  return raw ? String(raw) : null;
}

function extractText(tweet) {
  return tweet.text ?? tweet.fullText ?? tweet.content ?? "";
}

function extractCreatedAt(tweet) {
  const value = tweet.createdAt ?? tweet.created_at ?? tweet.date ?? null;
  if (!value) return new Date().toISOString();

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return new Date().toISOString();

  return parsed.toISOString();
}

function extractMetrics(tweet) {
  return {
    like_count: Number(tweet.likeCount ?? tweet.likes ?? tweet.favoriteCount ?? 0),
    retweet_count: Number(tweet.retweetCount ?? tweet.retweets ?? 0),
    reply_count: Number(tweet.replyCount ?? tweet.replies ?? 0),
    quote_count: Number(tweet.quoteCount ?? tweet.quotes ?? 0),
  };
}

async function insertRawTweet(tweet) {
  const tweetId = extractTweetId(tweet);
  if (!tweetId) {
    return {
      inserted: false,
      skipped: true,
      tweetId: null,
    };
  }

  const result = await pool.query(
    `
    INSERT INTO x_raw_tweets (
      tweet_id,
      author_id,
      author_username,
      text,
      created_at,
      public_metrics
    )
    VALUES ($1, $2, $3, $4, $5, $6::jsonb)
    ON CONFLICT (tweet_id) DO UPDATE
    SET
      author_id = EXCLUDED.author_id,
      author_username = EXCLUDED.author_username,
      text = EXCLUDED.text,
      created_at = EXCLUDED.created_at,
      public_metrics = EXCLUDED.public_metrics
    RETURNING tweet_id
    `,
    [
      tweetId,
      extractAuthorId(tweet),
      extractAuthorUsername(tweet),
      extractText(tweet) || null,
      extractCreatedAt(tweet),
      JSON.stringify(extractMetrics(tweet)),
    ]
  );

  return {
    inserted: result.rowCount > 0,
    skipped: false,
    tweetId,
  };
}

async function insertTokenMention(tokenAddress, tweetId, matchValue, matchType) {
  if (!tokenAddress || !tweetId) {
    return { inserted: false };
  }

  const result = await pool.query(
    `
    INSERT INTO pregrad_token_x_mentions (
      token_address,
      tweet_id,
      match_value,
      match_type
    )
    VALUES ($1::text, $2::text, $3::text, $4::text)
    ON CONFLICT (token_address, tweet_id) DO NOTHING
    RETURNING id
    `,
    [
      String(tokenAddress),
      String(tweetId),
      matchValue ? String(matchValue) : null,
      matchType ? String(matchType) : null,
    ]
  );

  return {
    inserted: result.rowCount > 0,
  };
}

async function insertSocialSnapshot(token, usedTerms) {
  const result = await pool.query(
    `
    INSERT INTO pregrad_token_social_snapshots (
      token_address,
      symbol,
      name,
      ts,
      window_minutes,
      mentions_count,
      ca_mentions_count,
      ticker_mentions_count,
      unique_authors,
      likes_total,
      rts_total,
      replies_total,
      quotes_total,
      searched,
      search_terms
    )
    SELECT
      $1::text AS token_address,
      $2::text AS symbol,
      $3::text AS name,
      NOW() AS ts,
      $4::int AS window_minutes,

      COUNT(txm.tweet_id)::int AS mentions_count,

      COUNT(txm.tweet_id) FILTER (
        WHERE txm.match_type IN ('ca', 'ca_exact')
      )::int AS ca_mentions_count,

      COUNT(txm.tweet_id) FILTER (
        WHERE txm.match_type NOT IN ('ca', 'ca_exact')
      )::int AS ticker_mentions_count,

      COUNT(DISTINCT x.author_id)::int AS unique_authors,

      COALESCE(SUM((x.public_metrics->>'like_count')::int), 0)::int AS likes_total,
      COALESCE(SUM((x.public_metrics->>'retweet_count')::int), 0)::int AS rts_total,
      COALESCE(SUM((x.public_metrics->>'reply_count')::int), 0)::int AS replies_total,
      COALESCE(SUM((x.public_metrics->>'quote_count')::int), 0)::int AS quotes_total,

      TRUE AS searched,
      $5::jsonb AS search_terms

    FROM (
      SELECT $1::text AS token_address
    ) base
    LEFT JOIN pregrad_token_x_mentions txm
      ON txm.token_address = base.token_address
    LEFT JOIN x_raw_tweets x
      ON x.tweet_id = txm.tweet_id
     AND x.created_at > NOW() - make_interval(mins => $4::int)
    `,
    [
      token.token_address,
      token.symbol || null,
      token.name || null,
      SNAPSHOT_WINDOW_MINUTES,
      JSON.stringify(usedTerms || []),
    ]
  );

  return result.rowCount;
}

async function processToken(token, index, total) {
  const tokenAddress = token.token_address;

  const searchResult = await searchTweetsForToken(token);

  if (!searchResult.ok && searchResult.reason === "apify_403") {
    return {
      ok: false,
      tokenAddress,
      reason: "apify_403",
      tweetsFetched: 0,
      rawInserted: 0,
      mentionsInserted: 0,
    };
  }

  let rawInserted = 0;
  let mentionsInserted = 0;
  let skippedNoId = 0;

  for (const item of searchResult.tweets || []) {
    try {
      const rawResult = await insertRawTweet(item.tweet);

      if (rawResult.skipped || !rawResult.tweetId) {
        skippedNoId += 1;
        continue;
      }

      if (rawResult.inserted) rawInserted += 1;

      const mentionResult = await insertTokenMention(
        tokenAddress,
        rawResult.tweetId,
        item.matchValue,
        item.matchType
      );

      if (mentionResult.inserted) mentionsInserted += 1;
    } catch (err) {
      logError("Failed storing tweet", {
        tokenAddress,
        error: err.message,
      });
    }
  }

  await insertSocialSnapshot(token, searchResult.usedTerms || []);

  logInfo("Token processed", {
    index,
    total,
    tokenAddress,
    symbol: token.symbol || null,
    name: token.name || null,
    marketCap: token.market_cap_usd,
    buys5m: token.buys_5m,
    uniqueWallets5m: token.unique_wallets_5m,
    tweetsFetched: searchResult.tweets?.length || 0,
    rawInserted,
    mentionsInserted,
    skippedNoId,
    searchedTerms: searchResult.usedTerms || [],
  });

  return {
    ok: true,
    tokenAddress,
    tweetsFetched: searchResult.tweets?.length || 0,
    rawInserted,
    mentionsInserted,
  };
}

async function fetchMentionsCycle() {
  const startedAt = Date.now();

  const tokens = await getPregradSocialCandidates(TOKEN_LIMIT);

  logInfo("Loaded pregrad social candidates", {
    count: tokens.length,
  });

  if (tokens.length === 0) return;

  let successCount = 0;
  let failureCount = 0;
  let totalFetched = 0;
  let totalRawInserted = 0;
  let totalMentionsInserted = 0;

  for (let i = 0; i < tokens.length; i += 1) {
    try {
      const result = await processToken(tokens[i], i + 1, tokens.length);

      if (result.ok) successCount += 1;
      else failureCount += 1;

      totalFetched += result.tweetsFetched || 0;
      totalRawInserted += result.rawInserted || 0;
      totalMentionsInserted += result.mentionsInserted || 0;

      if (result.reason === "apify_403") {
        logError("Stopping cycle because Apify returned 403", {});
        break;
      }
    } catch (err) {
      failureCount += 1;

      logError("Unhandled token failure", {
        tokenAddress: tokens[i]?.token_address || null,
        error: err.message,
      });
    }

    await sleep(PER_TOKEN_SLEEP_MS);
  }

  logInfo("Cycle complete", {
    durationSeconds: Math.round((Date.now() - startedAt) / 1000),
    tokenSuccesses: successCount,
    tokenFailures: failureCount,
    tweetsFetched: totalFetched,
    rawTweetsInserted: totalRawInserted,
    mentionsInserted: totalMentionsInserted,
  });
}

async function main() {
  try {
    await ensureSchema();

    logInfo("Pregrad X mentions worker started", {
      actorId: APIFY_ACTOR_ID,
      tokenLimit: TOKEN_LIMIT,
      maxTweetsPerSearch: MAX_TWEETS_PER_SEARCH,
      snapshotWindowMinutes: SNAPSHOT_WINDOW_MINUTES,
      loopSleepMs: LOOP_SLEEP_MS,
      minMarketCapUsd: MIN_MARKET_CAP_USD,
      maxMarketCapUsd: MAX_MARKET_CAP_USD,
      minBuys5m: MIN_BUYS_5M,
      rescrapeCooldownMinutes: RESCRAPE_COOLDOWN_MINUTES,
    });

    while (true) {
      try {
        await fetchMentionsCycle();
      } catch (err) {
        logError("Cycle failed", {
          error: err.message,
        });
      }

      logInfo("Sleeping before next cycle", {
        sleepSeconds: Math.round(LOOP_SLEEP_MS / 1000),
      });

      await sleep(LOOP_SLEEP_MS);
    }
  } catch (err) {
    logError("Fatal worker error", {
      error: err.message,
    });

    process.exit(1);
  }
}

process.on("SIGINT", async () => {
  logInfo("SIGINT received, closing pool");
  await pool.end();
  process.exit(0);
});

process.on("SIGTERM", async () => {
  logInfo("SIGTERM received, closing pool");
  await pool.end();
  process.exit(0);
});

main();
