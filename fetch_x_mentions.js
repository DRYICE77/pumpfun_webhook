require("dotenv").config();
const { Pool } = require("pg");

// Node 18+ has global fetch

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl:
    process.env.PGSSLMODE === "disable"
      ? false
      : { rejectUnauthorized: false },
});

const APIFY_API_TOKEN = process.env.APIFY_API_TOKEN || "";
const HELIUS_API_KEY = process.env.HELIUS_API_KEY || "";

const TOKEN_LIMIT = Number(process.env.X_TOKEN_LIMIT || 5);
const MAX_TWEETS_PER_TOKEN = Number(process.env.X_MAX_TWEETS_PER_TOKEN || 5);
const SNAPSHOT_WINDOW_MINUTES = Number(
  process.env.X_SNAPSHOT_WINDOW_MINUTES || 5
);
const LOOP_SLEEP_MS = Number(process.env.X_LOOP_SLEEP_MS || 60000);
const MIN_QUALITY_SCORE = Number(process.env.X_MIN_QUALITY_SCORE || 0);
const MIN_VOLUME_5M = Number(process.env.X_MIN_VOLUME_5M || 0);
const MIN_BUYS_5M = Number(process.env.X_MIN_BUYS_5M || 0);
const RESCRAPE_COOLDOWN_MINUTES = Number(
  process.env.X_RESCRAPE_COOLDOWN_MINUTES || 10
);
const PER_TOKEN_SLEEP_MS = Number(process.env.X_PER_TOKEN_SLEEP_MS || 1000);

function logInfo(message, meta = {}) {
  console.log(`[x-worker] ${message} ${JSON.stringify(meta)}`);
}

function logError(message, meta = {}) {
  console.error(`[x-worker] ${message} ${JSON.stringify(meta)}`);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function ensureSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS x_raw_tweets (
      tweet_id TEXT PRIMARY KEY,
      author_id TEXT,
      text TEXT,
      created_at TIMESTAMPTZ NOT NULL,
      public_metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_row_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS token_x_mentions (
      id BIGSERIAL PRIMARY KEY,
      token_id TEXT NOT NULL,
      tweet_id TEXT NOT NULL,
      match_value TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS token_social_snapshots (
      id BIGSERIAL PRIMARY KEY,
      token_id TEXT NOT NULL,
      ts TIMESTAMPTZ NOT NULL,
      window_minutes INTEGER NOT NULL,
      mentions_count INTEGER NOT NULL DEFAULT 0,
      unique_authors INTEGER NOT NULL DEFAULT 0,
      likes_total INTEGER NOT NULL DEFAULT 0,
      rts_total INTEGER NOT NULL DEFAULT 0,
      replies_total INTEGER NOT NULL DEFAULT 0,
      quotes_total INTEGER NOT NULL DEFAULT 0,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS token_metadata (
      token_id TEXT PRIMARY KEY,
      symbol TEXT,
      name TEXT,
      last_fetched TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_token_x_mentions_token_tweet
    ON token_x_mentions (token_id, tweet_id);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_x_raw_tweets_created_at
    ON x_raw_tweets (created_at DESC);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_token_social_snapshots_token_ts
    ON token_social_snapshots (token_id, ts DESC);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_token_x_mentions_token_id
    ON token_x_mentions (token_id);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_token_metadata_symbol
    ON token_metadata (symbol);
  `);
}

async function getSocialCandidates(limit = TOKEN_LIMIT) {
  const result = await pool.query(
    `
    WITH scored_tokens AS (
      SELECT
        tt.token_id,
        tt.token_address,
        tt.symbol,
        tt.volume_5m,
        tt.buys_5m,
        tt.sells_5m,
        tt.last_updated,
        ROUND(
          LEAST(tt.volume_5m / 50.0, 40) +
          LEAST(tt.buys_5m / 5.0, 40) +
          CASE
            WHEN tt.buys_5m = 0 THEN 0
            WHEN tt.sells_5m = 0 THEN 20
            WHEN tt.buys_5m::numeric / NULLIF(tt.sells_5m, 0) >= 3 THEN 20
            WHEN tt.buys_5m::numeric / NULLIF(tt.sells_5m, 0) >= 2 THEN 15
            WHEN tt.buys_5m > tt.sells_5m THEN 10
            ELSE 0
          END,
          2
        ) AS quality_score
      FROM tracked_tokens tt
      WHERE tt.token_address IS NOT NULL
        AND tt.token_address <> ''
    ),
    last_scrape AS (
      SELECT
        token_id,
        MAX(ts) AS last_snapshot_ts
      FROM token_social_snapshots
      GROUP BY token_id
    )
    SELECT
      s.token_id,
      s.token_address,
      s.symbol,
      s.quality_score,
      s.volume_5m,
      s.buys_5m,
      s.sells_5m,
      s.last_updated,
      ls.last_snapshot_ts
    FROM scored_tokens s
    LEFT JOIN last_scrape ls
      ON ls.token_id = s.token_id
    WHERE s.quality_score >= $1::numeric
      AND s.volume_5m >= $2::numeric
      AND s.buys_5m >= $3::int
      AND (
        ls.last_snapshot_ts IS NULL
        OR ls.last_snapshot_ts < NOW() - make_interval(mins => $4::int)
      )
    ORDER BY
      s.quality_score DESC,
      s.volume_5m DESC,
      s.buys_5m DESC,
      s.last_updated DESC NULLS LAST
    LIMIT $5::int
    `,
    [
      MIN_QUALITY_SCORE,
      MIN_VOLUME_5M,
      MIN_BUYS_5M,
      RESCRAPE_COOLDOWN_MINUTES,
      limit,
    ]
  );

  return result.rows;
}

function buildSearchTerms(token) {
  const terms = new Set();

  const address =
    token && token.token_address ? String(token.token_address).trim() : "";

  const symbol =
    token && token.symbol ? String(token.symbol).trim().replace(/^\$/, "") : "";

  if (address) {
    const short = address.slice(0, 6);

    terms.add(address);
    terms.add(`"${address}"`);
    terms.add(`CA ${address}`);
    terms.add(`CA: ${address}`);
    terms.add(short);
    terms.add(`"${short}"`);
  }

  if (symbol && symbol.length <= 12) {
    const upper = symbol.toUpperCase();
    terms.add(`$${upper}`);
    terms.add(`"$${upper}"`);

    if (address) {
      terms.add(`$${upper} ${address}`);
      terms.add(`$${upper} CA`);
      terms.add(`$${upper} contract`);
    }
  }

  return [...terms].filter(Boolean);
}

async function fetchTokenMetadata(tokenId) {
  if (!HELIUS_API_KEY) {
    logError("Missing HELIUS_API_KEY", { tokenId });
    return null;
  }

  try {
    const url = `https://api.helius.xyz/v0/token-metadata?api-key=${HELIUS_API_KEY}`;

    const res = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        mintAccounts: [tokenId],
      }),
    });

    const text = await res.text();

    logInfo("Helius metadata HTTP response", {
      tokenId,
      status: res.status,
      ok: res.ok,
      preview: text.slice(0, 500),
    });

    let json;
    try {
      json = JSON.parse(text);
    } catch (err) {
      logError("Failed to parse Helius metadata response", {
        tokenId,
        preview: text.slice(0, 500),
        error: err.message,
      });
      return null;
    }

    if (!res.ok) {
      logError("Helius metadata non-200 response", {
        tokenId,
        status: res.status,
        preview: JSON.stringify(json).slice(0, 500),
      });
      return null;
    }

    const item = Array.isArray(json) ? json[0] : json;

    if (!item || typeof item !== "object") {
      logInfo("No Helius metadata found", {
        tokenId,
        payloadType: typeof json,
      });
      return null;
    }

    const rawSymbol =
      item?.onChainMetadata?.metadata?.data?.symbol ??
      item?.offChainMetadata?.metadata?.symbol ??
      item?.offChainMetadata?.symbol ??
      item?.content?.metadata?.symbol ??
      item?.metadata?.symbol ??
      item?.tokenInfo?.symbol ??
      item?.symbol ??
      null;

    const rawName =
      item?.onChainMetadata?.metadata?.data?.name ??
      item?.offChainMetadata?.metadata?.name ??
      item?.offChainMetadata?.name ??
      item?.content?.metadata?.name ??
      item?.metadata?.name ??
      item?.tokenInfo?.name ??
      item?.name ??
      null;

    const clean = (value) => {
      if (value == null) return null;
      const str = String(value).replace(/\0/g, "").trim();
      return str.length > 0 ? str : null;
    };

    const symbol = clean(rawSymbol);
    const name = clean(rawName);

    logInfo("Helius metadata parsed", {
      tokenId,
      symbol,
      name,
      topLevelKeys: Object.keys(item).slice(0, 20),
    });

    if (!symbol && !name) {
      logInfo("Helius metadata returned no usable symbol/name", {
        tokenId,
        itemPreview: JSON.stringify(item).slice(0, 700),
      });
      return null;
    }

    return { symbol, name };
  } catch (err) {
    logError("Helius metadata fetch error", {
      tokenId,
      error: err.message,
    });
    return null;
  }
}
async function upsertTokenMetadata(tokenId, metadata) {
  if (!tokenId || !metadata) return false;

  const result = await pool.query(
    `
    INSERT INTO token_metadata (
      token_id,
      symbol,
      name,
      last_fetched
    )
    VALUES ($1::text, $2::text, $3::text, NOW())
    ON CONFLICT (token_id) DO UPDATE
    SET
      symbol = EXCLUDED.symbol,
      name = EXCLUDED.name,
      last_fetched = NOW()
    RETURNING token_id
    `,
    [String(tokenId), metadata.symbol || null, metadata.name || null]
  );

  return result.rowCount > 0;
}

async function runApifySearch(searchTerms, tokenId) {
  if (!APIFY_API_TOKEN) {
    logError("Missing APIFY_API_TOKEN");
    return {
      ok: false,
      tweets: [],
      reason: "missing_apify_token",
      searchTerms,
    };
  }

  const url = `https://api.apify.com/v2/acts/apidojo~tweet-scraper/run-sync-get-dataset-items?token=${APIFY_API_TOKEN}`;

  const body = {
    searchTerms,
    maxTweets: MAX_TWEETS_PER_TOKEN,
    sort: "Latest",
  };

  let res;
  try {
    res = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    });
  } catch (err) {
    logError("Apify network error", {
      tokenId,
      searchTerms,
      error: err.message,
    });
    return {
      ok: false,
      tweets: [],
      reason: "network_error",
      searchTerms,
    };
  }

  const text = await res.text();

  let json;
  try {
    json = JSON.parse(text);
  } catch (err) {
    logError("Failed to parse Apify response", {
      tokenId,
      status: res.status,
      searchTerms,
      preview: text.slice(0, 300),
    });
    return {
      ok: false,
      tweets: [],
      reason: "bad_json",
      searchTerms,
    };
  }

  if (!res.ok) {
    const preview = JSON.stringify(json).slice(0, 300);

    if (res.status === 403) {
      logError("Apify access denied or usage capped", {
        tokenId,
        status: res.status,
        searchTerms,
        preview,
      });
      return {
        ok: false,
        tweets: [],
        reason: "apify_403",
        searchTerms,
      };
    }

    logError("Apify returned non-200 response", {
      tokenId,
      status: res.status,
      searchTerms,
      preview,
    });
    return {
      ok: false,
      tweets: [],
      reason: "apify_error",
      searchTerms,
    };
  }

  if (!Array.isArray(json)) {
    logError("Apify returned unexpected payload", {
      tokenId,
      payloadType: typeof json,
      searchTerms,
      preview: JSON.stringify(json).slice(0, 300),
    });
    return {
      ok: false,
      tweets: [],
      reason: "unexpected_payload",
      searchTerms,
    };
  }

  const filteredTweets = json.filter((tweet) => {
    if (!tweet || typeof tweet !== "object") return false;
    if (tweet.noResults === true) return false;

    const hasId =
      tweet.id != null ||
      tweet.id_str != null ||
      tweet.tweetId != null ||
      tweet.postId != null ||
      tweet.url != null ||
      tweet.tweetUrl != null;

    return hasId;
  });

  if (json.length > 0 && filteredTweets.length === 0) {
    logInfo("Apify returned only non-tweet placeholder items", {
      tokenId,
      searchTerms,
      rawCount: json.length,
      sample: json[0],
    });
  }

  return {
    ok: true,
    tweets: filteredTweets,
    reason: null,
    searchTerms,
  };
}

async function searchTweetsForToken(token) {
  const searchTerms = buildSearchTerms(token);

  if (searchTerms.length === 0) {
    logError("No search terms could be built", {
      tokenId: token.token_id,
      tokenAddress: token.token_address,
      symbol: token.symbol || null,
    });
    return {
      ok: false,
      tweets: [],
      reason: "no_search_terms",
      searchTerms: [],
    };
  }

  // Try one term at a time for cleaner matching
  for (const term of searchTerms) {
    const result = await runApifySearch([term], token.token_id);

    if (!result.ok) {
      if (result.reason === "apify_403") {
        return result;
      }
      continue;
    }

    if (result.tweets.length > 0) {
      logInfo("Apify search completed", {
        tokenId: token.token_id,
        searchTerms: [term],
        tweetsReturned: result.tweets.length,
      });

      return {
        ok: true,
        tweets: result.tweets,
        reason: null,
        searchTerms: [term],
      };
    }

    logInfo("No usable tweets found for token search", {
      tokenId: token.token_id,
      searchTerms: [term],
      rawCount: 0,
    });
  }

  return {
    ok: true,
    tweets: [],
    reason: null,
    searchTerms,
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
  const rawAuthorId =
    tweet.authorId ??
    tweet.user?.id ??
    tweet.author?.id ??
    null;

  return rawAuthorId ? String(rawAuthorId) : null;
}

function extractText(tweet) {
  return tweet.text ?? tweet.fullText ?? "";
}

function extractCreatedAt(tweet) {
  const value = tweet.createdAt ?? tweet.created_at ?? null;
  if (!value) return new Date().toISOString();

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return new Date().toISOString();
  }

  return parsed.toISOString();
}

function extractMetrics(tweet) {
  return {
    like_count: Number(
      tweet.likeCount ?? tweet.likes ?? tweet.favoriteCount ?? 0
    ),
    retweet_count: Number(tweet.retweetCount ?? tweet.retweets ?? 0),
    reply_count: Number(tweet.replyCount ?? tweet.replies ?? 0),
    quote_count: Number(tweet.quoteCount ?? tweet.quotes ?? 0),
  };
}

async function insertRawTweet(tweet) {
  const tweetId = extractTweetId(tweet);
  if (!tweetId) {
    console.log(
      "[x-worker] 🚨 tweet missing ID, raw object:",
      JSON.stringify(tweet, null, 2)
    );
    return { inserted: false, tweetId: null, skipped: true };
  }

  const authorId = extractAuthorId(tweet);
  const text = extractText(tweet);
  const createdAt = extractCreatedAt(tweet);
  const publicMetrics = extractMetrics(tweet);

  const result = await pool.query(
    `
    INSERT INTO x_raw_tweets (
      tweet_id,
      author_id,
      text,
      created_at,
      public_metrics
    )
    VALUES ($1, $2, $3, $4, $5::jsonb)
    ON CONFLICT (tweet_id) DO UPDATE
    SET
      author_id = EXCLUDED.author_id,
      text = EXCLUDED.text,
      created_at = EXCLUDED.created_at,
      public_metrics = EXCLUDED.public_metrics
    RETURNING tweet_id
    `,
    [
      tweetId,
      authorId,
      text || null,
      createdAt,
      JSON.stringify(publicMetrics),
    ]
  );

  return {
    inserted: result.rowCount > 0,
    tweetId,
    skipped: false,
  };
}

async function insertTokenMention(tokenId, tweetId, matchValue) {
  if (!tweetId) return { inserted: false };

  const result = await pool.query(
    `
    INSERT INTO token_x_mentions (
      token_id,
      tweet_id,
      match_value
    )
    VALUES ($1::text, $2::text, $3::text)
    ON CONFLICT (token_id, tweet_id) DO NOTHING
    RETURNING id
    `,
    [String(tokenId), String(tweetId), matchValue ? String(matchValue) : null]
  );

  return { inserted: result.rowCount > 0 };
}

async function updateSocialSnapshots(windowMinutes = SNAPSHOT_WINDOW_MINUTES) {
  const result = await pool.query(
    `
    INSERT INTO token_social_snapshots (
      token_id,
      ts,
      window_minutes,
      mentions_count,
      unique_authors,
      likes_total,
      rts_total,
      replies_total,
      quotes_total
    )
    SELECT
      txm.token_id,
      NOW(),
      $1::int,
      COUNT(*)::INT AS mentions_count,
      COUNT(DISTINCT x.author_id)::INT AS unique_authors,
      COALESCE(SUM((x.public_metrics->>'like_count')::INT), 0)::INT AS likes_total,
      COALESCE(SUM((x.public_metrics->>'retweet_count')::INT), 0)::INT AS rts_total,
      COALESCE(SUM((x.public_metrics->>'reply_count')::INT), 0)::INT AS replies_total,
      COALESCE(SUM((x.public_metrics->>'quote_count')::INT), 0)::INT AS quotes_total
    FROM token_x_mentions txm
    JOIN x_raw_tweets x
      ON x.tweet_id = txm.tweet_id
    WHERE x.created_at > NOW() - make_interval(mins => $1::int)
    GROUP BY txm.token_id
    `,
    [windowMinutes]
  );

  return result.rowCount;
}

async function processToken(token, index, total) {
  const tokenId = token.token_id;
  const address = token.token_address;

  if (!address) {
    logError("Token missing address", { index, total, tokenId });
    return {
      ok: false,
      tokenId,
      tweetsFetched: 0,
      rawInserted: 0,
      mentionsInserted: 0,
      skippedNoId: 0,
      reason: "missing_token_address",
      searchTerms: [],
    };
  }

  try {
    if (!token.symbol) {
      const metadata = await fetchTokenMetadata(tokenId);

      if (metadata && (metadata.symbol || metadata.name)) {
        await upsertTokenMetadata(tokenId, metadata);
        token.symbol = metadata.symbol || null;

        logInfo("Token metadata updated", {
          tokenId,
          symbol: metadata.symbol,
          name: metadata.name,
        });
      }
    }
  } catch (err) {
    logError("Failed metadata enrichment", {
      tokenId,
      error: err.message,
    });
  }

  const searchResult = await searchTweetsForToken(token);

  if (!searchResult.ok) {
    return {
      ok: false,
      tokenId,
      tweetsFetched: 0,
      rawInserted: 0,
      mentionsInserted: 0,
      skippedNoId: 0,
      reason: searchResult.reason || "apify_error",
      searchTerms: searchResult.searchTerms || [],
    };
  }

  let rawInserted = 0;
  let mentionsInserted = 0;
  let skippedNoId = 0;

  for (const tweet of searchResult.tweets) {
    try {
      const rawResult = await insertRawTweet(tweet);

      if (rawResult.skipped || !rawResult.tweetId) {
        skippedNoId += 1;
        continue;
      }

      if (rawResult.inserted) {
        rawInserted += 1;
      }

      const mentionResult = await insertTokenMention(
        tokenId,
        rawResult.tweetId,
        (searchResult.searchTerms && searchResult.searchTerms[0]) || address
      );

      if (mentionResult.inserted) {
        mentionsInserted += 1;
      }
    } catch (err) {
      logError("Failed storing tweet", {
        tokenId,
        address,
        error: err.message,
      });
    }
  }

  logInfo("Token processed", {
    index,
    total,
    tokenId,
    address,
    symbol: token.symbol || null,
    searchTerms: searchResult.searchTerms || [],
    tweetsFetched: searchResult.tweets.length,
    rawInserted,
    mentionsInserted,
    skippedNoId,
    qualityScore: token.quality_score,
    volume5m: token.volume_5m,
    buys5m: token.buys_5m,
    sells5m: token.sells_5m,
  });

  return {
    ok: true,
    tokenId,
    tweetsFetched: searchResult.tweets.length,
    rawInserted,
    mentionsInserted,
    skippedNoId,
    searchTerms: searchResult.searchTerms || [],
  };
}

async function fetchMentionsCycle() {
  const startedAt = Date.now();

  const tokens = await getSocialCandidates(TOKEN_LIMIT);
  logInfo("Loaded social candidates", { count: tokens.length });

  if (tokens.length === 0) {
    logInfo("No social candidates found");
    return;
  }

  let successCount = 0;
  let failureCount = 0;
  let totalFetched = 0;
  let totalRawInserted = 0;
  let totalMentionsInserted = 0;
  let totalSkippedNoId = 0;
  let apify403Count = 0;

  for (let i = 0; i < tokens.length; i += 1) {
    try {
      const result = await processToken(tokens[i], i + 1, tokens.length);

      totalFetched += result.tweetsFetched;
      totalRawInserted += result.rawInserted;
      totalMentionsInserted += result.mentionsInserted;
      totalSkippedNoId += result.skippedNoId;

      if (result.ok) {
        successCount += 1;
      } else {
        failureCount += 1;

        if (result.reason === "apify_403") {
          apify403Count += 1;
        }

        logError("Token failed", {
          index: i + 1,
          total: tokens.length,
          tokenId: result.tokenId,
          reason: result.reason,
          searchTerms: result.searchTerms || [],
        });
      }
    } catch (err) {
      failureCount += 1;
      logError("Unhandled token processing failure", {
        index: i + 1,
        total: tokens.length,
        tokenId: tokens[i]?.token_id ?? null,
        error: err.message,
      });
    }

    await sleep(PER_TOKEN_SLEEP_MS);
  }

  let snapshotsInserted = 0;
  try {
    snapshotsInserted = await updateSocialSnapshots(SNAPSHOT_WINDOW_MINUTES);
  } catch (err) {
    logError("Failed updating social snapshots", {
      error: err.message,
    });
  }

  logInfo("Cycle complete", {
    durationSeconds: Math.round((Date.now() - startedAt) / 1000),
    tokenSuccesses: successCount,
    tokenFailures: failureCount,
    tweetsFetched: totalFetched,
    rawTweetsInserted: totalRawInserted,
    mentionsInserted: totalMentionsInserted,
    skippedNoId: totalSkippedNoId,
    snapshotsInserted,
    apify403Count,
  });
}

async function main() {
  try {
    await ensureSchema();

    logInfo("X mentions worker started", {
      tokenLimit: TOKEN_LIMIT,
      maxTweetsPerToken: MAX_TWEETS_PER_TOKEN,
      snapshotWindowMinutes: SNAPSHOT_WINDOW_MINUTES,
      loopSleepMs: LOOP_SLEEP_MS,
      minQualityScore: MIN_QUALITY_SCORE,
      minVolume5m: MIN_VOLUME_5M,
      minBuys5m: MIN_BUYS_5M,
      rescrapeCooldownMinutes: RESCRAPE_COOLDOWN_MINUTES,
      perTokenSleepMs: PER_TOKEN_SLEEP_MS,
    });

    // eslint-disable-next-line no-constant-condition
    while (true) {
      try {
        await fetchMentionsCycle();
      } catch (err) {
        logError("Cycle failed", { error: err.message });
      }

      logInfo("Sleeping before next cycle", {
        sleepSeconds: Math.round(LOOP_SLEEP_MS / 1000),
      });

      await sleep(LOOP_SLEEP_MS);
    }
  } catch (err) {
    logError("Fatal worker error", { error: err.message });
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
