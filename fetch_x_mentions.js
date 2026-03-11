import pg from "pg";

const { Pool } = pg;

const DATABASE_URL = process.env.DATABASE_URL;
const APIFY_API_TOKEN = process.env.APIFY_API_TOKEN;

const TOKEN_LIMIT = Number(process.env.X_TOKEN_LIMIT || 10);
const MAX_TWEETS_PER_TOKEN = Number(process.env.X_MAX_TWEETS_PER_TOKEN || 5);
const SNAPSHOT_WINDOW_MINUTES = Number(process.env.X_SNAPSHOT_WINDOW_MINUTES || 5);

if (!DATABASE_URL) {
  console.error("Missing DATABASE_URL");
  process.exit(1);
}

if (!APIFY_API_TOKEN) {
  console.error("Missing APIFY_API_TOKEN");
  process.exit(1);
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

function logInfo(message, extra = {}) {
  const payload = Object.keys(extra).length ? ` ${JSON.stringify(extra)}` : "";
  console.log(`[x-worker] ${message}${payload}`);
}

function logError(message, extra = {}) {
  const payload = Object.keys(extra).length ? ` ${JSON.stringify(extra)}` : "";
  console.error(`[x-worker] ${message}${payload}`);
}

async function ensureTables() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS x_raw_tweets (
      tweet_id TEXT PRIMARY KEY,
      author_id TEXT,
      text TEXT,
      created_at TIMESTAMPTZ,
      ingested_at TIMESTAMPTZ DEFAULT NOW(),
      public_metrics JSONB
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS token_x_mentions (
      id BIGSERIAL PRIMARY KEY,
      token_id TEXT NOT NULL,
      tweet_id TEXT NOT NULL,
      match_value TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
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
      created_at TIMESTAMPTZ DEFAULT NOW()
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
}

async function getRecentTokens(limit = TOKEN_LIMIT) {
  const result = await pool.query(
    `
    SELECT
      token_id,
      token_address
    FROM tracked_tokens
    WHERE token_address IS NOT NULL
      AND token_address <> ''
      AND (
        COALESCE(buys_5m, 0) > 0
        OR COALESCE(sells_5m, 0) > 0
        OR COALESCE(volume_5m, 0) > 0
      )
    ORDER BY last_updated DESC NULLS LAST
    LIMIT $1
    `,
    [limit]
  );

  return result.rows;
}

async function searchTweetsForAddress(address) {
  const url = `https://api.apify.com/v2/acts/apidojo~tweet-scraper/run-sync-get-dataset-items?token=${APIFY_API_TOKEN}`;

  const body = {
    searchTerms: [address],
    maxTweets: MAX_TWEETS_PER_TOKEN,
    sort: "Latest",
  };

  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });

  const text = await res.text();

  let json;
  try {
    json = JSON.parse(text);
  } catch (err) {
    logError("Failed to parse Apify response", {
      address,
      status: res.status,
      preview: text.slice(0, 300),
    });
    return { ok: false, tweets: [] };
  }

  if (!res.ok) {
    logError("Apify returned non-200 response", {
      address,
      status: res.status,
      preview: JSON.stringify(json).slice(0, 300),
    });
    return { ok: false, tweets: [] };
  }

  if (!Array.isArray(json)) {
    logError("Apify returned unexpected payload", {
      address,
      payloadType: typeof json,
      preview: JSON.stringify(json).slice(0, 300),
    });
    return { ok: false, tweets: [] };
  }

  return { ok: true, tweets: json };
}

function extractMetrics(tweet) {
  return {
    like_count:
      tweet.likeCount ??
      tweet.likes ??
      tweet.favoriteCount ??
      0,
    retweet_count:
      tweet.retweetCount ??
      tweet.retweets ??
      0,
    reply_count:
      tweet.replyCount ??
      tweet.replies ??
      0,
    quote_count:
      tweet.quoteCount ??
      tweet.quotes ??
      0,
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

async function insertRawTweet(tweet) {
  const tweetId = extractTweetId(tweet);
  if (!tweetId) {
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
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (tweet_id) DO NOTHING
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
    VALUES ($1, $2, $3)
    ON CONFLICT (token_id, tweet_id) DO NOTHING
    RETURNING id
    `,
    [tokenId, tweetId, matchValue]
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
      $1,
      COUNT(*)::INT AS mentions_count,
      COUNT(DISTINCT x.author_id)::INT AS unique_authors,
      COALESCE(SUM((x.public_metrics->>'like_count')::INT), 0)::INT AS likes_total,
      COALESCE(SUM((x.public_metrics->>'retweet_count')::INT), 0)::INT AS rts_total,
      COALESCE(SUM((x.public_metrics->>'reply_count')::INT), 0)::INT AS replies_total,
      COALESCE(SUM((x.public_metrics->>'quote_count')::INT), 0)::INT AS quotes_total
    FROM token_x_mentions txm
    JOIN x_raw_tweets x
      ON x.tweet_id = txm.tweet_id
    WHERE x.created_at > NOW() - ($1 || ' minutes')::INTERVAL
    GROUP BY txm.token_id
    `,
    [windowMinutes]
  );

  return result.rowCount;
}

async function processToken(token) {
  const tokenId = token.token_id;
  const address = token.token_address;

  if (!address) {
    return {
      tokenId,
      address: null,
      tweetsFetched: 0,
      rawInserted: 0,
      mentionsInserted: 0,
      skippedNoId: 0,
      ok: false,
      reason: "missing token_address",
    };
  }

  const result = await searchTweetsForAddress(address);
  if (!result.ok) {
    return {
      tokenId,
      address,
      tweetsFetched: 0,
      rawInserted: 0,
      mentionsInserted: 0,
      skippedNoId: 0,
      ok: false,
      reason: "apify_error",
    };
  }

  let rawInserted = 0;
  let mentionsInserted = 0;
  let skippedNoId = 0;

  for (const tweet of result.tweets) {
    try {
      const rawResult = await insertRawTweet(tweet);

      if (rawResult.skipped || !rawResult.tweetId) {
        skippedNoId += 1;
        continue;
      }

      if (rawResult.inserted) {
        rawInserted += 1;
      }

      const mentionResult = await insertTokenMention(tokenId, rawResult.tweetId, address);
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

  return {
    tokenId,
    address,
    tweetsFetched: result.tweets.length,
    rawInserted,
    mentionsInserted,
    skippedNoId,
    ok: true,
  };
}

async function fetchMentions() {
  const startedAt = Date.now();

  logInfo("Worker started", {
    tokenLimit: TOKEN_LIMIT,
    maxTweetsPerToken: MAX_TWEETS_PER_TOKEN,
    snapshotWindowMinutes: SNAPSHOT_WINDOW_MINUTES,
  });

  await ensureTables();
  logInfo("Tables ready");

  const tokens = await getRecentTokens(TOKEN_LIMIT);
  logInfo("Loaded tracked tokens", { count: tokens.length });

  if (tokens.length === 0) {
    logInfo("No tracked tokens found, exiting");
    return;
  }

  let totalFetched = 0;
  let totalRawInserted = 0;
  let totalMentionsInserted = 0;
  let totalSkippedNoId = 0;
  let successCount = 0;
  let failureCount = 0;

  for (let i = 0; i < tokens.length; i += 1) {
    const token = tokens[i];
    const result = await processToken(token);

    totalFetched += result.tweetsFetched;
    totalRawInserted += result.rawInserted;
    totalMentionsInserted += result.mentionsInserted;
    totalSkippedNoId += result.skippedNoId;

    if (result.ok) {
      successCount += 1;
      logInfo("Token processed", {
        index: i + 1,
        total: tokens.length,
        tokenId: result.tokenId,
        tweetsFetched: result.tweetsFetched,
        rawInserted: result.rawInserted,
        mentionsInserted: result.mentionsInserted,
        skippedNoId: result.skippedNoId,
      });
    } else {
      failureCount += 1;
      logError("Token failed", {
        index: i + 1,
        total: tokens.length,
        tokenId: result.tokenId,
        reason: result.reason,
      });
    }
  }

  const snapshotsInserted = await updateSocialSnapshots(SNAPSHOT_WINDOW_MINUTES);

  logInfo("Worker complete", {
    durationSeconds: Math.round((Date.now() - startedAt) / 1000),
    tokensProcessed: tokens.length,
    tokenSuccesses: successCount,
    tokenFailures: failureCount,
    tweetsFetched: totalFetched,
    rawTweetsInserted: totalRawInserted,
    mentionsInserted: totalMentionsInserted,
    skippedNoId: totalSkippedNoId,
    snapshotsInserted,
  });
}

fetchMentions()
  .then(async () => {
    await pool.end();
    process.exit(0);
  })
  .catch(async (err) => {
    logError("Worker failed", { error: err.message });
    try {
      await pool.end();
    } catch (_) {}
    process.exit(1);
  });
