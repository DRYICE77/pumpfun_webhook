import pg from "pg";

const { Pool } = pg;

const DATABASE_URL = process.env.DATABASE_URL;
const APIFY_API_TOKEN = process.env.APIFY_API_TOKEN;

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

async function getRecentTokens(limit = 50) {
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
    maxTweets: 5,
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
    console.error(`Failed to parse Apify response for ${address}:`, text.slice(0, 500));
    return { ok: false, tweets: [] };
  }

  if (!res.ok) {
    console.error(`Apify error for ${address}: status=${res.status}`, JSON.stringify(json).slice(0, 500));
    return { ok: false, tweets: [] };
  }

  if (!Array.isArray(json)) {
    console.log(`Apify returned non-array response for ${address}:`, JSON.stringify(json).slice(0, 500));
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
  const id =
    tweet.id ??
    tweet.id_str ??
    tweet.tweetId ??
    tweet.postId ??
    tweet.conversationId ??
    (tweet.url ? tweet.url.split("/").pop() : null) ??
    (tweet.tweetUrl ? tweet.tweetUrl.split("/").pop() : null);

  if (!id) {
    console.log("Tweet object with missing id:", JSON.stringify(tweet).slice(0,300));
  }

  return id ? String(id) : null;
}
function extractAuthorId(tweet) {
  return String(
    tweet.authorId ??
    tweet.user?.id ??
    tweet.author?.id ??
    ""
  );
}

function extractText(tweet) {
  return (
    tweet.text ??
    tweet.fullText ??
    ""
  );
}

function extractCreatedAt(tweet) {
  return (
    tweet.createdAt ??
    tweet.created_at ??
    new Date().toISOString()
  );
}

async function insertRawTweet(tweet) {
  const tweetId = extractTweetId(tweet);
  const authorId = extractAuthorId(tweet);
  const text = extractText(tweet);
  const createdAt = extractCreatedAt(tweet);
  const publicMetrics = extractMetrics(tweet);

 if (!tweetId) {
  console.log("Skipping tweet with no id");
  return null;
}

  await pool.query(
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
    `,
    [
      tweetId,
      authorId || null,
      text || null,
      createdAt,
      JSON.stringify(publicMetrics),
    ]
  );

  return tweetId;
}

async function insertTokenMention(tokenId, tweetId, matchValue) {
  await pool.query(
    `
    INSERT INTO token_x_mentions (
      token_id,
      tweet_id,
      match_value
    )
    VALUES ($1, $2, $3)
    ON CONFLICT (token_id, tweet_id) DO NOTHING
    `,
    [tokenId, tweetId, matchValue]
  );
}

async function updateSocialSnapshots(windowMinutes = 5) {
  await pool.query(
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
}

async function fetchMentions() {
  console.log("X worker started");

  await ensureTables();
  console.log("X tables ready");

  const tokens = await getRecentTokens(10);
  console.log("tokens found:", tokens.length);

  if (tokens.length === 0) {
    console.log("No tracked tokens found. Exiting.");
    return;
  }

  let totalTweetsInserted = 0;
  let totalMentionsInserted = 0;

  for (const token of tokens) {
    const tokenId = token.token_id;
    const address = token.token_address;

    if (!address) {
      console.log(`Skipping token ${tokenId}: missing token_address`);
      continue;
    }

    console.log(`Searching address: ${address}`);

    const result = await searchTweetsForAddress(address);

    if (!result.ok) {
      console.log(`Skipping address due to API error: ${address}`);
      continue;
    }

    const tweets = result.tweets;
    console.log(`Apify tweets found for ${address}: ${tweets.length}`);

    if (tweets.length === 0) {
      continue;
    }

    for (const tweet of tweets) {
      try {
    const tweetText = extractText(tweet);
const tweetId = await insertRawTweet(tweet);
await insertTokenMention(tokenId, tweetId, address);

console.log(`Stored tweet ${tweetId} for token ${tokenId}`);
console.log(`Tweet text preview: ${(tweetText || "").slice(0, 200)}`);

        totalTweetsInserted += 1;
        totalMentionsInserted += 1;

        console.log(`Stored tweet ${tweetId} for token ${tokenId}`);
      } catch (err) {
        console.error(`Failed storing tweet for token ${tokenId}:`, err.message);
      }
    }
  }

  await updateSocialSnapshots(5);
  console.log("Updated token_social_snapshots");

  console.log("X worker complete");
  console.log("total raw tweets processed:", totalTweetsInserted);
  console.log("total token mentions processed:", totalMentionsInserted);
}

fetchMentions()
  .then(async () => {
    await pool.end();
    process.exit(0);
  })
  .catch(async (err) => {
    console.error("X worker failed:", err);
    try {
      await pool.end();
    } catch (_) {}
    process.exit(1);
  });
