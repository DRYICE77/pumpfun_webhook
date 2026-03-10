import pg from "pg";


const { Pool } = pg;

const DATABASE_URL = process.env.DATABASE_URL;
const X_BEARER_TOKEN = process.env.X_BEARER_TOKEN;

if (!DATABASE_URL) {
  console.error("Missing DATABASE_URL");
  process.exit(1);
}

if (!X_BEARER_TOKEN) {
  console.error("Missing X_BEARER_TOKEN");
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
      token_id INTEGER NOT NULL,
      tweet_id TEXT NOT NULL,
      match_value TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS token_social_snapshots (
      id BIGSERIAL PRIMARY KEY,
      token_id INTEGER NOT NULL,
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
    CREATE INDEX IF NOT EXISTS idx_token_x_mentions_token_id
    ON token_x_mentions (token_id);
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
      token_id AS id,
      token_address
    FROM tracked_tokens
    WHERE token_address IS NOT NULL
      AND token_address <> ''
      AND (
        buys_5m > 0
        OR sells_5m > 0
        OR volume_5m > 0
      )
    ORDER BY last_updated DESC
    LIMIT $1
    `,
    [limit]
  );

  return result.rows;
}

async function searchTweetsForAddress(address) {
  const params = new URLSearchParams({
    query: `"${address}"`,
    "tweet.fields": "author_id,created_at,public_metrics",
    max_results: "10",
  });

  const url = `https://api.twitter.com/2/tweets/search/recent?${params.toString()}`;

  const res = await fetch(url, {
    method: "GET",
    headers: {
      Authorization: `Bearer ${X_BEARER_TOKEN}`,
      "Content-Type": "application/json",
    },
  });

  const text = await res.text();

  let json;
  try {
    json = JSON.parse(text);
  } catch (err) {
    console.error(`Failed to parse X response for ${address}:`, text.slice(0, 500));
    return { ok: false, status: res.status, json: null };
  }

  if (!res.ok) {
    console.error(`X API error for ${address}: status=${res.status}`, JSON.stringify(json).slice(0, 500));
    return { ok: false, status: res.status, json };
  }

  return { ok: true, status: res.status, json };
}

async function insertRawTweet(tweet) {
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
      tweet.id,
      tweet.author_id || null,
      tweet.text || null,
      tweet.created_at || null,
      JSON.stringify(tweet.public_metrics || {}),
    ]
  );
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

async function fetchMentions() {
  console.log("X worker started");

  await ensureTables();
  console.log("X tables ready");

  const tokens = await getRecentTokens(50);
  console.log("tokens found:", tokens.length);

  if (tokens.length === 0) {
    console.log("No tokens found. Exiting.");
    return;
  }

  let totalTweetsInserted = 0;
  let totalMentionsInserted = 0;

  for (const token of tokens) {
    const tokenId = token.id;
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

    const json = result.json;
    console.log("x response preview:", JSON.stringify(json).slice(0, 300));

    const tweets = json.data || [];

    if (tweets.length === 0) {
      console.log(`No tweets found for ${address}`);
      continue;
    }

    console.log(`Found ${tweets.length} tweets for ${address}`);

    for (const tweet of tweets) {
      try {
        await insertRawTweet(tweet);
        totalTweetsInserted += 1;

        await insertTokenMention(tokenId, tweet.id, address);
        totalMentionsInserted += 1;

        console.log(`Stored tweet ${tweet.id} for token ${tokenId}`);
      } catch (err) {
        console.error(`Failed storing tweet ${tweet.id} for token ${tokenId}:`, err.message);
      }
    }
  }

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

fetchMentions();
