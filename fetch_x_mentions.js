import pg from "pg";
import fetch from "node-fetch";

const { Pool } = pg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

const BEARER = process.env.X_BEARER_TOKEN;

async function fetchMentions() {

  const tokens = await pool.query(`
    SELECT id, token_address
    FROM tokens
    ORDER BY created_at DESC
    LIMIT 50
  `);

  for (const token of tokens.rows) {

    const address = token.token_address;

    const url = `https://api.twitter.com/2/tweets/search/recent?query="${address}"&tweet.fields=public_metrics,author_id,created_at&max_results=25`;

    const res = await fetch(url, {
      headers: {
        "Authorization": `Bearer ${BEARER}`
      }
    });

    const json = await res.json();

    if (!json.data) continue;

    for (const tweet of json.data) {

      const metrics = tweet.public_metrics;

      await pool.query(`
        INSERT INTO x_raw_tweets (
          tweet_id,
          text,
          author_id,
          created_at,
          public_metrics
        )
        VALUES ($1,$2,$3,$4,$5)
        ON CONFLICT (tweet_id) DO NOTHING
      `, [
        tweet.id,
        tweet.text,
        tweet.author_id,
        tweet.created_at,
        JSON.stringify(metrics)
      ]);

      await pool.query(`
        INSERT INTO token_x_mentions (
          token_id,
          tweet_id,
          match_value
        )
        VALUES ($1,$2,$3)
        ON CONFLICT DO NOTHING
      `, [
        token.id,
        tweet.id,
        address
      ]);

    }

    console.log(`indexed mentions for ${address}`);

  }

}

fetchMentions();
