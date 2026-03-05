import express from "express";
import pkg from "pg";

const { Pool } = pkg;

const app = express();
app.use(express.json({ limit: "2mb" }));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

// --- Helpers ---
function requireHeliusAuth(req, res) {
  const expected = process.env.HELIUS_WEBHOOK_SECRET;
  const auth = req.headers.authorization || "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7) : null;

  if (!expected || token !== expected) {
    res.status(401).send("unauthorized");
    return false;
  }
  return true;
}

// --- Health routes (nice for Railway/uptime checks) ---
app.get("/", (req, res) => res.status(200).send("ok"));
app.post("/", (req, res) => res.status(200).send("ok"));
app.get("/health", (req, res) => res.status(200).send("server alive"));

// --- Webhook ---
app.post("/webhooks/helius", async (req, res) => {
  try {
    if (!requireHeliusAuth(req, res)) return;

    // Helius "enhanced" webhooks typically send an array of tx objects
    const events = Array.isArray(req.body) ? req.body : [req.body];

    // Log minimal info (avoid spamming logs with huge payloads)
    console.log(`Received ${events.length} Helius event(s)`);

    // Persist raw events first (source of truth)
    // NOTE: requires raw_webhook_events.signature UNIQUE for ON CONFLICT to work
    for (const e of events) {
      const signature = e?.signature;
      if (!signature) continue;

      await pool.query(
        `
        INSERT INTO raw_webhook_events
          (signature, timestamp, slot, type, payload, created_at)
        VALUES
          ($1, $2, $3, $4, $5, NOW())
        ON CONFLICT (signature) DO NOTHING
        `,
        [
          signature,
          e?.timestamp ?? null,
          e?.slot ?? null,
          e?.type ?? null,
          e, // pg will JSON-encode object for json/jsonb column; if payload is TEXT, this becomes stringified
        ]
      );
    }

    // Later: parse pump.fun trades here into wallet_trades + wallet_token_positions

    return res.status(200).json({ success: true });
  } catch (err) {
    console.error("Webhook error:", err);
    return res.status(500).send("server error");
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("Server running on port", PORT));
