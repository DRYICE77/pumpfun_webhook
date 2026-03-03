import express from "express";
import pkg from "pg";

const { Pool } = pkg;

const app = express();
app.use(express.json({ limit: "2mb" }));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

app.get("/health", (req, res) => {
  res.send("server alive");
});

app.post("/webhooks/helius", async (req, res) => {
  try {

    const auth = req.headers.authorization;

    if (auth !== `Bearer ${process.env.HELIUS_WEBHOOK_SECRET}`) {
      return res.status(401).send("unauthorized");
    }

    console.log("Received Helius event:");
    console.log(JSON.stringify(req.body, null, 2));

    // Later we will parse pump.fun trades here

    res.json({ success: true });

  } catch (err) {
    console.error(err);
    res.status(500).send("server error");
  }
});

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log("Server running on port", PORT);
});
