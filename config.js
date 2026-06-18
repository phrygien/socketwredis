// ─── Configuration ────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 3006;

const ALLOWED_ORIGINS = process.env.ALLOWED_ORIGINS
  ? process.env.ALLOWED_ORIGINS.split(",")
  : [
      "https://www.auctav.com",
      "https://auctav.com",
      "https://dev.astucom.com",
      "https://dev.astucom.com:9022",
      "http://localhost",
      "http://localhost:9022",
      "http://127.0.0.1",
      "http://127.0.0.1:9022",
    ];

module.exports = { PORT, ALLOWED_ORIGINS };
