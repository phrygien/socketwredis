// ─── PM2 Ecosystem — Auctav Socket.IO ────────────────────────────────────────
//
//  Mode cluster : PM2 fork autant de workers que de cœurs CPU.
//  Chaque worker écoute sur le même port — PM2 distribue les connexions.
//  Le redis-adapter synchronise les rooms et événements entre workers.
// ─────────────────────────────────────────────────────────────────────────────
module.exports = {
  apps: [
    {
      name: "auctav-socket-server", // doit correspondre à ce que PM2 affiche
      script: "server.js",
      instances: "max",
      exec_mode: "cluster",

      max_memory_restart: "300M",

      // wait_ready nécessite process.send('ready') dans server.js
      wait_ready: true,
      listen_timeout: 8000,
      kill_timeout: 5000,

      env_production: {
        NODE_ENV: "production",
        REDIS_HOST: "127.0.0.1",
        REDIS_PORT: "6379",
        REDIS_DB: "0",
      },

      out_file: "./logs/out.log",
      error_file: "./logs/error.log",
      merge_logs: true,
      log_date_format: "YYYY-MM-DD HH:mm:ss",
    },
  ],
};
