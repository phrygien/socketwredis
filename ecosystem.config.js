// ─── PM2 Ecosystem — Auctav Socket.IO ────────────────────────────────────────
//
//  Mode cluster : PM2 fork autant de workers que de cœurs CPU.
//  Chaque worker écoute sur le même port — PM2 distribue les connexions.
//  Le redis-adapter synchronise les rooms et événements entre workers.
// ─────────────────────────────────────────────────────────────────────────────

module.exports = {
  apps: [
    {
      name: "auctav-socket-server",
      script: "server.js",
      instances: "max", // 1 worker par cœur CPU
      exec_mode: "cluster", // OBLIGATOIRE pour -i max

      // Redémarre un worker si sa mémoire dépasse 300 Mo
      max_memory_restart: "300M",

      // Variables d'environnement production
      env_production: {
        NODE_ENV: "production",
        REDIS_HOST: "127.0.0.1",
        REDIS_PORT: "6379",
        // REDIS_PASSWORD : '',
        REDIS_DB: "0",
      },

      // Logs
      out_file: "./logs/out.log",
      error_file: "./logs/error.log",
      merge_logs: true, // fusionne les logs de tous les workers
      log_date_format: "YYYY-MM-DD HH:mm:ss",

      // Redémarrage progressif (zero-downtime)
      // Attend que le nouveau worker soit prêt avant de tuer l'ancien
      wait_ready: true,
      listen_timeout: 8000, // ms pour que le worker démarre
      kill_timeout: 5000, // ms pour que le worker s'arrête proprement
    },
  ],
};
