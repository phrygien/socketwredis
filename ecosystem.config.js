module.exports = {
  apps: [
    {
      name: "socket-server-v2",
      script: "./server.js",

      // ── CLUSTER (fork multi-instances, PAS exec_mode "cluster") ──────────
      // 4 instances sur 8 cores : laisse de la marge CPU pour Apache et les
      // 2 autres apps PM2 (Laravel queue, etc.) déjà sur ce VPS.
      // Chaque instance écoute sur un port distinct (4000, 4001, 4002, 4003)
      // via NODE_APP_INSTANCE, injecté automatiquement par PM2 en fork mode
      // dès que `instances` > 1.
      instances: 4,
      exec_mode: "fork",

      // Redémarrage automatique si fuite mémoire
      max_memory_restart: "200M",

      // Délai entre redémarrages (évite les restart loops)
      restart_delay: 2000,
      exp_backoff_restart_delay: 100,

      // Variables d'environnement
      env: {
        NODE_ENV: "production",
        BASE_PORT: 4000,
        REDIS_URL: "redis://127.0.0.1:6379",
      },

      // Logs — un fichier partagé entre toutes les instances (merge_logs).
      // Chaque ligne reste identifiable via le pid loggé dans server.js.
      log_date_format: "YYYY-MM-DD HH:mm:ss",
      error_file: "./logs/socket-error.log",
      out_file: "./logs/socket-out.log",
      merge_logs: true,

      // Ne pas redémarrer si arrêt volontaire (SIGINT/SIGTERM)
      stop_exit_codes: [0],

      // Surveillance des fichiers désactivée en prod
      watch: false,
    },
  ],
};
