module.exports = {
  apps: [
    {
      name: "auctav-socket-server",
      script: "server.js",
      instances: "max",
      exec_mode: "cluster",

      max_memory_restart: "300M",

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
