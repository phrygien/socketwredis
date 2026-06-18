const Redis = require("ioredis");
const { log } = require("./utils/logger");

const redis = new Redis({
  host: process.env.REDIS_HOST || "127.0.0.1",
  port: parseInt(process.env.REDIS_PORT || "6379", 10),
  password: process.env.REDIS_PASSWORD || undefined,
  db: parseInt(process.env.REDIS_DB || "0", 10),

  retryStrategy(times) {
    if (times > 20) {
      log(`[REDIS] Abandon reconnexion apres ${times} tentatives`);
      return null;
    }
    const delay = Math.min(times * 200, 5000);
    log(`[REDIS] Reconnexion dans ${delay}ms (tentative ${times})`);
    return delay;
  },

  commandTimeout: 2000,
  connectTimeout: 5000,

  lazyConnect: false,
  enableOfflineQueue: true,
});

redis.on("connect", () => log("[REDIS] Connecte"));
redis.on("ready", () => log("[REDIS] Pret"));
redis.on("error", (err) => log(`[REDIS] Erreur : ${err.message}`));
redis.on("close", () => log("[REDIS] Connexion fermee"));
redis.on("reconnecting", (ms) => log(`[REDIS] Reconnexion dans ${ms}ms`));

module.exports = redis;
