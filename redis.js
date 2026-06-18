// ─── Redis client — Auctav Socket.IO ─────────────────────────────────────────
//
//  Client ioredis partagé par tous les modules.
//  Connexion lazy : le client se connecte au premier appel.
//  Reconnexion automatique intégrée à ioredis.
// ─────────────────────────────────────────────────────────────────────────────

const Redis = require("ioredis");
const { log } = require("./utils/logger");

const redis = new Redis({
  host: process.env.REDIS_HOST || "127.0.0.1",
  port: parseInt(process.env.REDIS_PORT || "6379", 10),
  password: process.env.REDIS_PASSWORD || undefined,
  db: parseInt(process.env.REDIS_DB || "0", 10),

  // Reconnexion exponentielle — ne jamais crasher sur perte Redis
  retryStrategy(times) {
    if (times > 20) {
      log(`[REDIS] Abandon reconnexion après ${times} tentatives`);
      return null; // stoppe les tentatives
    }
    const delay = Math.min(times * 200, 5000); // 200ms → 5s max
    log(`[REDIS] Reconnexion dans ${delay}ms (tentative ${times})`);
    return delay;
  },

  // Timeout commandes (évite les blocages si Redis est lent)
  commandTimeout: 2000,
  connectTimeout: 5000,

  // Ne pas bloquer l'app si Redis est indisponible au démarrage
  lazyConnect: false,
  enableOfflineQueue: true, // met en file les commandes pendant déconnexion
});

redis.on("connect", () => log("[REDIS] Connecté"));
redis.on("ready", () => log("[REDIS] Prêt"));
redis.on("error", (err) => log(`[REDIS] Erreur : ${err.message}`));
redis.on("close", () => log("[REDIS] Connexion fermée"));
redis.on("reconnecting", (ms) => log(`[REDIS] Reconnexion dans ${ms}ms`));

module.exports = redis;
