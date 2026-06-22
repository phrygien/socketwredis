// ─── Store hybride : Map locale + miroir Redis ────────────────────────────────
//
// CONTEXTE CLUSTER (PM2 fork multi-instances + Redis adapter) :
//
// Avant le passage en cluster, `socketMeta` était une simple Map locale.
// Avec plusieurs process Node indépendants (un par port, derrière Apache),
// chaque worker n'a plus une vue complète des sockets connectés — un admin
// peut être connecté au worker A pendant qu'un acheteur est sur le worker B.
//
// Pour que getAdminOfRoom(), broadcastUserList(), getRoomStats() restent
// corrects cross-worker, les champs nécessaires à ces calculs (room, isAdmin,
// pseudo) sont maintenant DUPLIQUÉS dans Redis (hash par socket.id), en plus
// d'être gardés dans la Map locale.
//
// Pourquoi garder la Map locale en plus de Redis :
//   - Les events socket.on(...) sont synchrones et très fréquents (chaque
//     bid, chaque message). Aller chercher meta.isAdmin dans Redis à CHAQUE
//     event ajouterait une latence réseau sur le hot path.
//   - La Map locale reste la source de vérité RAPIDE pour "mon propre socket
//     sur CE worker". Redis devient la source de vérité PARTAGÉE pour les
//     questions cross-worker (qui est admin de telle room, peu importe le
//     worker où il est connecté).
//
// Règle d'usage dans le reste du code :
//   - set/get/delete locaux : pour le hot path (accès au socket courant).
//   - les fonctions Redis (setShared/getAllShared/deleteShared) sont
//     utilisées par roomService.js pour les calculs cross-worker.
//
// Clé Redis : socketmeta:<socketId>  → hash { pseudo, room, isAdmin }
// TTL de sécurité : 1h — si un worker crash sans déclencher 'disconnect'
// proprement, l'entrée Redis ne reste pas orpheline indéfiniment.
// ─────────────────────────────────────────────────────────────────────────────

const { createClient } = require("redis");
const { log } = require("./utils/logger");

const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379";
const META_TTL_SECONDS = 3600;
const META_PREFIX = "socketmeta:";

// ── Map locale (hot path, accès synchrone) ────────────────────────────────────

const localMeta = new Map();

// ── Client Redis dédié au store (distinct des pub/sub clients de l'adapter) ──

const redisClient = createClient({ url: REDIS_URL });
redisClient.on("error", (err) => log(`[store/redis] ${err.message}`));

let redisReady = false;

async function connect() {
  if (redisReady) return;
  await redisClient.connect();
  redisReady = true;
  log("[store/redis] connecté");
}

// ─────────────────────────────────────────────────────────────────────────────
// API locale — synchrone, identique à l'ancienne Map (compat hot path)
// ─────────────────────────────────────────────────────────────────────────────

function get(socketId) {
  return localMeta.get(socketId);
}

function set(socketId, meta) {
  localMeta.set(socketId, meta);
  return meta;
}

function deleteLocal(socketId) {
  return localMeta.delete(socketId);
}

function entries() {
  return localMeta.entries();
}

function size() {
  return localMeta.size;
}

// ─────────────────────────────────────────────────────────────────────────────
// API partagée — async, miroir Redis pour les calculs cross-worker
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Pousse l'état courant d'un socket dans Redis (appelé à chaque mutation
 * significative : join, username, admin).
 */
async function syncShared(socketId, meta) {
  if (!redisReady) return;
  try {
    await redisClient.hSet(META_PREFIX + socketId, {
      pseudo: meta.pseudo || "unknown",
      room: meta.room || "",
      isAdmin: meta.isAdmin ? "1" : "0",
    });
    await redisClient.expire(META_PREFIX + socketId, META_TTL_SECONDS);
  } catch (err) {
    log(`[store/redis] syncShared erreur (${socketId}): ${err.message}`);
  }
}

/**
 * Supprime l'entrée Redis d'un socket (appelé sur disconnect).
 */
async function deleteShared(socketId) {
  if (!redisReady) return;
  try {
    await redisClient.del(META_PREFIX + socketId);
  } catch (err) {
    log(`[store/redis] deleteShared erreur (${socketId}): ${err.message}`);
  }
}

/**
 * Récupère TOUTES les metas connues dans Redis (tous workers confondus).
 * Utilisé par roomService.js pour getAdminOfRoom/getRoomStats.
 *
 * Coût : un SCAN + des HGETALL. Acceptable car appelé uniquement sur des
 * events peu fréquents (joinroom, admin, disconnect) — jamais sur le hot
 * path des messages/bids.
 */
async function getAllShared() {
  if (!redisReady) return [];

  const result = [];
  let cursor = "0";

  do {
    const reply = await redisClient.scan(cursor, {
      MATCH: META_PREFIX + "*",
      COUNT: 200,
    });
    cursor = reply.cursor;

    for (const key of reply.keys) {
      const hash = await redisClient.hGetAll(key);
      if (!hash || Object.keys(hash).length === 0) continue;
      result.push({
        socketId: key.slice(META_PREFIX.length),
        pseudo: hash.pseudo || "unknown",
        room: hash.room || null,
        isAdmin: hash.isAdmin === "1",
      });
    }
  } while (cursor !== "0");

  return result;
}

module.exports = {
  connect,
  // local (sync)
  get,
  set,
  delete: deleteLocal,
  entries,
  size,
  // partagé (async, cross-worker)
  syncShared,
  deleteShared,
  getAllShared,
};
