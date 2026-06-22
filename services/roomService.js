// ─── Room Service ─────────────────────────────────────────────────────────────
//
// CONTEXTE CLUSTER : ces fonctions doivent répondre correctement même si
// l'admin d'une room est connecté à un AUTRE worker que celui qui exécute
// le code. Elles lisent donc l'état partagé dans Redis (store.getAllShared())
// plutôt que la Map locale.
//
// Conséquence : ces fonctions sont maintenant ASYNC (elles font un aller-retour
// Redis). Elles ne doivent être appelées que sur des events peu fréquents
// (joinroom, admin, disconnect) — jamais dans le hot path des messages/bids.
// ─────────────────────────────────────────────────────────────────────────────

const store = require("../store");
const { log } = require("../utils/logger");

/**
 * Retourne le socket.id du premier admin connecté dans une salle,
 * ou null s'il n'y en a aucun — tous workers confondus.
 */
async function getAdminOfRoom(room) {
  const all = await store.getAllShared();
  for (const meta of all) {
    if (meta.isAdmin && meta.room === room) return meta.socketId;
  }
  return null;
}

/**
 * Diffuse userList({ admin }) à toute la salle.
 * io.to(room).emit(...) traverse déjà tous les workers grâce au Redis
 * adapter de Socket.IO (différent du store applicatif ci-dessus, mais
 * même instance Redis).
 */
async function broadcastUserList(io, room) {
  const adminId = await getAdminOfRoom(room);
  io.to(room).emit("userList", { admin: adminId });
  log(`  [userList] : room=${room} admin=${adminId || "none"}`);
}

/**
 * Retourne des statistiques sur les salles actives, tous workers confondus.
 */
async function getRoomStats() {
  const all = await store.getAllShared();
  const stats = {};

  for (const meta of all) {
    const r = meta.room || "none";
    if (!stats[r]) stats[r] = { count: 0, admins: [] };
    stats[r].count++;
    if (meta.isAdmin) stats[r].admins.push(meta.pseudo);
  }

  return stats;
}

module.exports = { getAdminOfRoom, broadcastUserList, getRoomStats };
