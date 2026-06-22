// ─── Admin Handler ────────────────────────────────────────────────────────────
// Événements émis par switcher.php
//
// CONTEXTE CLUSTER (PM2 fork multi-instances + Redis adapter) :
//   Sécurité complémentaire à celle de roomHandler.js (event 'joinroom'),
//   qui couvre le cas le plus fréquent (admin → joinroom). Ici on couvre
//   l'ordre inverse ou un re-emit de 'admin' sans nouveau 'joinroom'.
//
//   Comme dans roomHandler.js : getAdminOfRoom() interroge maintenant Redis
//   (cross-worker), et l'éviction de l'ancien admin utilise
//   io.in(socketId).disconnectSockets(true) au lieu de
//   io.sockets.sockets.get(id) — ce dernier ne fonctionne que localement et
//   renverrait toujours `undefined` si l'ancien admin est sur un autre
//   worker, qu'il soit vivant ou fantôme.
// ─────────────────────────────────────────────────────────────────────────────

const store = require("../store");
const { log } = require("../utils/logger");
const {
  broadcastUserList,
  getAdminOfRoom,
} = require("../services/roomService");

function registerAdminHandler(io, socket) {
  /**
   * Identification de l'admin — émis avant joinroom.
   * socket.emit('admin', pseudo)
   */
  socket.on("admin", async (pseudo) => {
    const meta = store.get(socket.id);

    if (meta) {
      meta.pseudo = pseudo || "Admin";
      meta.isAdmin = true;
      await store.syncShared(socket.id, meta);

      // Si ce socket est déjà dans une room (reconnexion rapide, ou 'admin'
      // émis après 'joinroom'), s'assurer qu'aucun autre socket n'est
      // encore marqué admin sur cette même room — cross-worker.
      if (meta.room) {
        const existingAdminId = await getAdminOfRoom(meta.room);

        if (existingAdminId && existingAdminId !== socket.id) {
          log(
            `  [ADMIN REPLACE] (via 'admin' event) ancien admin=${existingAdminId} expulsé de room="${meta.room}" (remplacé par ${socket.id})`,
          );

          // Cross-worker : no-op silencieux si le socket n'existe nulle part.
          io.in(existingAdminId).disconnectSockets(true);
          await store.deleteShared(existingAdminId);
        }
      }
    }

    log(`  [admin]    : ${socket.id} → "${pseudo}"`);

    // Si l'admin était déjà dans une salle (reconnexion rapide), notifier
    if (meta?.room) await broadcastUserList(io, meta.room);
  });
}

module.exports = { registerAdminHandler };
