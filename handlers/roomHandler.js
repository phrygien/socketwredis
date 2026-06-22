// ─── Room Handler ─────────────────────────────────────────────────────────────
//
//  Optimisations latence :
//  1. Écriture historique asynchrone + buffer (ne bloque pas la event loop)
//  2. socketMeta locale pour le hot path (une seule récupération par event)
//  3. Payload nettoyé en amont
//
//  Sécurité conservée :
//  1. Validation des entrées (room, type)
//  2. Le socket doit appartenir à data.room (anti-room-forgery)
//  3. Liste blanche des types autorisés (ALLOWED_TYPES)
//  4. Types sensibles réservés à l'admin (ADMIN_ONLY_TYPES)
//
//  CONTEXTE CLUSTER (PM2 fork multi-instances + Redis adapter) :
//  5. Anti double-admin CROSS-WORKER : getAdminOfRoom() interroge maintenant
//     l'état partagé Redis (tous workers), pas seulement la Map locale.
//     L'ancien admin peut donc être connecté à un AUTRE process que celui
//     qui traite l'event 'joinroom' courant.
//
//     io.sockets.sockets.get(id) ne fonctionne QUE sur le worker local —
//     si l'ancien admin est sur un autre worker, ça renvoie toujours
//     `undefined`, qu'il soit fantôme ou bien vivant. On ne peut donc plus
//     s'en servir pour décider "fantôme vs vivant".
//
//     À la place : io.in(socketId).disconnectSockets(true) — fourni par
//     Socket.IO, fonctionne cross-worker via le Redis adapter (chaque socket
//     rejoint automatiquement une room implicite nommée par son socket.id).
//     Si le socket n'existe nulle part (vraiment fantôme), cet appel est un
//     no-op silencieux — donc on nettoie systématiquement l'entrée Redis
//     juste après, sans avoir besoin de savoir au préalable s'il était vivant.
// ─────────────────────────────────────────────────────────────────────────────

const store = require("../store");
const { log } = require("../utils/logger");
const {
  getAdminOfRoom,
  broadcastUserList,
} = require("../services/roomService");
const fs = require("fs");
const path = require("path");

// ── Chemin vers l'historique ──────────────────────────────────────────────────
const HISTORIQUE_PATH = path.resolve(__dirname, "../historique.json");

// ── Listes de contrôle ────────────────────────────────────────────────────────

const ALLOWED_TYPES = new Set([
  "listLot",
  "numLot",
  "previousLot",
  "message",
  "users",
  "closeEnchere",
  "updateLot",
]);

const ADMIN_ONLY_TYPES = new Set([
  "listLot",
  "numLot",
  "previousLot",
  "closeEnchere",
  "updateLot",
]);

// ─────────────────────────────────────────────────────────────────────────────
// HISTORIQUE — ÉCRITURE ASYNCHRONE BUFFERISÉE
//
// Buffer en mémoire (tableau) + flush async toutes les 5 secondes.
// Zéro impact sur le hot path Socket.IO.
//
// Note cluster : ce buffer reste LOCAL à chaque worker — chaque process
// écrit dans son propre fichier historique.json. Si tu as besoin d'un
// historique unifié tous workers confondus, il faudra soit utiliser un
// suffixe de fichier par instance (historique-<PORT>.json) pour éviter
// les écritures concurrentes corrompues, soit centraliser via Redis/DB.
// Pour l'instant, le code suppose un fichier par instance (voir PORT
// injecté plus bas) — à adapter si un historique global est nécessaire.
// ─────────────────────────────────────────────────────────────────────────────

let historiqueBuffer = [];
let flushScheduled = false;

function appendHistorique(entry) {
  historiqueBuffer.push(entry);
  scheduleFlush();
}

function scheduleFlush() {
  if (flushScheduled) return;
  flushScheduled = true;
  setTimeout(flushHistorique, 5000);
}

function flushHistorique() {
  flushScheduled = false;
  if (historiqueBuffer.length === 0) return;

  const toWrite = historiqueBuffer.splice(0);
  const lines = toWrite.map((e) => JSON.stringify(e)).join("\n") + "\n";

  fs.appendFile(HISTORIQUE_PATH, lines, "utf8", (err) => {
    if (err) log(`[historique] ERREUR flush : ${err.message}`);
  });
}

function flushHistoriqueSync() {
  if (historiqueBuffer.length === 0) return;
  const toWrite = historiqueBuffer.splice(0);
  const lines = toWrite.map((e) => JSON.stringify(e)).join("\n") + "\n";
  try {
    fs.appendFileSync(HISTORIQUE_PATH, lines, "utf8");
  } catch (err) {
    log(`[historique] ERREUR flush sync : ${err.message}`);
  }
}

// Branché sur SIGTERM/SIGINT dans server.js — appeler flushHistoriqueSync() avant process.exit
process.on("SIGTERM", flushHistoriqueSync);
process.on("SIGINT", flushHistoriqueSync);

/**
 * Met à jour le pseudo dans le buffer en mémoire (pas de I/O).
 */
function updateBufferPseudo(socketId, pseudo) {
  for (let i = historiqueBuffer.length - 1; i >= 0; i--) {
    if (
      historiqueBuffer[i].socketId === socketId &&
      historiqueBuffer[i].event === "joinroom"
    ) {
      historiqueBuffer[i].pseudo = pseudo;
      break;
    }
  }
}

// ── Utilitaire IP ─────────────────────────────────────────────────────────────

function getClientIp(socket) {
  const forwarded = socket.handshake.headers["x-forwarded-for"];
  if (forwarded) return forwarded.split(",")[0].trim();
  return socket.handshake.address || "inconnue";
}

// ─────────────────────────────────────────────────────────────────────────────
// ANTI DOUBLE-ADMIN — CROSS-WORKER
//
// getAdminOfRoom() interroge Redis (tous workers). Si un admin existant est
// trouvé et que ce n'est pas le socket courant, on l'expulse via
// io.in(socketId).disconnectSockets(true), qui fonctionne peu importe le
// worker où ce socket est réellement connecté.
//
// On nettoie systématiquement l'entrée Redis de l'ancien admin juste après
// — que disconnectSockets ait trouvé un socket vivant ou non. Si le socket
// était vivant, son propre disconnectHandler fera aussi le ménage (idempotent,
// pas de risque à nettoyer deux fois).
// ─────────────────────────────────────────────────────────────────────────────

async function evictStaleAdmin(io, room, incomingSocketId) {
  const existingAdminId = await getAdminOfRoom(room);
  if (!existingAdminId || existingAdminId === incomingSocketId) return;

  log(
    `  [ADMIN REPLACE] ancien admin=${existingAdminId} expulsé de room="${room}" (remplacé par ${incomingSocketId})`,
  );

  // Cross-worker : no-op silencieux si le socket n'existe nulle part.
  io.in(existingAdminId).disconnectSockets(true);

  // Nettoyage Redis systématique — couvre le cas où le socket était déjà
  // fantôme (déconnecté sans que son disconnectHandler ait pu tourner,
  // ex: crash du worker qui l'hébergeait).
  await store.deleteShared(existingAdminId);
}

// ─────────────────────────────────────────────────────────────────────────────

function registerRoomHandler(io, socket) {
  /**
   * Rejoindre une salle.
   */
  socket.on("joinroom", async (room) => {
    if (typeof room !== "string" || !room.trim()) {
      log(`  [joinroom] REFUSÉ room invalide – socket=${socket.id}`);
      return;
    }

    const meta = store.get(socket.id);
    const clientIp = getClientIp(socket);

    // Quitter l'ancienne salle
    if (meta?.room) {
      socket.leave(meta.room);
      if (meta.isAdmin) await broadcastUserList(io, meta.room);
    }

    // ── Anti double-admin : expulser tout admin précédent sur cette room ──
    if (meta?.isAdmin) {
      await evictStaleAdmin(io, room, socket.id);
    }

    socket.join(room);
    if (meta) {
      meta.room = room;
      await store.syncShared(socket.id, meta);
    }

    log(
      `  [joinroom] socket=${socket.id} → room="${room}" ip=${clientIp} admin=${meta?.isAdmin}`,
    );

    // Historique — asynchrone, ne bloque pas
    appendHistorique({
      event: "joinroom",
      socketId: socket.id,
      ip: clientIp,
      room,
      pseudo: meta?.pseudo || "inconnu",
      isAdmin: meta?.isAdmin ?? false,
      timestamp: new Date().toISOString(),
    });

    if (meta?.isAdmin) {
      await broadcastUserList(io, room);
    } else {
      const adminId = await getAdminOfRoom(room);
      socket.emit("userList", { admin: adminId });
      log(`  [userList→${socket.id}] admin=${adminId || "none"}`);
    }
  });

  // ───────────────────────────────────────────────────────────────────────────

  socket.on("username", async (pseudo) => {
    if (typeof pseudo !== "string" || !pseudo.trim()) return;

    const trimmed = pseudo.trim();
    const meta = store.get(socket.id);
    if (meta) {
      meta.pseudo = trimmed;
      await store.syncShared(socket.id, meta);
    }

    log(`  [username] socket=${socket.id} pseudo="${trimmed}"`);

    // Mise à jour en mémoire uniquement — zéro I/O disque
    updateBufferPseudo(socket.id, trimmed);
  });

  // ───────────────────────────────────────────────────────────────────────────
  // HOT PATH — reste 100% synchrone, aucun accès Redis ici.
  // Le contrôle admin utilise la Map locale (store.get), suffisant car on ne
  // vérifie ici que "CE socket (sur CE worker) est-il admin", pas une
  // question cross-worker.
  // ───────────────────────────────────────────────────────────────────────────

  socket.on("getMsgRoom", (data) => {
    // Contrôle 1 : champs obligatoires
    if (!data || typeof data.room !== "string" || !data.room.trim()) {
      log(
        `  [getMsgRoom] REFUSÉ champ room absent/invalide – socket=${socket.id}`,
      );
      return;
    }
    if (typeof data.type !== "string" || !data.type.trim()) {
      log(
        `  [getMsgRoom] REFUSÉ champ type absent/invalide – socket=${socket.id}`,
      );
      return;
    }

    // Contrôle 2 : appartenance à la salle
    if (!socket.rooms.has(data.room)) {
      log(
        `  [getMsgRoom] REFUSÉ – ${socket.id} n'appartient pas à "${data.room}"`,
      );
      return;
    }

    // Contrôle 3 : type dans la liste blanche
    if (!ALLOWED_TYPES.has(data.type)) {
      log(
        `  [getMsgRoom] REFUSÉ – type non autorisé "${data.type}" depuis ${socket.id}`,
      );
      return;
    }

    // Contrôle 4 : types admin uniquement
    const meta = store.get(socket.id);
    if (ADMIN_ONLY_TYPES.has(data.type) && !meta?.isAdmin) {
      log(
        `  [getMsgRoom] REFUSÉ – type admin "${data.type}" émis par non-admin ${socket.id}`,
      );
      return;
    }

    const payload = {
      type: data.type,
      msg: data.msg || {},
      name: data.name || meta?.pseudo || "unknown",
      from: socket.id,
    };

    log(
      `  [room→${data.room}] type="${data.type}" from=${socket.id} (admin=${meta?.isAdmin})`,
    );

    // Diffuse à toute la salle — traverse tous les workers via le Redis
    // adapter de Socket.IO (transparent, géré par io.adapter() dans server.js).
    io.to(data.room).emit("sendMsg", payload);
  });
}

module.exports = { registerRoomHandler, flushHistoriqueSync };
