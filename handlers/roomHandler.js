// ─── Room Handler ─────────────────────────────────────────────────────────────
//
//  Sécurité appliquée :
//  1. Validation des entrées (room, type)
//  2. Le socket doit appartenir à data.room (anti-room-forgery)
//  3. Liste blanche des types autorisés (ALLOWED_TYPES)
//  4. Types sensibles réservés à l'admin (ADMIN_ONLY_TYPES)
//  5. listLot inclus dans ADMIN_ONLY_TYPES
//  6. Anti-flood joinroom via Redis (TTL auto-expirant)
//  7. Max 1 salle active par socket
//  8. Cap taille payload getMsgRoom (50 Ko)
// ─────────────────────────────────────────────────────────────────────────────

const socketMeta = require("../store");
const { log } = require("../utils/logger");
const {
  getAdminOfRoom,
  broadcastUserList,
} = require("../services/roomService");
const redis = require("../redis");
const fs = require("fs");
const path = require("path");

// ── Chemin vers l'historique ──────────────────────────────────────────────────
const HISTORIQUE_PATH = path.resolve(__dirname, "../historique.json");

// ── Throttle joinroom ─────────────────────────────────────────────────────────
//
//  Stratégie Redis : clé "throttle:joinroom:<socketId>" avec TTL 2s.
//  Si la clé existe → throttle actif → on refuse.
//  Fallback Map mémoire si Redis indisponible.
//
//  La Map est exportée pour que server.js la nettoie au disconnect
//  (cas fallback uniquement — Redis gère son propre TTL).

const joinroomThrottle = new Map(); // fallback mémoire
const JOINROOM_THROTTLE_S = 2; // secondes entre deux joinroom

// ── Cap taille payload ────────────────────────────────────────────────────────
const MAX_PAYLOAD_BYTES = 50_000; // 50 Ko max par événement getMsgRoom

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

// ── Utilitaires historique ────────────────────────────────────────────────────

function loadHistorique() {
  try {
    if (!fs.existsSync(HISTORIQUE_PATH)) return [];
    const raw = fs.readFileSync(HISTORIQUE_PATH, "utf8");
    return JSON.parse(raw);
  } catch {
    return [];
  }
}

function appendHistorique(entry) {
  try {
    const data = loadHistorique();
    data.push(entry);
    fs.writeFileSync(HISTORIQUE_PATH, JSON.stringify(data, null, 2), "utf8");
  } catch (err) {
    log(`  [historique] ERREUR écriture : ${err.message}`);
  }
}

function updateHistoriquePseudo(socketId, pseudo) {
  try {
    const data = loadHistorique();
    for (let i = data.length - 1; i >= 0; i--) {
      if (data[i].socketId === socketId && data[i].event === "joinroom") {
        data[i].pseudo = pseudo;
        break;
      }
    }
    fs.writeFileSync(HISTORIQUE_PATH, JSON.stringify(data, null, 2), "utf8");
    log(
      `  [historique] pseudo mis à jour → socketId=${socketId} pseudo="${pseudo}"`,
    );
  } catch (err) {
    log(`  [historique] ERREUR mise à jour pseudo : ${err.message}`);
  }
}

function getClientIp(socket) {
  const forwarded = socket.handshake.headers["x-forwarded-for"];
  if (forwarded) return forwarded.split(",")[0].trim();
  return socket.handshake.address || "inconnue";
}

// ── Throttle joinroom — Redis avec fallback mémoire ───────────────────────────

/**
 * Vérifie si le socket peut faire un joinroom.
 * Retourne true si autorisé, false si throttlé.
 */
async function checkJoinroomThrottle(socketId) {
  const key = `throttle:joinroom:${socketId}`;

  try {
    // SET NX EX : pose la clé seulement si elle n'existe pas, TTL 2s
    const result = await redis.set(key, "1", "EX", JOINROOM_THROTTLE_S, "NX");
    // result = "OK" si la clé a été posée (premier appel dans la fenêtre)
    // result = null si la clé existait déjà (throttle actif)
    return result === "OK";
  } catch {
    // Fallback mémoire si Redis indisponible
    const now = Date.now();
    const last = joinroomThrottle.get(socketId) || 0;
    if (now - last < JOINROOM_THROTTLE_S * 1000) return false;
    joinroomThrottle.set(socketId, now);
    return true;
  }
}

// ─────────────────────────────────────────────────────────────────────────────

function registerRoomHandler(io, socket) {
  // ───────────────────────────────────────────────────────────────────────────
  /**
   * Rejoindre une salle.
   * room = "auctav<saleId>"  ex: "auctav42"
   */
  socket.on("joinroom", async (room) => {
    // ── Contrôle 1 : throttle joinroom (Redis) ────────────────────────────
    const allowed = await checkJoinroomThrottle(socket.id);
    if (!allowed) {
      log(`  [joinroom] THROTTLE – socket=${socket.id}`);
      return;
    }

    // ── Contrôle 2 : format room ──────────────────────────────────────────
    if (typeof room !== "string" || !room.trim()) {
      log(`  [joinroom] REFUSÉ room invalide (type) – socket=${socket.id}`);
      return;
    }

    const roomTrimmed = room.trim();

    if (!/^auctav\d+$/.test(roomTrimmed)) {
      log(
        `  [joinroom] REFUSÉ room invalide (pattern) "${roomTrimmed}" – socket=${socket.id}`,
      );
      return;
    }

    // ── Contrôle 3 : max 1 salle active par socket ────────────────────────
    const meta = socketMeta.get(socket.id);
    const clientIp = getClientIp(socket);

    if (meta?.room && meta.room === roomTrimmed) {
      log(
        `  [joinroom] IGNORÉ – socket=${socket.id} déjà dans "${roomTrimmed}"`,
      );
      return;
    }

    if (meta?.room) {
      const oldRoom = meta.room;
      socket.leave(oldRoom);
      log(`  [joinroom] quitte "${oldRoom}" → rejoint "${roomTrimmed}"`);
      if (meta.isAdmin) broadcastUserList(io, oldRoom);
    }

    socket.join(roomTrimmed);
    if (meta) meta.room = roomTrimmed;

    log(
      `  [joinroom] socket=${socket.id} → room="${roomTrimmed}" ip=${clientIp} admin=${meta?.isAdmin}`,
    );

    appendHistorique({
      event: "joinroom",
      socketId: socket.id,
      ip: clientIp,
      room: roomTrimmed,
      pseudo: meta?.pseudo || "inconnu",
      isAdmin: meta?.isAdmin ?? false,
      timestamp: new Date().toISOString(),
    });

    if (meta?.isAdmin) {
      broadcastUserList(io, roomTrimmed);
    } else {
      const adminId = getAdminOfRoom(roomTrimmed);
      socket.emit("userList", { admin: adminId });
      log(`  [userList→${socket.id}] admin=${adminId || "none"}`);
    }
  });

  // ───────────────────────────────────────────────────────────────────────────
  /**
   * Réception du pseudo du client.
   */
  socket.on("username", (pseudo) => {
    if (typeof pseudo !== "string" || !pseudo.trim()) return;

    const safePseudo = pseudo.trim().slice(0, 64);
    const meta = socketMeta.get(socket.id);
    if (meta) meta.pseudo = safePseudo;

    log(`  [username] socket=${socket.id} pseudo="${safePseudo}"`);
    updateHistoriquePseudo(socket.id, safePseudo);
  });

  // ───────────────────────────────────────────────────────────────────────────
  /**
   * Diffusion d'un message vers toute la salle.
   * data = { room, type, msg, name }
   */
  socket.on("getMsgRoom", (data) => {
    // ── Contrôle 1 : champs obligatoires ─────────────────────────────────
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

    // ── Contrôle 2 : taille du payload ───────────────────────────────────
    try {
      const payloadSize = JSON.stringify(data).length;
      if (payloadSize > MAX_PAYLOAD_BYTES) {
        log(
          `  [getMsgRoom] REFUSÉ payload trop grand (${payloadSize} bytes) – socket=${socket.id}`,
        );
        return;
      }
    } catch {
      log(
        `  [getMsgRoom] REFUSÉ payload non sérialisable – socket=${socket.id}`,
      );
      return;
    }

    // ── Contrôle 3 : le socket appartient à la salle déclarée ────────────
    if (!socket.rooms.has(data.room)) {
      log(
        `  [getMsgRoom] REFUSÉ – ${socket.id} n'appartient pas à la salle "${data.room}"`,
      );
      return;
    }

    // ── Contrôle 4 : type dans liste blanche ──────────────────────────────
    if (!ALLOWED_TYPES.has(data.type)) {
      log(
        `  [getMsgRoom] REFUSÉ – type non autorisé "${data.type}" depuis ${socket.id}`,
      );
      return;
    }

    // ── Contrôle 5 : types sensibles réservés à l'admin ──────────────────
    const meta = socketMeta.get(socket.id);
    if (ADMIN_ONLY_TYPES.has(data.type) && !meta?.isAdmin) {
      log(
        `  [getMsgRoom] REFUSÉ – type admin "${data.type}" émis par un non-admin ${socket.id}`,
      );
      return;
    }

    // ── Payload nettoyé ───────────────────────────────────────────────────
    const payload = {
      type: data.type,
      msg: data.msg || {},
      name: data.name || meta?.pseudo || "unknown",
      from: socket.id,
    };

    log(
      `  [room→${data.room}] type="${data.type}" from=${socket.id} (admin=${meta?.isAdmin})`,
    );

    io.to(data.room).emit("sendMsg", payload);
  });
}

module.exports = { registerRoomHandler, joinroomThrottle };
