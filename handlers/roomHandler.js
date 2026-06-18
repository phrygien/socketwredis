// ─── Room Handler ─────────────────────────────────────────────────────────────
//
//  Sécurité appliquée :
//  1. Validation des entrées (room, type)
//  2. Le socket doit appartenir à data.room (anti-room-forgery)
//  3. Liste blanche des types autorisés (ALLOWED_TYPES)
//  4. Types sensibles réservés à l'admin (ADMIN_ONLY_TYPES)
//  5. listLot inclus dans ADMIN_ONLY_TYPES
//  6. Anti-flood joinroom (throttle 2 s par socket)
//  7. Max 1 salle active par socket (pas de multi-room)
//  8. Cap taille payload getMsgRoom (50 Ko)
// ─────────────────────────────────────────────────────────────────────────────

const socketMeta = require("../store");
const { log } = require("../utils/logger");
const {
  getAdminOfRoom,
  broadcastUserList,
} = require("../services/roomService");
const fs = require("fs");
const path = require("path");

// ── Chemin vers l'historique ──────────────────────────────────────────────────
const HISTORIQUE_PATH = path.resolve(__dirname, "../historique.json");

// ── Throttle joinroom (partagé avec server.js pour le nettoyage disconnect) ───
// Map socketId → timestamp de la dernière tentative joinroom
const joinroomThrottle = new Map();
const JOINROOM_THROTTLE_MS = 2000; // 1 joinroom max toutes les 2 secondes

// ── Cap taille payload ────────────────────────────────────────────────────────
const MAX_PAYLOAD_BYTES = 50_000; // 50 Ko max par événement getMsgRoom

// ── Listes de contrôle ────────────────────────────────────────────────────────

/**
 * Tous les types qu'un socket peut diffuser via getMsgRoom.
 * Tout type absent est bloqué, même s'il existe côté client.
 */
const ALLOWED_TYPES = new Set([
  "listLot", // liste complète des lots (admin → salle)
  "numLot", // changement de lot en cours (admin → screen.php)
  "previousLot", // lot précédent avec prix adjugé (admin → screen.php)
  "message", // message texte libre (admin → follow.php)
  "users", // liste HTML des bidders connectés (admin → follow.php)
  "closeEnchere", // clôture d'une enchère (admin → results.php)
  "updateLot", // mise à jour d'un lot (admin → results.php)
]);

/**
 * Types réservés à l'admin.
 * Un bidder ou un visiteur qui émet l'un de ces types est bloqué,
 * même s'il a rejoint la salle correctement.
 */
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

/**
 * Récupère l'IP réelle du socket (prend en compte les proxies).
 */
function getClientIp(socket) {
  const forwarded = socket.handshake.headers["x-forwarded-for"];
  if (forwarded) return forwarded.split(",")[0].trim();
  return socket.handshake.address || "inconnue";
}

// ─────────────────────────────────────────────────────────────────────────────

function registerRoomHandler(io, socket) {
  // ───────────────────────────────────────────────────────────────────────────
  /**
   * Rejoindre une salle.
   * room = "auctav<saleId>"  ex: "auctav42"
   *
   * Protections :
   *   - throttle : 1 joinroom max toutes les 2 secondes
   *   - format   : string non vide, pattern auctav + chiffres
   *   - max      : 1 seule salle active par socket
   */
  socket.on("joinroom", (room) => {
    // ── Contrôle 1 : throttle joinroom ───────────────────────────────────────
    const now = Date.now();
    const last = joinroomThrottle.get(socket.id) || 0;

    if (now - last < JOINROOM_THROTTLE_MS) {
      log(
        `  [joinroom] THROTTLE – socket=${socket.id} (${now - last}ms depuis dernier joinroom)`,
      );
      return;
    }
    joinroomThrottle.set(socket.id, now);

    // ── Contrôle 2 : format room ──────────────────────────────────────────────
    if (typeof room !== "string" || !room.trim()) {
      log(`  [joinroom] REFUSÉ room invalide (type) – socket=${socket.id}`);
      return;
    }

    const roomTrimmed = room.trim();

    // Pattern attendu : "auctav" suivi d'un ou plusieurs chiffres
    if (!/^auctav\d+$/.test(roomTrimmed)) {
      log(
        `  [joinroom] REFUSÉ room invalide (pattern) "${roomTrimmed}" – socket=${socket.id}`,
      );
      return;
    }

    // ── Contrôle 3 : max 1 salle active par socket ───────────────────────────
    const meta = socketMeta.get(socket.id);
    const clientIp = getClientIp(socket);

    if (meta?.room && meta.room === roomTrimmed) {
      // Déjà dans cette salle → silencieux, pas de double jointure
      log(
        `  [joinroom] IGNORÉ – socket=${socket.id} déjà dans "${roomTrimmed}"`,
      );
      return;
    }

    // Quitter l'ancienne salle si nécessaire
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

    // ── Sauvegarde historique ─────────────────────────────────────────────────
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

    // Limite la longueur du pseudo (anti-abus)
    const safePseudo = pseudo.trim().slice(0, 64);

    const meta = socketMeta.get(socket.id);
    if (meta) meta.pseudo = safePseudo;

    log(`  [username] socket=${socket.id} pseudo="${safePseudo}"`);
    updateHistoriquePseudo(socket.id, safePseudo);
  });

  // ───────────────────────────────────────────────────────────────────────────
  /**
   * Diffusion d'un message vers toute la salle.
   *
   * data = { room, type, msg, name }
   *
   * Protections :
   *   - champs obligatoires présents et valides
   *   - le socket appartient bien à la salle déclarée
   *   - type dans liste blanche
   *   - types sensibles réservés admin
   *   - taille du payload plafonnée à 50 Ko
   */
  socket.on("getMsgRoom", (data) => {
    // ── Contrôle 1 : présence et format des champs obligatoires ──────────────
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

    // ── Contrôle 2 : taille du payload ───────────────────────────────────────
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

    // ── Contrôle 3 : le socket doit appartenir à la salle déclarée ───────────
    if (!socket.rooms.has(data.room)) {
      log(
        `  [getMsgRoom] REFUSÉ – ${socket.id} n'appartient pas à la salle "${data.room}"`,
      );
      return;
    }

    // ── Contrôle 4 : type dans liste blanche ──────────────────────────────────
    if (!ALLOWED_TYPES.has(data.type)) {
      log(
        `  [getMsgRoom] REFUSÉ – type non autorisé "${data.type}" depuis ${socket.id}`,
      );
      return;
    }

    // ── Contrôle 5 : types sensibles réservés à l'admin ──────────────────────
    const meta = socketMeta.get(socket.id);
    if (ADMIN_ONLY_TYPES.has(data.type) && !meta?.isAdmin) {
      log(
        `  [getMsgRoom] REFUSÉ – type admin "${data.type}" émis par un non-admin ${socket.id}`,
      );
      return;
    }

    // ── Payload nettoyé ───────────────────────────────────────────────────────
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
