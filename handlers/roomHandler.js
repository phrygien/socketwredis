// -----------------------------------------------------------------------------
// Room Handler
// -----------------------------------------------------------------------------
//
//  Securite appliquee :
//  1. Validation des entrees (room, type)
//  2. Le socket doit appartenir a data.room (anti-room-forgery)
//  3. Liste blanche des types autorises (ALLOWED_TYPES)
//  4. Types sensibles reserves a l'admin (ADMIN_ONLY_TYPES)
//  5. listLot inclus dans ADMIN_ONLY_TYPES (un bidder ne peut pas
//     broadcaster une fausse liste de lots a toute la salle)
// -----------------------------------------------------------------------------

const socketMeta = require("../store");
const { log } = require("../utils/logger");
const {
  getAdminOfRoom,
  broadcastUserList,
} = require("../services/roomService");
const fs = require("fs");
const path = require("path");

// Chemin vers l'historique
const HISTORIQUE_PATH = path.resolve(__dirname, "../historique.json");

// Listes de controle
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

// Utilitaires historique

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
    log(`  [historique] ERREUR ecriture : ${err.message}`);
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
      `  [historique] pseudo mis a jour socketId=${socketId} pseudo="${pseudo}"`,
    );
  } catch (err) {
    log(`  [historique] ERREUR mise a jour pseudo : ${err.message}`);
  }
}

function getClientIp(socket) {
  const forwarded = socket.handshake.headers["x-forwarded-for"];
  if (forwarded) {
    return forwarded.split(",")[0].trim();
  }
  return socket.handshake.address || "inconnue";
}

// -----------------------------------------------------------------------------

function registerRoomHandler(io, socket) {
  socket.on("joinroom", (room) => {
    if (typeof room !== "string" || !room.trim()) {
      log(`  [joinroom] REFUSE room invalide socket=${socket.id}`);
      return;
    }

    const meta = socketMeta.get(socket.id);
    const clientIp = getClientIp(socket);

    if (meta?.room) {
      const oldRoom = meta.room;
      socket.leave(oldRoom);
      if (meta.isAdmin) broadcastUserList(io, oldRoom);
    }

    socket.join(room);
    if (meta) meta.room = room;

    log(
      `  [joinroom] socket=${socket.id} room="${room}" ip=${clientIp} admin=${meta?.isAdmin}`,
    );

    appendHistorique({
      event: "joinroom",
      socketId: socket.id,
      ip: clientIp,
      room: room,
      pseudo: meta?.pseudo || "inconnu",
      isAdmin: meta?.isAdmin ?? false,
      timestamp: new Date().toISOString(),
    });

    if (meta?.isAdmin) {
      broadcastUserList(io, room);
    } else {
      const adminId = getAdminOfRoom(room);
      socket.emit("userList", { admin: adminId });
      log(`  [userListsocket.id] admin=${adminId || "none"}`);
    }
  });

  // ---------------------------------------------------------------------------

  socket.on("username", (pseudo) => {
    if (typeof pseudo !== "string" || !pseudo.trim()) return;

    const meta = socketMeta.get(socket.id);
    if (meta) meta.pseudo = pseudo.trim();

    log(`  [username] socket=${socket.id} pseudo="${pseudo.trim()}"`);

    updateHistoriquePseudo(socket.id, pseudo.trim());
  });

  // ---------------------------------------------------------------------------

  socket.on("getMsgRoom", (data) => {
    if (!data || typeof data.room !== "string" || !data.room.trim()) {
      log(
        `  [getMsgRoom] REFUSE champ room absent/invalide socket=${socket.id}`,
      );
      return;
    }
    if (typeof data.type !== "string" || !data.type.trim()) {
      log(
        `  [getMsgRoom] REFUSE champ type absent/invalide socket=${socket.id}`,
      );
      return;
    }

    if (!socket.rooms.has(data.room)) {
      log(
        `  [getMsgRoom] REFUSE ${socket.id} n'appartient pas a la salle "${data.room}"`,
      );
      return;
    }

    if (!ALLOWED_TYPES.has(data.type)) {
      log(
        `  [getMsgRoom] REFUSE type non autorise "${data.type}" depuis ${socket.id}`,
      );
      return;
    }

    const meta = socketMeta.get(socket.id);
    if (ADMIN_ONLY_TYPES.has(data.type) && !meta?.isAdmin) {
      log(
        `  [getMsgRoom] REFUSE type admin "${data.type}" emis par un non-admin ${socket.id}`,
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
      `  [roomdata.room] type="${data.type}" from=${socket.id} (admin=${meta?.isAdmin})`,
    );

    io.to(data.room).emit("sendMsg", payload);
  });
}

module.exports = { registerRoomHandler };
