/**
 * Serveur Socket.IO — Auctav Live Sales
 * VERSION STABLE MOBILE + APACHE + SOCKET.IO v2/v3/v4
 */

const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const { PORT } = require("./config");
const socketMeta = require("./store");
const { log } = require("./utils/logger");

const { getRoomStats } = require("./services/roomService");
const { registerAdminHandler } = require("./handlers/adminHandler");
const { registerBidderHandler } = require("./handlers/bidderHandler");
const {
  registerRoomHandler,
  joinroomThrottle,
} = require("./handlers/roomHandler");
const { registerMessageHandler } = require("./handlers/messageHandler");
const { registerDisconnectHandler } = require("./handlers/disconnectHandler");

const {
  registerFollowHandler,
  getFollowersInRoom,
} = require("./handlers/followHandler");

const {
  registerScreenHandler,
  getScreensInRoom,
} = require("./handlers/screenHandler");

// ─────────────────────────────────────────────────────────────
// EXPRESS
// ─────────────────────────────────────────────────────────────

const app = express();
const server = http.createServer(app);

app.use(express.json());

// ─────────────────────────────────────────────────────────────
// CORS
// ─────────────────────────────────────────────────────────────

const ALLOWED_ORIGINS = [
  "https://www.auctav.com",
  "https://auctav.com",
  "https://dev.astucom.com",
  "http://localhost",
  "http://127.0.0.1",
];

// ─────────────────────────────────────────────────────────────
// HEALTH CHECK
// ─────────────────────────────────────────────────────────────

app.get("/", (_req, res) => {
  res.json({
    status: "ok",
    uptime: process.uptime(),
    memory: process.memoryUsage(),
    sockets: socketMeta.size,
  });
});

app.get("/follow/:room", (req, res) => {
  res.json({
    room: req.params.room,
    followers: getFollowersInRoom(req.params.room),
  });
});

app.get("/screen/:room", (req, res) => {
  res.json({
    room: req.params.room,
    screens: getScreensInRoom(req.params.room),
  });
});

// ─────────────────────────────────────────────────────────────
// SOCKET.IO
// ─────────────────────────────────────────────────────────────

const io = new Server(server, {
  // MOBILE / RÉSEAUX LENTS
  pingInterval: 10000,
  pingTimeout: 20000,

  // GROS PAYLOADS — plafond global côté transport (avant parsing applicatif)
  maxHttpBufferSize: 1e7, // 10 Mo

  // Compression — seuil relevé pour éviter de compresser les petits messages
  perMessageDeflate: { threshold: 8192 },

  // Compatibilité anciens clients
  allowEIO3: true,

  // polling + websocket — polling aide sur réseaux mobiles lents
  transports: ["polling", "websocket"],

  cors: {
    origin(origin, callback) {
      if (!origin) return callback(null, true);
      if (ALLOWED_ORIGINS.includes(origin)) return callback(null, true);
      log(`CORS bloqué : ${origin}`);
      return callback(new Error("CORS blocked"));
    },
    methods: ["GET", "POST"],
    credentials: true,
  },
});

// ─────────────────────────────────────────────────────────────
// MIDDLEWARE 1 — RATE-LIMIT PAR IP
// ─────────────────────────────────────────────────────────────

const connPerIP = new Map();
const MAX_CONN = 5; // max 5 sockets simultanés par IP

io.use((socket, next) => {
  const ip =
    socket.handshake.headers["x-forwarded-for"] || socket.handshake.address;
  const count = connPerIP.get(ip) || 0;

  if (count >= MAX_CONN) {
    log(`[RATE LIMIT IP] bloqué : ${ip} (${count} connexions actives)`);
    return next(new Error("Too many connections"));
  }

  connPerIP.set(ip, count + 1);

  socket.on("disconnect", () => {
    const n = (connPerIP.get(ip) || 1) - 1;
    n <= 0 ? connPerIP.delete(ip) : connPerIP.set(ip, n);
  });

  next();
});

// ─────────────────────────────────────────────────────────────
// MIDDLEWARE 2 — RATE-LIMIT PAR SOCKET (anti-flood d'événements)
// ─────────────────────────────────────────────────────────────

const EVENT_WINDOW_MS = 1000; // fenêtre glissante d'1 seconde
const MAX_EVENTS_PER_S = 10; // max 10 événements/s avant déconnexion

io.use((socket, next) => {
  let count = 0;
  let resetAt = Date.now() + EVENT_WINDOW_MS;

  socket.onAny((eventName) => {
    const now = Date.now();
    if (now > resetAt) {
      count = 0;
      resetAt = now + EVENT_WINDOW_MS;
    }
    count++;
    if (count > MAX_EVENTS_PER_S) {
      log(
        `[FLOOD] socket=${socket.id} event="${eventName}" (${count}/s) → déconnecté`,
      );
      socket.disconnect(true);
    }
  });

  next();
});

// ─────────────────────────────────────────────────────────────
// SOCKET CONNECTION
// ─────────────────────────────────────────────────────────────

const IDLE_TIMEOUT_MS = 5 * 60 * 1000; // 5 minutes sans activité → zombie

io.on("connection", (socket) => {
  log(`+ Connexion : ${socket.id}`);

  socketMeta.set(socket.id, {
    pseudo: "unknown",
    room: null,
    isAdmin: false,
  });

  // ── Debug transport ────────────────────────────────────────
  log(`Transport : ${socket.conn.transport.name}`);
  socket.conn.on("upgrade", () => {
    log(`[UPGRADE] ${socket.id} → ${socket.conn.transport.name}`);
  });

  // ─────────────────────────────────────────────────────────
  // IDLE TIMEOUT — déconnecte les zombies inactifs
  // ─────────────────────────────────────────────────────────

  let idleTimer = null;

  function resetIdle() {
    clearTimeout(idleTimer);
    idleTimer = setTimeout(() => {
      log(
        `[IDLE] socket=${socket.id} inactif depuis ${IDLE_TIMEOUT_MS / 1000}s → déconnecté`,
      );
      socket.disconnect(true);
    }, IDLE_TIMEOUT_MS);
  }

  resetIdle(); // démarre dès la connexion

  // Réinitialise le timer à chaque événement entrant (ping inclus)
  socket.onAny(() => resetIdle());

  // ── Debug disconnect ───────────────────────────────────────
  socket.on("disconnect", (reason) => {
    clearTimeout(idleTimer);
    joinroomThrottle.delete(socket.id); // nettoyage throttle joinroom
    log(`- Déconnexion : ${socket.id} (${reason})`);
  });

  socket.on("connect_error", (err) => {
    log(`Connect error ${socket.id}: ${err.message}`);
  });

  // ── Handlers métier ────────────────────────────────────────
  registerAdminHandler(io, socket);
  registerBidderHandler(io, socket);
  registerRoomHandler(io, socket);
  registerMessageHandler(io, socket);
  registerFollowHandler(io, socket);
  registerScreenHandler(io, socket);
  registerDisconnectHandler(io, socket);
});

// ─────────────────────────────────────────────────────────────
// START SERVER
// ─────────────────────────────────────────────────────────────

server.listen(PORT, () => {
  log(`Socket.IO server démarré sur port ${PORT}`);
  log(`Mode : PRODUCTION`);
  log(`Health : http://localhost:${PORT}/`);
});

// ─────────────────────────────────────────────────────────────
// GRACEFUL SHUTDOWN
// ─────────────────────────────────────────────────────────────

process.on("SIGTERM", () => {
  log("SIGTERM reçu — arrêt propre");
  server.close(() => process.exit(0));
});

process.on("SIGINT", () => {
  log("SIGINT reçu — arrêt propre");
  server.close(() => process.exit(0));
});

// ─────────────────────────────────────────────────────────────
// PROTECTION GLOBALE CONTRE LES CRASHS
// ─────────────────────────────────────────────────────────────

process.on("uncaughtException", (err) =>
  log(`[FATAL] uncaughtException: ${err.message}`),
);
process.on("unhandledRejection", (reason) =>
  log(`[FATAL] unhandledRejection: ${reason}`),
);
