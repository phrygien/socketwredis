/**
 * Serveur Socket.IO — Auctav Live Sales
 * VERSION CLUSTER PM2 (-i max) + Redis adapter + Redis rate-limit
 */

const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const { createAdapter } = require("@socket.io/redis-adapter");

const { PORT } = require("./config");
const socketMeta = require("./store");
const { log } = require("./utils/logger");
const redis = require("./redis");

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

app.get("/", async (_req, res) => {
  let redisStatus = "ok";
  try {
    await redis.ping();
  } catch {
    redisStatus = "unavailable";
  }

  res.json({
    status: "ok",
    uptime: process.uptime(),
    memory: process.memoryUsage(),
    sockets: socketMeta.size,
    worker: process.pid,
    redis: redisStatus,
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
  pingInterval: 10000,
  pingTimeout: 20000,
  maxHttpBufferSize: 1e7,
  perMessageDeflate: { threshold: 8192 },
  allowEIO3: true,

  // En cluster, websocket seul évite les problèmes de sticky session
  // si jamais un client ne supporte pas le upgrade.
  // On garde polling EN PREMIER pour la compatibilité mobile,
  // mais le sticky session Apache garantit que polling tombe
  // toujours sur le même worker.
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
// REDIS ADAPTER
//
//  Permet à io.to(room).emit() de traverser tous les workers.
//  pubClient  → publie les événements vers les autres workers
//  subClient  → reçoit les événements des autres workers
//
//  On duplique le client Redis pour respecter la contrainte
//  ioredis : un client en mode subscribe ne peut pas émettre.
// ─────────────────────────────────────────────────────────────

const pubClient = redis; // client principal déjà connecté
const subClient = redis.duplicate(); // client dédié à la souscription

subClient.on("error", (err) => {
  log(`[REDIS-SUB] Erreur : ${err.message}`);
});

io.adapter(createAdapter(pubClient, subClient));
log(`[REDIS-ADAPTER] Initialisé (worker PID ${process.pid})`);

// ─────────────────────────────────────────────────────────────
// MIDDLEWARE 1 — RATE-LIMIT PAR IP (Redis, partagé entre workers)
//
//  Clé : "rl:ip:<ip>"  |  TTL 1h de sécurité
//  Fallback : autorise si Redis down
// ─────────────────────────────────────────────────────────────

const MAX_CONN_PER_IP = 5;

io.use(async (socket, next) => {
  const raw =
    socket.handshake.headers["x-forwarded-for"] || socket.handshake.address;
  const ip = typeof raw === "string" ? raw.split(",")[0].trim() : String(raw);
  const key = `rl:ip:${ip}`;

  try {
    const count = await redis.incr(key);
    if (count === 1) await redis.expire(key, 3600);

    if (count > MAX_CONN_PER_IP) {
      await redis.decr(key);
      log(
        `[RATE LIMIT IP] bloqué : ${ip} (${count} connexions) worker=${process.pid}`,
      );
      return next(new Error("Too many connections"));
    }

    socket.on("disconnect", async () => {
      try {
        const n = await redis.decr(key);
        if (n <= 0) await redis.del(key);
      } catch (err) {
        log(`[REDIS] Erreur decr connPerIP : ${err.message}`);
      }
    });

    socket.data.ip = ip;
  } catch (err) {
    log(
      `[REDIS] rate-limit IP indisponible, connexion autorisée : ${err.message}`,
    );
  }

  next();
});

// ─────────────────────────────────────────────────────────────
// MIDDLEWARE 2 — RATE-LIMIT PAR SOCKET (anti-flood, par worker)
//
//  Clé : "rl:sock:<socketId>"  |  TTL 1s (fenêtre auto-expirante)
//  Fallback : compteur mémoire locale si Redis down
// ─────────────────────────────────────────────────────────────

const MAX_EVENTS_PER_S = 10;

io.use((socket, next) => {
  let localCount = 0;
  let localResetAt = Date.now() + 1000;

  socket.onAny(async (eventName) => {
    const key = `rl:sock:${socket.id}`;
    try {
      const count = await redis.incr(key);
      if (count === 1) await redis.expire(key, 1);

      if (count > MAX_EVENTS_PER_S) {
        log(
          `[FLOOD] socket=${socket.id} event="${eventName}" (${count}/s) worker=${process.pid} → déconnecté`,
        );
        socket.disconnect(true);
      }
    } catch {
      const now = Date.now();
      if (now > localResetAt) {
        localCount = 0;
        localResetAt = now + 1000;
      }
      localCount++;
      if (localCount > MAX_EVENTS_PER_S) {
        log(
          `[FLOOD-LOCAL] socket=${socket.id} event="${eventName}" (${localCount}/s) → déconnecté`,
        );
        socket.disconnect(true);
      }
    }
  });

  socket.on("disconnect", async () => {
    try {
      await redis.del(`rl:sock:${socket.id}`);
    } catch {
      /* ignoré */
    }
  });

  next();
});

// ─────────────────────────────────────────────────────────────
// SOCKET CONNECTION
// ─────────────────────────────────────────────────────────────

const IDLE_TIMEOUT_MS = 5 * 60 * 1000;

io.on("connection", (socket) => {
  log(`+ Connexion : ${socket.id} worker=${process.pid}`);

  socketMeta.set(socket.id, {
    pseudo: "unknown",
    room: null,
    isAdmin: false,
  });

  log(`Transport : ${socket.conn.transport.name}`);
  socket.conn.on("upgrade", () => {
    log(`[UPGRADE] ${socket.id} → ${socket.conn.transport.name}`);
  });

  // ── Idle timeout ───────────────────────────────────────────
  let idleTimer = null;

  function resetIdle() {
    clearTimeout(idleTimer);
    idleTimer = setTimeout(() => {
      log(
        `[IDLE] socket=${socket.id} inactif ${IDLE_TIMEOUT_MS / 1000}s → déconnecté`,
      );
      socket.disconnect(true);
    }, IDLE_TIMEOUT_MS);
  }

  resetIdle();
  socket.onAny(() => resetIdle());

  socket.on("disconnect", (reason) => {
    clearTimeout(idleTimer);
    joinroomThrottle.delete(socket.id);
    log(`- Déconnexion : ${socket.id} (${reason}) worker=${process.pid}`);
  });

  socket.on("connect_error", (err) => {
    log(`Connect error ${socket.id}: ${err.message}`);
  });

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
  log(`Socket.IO server démarré sur port ${PORT} (worker PID ${process.pid})`);
  // Signal PM2 que le worker est prêt (requis si wait_ready: true)
  if (process.send) process.send("ready");
});

// ─────────────────────────────────────────────────────────────
// GRACEFUL SHUTDOWN
// ─────────────────────────────────────────────────────────────

process.on("SIGTERM", () => {
  log("SIGTERM reçu — arrêt propre");
  subClient
    .quit()
    .finally(() =>
      redis.quit().finally(() => server.close(() => process.exit(0))),
    );
});

process.on("SIGINT", () => {
  log("SIGINT reçu — arrêt propre");
  subClient
    .quit()
    .finally(() =>
      redis.quit().finally(() => server.close(() => process.exit(0))),
    );
});

process.on("uncaughtException", (err) =>
  log(`[FATAL] uncaughtException: ${err.message}`),
);
process.on("unhandledRejection", (reason) =>
  log(`[FATAL] unhandledRejection: ${reason}`),
);
