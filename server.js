/**
 * Serveur Socket.IO — Auctav Live Sales
 * VERSION STABLE MOBILE + APACHE + SOCKET.IO v2/v3/v4
 * MODE CLUSTER AVEC REDIS ADAPTER
 */

const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const { createAdapter } = require("@socket.io/redis-adapter");

const { PORT } = require("./config");
const socketMeta = require("./store");
const { log } = require("./utils/logger");
const redis = require("./redis");

const { getRoomStats } = require("./services/roomService");

const { registerAdminHandler } = require("./handlers/adminHandler");
const { registerBidderHandler } = require("./handlers/bidderHandler");
const { registerRoomHandler } = require("./handlers/roomHandler");
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

// -----------------------------------------------------------------------------
// EXPRESS
// -----------------------------------------------------------------------------

const app = express();
const server = http.createServer(app);

app.use(express.json());

// -----------------------------------------------------------------------------
// CORS
// -----------------------------------------------------------------------------

const ALLOWED_ORIGINS = [
  "https://www.auctav.com",
  "https://auctav.com",
  "https://dev.astucom.com",
  "http://localhost",
  "http://127.0.0.1",
];

// -----------------------------------------------------------------------------
// HEALTH CHECK
// -----------------------------------------------------------------------------

app.get("/", (_req, res) => {
  res.json({
    status: "ok",
    uptime: process.uptime(),
    memory: process.memoryUsage(),
    sockets: socketMeta.size,
    cluster: process.env.NODE_APP_INSTANCE || "standalone",
  });
});

// Followers debug
app.get("/follow/:room", (req, res) => {
  res.json({
    room: req.params.room,
    followers: getFollowersInRoom(req.params.room),
  });
});

// Screens debug
app.get("/screen/:room", (req, res) => {
  res.json({
    room: req.params.room,
    screens: getScreensInRoom(req.params.room),
  });
});

// -----------------------------------------------------------------------------
// SOCKET.IO
// -----------------------------------------------------------------------------

const io = new Server(server, {
  // MOBILE / RESEAUX LENTS
  pingInterval: 10000,
  pingTimeout: 20000,

  // GROS PAYLOADS
  maxHttpBufferSize: 1e7,

  // Compression
  perMessageDeflate: {
    threshold: 8192,
  },

  // Compatibilite anciens clients
  allowEIO3: true,

  // polling + websocket
  transports: ["polling", "websocket"],

  cors: {
    origin: function (origin, callback) {
      if (!origin) {
        return callback(null, true);
      }

      if (ALLOWED_ORIGINS.includes(origin)) {
        return callback(null, true);
      }

      log(`CORS bloque : ${origin}`);
      return callback(new Error("CORS blocked"));
    },

    methods: ["GET", "POST"],
    credentials: true,
  },
});

// -----------------------------------------------------------------------------
// INTEGRATION REDIS ADAPTER POUR LE MODE CLUSTER
// -----------------------------------------------------------------------------

const useCluster =
  process.env.NODE_ENV === "production" ||
  process.env.USE_REDIS_ADAPTER === "true" ||
  process.env.NODE_APP_INSTANCE !== undefined;

if (useCluster) {
  try {
    const pubClient = redis;
    const subClient = redis.duplicate();

    io.adapter(createAdapter(pubClient, subClient));

    const instanceId = process.env.NODE_APP_INSTANCE || "standalone";
    log(`[REDIS ADAPTER] Active pour l'instance ${instanceId}`);

    subClient.on("error", (err) => {
      log(`[REDIS ADAPTER] Erreur subClient: ${err.message}`);
    });

    io.of("/").adapter.on("create-room", (room) => {
      log(`[REDIS ADAPTER] Room creee: ${room}`);
    });
  } catch (err) {
    log(`[REDIS ADAPTER] Echec: ${err.message}`);
    log("[REDIS ADAPTER] Le serveur fonctionne en mode standalone");
  }
} else {
  log("[REDIS ADAPTER] Desactive (mode developpement/standalone)");
}

// -----------------------------------------------------------------------------
// RATE LIMITING PAR IP
// -----------------------------------------------------------------------------

const connPerIP = new Map();
const MAX_CONN = 5;

io.use((socket, next) => {
  const ip =
    socket.handshake.headers["x-forwarded-for"] || socket.handshake.address;

  const count = connPerIP.get(ip) || 0;

  if (count >= MAX_CONN) {
    log(`[RATE LIMIT] IP bloquee : ${ip} (${count} connexions)`);
    return next(new Error("Too many connections"));
  }

  connPerIP.set(ip, count + 1);

  socket.on("disconnect", () => {
    const n = (connPerIP.get(ip) || 1) - 1;
    n <= 0 ? connPerIP.delete(ip) : connPerIP.set(ip, n);
  });

  next();
});

// -----------------------------------------------------------------------------
// SOCKET CONNECTION
// -----------------------------------------------------------------------------

io.on("connection", (socket) => {
  const instanceId = process.env.NODE_APP_INSTANCE || "?";
  log(`+ Connexion : ${socket.id} (instance ${instanceId})`);

  socketMeta.set(socket.id, {
    pseudo: "unknown",
    room: null,
    isAdmin: false,
  });

  log(`Transport : ${socket.conn.transport.name}`);

  socket.conn.on("upgrade", () => {
    log(`[UPGRADE] ${socket.id} -> ${socket.conn.transport.name}`);
  });

  socket.on("disconnect", (reason) => {
    log(`- Deconnexion: ${socket.id} (${reason})`);
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

// -----------------------------------------------------------------------------
// START SERVER
// -----------------------------------------------------------------------------

server.listen(PORT, () => {
  const instanceId = process.env.NODE_APP_INSTANCE || "standalone";
  log(`Socket.IO server demarre sur port ${PORT}`);
  log(`Instance: ${instanceId}`);
  log(`Mode: ${process.env.NODE_ENV || "development"}`);
  log(`Cluster: ${useCluster ? "Active (Redis Adapter)" : "Standalone"}`);
  log(`Health: http://localhost:${PORT}/`);

  if (process.send) {
    process.send("ready");
    log("PM2 ready signal envoye");
  }
});

// -----------------------------------------------------------------------------
// GRACEFUL SHUTDOWN
// -----------------------------------------------------------------------------

process.on("SIGTERM", () => {
  log("SIGTERM recu - arret propre");
  server.close(() => {
    redis.quit();
    process.exit(0);
  });
});

process.on("SIGINT", () => {
  log("SIGINT recu - arret propre");
  server.close(() => {
    redis.quit();
    process.exit(0);
  });
});

// -----------------------------------------------------------------------------
// PROTECTION GLOBALE CONTRE LES CRASHS
// -----------------------------------------------------------------------------

process.on("uncaughtException", (err) => {
  log(`[FATAL] uncaughtException: ${err.message}`);
});

process.on("unhandledRejection", (reason) => {
  log(`[FATAL] unhandledRejection: ${reason}`);
});
