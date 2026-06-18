/**
 * Serveur Socket.IO — Auctav Live Sales
 * Version compatible avec Socket.IO v2.3.0 (client existant)
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
// CORS POUR EXPRESS
// -----------------------------------------------------------------------------

const ALLOWED_ORIGINS = [
  "https://www.auctav.com",
  "https://auctav.com",
  "https://dev.astucom.com",
  "https://dev.astucom.com:9022",
  "http://localhost",
  "http://localhost:9022",
  "http://127.0.0.1",
  "http://127.0.0.1:9022",
];

app.use((req, res, next) => {
  const origin = req.headers.origin;

  if (ALLOWED_ORIGINS.includes(origin)) {
    res.header("Access-Control-Allow-Origin", origin);
    res.header("Access-Control-Allow-Credentials", "true");
    res.header(
      "Access-Control-Allow-Methods",
      "GET, POST, PUT, DELETE, OPTIONS",
    );
    res.header(
      "Access-Control-Allow-Headers",
      "Content-Type, Authorization, X-Requested-With",
    );
  }

  if (req.method === "OPTIONS") {
    return res.sendStatus(200);
  }
  next();
});

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

app.get("/test", (_req, res) => {
  res.json({
    message: "Server is running",
    timestamp: new Date().toISOString(),
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

// -----------------------------------------------------------------------------
// SOCKET.IO - Configuration spécifique pour v2
// -----------------------------------------------------------------------------

const io = new Server(server, {
  // Configuration pour Socket.IO v2
  pingInterval: 25000,
  pingTimeout: 60000,
  maxHttpBufferSize: 1e7,

  // Transports - matching client v2
  transports: ["polling", "websocket"],
  allowUpgrades: true,

  // CORS - Support credentials
  cors: {
    origin: function (origin, callback) {
      if (!origin) {
        return callback(null, true);
      }

      if (ALLOWED_ORIGINS.includes(origin)) {
        return callback(null, true);
      }

      log(`[CORS] Origine bloquee: ${origin}`);
      return callback(new Error("CORS not allowed"));
    },
    methods: ["GET", "POST"],
    credentials: true,
    allowedHeaders: ["Content-Type", "Authorization", "X-Requested-With"],
  },

  // Désactiver le cookie
  cookie: false,

  // Path par défaut
  path: "/socket.io/",

  // Permettre les anciennes versions
  allowEIO3: false,
});

// -----------------------------------------------------------------------------
// ENGINE.IO CONFIGURATION - Important pour v2
// -----------------------------------------------------------------------------

io.engine.on("connection_error", (err) => {
  log(`[ENGINE ERROR] ${err.code} - ${err.message}`);
});

io.engine.on("connection", (socket) => {
  log(`[ENGINE] Connection from: ${socket.id}`);
  log(`[ENGINE] Transport: ${socket.transport.name}`);

  socket.on("upgrade", () => {
    log(`[ENGINE] Upgrade to: ${socket.transport.name}`);
  });
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
const MAX_CONN = 10;

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
  log(`[CONNECTION] ${socket.id} (instance ${instanceId})`);

  socketMeta.set(socket.id, {
    pseudo: "unknown",
    room: null,
    isAdmin: false,
  });

  log(`[TRANSPORT] ${socket.id} -> ${socket.conn.transport.name}`);

  socket.conn.on("upgrade", () => {
    log(`[UPGRADE] ${socket.id} -> ${socket.conn.transport.name}`);
  });

  socket.on("disconnect", (reason) => {
    log(`[DISCONNECT] ${socket.id} (${reason})`);
  });

  socket.on("connect_error", (err) => {
    log(`[ERROR] ${socket.id}: ${err.message}`);
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

server.listen(PORT, "0.0.0.0", () => {
  const instanceId = process.env.NODE_APP_INSTANCE || "standalone";
  log(`========================================`);
  log(`Socket.IO server demarre sur port ${PORT}`);
  log(`Instance: ${instanceId}`);
  log(`Mode: ${process.env.NODE_ENV || "development"}`);
  log(`Cluster: ${useCluster ? "Active (Redis Adapter)" : "Standalone"}`);
  log(`URL: https://dev.astucom.com:${PORT}`);
  log(`Health: http://localhost:${PORT}/`);
  log(`========================================`);

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
  log(err.stack);
});

process.on("unhandledRejection", (reason) => {
  log(`[FATAL] unhandledRejection: ${reason}`);
});
