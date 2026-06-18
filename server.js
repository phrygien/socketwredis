/**
 * Serveur Socket.IO — Auctav Live Sales
 * Version adaptée pour le client existant
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
// CORS POUR EXPRESS - Support credentials
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
// SOCKET.IO - Configuration pour client existant
// -----------------------------------------------------------------------------

const io = new Server(server, {
  // Configuration pour Socket.IO v2/v3 (compatible avec votre client)
  pingInterval: 25000,
  pingTimeout: 60000,
  maxHttpBufferSize: 1e7,
  perMessageDeflate: {
    threshold: 8192,
  },

  // Transports - matching client
  transports: ["polling", "websocket"],
  allowUpgrades: true,
  allowEIO3: true,

  // CORS - Support credentials comme le client l'utilise
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

  // Désactiver le cookie pour éviter les problèmes
  cookie: false,

  // Path par défaut
  path: "/socket.io/",
});

// -----------------------------------------------------------------------------
// LOGGING DES REQUETES (pour debug)
// -----------------------------------------------------------------------------

io.use((socket, next) => {
  const req = socket.request;
  log(`[SOCKET REQUEST] URL: ${req.url}`);
  next();
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

  // Enregistrer les handlers
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
