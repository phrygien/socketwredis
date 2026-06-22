/**
 * Serveur Socket.IO — Auctav Live Sales
 * VERSION CLUSTER : PM2 fork multi-instances + Redis adapter
 * Compatible Apache + Socket.IO v2/v3/v4
 *
 * ─────────────────────────────────────────────────────────────────────────
 * ARCHITECTURE CLUSTER (résumé) :
 *
 *   - PM2 reste en exec_mode "fork" (PAS "cluster") — chaque instance a son
 *     propre port (4000, 4001, ...), injecté via NODE_APP_INSTANCE.
 *     Choix volontaire : exec_mode "cluster" + module cluster natif de Node
 *     nécessite @socket.io/sticky + un primary process dédié, incompatible
 *     avec un PM2 standard partagé avec d'autres apps sur ce VPS. Le fork
 *     mode multi-ports évite ce risque.
 *
 *   - Apache fait le sticky load-balancing (cookie ROUTEID) entre les ports
 *     — nécessaire pour que le long-polling fonctionne (un client doit
 *     retomber sur le même worker pendant tout son handshake HTTP).
 *
 *   - Le Redis adapter (@socket.io/redis-adapter) permet à io.emit() /
 *     io.to(room).emit() d'atteindre TOUS les clients, même connectés à un
 *     autre worker — sans ça, chaque worker serait une île isolée.
 *
 *   - Le store applicatif (room, isAdmin, pseudo par socket) est lui aussi
 *     mis en miroir dans Redis (voir store.js) car c'est un état métier
 *     distinct du transport Socket.IO — le Redis adapter ne le gère pas
 *     automatiquement.
 * ─────────────────────────────────────────────────────────────────────────
 */

const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const { createClient } = require("redis");
const { createAdapter } = require("@socket.io/redis-adapter");

const { log } = require("./utils/logger");
const store = require("./store");

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

// ─────────────────────────────────────────────────────────────
// PORT — un par instance PM2 (fork mode, pas cluster mode)
// ─────────────────────────────────────────────────────────────
//
// PM2 injecte NODE_APP_INSTANCE (0, 1, 2, ...) automatiquement quand
// `instances` > 1 en exec_mode "fork". BASE_PORT reste configurable via
// l'env si besoin (ex: staging sur une autre plage de ports).

const BASE_PORT = Number(process.env.BASE_PORT || 4000);
const INSTANCE_OFFSET = process.env.NODE_APP_INSTANCE
  ? Number(process.env.NODE_APP_INSTANCE)
  : 0;
const PORT = BASE_PORT + INSTANCE_OFFSET;

// ─────────────────────────────────────────────────────────────
// REDIS — URL partagée pour l'adapter ET le store applicatif
// ─────────────────────────────────────────────────────────────

const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379";

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
// RATE LIMITING PAR IP
// ─────────────────────────────────────────────────────────────
//
// NOTE CLUSTER : connPerIP reste local à ce worker. Avec N instances, un
// client peut en théorie ouvrir MAX_CONN connexions PAR worker s'il retombe
// sur des process différents avant l'établissement du cookie sticky. Le
// sticky Apache (ROUTEID) limite fortement ce risque en pratique : une fois
// le cookie posé, le même client IP retombe toujours sur le même worker.
// Si une limite STRICTE tous-workers-confondus est nécessaire, ce compteur
// devrait être déplacé dans Redis (INCR + EXPIRE) — non fait ici pour
// garder le hot path de connexion rapide ; à revoir si abus constatés.

const connPerIP = new Map();
const MAX_CONN = 5;

setInterval(() => {
  let cleaned = 0;
  for (const [ip, count] of connPerIP.entries()) {
    if (count <= 0) {
      connPerIP.delete(ip);
      cleaned++;
    }
  }
  if (cleaned > 0) {
    log(`[RATE LIMIT] Nettoyage : ${cleaned} entrées supprimées`);
  }
}, 60_000);

// ─────────────────────────────────────────────────────────────
// HEALTH CHECK
// ─────────────────────────────────────────────────────────────
//
// NOTE CLUSTER : /rooms-stats (cross-worker, via Redis) est ASYNC et séparé
// du health check de base, qui reste synchrone et local pour répondre
// instantanément même si Redis a un souci (le process Node tourne quand
// même, l'info est juste incomplète plutôt que silencieuse).

app.get("/", (_req, res) => {
  res.json({
    status: "ok",
    pid: process.pid,
    port: PORT,
    uptime: process.uptime(),
    memory: process.memoryUsage(),
    sockets: store.size(), // local à ce worker uniquement
    connPerIP: connPerIP.size, // local à ce worker uniquement
  });
});

// Stats cross-worker (toutes instances confondues, via Redis)
app.get("/rooms-stats", async (_req, res) => {
  try {
    const stats = await getRoomStats();
    res.json(stats);
  } catch (err) {
    log(`[/rooms-stats] erreur: ${err.message}`);
    res.status(500).json({ error: "stats indisponibles" });
  }
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

// ─────────────────────────────────────────────────────────────
// SOCKET.IO
// ─────────────────────────────────────────────────────────────

const io = new Server(server, {
  // MOBILE / RÉSEAUX LENTS
  pingInterval: 10000,
  pingTimeout: 20000,

  // Timeout handshake — coupe les connexions qui traînent
  connectTimeout: 10000,

  // GROS PAYLOADS
  maxHttpBufferSize: 1e7, // 10Mo

  // Compression — seuil relevé pour éviter de compresser les petits messages
  perMessageDeflate: {
    threshold: 8192,
  },

  // Compatibilité anciens clients
  allowEIO3: true,

  // polling + websocket — conservé pour le fallback réseaux mobiles/lents.
  // Implique le sticky load-balancing côté Apache (cookie ROUTEID).
  transports: ["polling", "websocket"],

  cors: {
    origin: function (origin, callback) {
      if (!origin) {
        return callback(null, true);
      }

      if (ALLOWED_ORIGINS.includes(origin)) {
        return callback(null, true);
      }

      log(`CORS bloqué : ${origin}`);

      return callback(new Error("CORS blocked"));
    },

    methods: ["GET", "POST"],
    credentials: true,
  },
});

io.use((socket, next) => {
  const ip =
    socket.handshake.headers["x-forwarded-for"] || socket.handshake.address;

  const count = connPerIP.get(ip) || 0;

  if (count >= MAX_CONN) {
    log(`[RATE LIMIT] IP bloquée : ${ip} (${count} connexions)`);
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
// SOCKET CONNECTION
// ─────────────────────────────────────────────────────────────

io.on("connection", (socket) => {
  log(`+ Connexion : ${socket.id} (worker pid=${process.pid}, port=${PORT})`);

  store.set(socket.id, {
    pseudo: "unknown",
    room: null,
    isAdmin: false,
  });

  // ───────────────────────────────────────────────────
  // DEBUG TRANSPORT
  // ───────────────────────────────────────────────────

  log(`Transport : ${socket.conn.transport.name}`);

  socket.conn.on("upgrade", () => {
    log(`[UPGRADE] ${socket.id} -> ${socket.conn.transport.name}`);
  });

  // ───────────────────────────────────────────────────
  // DEBUG DISCONNECT
  // ───────────────────────────────────────────────────

  socket.on("disconnect", (reason) => {
    log(`- Déconnexion: ${socket.id} (${reason})`);
  });

  socket.on("connect_error", (err) => {
    log(`Connect error ${socket.id}: ${err.message}`);
  });

  // ───────────────────────────────────────────────────
  // HANDLERS
  // ───────────────────────────────────────────────────

  registerAdminHandler(io, socket);
  registerBidderHandler(io, socket);
  registerRoomHandler(io, socket);
  registerMessageHandler(io, socket);
  registerFollowHandler(io, socket);
  registerScreenHandler(io, socket);
  registerDisconnectHandler(io, socket);
});

// ─────────────────────────────────────────────────────────────
// REDIS ADAPTER — broadcast cross-worker pour Socket.IO
// + connexion du store applicatif partagé (room/isAdmin/pseudo)
// ─────────────────────────────────────────────────────────────
//
// Le serveur HTTP n'écoute QUE lorsque Redis est connecté — tourner sans
// l'adapter en prod cluster signifierait des rooms qui ne se voient plus
// entre workers (silencieux, donc pire qu'un crash franc). On préfère
// process.exit(1) et laisser PM2 redémarrer proprement.

const pubClient = createClient({ url: REDIS_URL });
const subClient = pubClient.duplicate();

pubClient.on("error", (err) => log(`[REDIS pubClient] ${err.message}`));
subClient.on("error", (err) => log(`[REDIS subClient] ${err.message}`));

Promise.all([pubClient.connect(), subClient.connect(), store.connect()])
  .then(() => {
    io.adapter(createAdapter(pubClient, subClient));
    log("[REDIS] Adapter Socket.IO connecté — broadcast cross-worker actif");

    server.listen(PORT, () => {
      log(`Socket.IO server démarré sur port ${PORT} (pid=${process.pid})`);
      log(`Mode : PRODUCTION (cluster via Redis adapter)`);
      log(`Health : http://localhost:${PORT}/`);
    });
  })
  .catch((err) => {
    log(`[FATAL] Connexion Redis échouée: ${err.message}`);
    process.exit(1); // PM2 redémarre — ne pas tourner sans adapter en prod cluster
  });

// ─────────────────────────────────────────────────────────────
// GRACEFUL SHUTDOWN
// ─────────────────────────────────────────────────────────────

function shutdown(signal) {
  log(`${signal} reçu — arrêt propre`);
  Promise.allSettled([pubClient.quit(), subClient.quit()]).finally(() => {
    server.close(() => process.exit(0));
  });
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

// ─────────────────────────────────────────────────────────────
// PROTECTION GLOBALE CONTRE LES CRASHS
// process.exit(1) — laisse PM2 redémarrer proprement plutôt
// que de continuer dans un état corrompu
// ─────────────────────────────────────────────────────────────

process.on("uncaughtException", (err) => {
  log(`[FATAL] uncaughtException: ${err.message}`);
  log(err.stack || "");
  process.exit(1);
});

process.on("unhandledRejection", (reason) => {
  log(`[FATAL] unhandledRejection: ${reason}`);
  process.exit(1);
});
