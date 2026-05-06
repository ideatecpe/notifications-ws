require("dotenv").config();
const { WebSocketServer } = require("ws");
const { getDashboardNotifications, closeService } = require("./notificationService");
const pool = require("./db");
const http = require("http");

const clients = new Map();
const MAX_BODY_BYTES = 64 * 1024;
const VALID_EVENTOS = new Set(["pending", "status", "all"]);

function makeKey(sucursalId, empresaRuc) {
  return (sucursalId !== null && sucursalId !== undefined)
    ? `sucursal:${sucursalId}`
    : `ruc:${empresaRuc}`;
}

const httpServer = http.createServer(async (req, res) => {
  if (req.method === "GET" && req.url === "/health") {
    res.writeHead(200);
    return res.end("OK");
  }

  if (req.method === "POST" && req.url === "/notify") {
    let body = "";
    let bodyLength = 0;
    let aborted = false;

    req.on("error", (err) => {
      console.error("❌ Error en request stream:", err.message);
      if (!res.headersSent) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "Request stream error" }));
      }
    });

    req.on("data", (chunk) => {
      bodyLength += chunk.length;
      if (bodyLength > MAX_BODY_BYTES) {
        aborted = true;
        req.destroy();
        if (!res.headersSent) {
          res.writeHead(413);
          res.end(JSON.stringify({ ok: false, error: "Payload too large" }));
        }
        return;
      }
      body += chunk;
    });

    req.on("end", async () => {
      if (aborted) return;
      try {
        const { sucursalId, empresaRuc, evento: rawEvento = "all" } = JSON.parse(body);
        const evento = VALID_EVENTOS.has(rawEvento) ? rawEvento : "all";

        const key = makeKey(sucursalId, empresaRuc);
        const clientSet = clients.get(key) || new Set();

        console.log(`📨 /notify [${evento}] → ${clientSet.size} cliente(s)`);

        if (clientSet.size > 0) {
          const data = await getDashboardNotifications({
            sucursalId,
            empresaRuc,
            evento,
          });

          for (const ws of clientSet) {
            if (ws.readyState === ws.OPEN) {
              ws.send(JSON.stringify({ type: "dashboard", data }));
            }
          }
        }

        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true, notified: clientSet.size }));
      } catch (err) {
        console.error("❌ Error en /notify:", err.message);
        if (!res.headersSent) {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: err.message }));
        }
      }
    });
    return;
  }

  res.writeHead(404);
  res.end("Not found");
});

const wss = new WebSocketServer({ server: httpServer });

wss.on("connection", (ws) => {
  console.log("✅ Cliente conectado");
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", async (message) => {
    try {
      const { sucursalId, empresaRuc } = JSON.parse(message);
      ws.sucursalId = sucursalId ?? null;
      ws.empresaRuc = empresaRuc ?? null;

      console.log("🔌 Identificado:", { sucursalId, empresaRuc });

      const key = makeKey(sucursalId, empresaRuc);
      if (!clients.has(key)) clients.set(key, new Set());
      clients.get(key).add(ws);

      // Al conectarse siempre carga todo
      const data = await getDashboardNotifications({ sucursalId, empresaRuc, evento: "all" });
      ws.send(JSON.stringify({ type: "dashboard", data }));
    } catch (err) {
      console.error("❌ Error:", err.message);
    }
  });

  ws.on("close", () => {
    const identified = (ws.sucursalId !== null && ws.sucursalId !== undefined)
      || (ws.empresaRuc !== null && ws.empresaRuc !== undefined);
    if (identified) {
      const key = makeKey(ws.sucursalId, ws.empresaRuc);
      const clientSet = clients.get(key);
      if (clientSet) {
        clientSet.delete(ws);
        if (clientSet.size === 0) clients.delete(key);
      }
    }
    console.log("❌ Cliente desconectado");
  });
});

const heartbeatInterval = setInterval(() => {
  for (const [key, clientSet] of clients) {
    for (const ws of clientSet) {
      if (!ws.isAlive) {
        clientSet.delete(ws);
        ws.terminate();
        console.log(`💀 Cliente zombie eliminado [${key}]`);
      } else {
        ws.isAlive = false;
        ws.ping();
      }
    }
    if (clientSet.size === 0) clients.delete(key);
  }
}, 30_000);

function shutdown() {
  console.log("🛑 Apagando servidor...");
  clearInterval(heartbeatInterval);
  closeService();

  for (const clientSet of clients.values()) {
    for (const ws of clientSet) {
      if (ws.readyState === ws.OPEN) {
        ws.send(JSON.stringify({ type: "shutdown", message: "Servidor reiniciando" }));
        ws.close(1001, "Server shutting down");
      }
    }
  }

  const forceExit = setTimeout(() => process.exit(1), 10_000);

  wss.close(() => {
    httpServer.close(async () => {
      try {
        await pool.end();
      } catch (err) {
        console.error("❌ Error cerrando pool:", err.message);
      }
      clearTimeout(forceExit);
      console.log("✅ Servidor cerrado correctamente");
      process.exit(0);
    });
  });
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

const PORT = process.env.WS_PORT || 8080;
httpServer.listen(PORT, () => {
  console.log(`🚀 WebSocket listo en puerto ${PORT}`);
});