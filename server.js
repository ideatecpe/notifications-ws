require("dotenv").config();
const { WebSocketServer } = require("ws");
const {
  getDashboardNotifications,
  closeService,
} = require("./notificationService");
const { getDashboardRest } = require("./dashboardRestService");
const pool = require("./db");
const http = require("http");

const clients = new Map();
const MAX_BODY_BYTES = 64 * 1024;
const VALID_EVENTOS = new Set(["pending", "status", "all"]);

function makeKey(sucursalId, empresaRuc) {
  return sucursalId !== null && sucursalId !== undefined
    ? `sucursal:${sucursalId}`
    : `ruc:${empresaRuc}`;
}

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

const httpServer = http.createServer(async (req, res) => {
  Object.entries(corsHeaders).forEach(([k, v]) => res.setHeader(k, v));
  if (req.method === "OPTIONS") {
    res.writeHead(204);
    return res.end();
  }

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
        const {
          sucursalId,
          empresaRuc,
          evento: rawEvento = "all",
        } = JSON.parse(body);
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

  if (req.method === "GET" && req.url.startsWith("/dashboard-notifications")) {
    try {
      const url = new URL(req.url, `http://localhost`);
      const sucursalId = url.searchParams.get("sucursalId")
        ? Number(url.searchParams.get("sucursalId"))
        : null;
      const empresaRuc = url.searchParams.get("empresaRuc") || null;

      if (!sucursalId && !empresaRuc) {
        res.writeHead(400, { "Content-Type": "application/json" });
        return res.end(
          JSON.stringify({ ok: false, error: "Falta sucursalId o empresaRuc" }),
        );
      }

      const data = await getDashboardRest({ sucursalId, empresaRuc });
      if (!data) {
        res.writeHead(404, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ ok: false, error: "No encontrado" }));
      }

      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: true, ...data }));
    } catch (err) {
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: false, error: err.message }));
    }
    return;
  }

  if (req.method === "POST" && req.url === "/yape") {
    let body = "";
    let bodyLength = 0;

    req.on("data", (chunk) => {
      bodyLength += chunk.length;
      if (bodyLength > MAX_BODY_BYTES) { req.destroy(); return; }
      body += chunk;
    });

    req.on("end", () => {
      try {
        const { monto, remitente, codigoSeguridad, fechaHora, title, body: notifBody } = JSON.parse(body);

        const payload = JSON.stringify({
          type: "yape",
          data: { monto, remitente, codigoSeguridad, fechaHora, body: notifBody, title },
        });

        let enviados = 0;
        for (const clientSet of clients.values()) {
          for (const ws of clientSet) {
            if (ws.readyState === ws.OPEN) {
              ws.send(payload);
              enviados++;
            }
          }
        }

        console.log(`💜 /yape S/ ${monto} de ${remitente} → ${enviados} cliente(s)`);
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true, enviados }));
      } catch (err) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: err.message }));
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
  ws.on("pong", () => {
    ws.isAlive = true;
  });

  ws.on("message", async (message) => {
    try {
      const { sucursalId, empresaRuc } = JSON.parse(message);
      ws.sucursalId = sucursalId ?? null;
      ws.empresaRuc = empresaRuc ?? null;

      const key = makeKey(sucursalId, empresaRuc);
      if (!clients.has(key)) clients.set(key, new Set());
      clients.get(key).add(ws);

      // Al conectarse siempre carga todo
      const data = await getDashboardNotifications({
        sucursalId,
        empresaRuc,
        evento: "all",
      });
      ws.send(JSON.stringify({ type: "dashboard", data }));
    } catch (err) {
      console.error("❌ Error:", err.message);
    }
  });

  ws.on("close", () => {
    const identified =
      (ws.sucursalId !== null && ws.sucursalId !== undefined) ||
      (ws.empresaRuc !== null && ws.empresaRuc !== undefined);
    if (identified) {
      const key = makeKey(ws.sucursalId, ws.empresaRuc);
      const clientSet = clients.get(key);
      if (clientSet) {
        clientSet.delete(ws);
        if (clientSet.size === 0) clients.delete(key);
      }
    }
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
  clearInterval(heartbeatInterval);
  closeService();

  for (const clientSet of clients.values()) {
    for (const ws of clientSet) {
      if (ws.readyState === ws.OPEN) {
        ws.send(
          JSON.stringify({ type: "shutdown", message: "Servidor reiniciando" }),
        );
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
