const pool = require("./db");
const crypto = require("crypto");

const RETRYABLE_ERRORS = new Set([
  "ECONNRESET",
  "PROTOCOL_CONNECTION_LOST",
  "ECONNREFUSED",
  "ETIMEDOUT",
  "ER_LOCK_DEADLOCK",
]);

async function queryWithRetry(sql, params, retries = 2) {
  for (let i = 0; i <= retries; i++) {
    try {
      return await pool.query(sql, params);
    } catch (err) {
      if (RETRYABLE_ERRORS.has(err.code) && i < retries) {
        const delay = 300 * (i + 1);
        await new Promise((r) => setTimeout(r, delay));
      } else {
        throw err;
      }
    }
  }
}

async function getCertificateExpiry(empresaRuc) {
  const [rows] = await queryWithRetry(
    `SELECT certificadoPem FROM empresa WHERE ruc = ? AND activo = 1 LIMIT 1`,
    [empresaRuc]
  );
  if (!rows || rows.length === 0 || !rows[0].certificadoPem) return null;
  try {
    const pem = Buffer.from(rows[0].certificadoPem, "base64").toString("utf-8");
    const cert = new crypto.X509Certificate(pem);
    const expiryDate = new Date(cert.validTo);
    const daysLeft = Math.ceil((expiryDate - new Date()) / (1000 * 60 * 60 * 24));
    return {
      expiryDate: expiryDate.toISOString(),
      daysLeft,
      isExpiringSoon: daysLeft <= 30,
      isExpired: daysLeft <= 0,
    };
  } catch (err) {
    console.error("❌ Error leyendo certificado:", err.message);
    return null;
  }
}

// ─── Resolución de filtros ────────────────────────────────────────────────────
const filtersCache = new Map();
const CACHE_TTL_MS = 5 * 60 * 1000;

const cacheCleanupInterval = setInterval(() => {
  const now = Date.now();
  for (const [key, entry] of filtersCache) {
    if (now >= entry.expiresAt) filtersCache.delete(key);
  }
}, 10 * 60 * 1000);

async function resolveFilters(sucursalId, empresaRuc) {
  const cacheKey = sucursalId ? `s:${sucursalId}` : `r:${empresaRuc}`;
  const cached = filtersCache.get(cacheKey);

  if (cached && Date.now() < cached.expiresAt) {
    return cached.data;
  }

  const useSucursal = sucursalId !== null && sucursalId !== undefined;
  let compRuc, compAnexo;

  if (useSucursal) {
    const [rows] = await queryWithRetry(
      `SELECT empresaRuc, codEstablecimiento FROM sucursal WHERE sucursalId = ?`,
      [sucursalId]
    );
    if (rows.length === 0) return null;
    compRuc = rows[0].empresaRuc;
    compAnexo = rows[0].codEstablecimiento;
  } else {
    compRuc = empresaRuc;
    compAnexo = null;
  }

  const compParams = useSucursal ? [compRuc, compAnexo] : [compRuc];
  const guiaParam = useSucursal ? sucursalId : empresaRuc;
  const rucParaCert = compRuc || empresaRuc;

  const compFilter = useSucursal
    ? `empresaRuc = ? AND establecimientoAnexo = ? AND DATE(fechaCreacion) = CURDATE()`
    : `empresaRuc = ? AND DATE(fechaCreacion) = CURDATE()`;

  const compFilterEnvio = useSucursal
    ? `empresaRuc = ? AND establecimientoAnexo = ? AND DATE(fechaEnvioSunat) = CURDATE()`
    : `empresaRuc = ? AND DATE(fechaEnvioSunat) = CURDATE()`;

  const guiaFilter = useSucursal
    ? `sucursalId = ? AND DATE(fechaCreacion) = CURDATE()`
    : `empresaRuc = ? AND DATE(fechaCreacion) = CURDATE()`;

  const guiaFilterEnvio = useSucursal
    ? `sucursalId = ? AND DATE(fechaEnvioSunat) = CURDATE()`
    : `empresaRuc = ? AND DATE(fechaEnvioSunat) = CURDATE()`;

  // Filtros SIN restricción de fecha (para "los últimos N sin importar el día")
  const compFilterRecent = useSucursal
    ? `empresaRuc = ? AND establecimientoAnexo = ?`
    : `empresaRuc = ?`;

  const guiaFilterRecent = useSucursal
    ? `sucursalId = ?`
    : `empresaRuc = ?`;

  const result = { compRuc, compAnexo, compParams, guiaParam, rucParaCert, compFilter, compFilterEnvio, guiaFilter, guiaFilterEnvio, compFilterRecent, guiaFilterRecent };
  filtersCache.set(cacheKey, { data: result, expiresAt: Date.now() + CACHE_TTL_MS });
  return result;
}

// ─── Solo pendientes ──────────────────────────────────────────────────────────
async function getPendingData(filters) {
  const { compFilter, compParams, guiaFilter, guiaParam } = filters;

  const [pendingComprobantes] = await queryWithRetry(
    `SELECT comprobanteID AS id, 'comprobante' AS tipo,
            numeroCompleto, tipoComprobante,
            clienteRznSocial AS destinatario,
            importeTotal, tipoMoneda, fechaCreacion, estadoSunat
     FROM comprobante
     WHERE ${compFilter} AND estadoSunat = 'PENDIENTE'
       AND tipoComprobante IN ('01', '03')
     ORDER BY fechaCreacion DESC`,
    compParams
  );

  const [pendingGuias] = await queryWithRetry(
    `SELECT guiaId AS id, 'guia' AS tipo,
            numeroCompleto, tipoDoc AS tipoComprobante,
            destinatarioRznSocial AS destinatario,
            NULL AS importeTotal, NULL AS tipoMoneda,
            fechaCreacion, estadoSunat
     FROM guiaremision
     WHERE ${guiaFilter} AND estadoSunat = 'PENDIENTE'
       AND tipoDoc = '09'
     ORDER BY fechaCreacion DESC`,
    [guiaParam]
  );

  return {
    pendingDocs: [...pendingComprobantes, ...pendingGuias],
    totalPending: pendingComprobantes.length + pendingGuias.length,
  };
}

// ─── Solo último aceptado y rechazado ────────────────────────────────────────
async function getStatusData(filters) {
  const { compFilterEnvio, compParams, guiaFilterEnvio, guiaParam } = filters;

  const [[lastAccepted]] = await queryWithRetry(
    `SELECT id, tipo, numeroCompleto, tipoComprobante, destinatario,
            importeTotal, tipoMoneda, estadoSunat, fechaActualizacion
     FROM (
       SELECT comprobanteID AS id, 'comprobante' AS tipo,
              numeroCompleto, tipoComprobante,
              clienteRznSocial AS destinatario,
              importeTotal, tipoMoneda, estadoSunat,
              fechaEnvioSunat AS fechaActualizacion
       FROM comprobante
       WHERE ${compFilterEnvio} AND estadoSunat = 'ACEPTADO'
       UNION ALL
       SELECT guiaId AS id, 'guia' AS tipo,
              numeroCompleto, tipoDoc AS tipoComprobante,
              destinatarioRznSocial AS destinatario,
              NULL AS importeTotal, NULL AS tipoMoneda,
              estadoSunat, fechaEnvioSunat AS fechaActualizacion
       FROM guiaremision
       WHERE ${guiaFilterEnvio} AND estadoSunat = 'ACEPTADO'
     ) AS todos
     ORDER BY fechaActualizacion DESC LIMIT 1`,
    [...compParams, guiaParam]
  );

  const [[lastRejected]] = await queryWithRetry(
    `SELECT id, tipo, numeroCompleto, tipoComprobante, destinatario,
            importeTotal, tipoMoneda, estadoSunat,
            codigoRespuestaSunat, mensajeRespuestaSunat, fechaActualizacion
     FROM (
       SELECT comprobanteID AS id, 'comprobante' AS tipo,
              numeroCompleto, tipoComprobante,
              clienteRznSocial AS destinatario,
              importeTotal, tipoMoneda, estadoSunat,
              codigoRespuestaSunat, mensajeRespuestaSunat,
              fechaEnvioSunat AS fechaActualizacion
       FROM comprobante
       WHERE ${compFilterEnvio} AND estadoSunat = 'RECHAZADO'
       UNION ALL
       SELECT guiaId AS id, 'guia' AS tipo,
              numeroCompleto, tipoDoc AS tipoComprobante,
              destinatarioRznSocial AS destinatario,
              NULL AS importeTotal, NULL AS tipoMoneda,
              estadoSunat, codigoRespuestaSunat, mensajeRespuestaSunat,
              fechaEnvioSunat AS fechaActualizacion
       FROM guiaremision
       WHERE ${guiaFilterEnvio} AND estadoSunat = 'RECHAZADO'
     ) AS todos
     ORDER BY fechaActualizacion DESC LIMIT 1`,
    [...compParams, guiaParam]
  );

  return {
    lastAccepted: lastAccepted || null,
    lastRejected: lastRejected || null,
  };
}

// ─── Últimos N documentos, sin importar estado ni día ─────────────────────────
async function getRecentDocs(filters, limit = 10) {
  const { compFilterRecent, compParams, guiaFilterRecent, guiaParam } = filters;

  const [comprobantes] = await queryWithRetry(
    `SELECT comprobanteID AS id, 'comprobante' AS tipo,
            numeroCompleto, tipoComprobante,
            clienteRznSocial AS destinatario,
            importeTotal, tipoMoneda,
            fechaCreacion AS fechaActualizacion,
            estadoSunat,
            codigoRespuestaSunat,
            mensajeRespuestaSunat
     FROM comprobante
     WHERE ${compFilterRecent}
     ORDER BY fechaCreacion DESC
     LIMIT ?`,
    [...compParams, limit]
  );

  const [guias] = await queryWithRetry(
    `SELECT guiaId AS id, 'guia' AS tipo,
            numeroCompleto, tipoDoc AS tipoComprobante,
            destinatarioRznSocial AS destinatario,
            NULL AS importeTotal, NULL AS tipoMoneda,
            fechaCreacion AS fechaActualizacion,
            estadoSunat,
            codigoRespuestaSunat,
            mensajeRespuestaSunat
     FROM guiaremision
     WHERE ${guiaFilterRecent}
     ORDER BY fechaCreacion DESC
     LIMIT ?`,
    [guiaParam, limit]
  );

  return [...comprobantes, ...guias]
    .sort((a, b) => new Date(b.fechaActualizacion) - new Date(a.fechaActualizacion))
    .slice(0, limit);
}

// ─── Función principal ────────────────────────────────────────────────────────
async function getDashboardNotifications({ sucursalId, empresaRuc, evento = 'all' }) {
  const filters = await resolveFilters(sucursalId, empresaRuc);
  if (!filters) return emptyResult(evento);

  if (evento === 'pending') {
    const [pending, recentDocs] = await Promise.all([
      getPendingData(filters),
      getRecentDocs(filters, 10),
    ]);
    return { ...pending, recentDocs, evento, generatedAt: new Date().toISOString() };
  }

  if (evento === 'status') {
  const [pending, status, recentDocs] = await Promise.all([
    getPendingData(filters),
    getStatusData(filters),
    getRecentDocs(filters, 10),
  ]);
  return {
    ...pending,
    ...status,
    recentDocs,
    evento,
    generatedAt: new Date().toISOString(),
  };
}

  // 'all' — al conectarse por primera vez
  const [pendingResult, statusResult, recentResult, certResult] = await Promise.allSettled([
    getPendingData(filters),
    getStatusData(filters),
    getRecentDocs(filters, 10),
    getCertificateExpiry(filters.rucParaCert),
  ]);

  const pending = pendingResult.status === 'fulfilled' ? pendingResult.value : { pendingDocs: [], totalPending: 0 };
  const status = statusResult.status === 'fulfilled' ? statusResult.value : { lastAccepted: null, lastRejected: null };
  const recentDocs = recentResult.status === 'fulfilled' ? recentResult.value : [];
  const certInfo = certResult.status === 'fulfilled' ? certResult.value : null;

  return {
    ...pending,
    ...status,
    recentDocs,
    certInfo,
    evento: 'all',
    generatedAt: new Date().toISOString(),
  };
}

function emptyResult(evento = 'all') {
  return {
    pendingDocs: [],
    lastAccepted: null,
    lastRejected: null,
    recentDocs: [],
    totalPending: 0,
    certInfo: null,
    evento,
    generatedAt: new Date().toISOString(),
  };
}

function closeService() {
  clearInterval(cacheCleanupInterval);
}

module.exports = { getDashboardNotifications, closeService };