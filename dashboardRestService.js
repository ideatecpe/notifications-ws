/**
 * dashboardRestService.js
 *
 * Servicio REST independiente para notificaciones del dashboard.
 * NO modifica ni interfiere con notificationService.js ni el WebSocket.
 *
 * Uso en server.js:
 *   const { getDashboardRest } = require("./dashboardRestService");
 */

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
        await new Promise((r) => setTimeout(r, 300 * (i + 1)));
      } else {
        throw err;
      }
    }
  }
}

// ─── Certificado ──────────────────────────────────────────────────────────────

async function getCertInfo(empresaRuc) {
  const [rows] = await queryWithRetry(
    `SELECT certificadoPem FROM empresa WHERE ruc = ? AND activo = 1 LIMIT 1`,
    [empresaRuc]
  );
  if (!rows?.length || !rows[0].certificadoPem) return null;
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
  } catch {
    return null;
  }
}

// ─── Resolución de filtros (independiente, sin cache compartido) ──────────────

async function resolveFilters(sucursalId, empresaRuc) {
  const useSucursal = sucursalId !== null && sucursalId !== undefined;
  let compRuc, compAnexo;

  if (useSucursal) {
    const [rows] = await queryWithRetry(
      `SELECT empresaRuc, codEstablecimiento FROM sucursal WHERE sucursalId = ?`,
      [sucursalId]
    );
    if (!rows?.length) return null;
    compRuc  = rows[0].empresaRuc;
    compAnexo = rows[0].codEstablecimiento;
  } else {
    compRuc   = empresaRuc;
    compAnexo = null;
  }

  const compParams    = useSucursal ? [compRuc, compAnexo] : [compRuc];
  const guiaParam     = useSucursal ? sucursalId : empresaRuc;
  const rucParaCert   = compRuc || empresaRuc;

  // Filtros por fechaCreacion (pendientes y rechazados)
  const compFilter = useSucursal
    ? `empresaRuc = ? AND establecimientoAnexo = ? AND DATE(fechaCreacion) = CURDATE()`
    : `empresaRuc = ? AND DATE(fechaCreacion) = CURDATE()`;

  const guiaFilter = useSucursal
    ? `sucursalId = ? AND DATE(fechaCreacion) = CURDATE()`
    : `empresaRuc = ? AND DATE(fechaCreacion) = CURDATE()`;

  // Filtros por fechaEnvioSunat (aceptados)
  const compFilterEnvio = useSucursal
    ? `empresaRuc = ? AND establecimientoAnexo = ? AND DATE(fechaEnvioSunat) = CURDATE()`
    : `empresaRuc = ? AND DATE(fechaEnvioSunat) = CURDATE()`;

  const guiaFilterEnvio = useSucursal
    ? `sucursalId = ? AND DATE(fechaEnvioSunat) = CURDATE()`
    : `empresaRuc = ? AND DATE(fechaEnvioSunat) = CURDATE()`;

  return {
    compRuc, compAnexo, compParams, guiaParam, rucParaCert,
    compFilter, guiaFilter,
    compFilterEnvio, guiaFilterEnvio,
  };
}

// ─── Todos los pendientes del día ─────────────────────────────────────────────

async function getAllPending(filters) {
  const { compFilter, compParams, guiaFilter, guiaParam } = filters;

  const [comprobantes] = await queryWithRetry(
    `SELECT comprobanteID AS id, 'comprobante' AS tipo,
            numeroCompleto, tipoComprobante,
            clienteRznSocial AS destinatario,
            importeTotal, tipoMoneda,
            fechaCreacion AS fechaActualizacion,
            estadoSunat,
            NULL AS codigoRespuestaSunat,
            NULL AS mensajeRespuestaSunat
     FROM comprobante
     WHERE ${compFilter} AND estadoSunat = 'PENDIENTE'
       AND tipoComprobante IN ('01','03','07','08')
     ORDER BY fechaCreacion DESC`,
    compParams
  );

  const [guias] = await queryWithRetry(
    `SELECT guiaId AS id, 'guia' AS tipo,
            numeroCompleto, tipoDoc AS tipoComprobante,
            destinatarioRznSocial AS destinatario,
            NULL AS importeTotal, NULL AS tipoMoneda,
            fechaCreacion AS fechaActualizacion,
            estadoSunat,
            NULL AS codigoRespuestaSunat,
            NULL AS mensajeRespuestaSunat
     FROM guiaremision
     WHERE ${guiaFilter} AND estadoSunat = 'PENDIENTE'
     ORDER BY fechaCreacion DESC`,
    [guiaParam]
  );

  return [...comprobantes, ...guias];
}

// ─── Todos los aceptados del día ──────────────────────────────────────────────

async function getAllAccepted(filters) {
  const { compFilterEnvio, compParams, guiaFilterEnvio, guiaParam } = filters;

  const [comprobantes] = await queryWithRetry(
    `SELECT comprobanteID AS id, 'comprobante' AS tipo,
            numeroCompleto, tipoComprobante,
            clienteRznSocial AS destinatario,
            importeTotal, tipoMoneda,
            fechaEnvioSunat AS fechaActualizacion,
            estadoSunat,
            NULL AS codigoRespuestaSunat,
            NULL AS mensajeRespuestaSunat
     FROM comprobante
     WHERE ${compFilterEnvio} AND estadoSunat = 'ACEPTADO'
     ORDER BY fechaEnvioSunat DESC`,
    compParams
  );

  const [guias] = await queryWithRetry(
    `SELECT guiaId AS id, 'guia' AS tipo,
            numeroCompleto, tipoDoc AS tipoComprobante,
            destinatarioRznSocial AS destinatario,
            NULL AS importeTotal, NULL AS tipoMoneda,
            fechaEnvioSunat AS fechaActualizacion,
            estadoSunat,
            NULL AS codigoRespuestaSunat,
            NULL AS mensajeRespuestaSunat
     FROM guiaremision
     WHERE ${guiaFilterEnvio} AND estadoSunat = 'ACEPTADO'
     ORDER BY fechaEnvioSunat DESC`,
    [guiaParam]
  );

  return [...comprobantes, ...guias];
}

// ─── Todos los rechazados del día ─────────────────────────────────────────────
// Usa fechaCreacion porque fechaEnvioSunat puede ser NULL en rechazos

async function getAllRejected(filters) {
  const { compFilter, compParams, guiaFilter, guiaParam } = filters;

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
     WHERE ${compFilter} AND estadoSunat = 'RECHAZADO'
     ORDER BY fechaCreacion DESC`,
    compParams
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
     WHERE ${guiaFilter} AND estadoSunat = 'RECHAZADO'
     ORDER BY fechaCreacion DESC`,
    [guiaParam]
  );

  return [...comprobantes, ...guias];
}

// ─── Función principal ────────────────────────────────────────────────────────

async function getDashboardRest({ sucursalId, empresaRuc }) {
  const filters = await resolveFilters(sucursalId, empresaRuc);
  if (!filters) return null;

  const [pendingRes, acceptedRes, rejectedRes, certRes] = await Promise.allSettled([
    getAllPending(filters),
    getAllAccepted(filters),
    getAllRejected(filters),
    getCertInfo(filters.rucParaCert),
  ]);

  return {
    pendingDocs:  pendingRes.status  === "fulfilled" ? pendingRes.value  : [],
    allAccepted:  acceptedRes.status === "fulfilled" ? acceptedRes.value : [],
    allRejected:  rejectedRes.status === "fulfilled" ? rejectedRes.value : [],
    certInfo:     certRes.status     === "fulfilled" ? certRes.value     : null,
    totalPending: pendingRes.status  === "fulfilled" ? pendingRes.value.length : 0,
    generatedAt:  new Date().toISOString(),
  };
}

module.exports = { getDashboardRest };