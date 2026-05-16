import type { VercelRequest, VercelResponse } from "@vercel/node";
import { ensureSchema, sql } from "../lib/db.js";
import { env } from "../lib/env.js";

function escapeHtml(value: string) {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;")
    .replaceAll("'", "&#039;");
}

export default async function handler(req: VercelRequest, res: VercelResponse) {
  if (req.method !== "GET") {
    res.status(405).json({ ok: false, error: "Method not allowed" });
    return;
  }
  const token = String(req.query.token || "");
  if (!env.TEST_PASS || token !== env.TEST_PASS) {
    res.status(403).json({ ok: false, error: "Forbidden" });
    return;
  }
  const limitRaw = Number(req.query.limit || 200);
  const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(1000, Math.round(limitRaw))) : 200;
  await ensureSchema();
  const rows = await sql`
    SELECT id, level, event, payload, created_at
    FROM runtime_logs
    ORDER BY id DESC
    LIMIT ${limit};
  `;
  const asJson = String(req.query.format || "").toLowerCase() === "json";
  if (asJson) {
    res.status(200).json({ ok: true, count: rows.length, logs: rows });
    return;
  }
  const lines = rows.map((row: any) => {
    const when = escapeHtml(String(row.created_at || ""));
    const level = escapeHtml(String(row.level || ""));
    const event = escapeHtml(String(row.event || ""));
    const payload = escapeHtml(JSON.stringify(row.payload || {}, null, 2));
    return `<details><summary>[${when}] ${level} ${event}</summary><pre>${payload}</pre></details>`;
  });
  const html =
    "<!doctype html><html><head><meta charset=\"utf-8\"/>" +
    "<title>Runtime Logs</title>" +
    "<style>body{font-family:ui-monospace,Consolas,monospace;padding:16px;background:#0b1020;color:#e5e7eb}" +
    "a{color:#93c5fd}details{margin:8px 0;padding:8px;border:1px solid #334155;border-radius:8px;background:#111827}" +
    "pre{white-space:pre-wrap;word-break:break-word}</style></head><body>" +
    `<h1>Runtime Logs (${rows.length})</h1>` +
    "<p>Use <code>?format=json</code> for JSON output.</p>" +
    lines.join("\n") +
    "</body></html>";
  res.setHeader("Content-Type", "text/html; charset=utf-8");
  res.status(200).send(html);
}

