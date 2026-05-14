import { ensureSchema, sql } from "./db.js";
export async function getSetting(key) {
    await ensureSchema();
    const rows = await sql `SELECT value FROM settings WHERE key = ${key} LIMIT 1;`;
    return rows.length ? String(rows[0].value) : null;
}
export async function setSetting(key, value) {
    await ensureSchema();
    await sql `
    INSERT INTO settings (key, value)
    VALUES (${key}, ${value})
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
  `;
}
export async function getAdminIds() {
    const envIds = String(process.env.ADMIN_IDS || "")
        .split(",")
        .map((x) => Number(x.trim()))
        .filter((x) => Number.isFinite(x));
    const adminSetting = (await getSetting("admin_ids")) || "";
    const settingIds = String(adminSetting)
        .split(/[,\s]+/)
        .map((x) => Number(x.trim()))
        .filter((x) => Number.isFinite(x));
    return Array.from(new Set([...envIds, ...settingIds]));
}
export async function getBoolSetting(key, fallback = false) {
    const value = await getSetting(key);
    if (value === null)
        return fallback;
    return value.toLowerCase() === "true";
}
export async function getNumberSetting(key) {
    const raw = await getSetting(key);
    if (!raw)
        return null;
    const n = Number(raw);
    if (!Number.isFinite(n))
        return null;
    return n;
}
export async function getPublicBaseUrl(fallbackEnv) {
    const fromSetting = await getSetting("public_base_url");
    const normalized = (fromSetting || "").trim();
    if (normalized)
        return normalized;
    if (fallbackEnv)
        return fallbackEnv;
    if (process.env.VERCEL_URL)
        return `https://${process.env.VERCEL_URL}`;
    return "";
}
