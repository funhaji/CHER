import { ensureSchema, sql } from "./db.js";
const CACHE_TTL_MS = 30_000; 

let cacheData: Map<string, string | null> | null = null;
let cacheFetchedAt = 0;
let cacheLoadPromise: Promise<Map<string, string | null>> | null = null;

async function loadAllSettings(): Promise<Map<string, string | null>> {
  await ensureSchema();
  const rows = await sql`SELECT key, value FROM settings;`;
  const map = new Map<string, string | null>();
  for (const row of rows) {
    map.set(String(row.key), row.value != null ? String(row.value) : null);
  }
  cacheData = map;
  cacheFetchedAt = Date.now();
  cacheLoadPromise = null;
  return map;
}

async function getCache(): Promise<Map<string, string | null>> {
  if (cacheData && Date.now() - cacheFetchedAt < CACHE_TTL_MS) {
    return cacheData;
  }
  if (!cacheLoadPromise) {
    cacheLoadPromise = loadAllSettings();
  }
  return cacheLoadPromise;
}

export function invalidateSettingsCache() {
  cacheData = null;
  cacheFetchedAt = 0;
  cacheLoadPromise = null;
}

export async function getSetting(key: string): Promise<string | null> {
  const cache = await getCache();
  return cache.has(key) ? (cache.get(key) ?? null) : null;
}

export async function setSetting(key: string, value: string) {
  await ensureSchema();
  await sql`
    INSERT INTO settings (key, value)
    VALUES (${key}, ${value})
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
  `;
  // Write-through: keep cache consistent without waiting for TTL expiry
  if (cacheData) {
    cacheData.set(key, value);
  }
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

export async function getBoolSetting(key: string, fallback = false) {
  const value = await getSetting(key);
  if (value === null) return fallback;
  return value.toLowerCase() === "true";
}

export async function getNumberSetting(key: string) {
  const raw = await getSetting(key);
  if (!raw) return null;
  const n = Number(raw);
  if (!Number.isFinite(n)) return null;
  return n;
}

export async function getPublicBaseUrl(fallbackEnv?: string) {
  const fromSetting = await getSetting("public_base_url");
  const normalized = (fromSetting || "").trim();
  if (normalized) return normalized;
  if (fallbackEnv) return fallbackEnv;
  if (process.env.VERCEL_URL) return `https://${process.env.VERCEL_URL}`;
  return "";
}
