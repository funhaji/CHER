import { sql } from "./db.js";

const coingeckoIdCache = new Map<string, string>();

function fetchWithTimeout(url: string, timeoutMs = 6000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  return fetch(url, { signal: controller.signal }).finally(() => clearTimeout(timer));
}

function snippet(raw: string, limit = 180) {
  const s = raw.trim().slice(0, limit);
  return s || "empty_response";
}

async function fetchBinanceUsdtPerUnit(symbol: string) {
  const pair = `${symbol.toUpperCase()}USDT`;
  const url = `https://api.binance.com/api/v3/ticker/price?symbol=${encodeURIComponent(pair)}`;
  const res = await fetchWithTimeout(url, 6000);
  const raw = await res.text();
  if (!res.ok) throw new Error(`binance_http_${res.status}:${snippet(raw)}`);
  const data = JSON.parse(raw) as any;
  const price = Number(data?.price);
  if (!Number.isFinite(price) || price <= 0) throw new Error(`binance_invalid_payload:${snippet(raw)}`);
  return price;
}

function pickNavasanApiKey() {
  const a = (process.env.NAVASAN_KEY_1 || "").trim();
  const b = (process.env.NAVASAN_KEY_2 || "").trim();
  if (a && b) return Date.now() % 2 === 0 ? a : b;
  return a || b || "";
}

async function fetchNavasanUsdToman() {
  const cached = await sql`
    SELECT toman_per_unit
    FROM crypto_rate_cache
    WHERE symbol = 'USD_TOMAN'
      AND updated_at > NOW() - INTERVAL '6 hours'
    LIMIT 1;
  `;
  if (cached.length) {
    const n = Number((cached[0] as any).toman_per_unit);
    if (Number.isFinite(n) && n > 0) return n;
  }
  const apiKey = pickNavasanApiKey();
  if (!apiKey) {
    throw new Error("navasan_api_key_missing");
  }
  const url = `https://api.navasan.tech/latest/?api_key=${encodeURIComponent(apiKey)}`;
  const res = await fetchWithTimeout(url, 6000);
  const raw = await res.text();
  if (!res.ok) throw new Error(`navasan_http_${res.status}:${snippet(raw)}`);
  let data: any;
  try {
    data = JSON.parse(raw) as any;
  } catch {
    throw new Error(`navasan_parse_failed:${snippet(raw)}`);
  }
  const candidates = ["usd_sell", "usd", "usd_buy", "dollar_sell", "dollar", "usd_irr", "usd_market"];
  for (const key of candidates) {
    const v = data?.[key]?.value ?? data?.[key];
    const n = parseInt(String(v ?? ""), 10);
    if (Number.isFinite(n) && n > 0) {
      await sql`
        INSERT INTO crypto_rate_cache (symbol, toman_per_unit, updated_at)
        VALUES ('USD_TOMAN', ${n}, NOW())
        ON CONFLICT (symbol) DO UPDATE
          SET toman_per_unit = EXCLUDED.toman_per_unit, updated_at = NOW();
      `;
      return n;
    }
  }
  for (const [k, obj] of Object.entries(data || {})) {
    if (!String(k).toLowerCase().includes("usd")) continue;
    const v = (obj as any)?.value ?? obj;
    const n = parseInt(String(v ?? ""), 10);
    if (Number.isFinite(n) && n > 0) {
      await sql`
        INSERT INTO crypto_rate_cache (symbol, toman_per_unit, updated_at)
        VALUES ('USD_TOMAN', ${n}, NOW())
        ON CONFLICT (symbol) DO UPDATE
          SET toman_per_unit = EXCLUDED.toman_per_unit, updated_at = NOW();
      `;
      return n;
    }
  }
  throw new Error(`navasan_invalid_payload:${snippet(raw)}`);
}

async function resolveCoinGeckoId(symbol: string) {
  const cached = coingeckoIdCache.get(symbol.toUpperCase());
  if (cached) return cached;
  const url = `https://api.coingecko.com/api/v3/search?query=${encodeURIComponent(symbol)}`;
  const res = await fetchWithTimeout(url, 6000);
  const raw = await res.text();
  if (!res.ok) throw new Error(`coingecko_search_http_${res.status}:${snippet(raw)}`);
  const data = JSON.parse(raw) as any;
  const coins = Array.isArray(data?.coins) ? data.coins : [];
  const sym = symbol.toUpperCase();
  const best =
    coins.find((c: any) => String(c?.symbol || "").toUpperCase() === sym) ||
    coins.find((c: any) => String(c?.name || "").toUpperCase() === sym) ||
    coins[0];
  const id = String(best?.id || "").trim();
  if (!id) throw new Error(`coingecko_search_no_match:${snippet(raw)}`);
  coingeckoIdCache.set(sym, id);
  return id;
}

async function fetchCoinGeckoUsdPerUnitById(id: string) {
  const url = `https://api.coingecko.com/api/v3/simple/price?ids=${encodeURIComponent(id)}&vs_currencies=usd`;
  const res = await fetchWithTimeout(url, 6000);
  const raw = await res.text();
  if (!res.ok) throw new Error(`coingecko_price_http_${res.status}:${snippet(raw)}`);
  const data = JSON.parse(raw) as any;
  const usd = Number(data?.[id]?.usd);
  if (!Number.isFinite(usd) || usd <= 0) throw new Error(`coingecko_price_invalid:${snippet(raw)}`);
  return usd;
}

async function fetchCoinGeckoIrrPerUnitById(id: string) {
  const url = `https://api.coingecko.com/api/v3/simple/price?ids=${encodeURIComponent(id)}&vs_currencies=irr`;
  const res = await fetchWithTimeout(url, 6000);
  const raw = await res.text();
  if (!res.ok) throw new Error(`coingecko_irr_http_${res.status}:${snippet(raw)}`);
  const data = JSON.parse(raw) as any;
  const irr = Number(data?.[id]?.irr);
  if (!Number.isFinite(irr) || irr <= 0) throw new Error(`coingecko_irr_invalid:${snippet(raw)}`);
  return irr;
}

function isUsdPeg(symbol: string) {
  const s = symbol.toUpperCase();
  return s === "USDT" || s === "USDC" || s === "DAI" || s === "TUSD" || s === "BUSD";
}

function coingeckoIdOverride(symbol: string) {
  const s = symbol.toUpperCase();
  if (s === "USDT") return "tether";
  if (s === "TRX") return "tron";
  if (s === "TON") return "the-open-network";
  return "";
}

export async function getCryptoTomanPerUnitCached(symbol: string, options?: { cacheMs?: number }) {
  const cacheMs = options?.cacheMs ?? 5 * 60_000;
  const key = symbol.toUpperCase();
  const cacheSeconds = Math.max(1, Math.floor(cacheMs / 1000));
  const fresh = await sql`
    SELECT toman_per_unit
    FROM crypto_rate_cache
    WHERE symbol = ${key}
      AND updated_at > NOW() - (${cacheSeconds} || ' seconds')::interval
    LIMIT 1;
  `;
  if (fresh.length) {
    const n = Number((fresh[0] as any).toman_per_unit);
    if (Number.isFinite(n) && n > 0) return n;
  }

  const errors: string[] = [];
  let tomanPerUnit = 0;

  try {
    const id = coingeckoIdOverride(key) || (await resolveCoinGeckoId(key));
    const irrPerUnit = await fetchCoinGeckoIrrPerUnitById(id);
    tomanPerUnit = irrPerUnit / 10;
  } catch (e) {
    errors.push(String((e as Error)?.message || e));
  }

  if (!Number.isFinite(tomanPerUnit) || tomanPerUnit <= 0) {
    const usdTomanErrors: string[] = [];
    let usdToman = 0;
    try {
      usdToman = await fetchNavasanUsdToman();
    } catch (e) {
      usdTomanErrors.push(String((e as Error)?.message || e));
    }

    if (usdToman > 0) {
      if (isUsdPeg(key)) {
        tomanPerUnit = usdToman;
      } else {
        try {
          const usdtPerUnit = await fetchBinanceUsdtPerUnit(key);
          tomanPerUnit = usdtPerUnit * usdToman;
        } catch (e) {
          errors.push(String((e as Error)?.message || e));
        }
        if (!Number.isFinite(tomanPerUnit) || tomanPerUnit <= 0) {
          try {
            const id = coingeckoIdOverride(key) || (await resolveCoinGeckoId(key));
            const usdPerUnit = await fetchCoinGeckoUsdPerUnitById(id);
            tomanPerUnit = usdPerUnit * usdToman;
          } catch (e) {
            errors.push(String((e as Error)?.message || e));
          }
        }
      }
    } else if (usdTomanErrors.length) {
      errors.push(...usdTomanErrors);
    }
  }

  if (!Number.isFinite(tomanPerUnit) || tomanPerUnit <= 0) {
    throw new Error(`crypto_rate_fetch_failed:${errors.join(" | ")}`);
  }

  const tomanPerUnitFixed = Number(tomanPerUnit);
  await sql`
    INSERT INTO crypto_rate_cache (symbol, toman_per_unit, updated_at)
    VALUES (${key}, ${tomanPerUnitFixed}, NOW())
    ON CONFLICT (symbol) DO UPDATE
      SET toman_per_unit = EXCLUDED.toman_per_unit, updated_at = NOW();
  `;
  return tomanPerUnitFixed;
}
