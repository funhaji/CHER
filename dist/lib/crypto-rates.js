import { getUsdtRateTomanCached } from "./rates.js";
const cache = new Map();
const coingeckoIdCache = new Map();
function fetchWithTimeout(url, timeoutMs = 6000) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    return fetch(url, { signal: controller.signal }).finally(() => clearTimeout(timer));
}
function snippet(raw, limit = 180) {
    const s = raw.trim().slice(0, limit);
    return s || "empty_response";
}
async function fetchBinanceUsdtPerUnit(symbol) {
    const pair = `${symbol.toUpperCase()}USDT`;
    const url = `https://api.binance.com/api/v3/ticker/price?symbol=${encodeURIComponent(pair)}`;
    const res = await fetchWithTimeout(url, 6000);
    const raw = await res.text();
    if (!res.ok)
        throw new Error(`binance_http_${res.status}:${snippet(raw)}`);
    const data = JSON.parse(raw);
    const price = Number(data?.price);
    if (!Number.isFinite(price) || price <= 0)
        throw new Error(`binance_invalid_payload:${snippet(raw)}`);
    return price;
}
async function resolveCoinGeckoId(symbol) {
    const cached = coingeckoIdCache.get(symbol.toUpperCase());
    if (cached)
        return cached;
    const url = `https://api.coingecko.com/api/v3/search?query=${encodeURIComponent(symbol)}`;
    const res = await fetchWithTimeout(url, 6000);
    const raw = await res.text();
    if (!res.ok)
        throw new Error(`coingecko_search_http_${res.status}:${snippet(raw)}`);
    const data = JSON.parse(raw);
    const coins = Array.isArray(data?.coins) ? data.coins : [];
    const sym = symbol.toUpperCase();
    const best = coins.find((c) => String(c?.symbol || "").toUpperCase() === sym) ||
        coins.find((c) => String(c?.name || "").toUpperCase() === sym) ||
        coins[0];
    const id = String(best?.id || "").trim();
    if (!id)
        throw new Error(`coingecko_search_no_match:${snippet(raw)}`);
    coingeckoIdCache.set(sym, id);
    return id;
}
async function fetchCoinGeckoUsdPerUnitById(id) {
    const url = `https://api.coingecko.com/api/v3/simple/price?ids=${encodeURIComponent(id)}&vs_currencies=usd`;
    const res = await fetchWithTimeout(url, 6000);
    const raw = await res.text();
    if (!res.ok)
        throw new Error(`coingecko_price_http_${res.status}:${snippet(raw)}`);
    const data = JSON.parse(raw);
    const usd = Number(data?.[id]?.usd);
    if (!Number.isFinite(usd) || usd <= 0)
        throw new Error(`coingecko_price_invalid:${snippet(raw)}`);
    return usd;
}
function isUsdPeg(symbol) {
    const s = symbol.toUpperCase();
    return s === "USDT" || s === "USDC" || s === "DAI" || s === "TUSD" || s === "BUSD";
}
export async function getCryptoTomanPerUnitCached(symbol, options) {
    const cacheMs = options?.cacheMs ?? 60_000;
    const now = Date.now();
    const key = symbol.toUpperCase();
    const hit = cache.get(key);
    if (hit && now - hit.updatedAt < cacheMs)
        return hit.value;
    const { rateTomanPerUsdt } = await getUsdtRateTomanCached({ cacheMs: 60_000 });
    if (isUsdPeg(key)) {
        const v = rateTomanPerUsdt;
        cache.set(key, { value: v, updatedAt: now });
        return v;
    }
    const errors = [];
    try {
        const usdtPerUnit = await fetchBinanceUsdtPerUnit(key);
        const v = usdtPerUnit * rateTomanPerUsdt;
        cache.set(key, { value: v, updatedAt: now });
        return v;
    }
    catch (e) {
        errors.push(String(e?.message || e));
    }
    try {
        const id = await resolveCoinGeckoId(key);
        const usdPerUnit = await fetchCoinGeckoUsdPerUnitById(id);
        const v = usdPerUnit * rateTomanPerUsdt;
        cache.set(key, { value: v, updatedAt: now });
        return v;
    }
    catch (e) {
        errors.push(String(e?.message || e));
    }
    throw new Error(`crypto_rate_fetch_failed:${errors.join(" | ")}`);
}
