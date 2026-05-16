import { getBoolSetting, getNumberSetting } from "./settings.js";
let usdtTomanCache = null;
function fetchWithTimeout(url, timeoutMs = 6000) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    return fetch(url, { signal: controller.signal }).finally(() => clearTimeout(timer));
}
function snippet(raw, limit = 180) {
    const s = raw.trim().slice(0, limit);
    return s || "empty_response";
}
async function fetchUsdtIrrFromCoinGecko() {
    const url = "https://api.coingecko.com/api/v3/simple/price?ids=tether&vs_currencies=irr";
    const res = await fetchWithTimeout(url, 6000);
    const raw = await res.text();
    if (!res.ok) {
        throw new Error(`coingecko_http_${res.status}:${snippet(raw)}`);
    }
    let data;
    try {
        data = JSON.parse(raw);
    }
    catch {
        throw new Error(`coingecko_parse_failed:${snippet(raw)}`);
    }
    const irrPerUsdt = Number(data?.tether?.irr);
    if (!Number.isFinite(irrPerUsdt) || irrPerUsdt <= 0) {
        throw new Error(`coingecko_invalid_payload:${snippet(raw)}`);
    }
    return irrPerUsdt;
}
async function fetchUsdIrrFromOpenErApi() {
    const url = "https://open.er-api.com/v6/latest/USD";
    const res = await fetchWithTimeout(url, 6000);
    const raw = await res.text();
    if (!res.ok) {
        throw new Error(`open_er_api_http_${res.status}:${snippet(raw)}`);
    }
    let data;
    try {
        data = JSON.parse(raw);
    }
    catch {
        throw new Error(`open_er_api_parse_failed:${snippet(raw)}`);
    }
    const irrPerUsd = Number(data?.rates?.IRR);
    if (!Number.isFinite(irrPerUsd) || irrPerUsd <= 0) {
        throw new Error(`open_er_api_invalid_payload:${snippet(raw)}`);
    }
    return irrPerUsd;
}
async function fetchUsdIrrFromExchangeRateFun() {
    const url = "https://api.exchangerate.fun/latest?base=USD";
    const res = await fetchWithTimeout(url, 6000);
    const raw = await res.text();
    if (!res.ok) {
        throw new Error(`exchangerate_fun_http_${res.status}:${snippet(raw)}`);
    }
    let data;
    try {
        data = JSON.parse(raw);
    }
    catch {
        throw new Error(`exchangerate_fun_parse_failed:${snippet(raw)}`);
    }
    const irrPerUsd = Number(data?.rates?.IRR);
    if (!Number.isFinite(irrPerUsd) || irrPerUsd <= 0) {
        throw new Error(`exchangerate_fun_invalid_payload:${snippet(raw)}`);
    }
    return irrPerUsd;
}
export async function getUsdtRateTomanCached(options) {
    const cacheMs = options?.cacheMs ?? 60_000;
    const allowStaleMs = options?.allowStaleMs ?? 15 * 60_000;
    const now = Date.now();
    if (usdtTomanCache && now - usdtTomanCache.updatedAt < cacheMs) {
        return { rateTomanPerUsdt: usdtTomanCache.value, source: "cache" };
    }
    const errors = [];
    const manual = (await getNumberSetting("usdt_toman_rate")) || 0;
    if (manual > 0) {
        usdtTomanCache = { value: manual, updatedAt: now };
        return { rateTomanPerUsdt: manual, source: "setting" };
    }
    const autoEnabled = await getBoolSetting("usdt_auto_rate", true);
    if (!autoEnabled) {
        throw new Error("usdt_auto_rate_disabled_and_no_manual_rate");
    }
    try {
        const irrPerUsdt = await fetchUsdtIrrFromCoinGecko();
        const tomanPerUsdt = irrPerUsdt / 10;
        usdtTomanCache = { value: tomanPerUsdt, updatedAt: now };
        return { rateTomanPerUsdt: tomanPerUsdt, source: "coingecko" };
    }
    catch (error) {
        errors.push(String(error?.message || error));
    }
    try {
        const irrPerUsd = await fetchUsdIrrFromOpenErApi();
        const tomanPerUsdt = irrPerUsd / 10;
        usdtTomanCache = { value: tomanPerUsdt, updatedAt: now };
        return { rateTomanPerUsdt: tomanPerUsdt, source: "open_er_api" };
    }
    catch (error) {
        errors.push(String(error?.message || error));
    }
    try {
        const irrPerUsd = await fetchUsdIrrFromExchangeRateFun();
        const tomanPerUsdt = irrPerUsd / 10;
        usdtTomanCache = { value: tomanPerUsdt, updatedAt: now };
        return { rateTomanPerUsdt: tomanPerUsdt, source: "exchangerate_fun" };
    }
    catch (error) {
        errors.push(String(error?.message || error));
        if (usdtTomanCache && now - usdtTomanCache.updatedAt < allowStaleMs) {
            return { rateTomanPerUsdt: usdtTomanCache.value, source: "stale_cache" };
        }
        throw new Error(`rate_fetch_failed:${errors.join(" | ")}`);
    }
}
