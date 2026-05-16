import { fetchWithTimeout, parseJsonObject, responseSnippet } from "./bot.js";
function assertApiKey(apiKey) {
    const key = apiKey.trim();
    if (!key)
        throw new Error("SWAPWALLET api key is not configured");
    return key;
}
function assertShopUsername(username) {
    const u = username.trim();
    if (!u)
        throw new Error("SWAPWALLET shop username is not configured");
    return u;
}
export async function getSwapwalletAllowedTokens() {
    const url = "https://swapwallet.app/api/v1/payment/invoice/allowed-tokens";
    const res = await fetchWithTimeout(url, { method: "GET" }, 8000);
    const raw = await res.text();
    const parsed = parseJsonObject(raw);
    if (!res.ok || !parsed || String(parsed.status || "").toUpperCase() !== "OK") {
        throw new Error(`SwapWallet allowed-tokens failed: HTTP ${res.status} ${responseSnippet(raw)}`);
    }
    const result = Array.isArray(parsed.result) ? parsed.result : [];
    return result
        .map((x) => ({ token: String(x.token || "").trim().toUpperCase(), network: String(x.network || "").trim().toUpperCase() }))
        .filter((x) => x.token && x.network);
}
export async function createSwapwalletTemporaryWalletInvoice(params) {
    const url = `https://swapwallet.app/api/v2/payment/${encodeURIComponent(assertShopUsername(params.shopUsername))}/invoices/temporary-wallet`;
    const payload = {
        amount: { number: String(Math.round(params.amountToman)), unit: "IRT" },
        allowedToken: String(params.allowedToken).toUpperCase(),
        network: String(params.network).toUpperCase(),
        ttl: Math.max(300, Math.min(21600, Math.round(params.ttlSeconds))),
        orderId: params.orderId,
        webhookUrl: params.webhookUrl,
        description: params.description ?? null,
        customData: params.customData ?? null,
        userId: params.userId ?? null,
        userEmail: params.userEmail ?? null,
        userPhoneNumber: params.userPhoneNumber ?? null
    };
    const res = await fetchWithTimeout(url, {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${assertApiKey(params.apiKey)}` },
        body: JSON.stringify(payload)
    }, 10_000);
    const raw = await res.text();
    const parsed = parseJsonObject(raw);
    if (!res.ok || !parsed || String(parsed.status || "").toUpperCase() !== "OK") {
        throw new Error(`SwapWallet create invoice failed: HTTP ${res.status} ${responseSnippet(raw)}`);
    }
    const result = parsed.result || {};
    const invoiceId = String(result.id || "").trim();
    const walletAddress = String(result.walletAddress || "").trim();
    if (!invoiceId || !walletAddress) {
        throw new Error(`SwapWallet create invoice missing fields: ${responseSnippet(raw)}`);
    }
    const links = Array.isArray(result.links) ? result.links : [];
    const urls = links.map((l) => String(l?.url || "").trim()).filter(Boolean);
    const expiredAt = result.expiredAt ? String(result.expiredAt) : null;
    return { invoiceId, walletAddress, urls, expiredAt, rawResult: result };
}
export async function getSwapwalletInvoiceByOrderId(params) {
    const url = `https://swapwallet.app/api/v2/payment/${encodeURIComponent(assertShopUsername(params.shopUsername))}/invoices/with-order-id/${encodeURIComponent(params.orderId)}`;
    const res = await fetchWithTimeout(url, { method: "GET", headers: { Authorization: `Bearer ${assertApiKey(params.apiKey)}` } }, 10_000);
    const raw = await res.text();
    const parsed = parseJsonObject(raw);
    if (!res.ok || !parsed || String(parsed.status || "").toUpperCase() !== "OK") {
        throw new Error(`SwapWallet invoice lookup failed: HTTP ${res.status} ${responseSnippet(raw)}`);
    }
    return parsed.result || {};
}
export async function getSwapwalletInvoiceById(params) {
    const url = `https://swapwallet.app/api/v2/payment/${encodeURIComponent(assertShopUsername(params.shopUsername))}/invoices/${encodeURIComponent(params.invoiceId)}`;
    const res = await fetchWithTimeout(url, { method: "GET", headers: { Authorization: `Bearer ${assertApiKey(params.apiKey)}` } }, 10_000);
    const raw = await res.text();
    const parsed = parseJsonObject(raw);
    if (!res.ok || !parsed || String(parsed.status || "").toUpperCase() !== "OK") {
        throw new Error(`SwapWallet invoice lookup failed: HTTP ${res.status} ${responseSnippet(raw)}`);
    }
    return parsed.result || {};
}
export function isSwapwalletInvoicePaid(invoice) {
    const status = String(invoice.status || "").toUpperCase().trim();
    const paidAt = invoice.paidAt ? String(invoice.paidAt).trim() : "";
    if (paidAt)
        return true;
    return status === "PAID" || status === "DONE" || status === "COMPLETED" || status === "SUCCESS";
}
