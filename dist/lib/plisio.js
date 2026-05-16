import { fetchWithTimeout, parseJsonObject, responseSnippet } from "./bot.js";
import * as crypto from "node:crypto";
function assertApiKey(apiKey) {
    const key = apiKey.trim();
    if (!key)
        throw new Error("PLISIO api key is not configured");
    return key;
}
export function verifyPlisioCallbackHash(payload, apiKey) {
    const verifyHash = String(payload.verify_hash || "").trim();
    if (!verifyHash)
        return false;
    const ordered = { ...payload };
    delete ordered.verify_hash;
    const raw = JSON.stringify(ordered);
    const hmac = crypto.createHmac("sha1", assertApiKey(apiKey));
    hmac.update(raw);
    const computed = hmac.digest("hex");
    const a = Buffer.from(computed, "utf8");
    const b = Buffer.from(verifyHash, "utf8");
    if (a.length !== b.length)
        return false;
    return crypto.timingSafeEqual(a, b);
}
export async function createPlisioInvoice(params) {
    const url = new URL("https://api.plisio.net/api/v1/invoices/new");
    url.searchParams.set("api_key", assertApiKey(params.apiKey));
    url.searchParams.set("order_number", params.orderNumber);
    url.searchParams.set("order_name", params.orderName);
    url.searchParams.set("source_currency", params.sourceCurrency);
    url.searchParams.set("source_amount", String(params.sourceAmount));
    url.searchParams.set("callback_url", params.callbackUrl);
    if (params.successCallbackUrl)
        url.searchParams.set("success_callback_url", params.successCallbackUrl);
    if (params.failCallbackUrl)
        url.searchParams.set("fail_callback_url", params.failCallbackUrl);
    if (params.allowedPsysCids)
        url.searchParams.set("allowed_psys_cids", params.allowedPsysCids);
    if (params.email)
        url.searchParams.set("email", params.email);
    if (params.expireMin)
        url.searchParams.set("expire_min", String(params.expireMin));
    url.searchParams.set("return_existing", "1");
    const res = await fetchWithTimeout(url.toString(), { method: "GET" }, 8000);
    const raw = await res.text();
    const parsed = parseJsonObject(raw);
    if (!res.ok || !parsed || parsed.status !== "success") {
        throw new Error(`Plisio create invoice failed: HTTP ${res.status} ${responseSnippet(raw)}`);
    }
    const txnId = String(parsed.data?.txn_id || "").trim();
    const invoiceUrl = String(parsed.data?.invoice_url || "").trim();
    if (!txnId || !invoiceUrl) {
        throw new Error(`Plisio create invoice missing fields: ${responseSnippet(raw)}`);
    }
    return {
        txnId,
        invoiceUrl,
        invoiceTotalSum: parsed.data?.invoice_total_sum ? String(parsed.data.invoice_total_sum) : null
    };
}
export async function getPlisioOperation(params) {
    const url = new URL(`https://api.plisio.net/api/v1/operations/${encodeURIComponent(params.operationId)}`);
    url.searchParams.set("api_key", assertApiKey(params.apiKey));
    const res = await fetchWithTimeout(url.toString(), { method: "GET" }, 8000);
    const raw = await res.text();
    const parsed = parseJsonObject(raw);
    if (!res.ok || !parsed || parsed.status !== "success") {
        throw new Error(`Plisio operation lookup failed: HTTP ${res.status} ${responseSnippet(raw)}`);
    }
    return parsed.data || {};
}
