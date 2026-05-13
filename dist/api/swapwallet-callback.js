import { ensureSchema, sql } from "../lib/db.js";
import { adminIds } from "../lib/env.js";
import { fulfillOrderByPaymentId } from "../lib/bot.js";
import { logError, logInfo } from "../lib/log.js";
import { getSetting } from "../lib/settings.js";
import { getSwapwalletInvoiceById, getSwapwalletInvoiceByOrderId, isSwapwalletInvoicePaid } from "../lib/swapwallet.js";
import { tg } from "../lib/telegram.js";
function pickString(body, keys) {
    if (!body || typeof body !== "object")
        return "";
    const obj = body;
    for (const k of keys) {
        const v = obj[k];
        const s = typeof v === "string" ? v.trim() : "";
        if (s)
            return s;
    }
    return "";
}
export default async function handler(req, res) {
    if (req.method !== "POST") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }
    try {
        const apiKey = ((await getSetting("swapwallet_api_key")) || "").trim();
        const shopUsername = ((await getSetting("swapwallet_shop_username")) || "").trim();
        if (!apiKey || !shopUsername) {
            res.status(500).json({ ok: false, error: "SwapWallet settings not configured" });
            return;
        }
        const body = req.body && typeof req.body === "object" ? req.body : {};
        const orderId = pickString(body, ["orderId", "order_id", "externalOrderId"]) ||
            pickString(body.invoice, ["orderId", "order_id"]) ||
            pickString(body.result, ["orderId", "order_id"]);
        const invoiceId = pickString(body, ["invoiceId", "invoice_id", "id"]) ||
            pickString(body.invoice, ["id", "invoiceId", "invoice_id"]) ||
            pickString(body.result, ["id", "invoiceId", "invoice_id"]);
        let invoice = null;
        try {
            if (orderId) {
                invoice = await getSwapwalletInvoiceByOrderId({ apiKey, shopUsername, orderId });
            }
            else if (invoiceId) {
                invoice = await getSwapwalletInvoiceById({ apiKey, shopUsername, invoiceId });
            }
        }
        catch (e) {
            logError("swapwallet_callback_invoice_lookup_failed", e, { orderId, invoiceId });
            for (const adminId of adminIds) {
                await tg("sendMessage", {
                    chat_id: adminId,
                    text: `⚠️ خطا در بررسی پرداخت SwapWallet\norderId: ${orderId || "-"}\ninvoiceId: ${invoiceId || "-"}\nعلت: ${e.message || String(e)}`
                }).catch(() => { });
            }
            res.status(500).json({ ok: false, error: "Could not verify invoice with SwapWallet" });
            return;
        }
        const resolvedOrderId = String(orderId || invoice?.orderId || "").trim();
        const status = String(invoice?.status || "").trim();
        const paid = invoice ? isSwapwalletInvoicePaid(invoice) : false;
        if (resolvedOrderId) {
            try {
                await ensureSchema();
                await sql `UPDATE orders SET swapwallet_status = ${status} WHERE purchase_id = ${resolvedOrderId};`;
            }
            catch (e) {
                logError("swapwallet_callback_update_order_failed", e, { resolvedOrderId, status });
            }
        }
        if (!paid) {
            logInfo("swapwallet_callback_not_paid", { orderId: resolvedOrderId, status });
            res.status(200).json({ ok: true, received: true, paid: false });
            return;
        }
        if (!resolvedOrderId) {
            res.status(400).json({ ok: false, error: "Missing orderId" });
            return;
        }
        const result = await fulfillOrderByPaymentId(resolvedOrderId);
        logInfo("swapwallet_callback_processed", { orderId: resolvedOrderId, ok: result.ok, reason: result.reason, status });
        res.status(200).json({ ok: result.ok, reason: result.reason });
    }
    catch (error) {
        logError("swapwallet_callback_failed", error, { method: req.method, hasBody: Boolean(req.body) });
        res.status(500).json({ ok: false, error: "Internal server error" });
    }
}
