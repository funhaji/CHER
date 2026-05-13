import type { VercelRequest, VercelResponse } from "@vercel/node";
import { fulfillOrderByPaymentId } from "../lib/bot.js";
import { logError, logInfo } from "../lib/log.js";
import { getSetting } from "../lib/settings.js";
import { getPlisioOperation, verifyPlisioCallbackHash } from "../lib/plisio.js";
import { ensureSchema, sql } from "../lib/db.js";
import { adminIds } from "../lib/env.js";
import { tg } from "../lib/telegram.js";

function normalizePaymentIdFromCallback(data: Record<string, unknown>) {
  const orderName = String(data.order_name || "").trim();
  if (orderName) return orderName;
  const orderNumber = String(data.order_number || "").trim();
  if (orderNumber) return `P${orderNumber}`;
  return "";
}

function isPaidStatus(status: string) {
  const s = status.toLowerCase().trim();
  return s === "completed" || s === "mismatch";
}

function isFailureStatus(status: string) {
  const s = status.toLowerCase().trim();
  return s === "expired" || s === "cancelled" || s === "error" || s === "cancelled duplicate";
}

async function notifyAdmins(text: string, replyMarkup?: Record<string, unknown>) {
  for (const adminId of adminIds) {
    await tg("sendMessage", { chat_id: adminId, text, reply_markup: replyMarkup }).catch(() => {});
  }
}

export default async function handler(req: VercelRequest, res: VercelResponse) {
  if (req.method !== "POST") {
    res.status(405).json({ ok: false, error: "Method not allowed" });
    return;
  }

  try {
    const apiKey = (await getSetting("plisio_api_key")) || "";
    if (!apiKey.trim()) {
      res.status(500).json({ ok: false, error: "Plisio api key not configured" });
      return;
    }

    const body: Record<string, unknown> =
      req.body && typeof req.body === "object" ? (req.body as Record<string, unknown>) : {};

    const txnId = String(body.txn_id || "").trim();
    const status = String(body.status || "").trim();
    const paymentId = normalizePaymentIdFromCallback(body);

    if (!txnId || !status || !paymentId) {
      res.status(400).json({ ok: false, error: "Missing required fields" });
      return;
    }

    const hashOk = verifyPlisioCallbackHash(body, apiKey);
    if (!hashOk) {
      logError("plisio_callback_invalid_hash", new Error("verify_hash mismatch"), { txnId, paymentId, status });
      await notifyAdmins(`❌ Plisio callback verify_hash نامعتبر\nid: ${paymentId}\ntxn: ${txnId}\nstatus: ${status}`);
      res.status(422).json({ ok: false, error: "Invalid verify_hash" });
      return;
    }

    try {
      await ensureSchema();
      await sql`UPDATE orders SET plisio_status = ${status} WHERE purchase_id = ${paymentId};`;
    } catch (e) {
      logError("plisio_callback_update_order_failed", e, { txnId, paymentId, status });
    }

    let operation: Record<string, unknown> | null = null;
    try {
      const op = await getPlisioOperation({ apiKey, operationId: txnId });
      operation = op as unknown as Record<string, unknown>;
      const opStatus = String(op.status || "").trim().toLowerCase();
      const paid = isPaidStatus(opStatus);
      if (!paid && isPaidStatus(status)) {
        logError("plisio_callback_status_mismatch", new Error("callback paid but operation not paid"), {
          txnId,
          paymentId,
          callbackStatus: status,
          operationStatus: op.status
        });
      }
    } catch (e) {
      logError("plisio_callback_operation_lookup_failed", e, { txnId, paymentId, status });
    }

    if (!isPaidStatus(status)) {
      logInfo("plisio_callback_not_paid", { txnId, paymentId, status, operation });
      if (isFailureStatus(status)) {
        const button =
          paymentId.startsWith("P")
            ? { inline_keyboard: [[{ text: "🔎 باز کردن سفارش", callback_data: `admin_open_purchase_${paymentId}` }]] }
            : undefined;
        await notifyAdmins(`⚠️ پرداخت Plisio ناموفق/منقضی شد\nid: ${paymentId}\ntxn: ${txnId}\nstatus: ${status}`, button);
      }
      res.status(200).json({ ok: true, received: true, paid: false });
      return;
    }

    const result = await fulfillOrderByPaymentId(paymentId);
    logInfo("plisio_callback_processed", { txnId, paymentId, ok: result.ok, reason: result.reason, operation });
    res.status(200).json({ ok: result.ok, reason: result.reason });
  } catch (error) {
    logError("plisio_callback_failed", error, { method: req.method, hasBody: Boolean(req.body) });
    res.status(500).json({ ok: false, error: "Internal server error" });
  }
}
