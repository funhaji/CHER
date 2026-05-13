import type { VercelRequest, VercelResponse } from "@vercel/node";
import { fulfillOrderByPaymentId } from "../lib/bot.js";
import { logError, logInfo } from "../lib/log.js";
import { getStatusByPaymentId } from "../lib/tronado.js";
import { getSetting } from "../lib/settings.js";
import { adminIds } from "../lib/env.js";
import { tg } from "../lib/telegram.js";

export default async function handler(req: VercelRequest, res: VercelResponse) {
  if (req.method !== "POST") {
    res.status(405).json({ ok: false, error: "Method not allowed" });
    return;
  }
  try {
    const paymentId =
      req.body?.PaymentID || req.body?.paymentId || req.body?.paymentID || req.query?.PaymentID || req.query?.paymentId;
    if (!paymentId || typeof paymentId !== "string") {
      res.status(400).json({ ok: false, error: "PaymentID is required" });
      return;
    }
    
    // VERIFY with Tronado API that this payment is ACTUALLY successful
    try {
      const apiKey = (await getSetting("tronado_api_key")) || "";
      const statusRes = await getStatusByPaymentId(paymentId, apiKey) as any;
      const orderStatusTitle = statusRes?.OrderStatusTitle || statusRes?.Data?.OrderStatusTitle || statusRes?.orderStatusTitle || statusRes?.Data?.orderStatusTitle;
      const isPaid = statusRes?.IsPaid === true || statusRes?.Data?.IsPaid === true || statusRes?.isPaid === true || statusRes?.Data?.isPaid === true;
      const isAccepted = orderStatusTitle === "PaymentAccepted" || isPaid;
      
      if (!isAccepted) {
        logError("tronado_callback_spoofed", new Error("Payment status not accepted"), { paymentId, statusRes });
        res.status(400).json({ ok: false, error: "Payment not completed or spoofed" });
        return;
      }
    } catch (statusErr) {
      logError("tronado_callback_verify_failed", statusErr, { paymentId });
      for (const adminId of adminIds) {
        await tg("sendMessage", {
          chat_id: adminId,
          text: `⚠️ خطا در تایید پرداخت Tronado\nسفارش: ${paymentId}\nعلت: ${(statusErr as Error).message || String(statusErr)}`
        }).catch(() => {});
      }
      res.status(500).json({ ok: false, error: "Could not verify payment status with Tronado" });
      return;
    }

    const result = await fulfillOrderByPaymentId(paymentId);
    logInfo("tronado_callback_processed", { paymentId, ok: result.ok, reason: result.reason });
    res.status(200).json({ ok: result.ok, reason: result.reason });
  } catch (error) {
    logError("tronado_callback_failed", error, {
      method: req.method,
      hasBody: Boolean(req.body)
    });
    res.status(500).json({ ok: false, error: "Internal server error" });
  }
}
