import { fulfillOrderByPaymentId } from "../lib/bot.js";
import { logError, logInfo } from "../lib/log.js";
import { verifyTetrapayOrder } from "../lib/tetrapay.js";
import { getSetting, getAdminIds } from "../lib/settings.js";
import { tg } from "../lib/telegram.js";
export default async function handler(req, res) {
    try {
        const authority = req.body?.authority || req.query?.authority;
        const status = req.body?.status || req.query?.status;
        const hashId = req.body?.hash_id || req.query?.hash_id;
        if (!authority || typeof authority !== "string") {
            res.status(400).json({ ok: false, error: "authority is required" });
            return;
        }
        if (status != 100 && status != "100") {
            res.status(400).json({ ok: false, error: "Payment not completed" });
            return;
        }
        try {
            const apiKey = (await getSetting("tetrapay_api_key")) || "";
            const verifyRes = await verifyTetrapayOrder(authority, apiKey);
            if (!verifyRes.ok) {
                logError("tetrapay_callback_spoofed", new Error("Payment status not verified"), { authority, verifyRes });
                res.status(400).json({ ok: false, error: "Payment not completed or spoofed" });
                return;
            }
        }
        catch (statusErr) {
            logError("tetrapay_callback_verify_failed", statusErr, { authority });
            for (const adminId of await getAdminIds()) {
                await tg("sendMessage", {
                    chat_id: adminId,
                    text: `⚠️ خطا در تایید پرداخت TetraPay\nauthority: ${authority}\nعلت: ${statusErr.message || String(statusErr)}`
                }).catch(() => { });
            }
            res.status(500).json({ ok: false, error: "Could not verify payment status with TetraPay" });
            return;
        }
        const paymentId = String(hashId);
        if (!paymentId) {
            res.status(400).json({ ok: false, error: "hash_id is missing" });
            return;
        }
        const result = await fulfillOrderByPaymentId(paymentId);
        logInfo("tetrapay_callback_processed", { paymentId, authority, ok: result.ok, reason: result.reason });
        // TetraPay might redirect the user to this URL if it's a GET request from the browser
        if (req.method === "GET") {
            res.status(200).send(`
        <html dir="rtl" lang="fa">
          <head>
            <meta charset="utf-8">
            <title>نتیجه پرداخت</title>
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
              body { font-family: Tahoma, sans-serif; text-align: center; padding: 50px; background: #f9f9f9; }
              .card { background: white; padding: 20px; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); max-width: 400px; margin: 0 auto; }
              h2 { color: ${result.ok ? "#4caf50" : "#e53935"}; }
            </style>
          </head>
          <body>
            <div class="card">
              <h2>${result.ok ? "پرداخت موفق" : "خطا یا پرداخت تکراری"}</h2>
              <p>وضعیت پرداخت شما ثبت شد. می‌توانید به ربات تلگرام برگردید.</p>
            </div>
          </body>
        </html>
      `);
            return;
        }
        res.status(200).json({ ok: result.ok, reason: result.reason });
    }
    catch (error) {
        logError("tetrapay_callback_failed", error, {
            method: req.method,
            hasBody: Boolean(req.body)
        });
        res.status(500).json({ ok: false, error: "Internal server error" });
    }
}
