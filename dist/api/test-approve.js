import { env } from "../lib/env.js";
import { fulfillOrderByPaymentId } from "../lib/bot.js";
import { logError, logInfo } from "../lib/log.js";
export default async function handler(req, res) {
    if (req.method !== "POST") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }
    const pass = String(req.headers["x-test-pass"] || req.body?.pass || req.query?.pass || "");
    if (pass !== env.TEST_PASS) {
        res.status(401).json({ ok: false, error: "Unauthorized" });
        return;
    }
    const purchaseId = String(req.body?.purchaseId || req.query?.purchaseId || "");
    if (!purchaseId) {
        res.status(400).json({ ok: false, error: "purchaseId is required" });
        return;
    }
    try {
        const result = await fulfillOrderByPaymentId(purchaseId);
        logInfo("test_approve_processed", { purchaseId, ok: result.ok, reason: result.reason });
        res.status(200).json({ ok: result.ok, reason: result.reason, purchaseId });
    }
    catch (error) {
        logError("test_approve_failed", error, { purchaseId });
        res.status(500).json({ ok: false, error: "Internal server error" });
    }
}
