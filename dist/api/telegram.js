import { handleTelegramUpdate } from "../lib/bot.js";
import { tg } from "../lib/telegram.js";
import { logError } from "../lib/log.js";
import { ensureSchema } from "../lib/db.js"; 

export default async function handler(req, res) {
    if (req.method !== "POST") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    try {
        await ensureSchema();

        await handleTelegramUpdate(req.body);

        res.status(200).json({ ok: true });
    }
    catch (error) {
        logError("telegram_webhook_failed", error, {
            method: req.method,
            hasBody: Boolean(req.body),
            updateId: req.body?.update_id
        });

        try {
            const chatId = req.body?.message?.chat?.id || req.body?.callback_query?.message?.chat?.id;
            if (chatId) {
                await tg("sendMessage", {
                    chat_id: chatId,
                    text: "❌ متاسفانه خطایی در سرور رخ داد. لطفا مجدداً تلاش کنید."
                });
            }
        } catch (e) {}

        res.status(200).json({ ok: false, error: String(error.message || error) });
    }
}
