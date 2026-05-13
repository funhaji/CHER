import type { VercelRequest, VercelResponse } from "@vercel/node";
import { handleTelegramUpdate } from "../lib/bot.js";
import { ensureSchema } from "../lib/db.js";
import { tg } from "../lib/telegram.js";
import { logError } from "../lib/log.js";

/** Cold-start `ensureSchema()` can exceed the default 10s on large migrations (use Pro for >60s). */
export const config = {
  maxDuration: 60
};

export default async function handler(req: VercelRequest, res: VercelResponse) {
  if (req.method !== "POST") {
    res.status(405).json({ ok: false, error: "Method not allowed" });
    return;
  }
  try {
    await ensureSchema();
    await handleTelegramUpdate(req.body);
    res.status(200).json({ ok: true });
  } catch (error) {
    logError("telegram_webhook_failed", error, {
      method: req.method,
      hasBody: Boolean(req.body),
      updateId: req.body?.update_id
    });
    
    // Attempt to notify the user that an internal error occurred so they don't face silent failure
    try {
      const chatId = req.body?.message?.chat?.id || req.body?.callback_query?.message?.chat?.id;
      if (chatId) {
        await tg("sendMessage", {
          chat_id: chatId,
          text: "❌ متاسفانه خطایی در سرور رخ داد. لطفا مجدداً تلاش کنید."
        });
      }
    } catch (e) {
      // Ignore errors here (e.g. if user blocked bot)
    }

    res.status(200).json({ ok: false, error: String((error as Error).message || error) });
  }
}
