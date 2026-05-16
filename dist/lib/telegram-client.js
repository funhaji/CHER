import { env } from "./env.js";
function getApiBase() {
    if (!env.TELEGRAM_BOT_TOKEN) {
        throw new Error("TELEGRAM_BOT_TOKEN is not configured");
    }
    return `https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}`;
}
export async function tg(method, body) {
    const res = await fetch(`${getApiBase()}/${method}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body)
    });
    const data = (await res.json());
    if (!data.ok) {
        throw new Error(data.description || "Telegram API error");
    }
    return data.result;
}
export function escapeHtml(value) {
    return value
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;");
}
