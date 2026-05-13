import { ensureSchema, sql } from "./db.js";
import { adminIds, env } from "./env.js";
import { logError, logInfo } from "./log.js";
import { getOrderToken, getStatusByPaymentId, getTronPriceToman } from "./tronado.js";
import { getBoolSetting, getNumberSetting, getPublicBaseUrl, getSetting, setSetting } from "./settings.js";
import { getUsdtRateTomanCached } from "./rates.js";
import { getCryptoTomanPerUnitCached } from "./crypto-rates.js";
import { escapeHtml, tg } from "./telegram.js";
import { randomUUID } from "node:crypto";
import * as crypto from "node:crypto";
function isAdmin(userId) {
    return adminIds.includes(userId);
}
function randomCode(length = 8) {
    const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
    return Array.from({ length }, () => chars[Math.floor(Math.random() * chars.length)]).join("");
}
function qrCodeUrl(value) {
    return `https://quickchart.io/qr?size=320&text=${encodeURIComponent(value)}`;
}
function startMediaTitle(kind, value) {
    const v = String(value || "").trim();
    if (kind === "none" || !v)
        return "خاموش";
    if (kind === "text")
        return `متن: ${v.slice(0, 40)}${v.length > 40 ? "…" : ""}`;
    if (kind === "sticker")
        return "استیکر";
    if (kind === "animation")
        return "گیف";
    if (kind === "photo")
        return "عکس";
    return "خاموش";
}
async function sendStartMedia(chatId) {
    const kindRaw = (await getSetting("start_media_kind")) || "none";
    const value = (await getSetting("start_media_value")) || "";
    const kind = ["none", "text", "sticker", "animation", "photo"].includes(kindRaw)
        ? kindRaw
        : "none";
    const v = String(value || "").trim();
    if (kind === "none" || !v)
        return;
    try {
        if (kind === "text") {
            await tg("sendMessage", { chat_id: chatId, text: v });
            return;
        }
        if (kind === "sticker") {
            await tg("sendSticker", { chat_id: chatId, sticker: v });
            return;
        }
        if (kind === "animation") {
            await tg("sendAnimation", { chat_id: chatId, animation: v });
            return;
        }
        if (kind === "photo") {
            await tg("sendPhoto", { chat_id: chatId, photo: v });
            return;
        }
    }
    catch (e) {
        logError("send_start_media_failed", e, { kind, chatId });
    }
}
function truncateText(value, max) {
    const v = String(value || "");
    if (v.length <= max)
        return v;
    return v.slice(0, Math.max(0, max - 1)) + "…";
}
function formatPriceToman(value) {
    const amount = Math.round(Number(value) || 0);
    return amount.toLocaleString("en-US");
}
function formatPaymentMethodTitle(methodRaw) {
    const method = String(methodRaw || "").trim().toLowerCase();
    if (method === "wallet")
        return "کیف پول";
    if (method === "card2card")
        return "کارت‌به‌کارت";
    if (method === "tronado")
        return "TRON (Tronado)";
    if (method === "tetrapay")
        return "تتراپی";
    if (method === "plisio")
        return "Plisio";
    if (method === "crypto")
        return "کریپتو";
    return methodRaw ? String(methodRaw) : "-";
}
function formatOrderStatusTitle(statusRaw) {
    const status = String(statusRaw || "").trim().toLowerCase();
    if (status === "pending")
        return "⏳ در انتظار پرداخت";
    if (status === "awaiting_receipt")
        return "📷 منتظر ارسال رسید";
    if (status === "receipt_submitted")
        return "🕵️ در انتظار بررسی";
    if (status === "fulfilling")
        return "⚙️ در حال آماده‌سازی";
    if (status === "paid")
        return "✅ تحویل شده";
    if (status === "denied")
        return "❌ رد شده";
    if (status === "cancelled")
        return "🗑 لغو شده";
    if (status === "awaiting_config")
        return "🧩 نیازمند کانفیگ دستی";
    return statusRaw ? String(statusRaw) : "-";
}
function normalizePricePerGb(raw, fallback = 500000) {
    const n = Number(raw);
    if (!Number.isFinite(n) || n <= 0)
        return fallback;
    if (n < 10000)
        return Math.round(n * 1000);
    return Math.round(n);
}
function parseDataAmountToMb(raw) {
    const value = raw.trim().replaceAll(" ", "").toLowerCase();
    const gbMatch = value.match(/^(\d+(?:\.\d+)?)(gb|g)$/i);
    if (gbMatch) {
        const n = Number(gbMatch[1]);
        return (Number.isFinite(n) && n > 0) ? Math.round(n * 1024) : null;
    }
    const mbMatch = value.match(/^(\d+(?:\.\d+)?)(mb|m)$/i);
    if (mbMatch) {
        const n = Number(mbMatch[1]);
        return (Number.isFinite(n) && n > 0) ? Math.round(n) : null;
    }
    const tbMatch = value.match(/^(\d+(?:\.\d+)?)(tb|t)$/i);
    if (tbMatch) {
        const n = Number(tbMatch[1]);
        return (Number.isFinite(n) && n > 0) ? Math.round(n * 1024 * 1024) : null;
    }
    const plain = Number(value);
    return (Number.isFinite(plain) && plain > 0) ? Math.round(plain) : null;
}
function parseInfiniteDataFlag(raw) {
    const normalized = raw.trim().toLowerCase();
    return ["infinite", "unlimited", "∞", "inf", "نامحدود", "بینهایت", "بی‌نهایت", "بى‌نهايت"].includes(normalized);
}
function formatBytesShort(value) {
    const bytes = Number(value);
    if (!Number.isFinite(bytes) || bytes <= 0)
        return "0";
    const mb = bytes / (1024 * 1024);
    if (mb < 1024)
        return `${mb.toFixed(0)}MB`;
    return `${(mb / 1024).toFixed(2)}GB`;
}
function formatExpiryLabelFromSeconds(unixSeconds) {
    const n = Number(unixSeconds);
    if (!Number.isFinite(n) || n <= 0)
        return "بدون انقضا";
    const ts = n * 1000;
    return `${new Date(ts).toLocaleString("en-US")} (${Math.max(0, Math.ceil((ts - Date.now()) / (24 * 60 * 60 * 1000)))} روز مانده)`;
}
function formatExpiryLabelFromMilliseconds(ms) {
    const n = Number(ms);
    if (!Number.isFinite(n) || n <= 0)
        return "بدون انقضا";
    return `${new Date(n).toLocaleString("en-US")} (${Math.max(0, Math.ceil((n - Date.now()) / (24 * 60 * 60 * 1000)))} روز مانده)`;
}
function parsePanelType(raw) {
    const value = raw.trim().toLowerCase();
    if (value === "marzban" || value === "sanaei")
        return value;
    return null;
}
export function normalizeBaseUrl(raw) {
    return raw.trim().replace(/\/+$/, "");
}
function normalizeFieldKey(raw) {
    return raw.trim().toLowerCase().replace(/[ \-]+/g, "_");
}
function parseSellMode(raw) {
    const value = String(raw || "").trim().toLowerCase();
    if (value === "panel" || value === "auto_panel" || value === "panel_sale")
        return "panel";
    return "manual";
}
function parseDeliveryMode(raw) {
    const value = String(raw || "").trim().toLowerCase();
    if (value === "sub" || value === "subscription" || value === "sub_only")
        return "sub";
    if (value === "configs" || value === "config" || value === "configs_only")
        return "configs";
    return "both";
}
function formatDeliveryModeLabel(mode) {
    if (mode === "both")
        return "ساب + کانفیگ";
    if (mode === "sub")
        return "فقط ساب";
    return "فقط کانفیگ";
}
function parseProductKind(raw) {
    const value = String(raw || "").trim().toLowerCase();
    if (value === "account" || value === "acc")
        return "account";
    return "v2ray";
}
function toJsonObject(value) {
    if (value && typeof value === "object" && !Array.isArray(value)) {
        return value;
    }
    return null;
}
function parseJsonValue(raw) {
    try {
        return JSON.parse(raw);
    }
    catch {
        return null;
    }
}
function parseFlexibleFields(raw) {
    const trimmed = raw.trim();
    if (!trimmed)
        return {};
    if (trimmed.startsWith("{")) {
        const parsed = toJsonObject(parseJsonValue(trimmed));
        return parsed || {};
    }
    if (!trimmed.includes(":") && !trimmed.includes("="))
        return {};
    const fields = {};
    const lines = trimmed.split(/\r?\n/);
    for (let i = 0; i < lines.length; i += 1) {
        const line = lines[i].trim();
        if (!line)
            continue;
        const match = line.match(/^([^:=]+)\s*[:=]\s*(.*)$/);
        if (!match)
            return {};
        const key = normalizeFieldKey(match[1]);
        let value = match[2].trim();
        if (!value && i + 1 < lines.length && lines[i + 1].trim().startsWith("{")) {
            const block = [];
            let balance = 0;
            for (let j = i + 1; j < lines.length; j += 1) {
                const blockLine = lines[j];
                block.push(blockLine);
                const opens = (blockLine.match(/\{/g) || []).length;
                const closes = (blockLine.match(/\}/g) || []).length;
                balance += opens - closes;
                i = j;
                if (balance <= 0 && block.length)
                    break;
            }
            value = block.join("\n").trim();
        }
        fields[key] = value;
    }
    return fields;
}
function getFieldValue(fields, ...keys) {
    for (const key of keys) {
        const direct = fields[key];
        if (direct !== undefined && direct !== null && String(direct).trim() !== "") {
            return direct;
        }
    }
    return null;
}
function parseMaybeNumber(value) {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
}
function parseMaybeBoolean(value) {
    const normalized = String(value ?? "").trim().toLowerCase();
    if (!normalized)
        return null;
    if (["true", "1", "yes", "on", "فعال", "روشن"].includes(normalized))
        return true;
    if (["false", "0", "no", "off", "غیرفعال", "خاموش"].includes(normalized))
        return false;
    return null;
}
function mergeDeep(base, override) {
    const baseObj = toJsonObject(base);
    const overrideObj = toJsonObject(override);
    if (!baseObj || !overrideObj) {
        return override === undefined ? base : override;
    }
    const merged = { ...baseObj };
    for (const [key, value] of Object.entries(overrideObj)) {
        merged[key] = key in merged ? mergeDeep(merged[key], value) : value;
    }
    return merged;
}
function applyTemplate(value, context) {
    if (typeof value === "string") {
        return value.replace(/\{\{\s*([a-z0-9_]+)\s*\}\}/gi, (_, key) => context[key] ?? "");
    }
    if (Array.isArray(value)) {
        return value.map((item) => applyTemplate(item, context));
    }
    if (value && typeof value === "object") {
        return Object.fromEntries(Object.entries(value).map(([key, child]) => [key, applyTemplate(child, context)]));
    }
    return value;
}
function sanitizePanelConfig(raw) {
    return toJsonObject(raw) || {};
}
function serializeDeliveryPayload(payload) {
    return JSON.stringify({
        subscriptionUrl: payload.subscriptionUrl || null,
        configLinks: payload.configLinks || [],
        previousConfigs: payload.previousConfigs || [],
        primaryQr: payload.primaryQr || null,
        primaryText: payload.primaryText || null,
        metadata: payload.metadata || {}
    });
}
export function parseDeliveryPayload(raw) {
    const payload = toJsonObject(raw) || {};
    const configLinks = Array.isArray(payload.configLinks)
        ? payload.configLinks.map((item) => String(item || "").trim()).filter(Boolean)
        : [];
    const previousConfigs = Array.isArray(payload.previousConfigs)
        ? payload.previousConfigs.map((item) => String(item || "").trim()).filter(Boolean)
        : [];
    return {
        subscriptionUrl: payload.subscriptionUrl ? String(payload.subscriptionUrl) : null,
        configLinks,
        previousConfigs,
        primaryQr: payload.primaryQr ? String(payload.primaryQr) : null,
        primaryText: payload.primaryText ? String(payload.primaryText) : null,
        metadata: toJsonObject(payload.metadata) || {}
    };
}
function configSummaryLine(payload) {
    const configCount = payload.configLinks?.length || 0;
    if (payload.subscriptionUrl && configCount)
        return `ساب + ${configCount} کانفیگ`;
    if (payload.subscriptionUrl)
        return "فقط ساب";
    if (configCount)
        return `${configCount} کانفیگ`;
    return "نامشخص";
}
function getV2rayProductKindFromRow(row) {
    const panelConfig = sanitizePanelConfig(row.panel_config);
    return parseProductKind(panelConfig.product_kind);
}
function parseDelimitedOrFields(raw, orderedKeys) {
    if (raw.includes("|")) {
        const parts = raw.split("|").map((item) => item.trim());
        return Object.fromEntries(orderedKeys.map((key, index) => [key, parts[index] ?? ""]));
    }
    return parseFlexibleFields(raw);
}
function parseProductInput(raw, current) {
    const fields = parseDelimitedOrFields(raw, ["name", "size_mb", "price_toman"]);
    const currentPanelConfig = sanitizePanelConfig(current?.panel_config);
    const nameRaw = getFieldValue(fields, "name", "title");
    const sizeRaw = getFieldValue(fields, "size_mb", "size", "volume_mb", "mb");
    const priceRaw = getFieldValue(fields, "price_toman", "price", "price_tmn");
    const productKind = parseProductKind(getFieldValue(fields, "product_kind", "kind", "type") ?? currentPanelConfig.product_kind);
    const sellMode = parseSellMode(String(getFieldValue(fields, "sell_mode", "mode") ?? current?.sell_mode ?? "manual"));
    const isInfiniteRaw = getFieldValue(fields, "is_infinite", "infinite");
    const panelIdRaw = getFieldValue(fields, "panel_id", "panel");
    const panelLimitRaw = getFieldValue(fields, "panel_sell_limit", "sell_limit", "limit");
    const deliveryMode = parseDeliveryMode(String(getFieldValue(fields, "panel_delivery_mode", "delivery_mode", "delivery") ?? current?.panel_delivery_mode ?? "both"));
    const panelConfigValue = getFieldValue(fields, "panel_config", "config_json", "config", "panel_json") ?? current?.panel_config ?? {};
    const parsedPanelConfig = typeof panelConfigValue === "string" ? sanitizePanelConfig(parseJsonValue(panelConfigValue) || {}) : sanitizePanelConfig(panelConfigValue);
    const convenienceConfig = sanitizePanelConfig({
        inbound_id: parseMaybeNumber(getFieldValue(fields, "inbound_id", "inbound")),
        protocol: getFieldValue(fields, "protocol"),
        flow: getFieldValue(fields, "flow"),
        expire_days: parseMaybeNumber(getFieldValue(fields, "expire_days", "days")),
        data_limit_mb: parseMaybeNumber(getFieldValue(fields, "data_limit_mb", "traffic_mb")),
        subscription_path: getFieldValue(fields, "subscription_path", "sub_path"),
        server_host: getFieldValue(fields, "server_host", "host"),
        sni: getFieldValue(fields, "sni"),
        fingerprint: getFieldValue(fields, "fingerprint", "fp"),
        path: getFieldValue(fields, "path"),
        service_name: getFieldValue(fields, "service_name"),
        method: getFieldValue(fields, "method")
    });
    return {
        name: nameRaw ? String(nameRaw).trim() : String(current?.name || "").trim(),
        productKind,
        sizeMb: productKind === "account" ? 0 : (sizeRaw !== null ? Number(sizeRaw) : Number(current?.size_mb || 0)),
        priceRaw: priceRaw !== null ? String(priceRaw).trim() : "",
        sellMode,
        isInfinite: parseMaybeBoolean(isInfiniteRaw) ?? Boolean(current?.is_infinite),
        panelId: panelIdRaw === null || String(panelIdRaw).trim() === "" ? Number(current?.panel_id || 0) || null : Number(panelIdRaw),
        panelSellLimit: panelLimitRaw === null || String(panelLimitRaw).trim() === ""
            ? (current?.panel_sell_limit === null || current?.panel_sell_limit === undefined ? null : Number(current.panel_sell_limit))
            : Number(panelLimitRaw),
        panelDeliveryMode: deliveryMode,
        panelConfig: sanitizePanelConfig(mergeDeep(currentPanelConfig, mergeDeep(parsedPanelConfig, mergeDeep(convenienceConfig, { product_kind: productKind }))))
    };
}
function parseCardInput(raw) {
    const fields = parseDelimitedOrFields(raw, ["label", "card_number", "holder_name", "bank_name"]);
    return {
        label: String(getFieldValue(fields, "label", "title") || "").trim(),
        cardNumber: String(getFieldValue(fields, "card_number", "number") || "").trim(),
        holderName: String(getFieldValue(fields, "holder_name", "owner", "name") || "").trim(),
        bankName: String(getFieldValue(fields, "bank_name", "bank") || "").trim()
    };
}
function parseDiscountInput(raw, currentCode) {
    const fields = parseDelimitedOrFields(raw, currentCode ? ["type", "amount", "usage_limit"] : ["code", "type", "amount", "usage_limit"]);
    const codeSource = currentCode ? currentCode : String(getFieldValue(fields, "code") || "");
    const code = codeSource.toUpperCase() === "RANDOM" ? randomCode(10) : codeSource.toUpperCase();
    return {
        code,
        type: String(getFieldValue(fields, "type") || "").trim().toLowerCase(),
        amount: Number(getFieldValue(fields, "amount", "value")),
        usageLimit: getFieldValue(fields, "usage_limit", "limit") === null || String(getFieldValue(fields, "usage_limit", "limit")).trim() === ""
            ? null
            : Number(getFieldValue(fields, "usage_limit", "limit"))
    };
}
function parseAdminMessageInput(raw) {
    if (raw.includes("|")) {
        const parts = raw.split("|");
        return {
            targetRaw: String(parts[0] || "").trim(),
            messageText: parts.slice(1).join("|").trim()
        };
    }
    const fields = parseFlexibleFields(raw);
    return {
        targetRaw: String(getFieldValue(fields, "telegram_id", "target", "user", "username") || "").trim(),
        messageText: String(getFieldValue(fields, "text", "message", "body") || "").trim()
    };
}
function parseDirectMigrateInput(raw) {
    const fields = parseDelimitedOrFields(raw, ["source_inventory_id", "target_panel_id", "user_telegram_id", "config"]);
    return {
        sourceInventoryId: Number(getFieldValue(fields, "source_inventory_id", "inventory_id", "inventory")),
        targetPanelId: Number(getFieldValue(fields, "target_panel_id", "panel_id", "panel")),
        requestedFor: Number(getFieldValue(fields, "user_telegram_id", "telegram_id", "user_id", "user")),
        config: String(getFieldValue(fields, "config", "config_value") || "").trim()
    };
}
function panelTypeTitle(panelType) {
    if (panelType === "marzban")
        return "Marzban";
    if (panelType === "sanaei")
        return "Sanaei / 3x-ui";
    return panelType.toUpperCase();
}
function panelResultLabel(ok) {
    if (ok === null || ok === undefined)
        return "ندارد";
    return ok ? "موفق" : "ناموفق";
}
function maskSecret(value) {
    if (!value)
        return "-";
    return "•".repeat(Math.min(Math.max(value.length, 4), 12));
}
function isValidHttpUrl(raw) {
    try {
        const url = new URL(raw);
        return url.protocol === "http:" || url.protocol === "https:";
    }
    catch {
        return false;
    }
}
function shortAddr(addr) {
    const v = (addr || "").trim();
    if (!v)
        return "-";
    return v.length <= 16 ? v : `${v.slice(0, 8)}...${v.slice(-6)}`;
}
function cryptoWalletTitle(w) {
    return `${w.currency} (${w.network})`;
}
function cryptoWalletReady(w) {
    const hasAddress = Boolean((w.address || "").trim());
    const hasRate = w.rate_mode === "auto" ? true : Number(w.rate_toman_per_unit || 0) > 0;
    return w.active && hasAddress && hasRate;
}
function cb(text, callback_data, style) {
    return style ? { text, callback_data, style } : { text, callback_data };
}
function homeButton() {
    return cb("🏠 منوی اصلی", "home", "primary");
}
function backButton(callback_data, text = "🔙 بازگشت") {
    return cb(text, callback_data, "primary");
}
function cancelButton(callback_data = "home", text = "❌ لغو") {
    return cb(text, callback_data, "danger");
}
function confirmButton(callback_data, text = "✅ تایید") {
    return cb(text, callback_data, "success");
}
async function getPlisioTomanPerUsdt() {
    const auto = await getBoolSetting("plisio_auto_rate", true);
    const extra = (await getNumberSetting("plisio_usdt_extra_toman")) || 0;
    const manual = (await getNumberSetting("plisio_usdt_rate_fallback_toman")) ||
        (await getNumberSetting("plisio_usd_rate_toman")) ||
        0;
    if (!auto) {
        if (manual <= 0) {
            throw new Error("plisio_manual_rate_not_set");
        }
        return Math.max(1, manual + extra);
    }
    try {
        const { rateTomanPerUsdt, source } = await getUsdtRateTomanCached();
        logInfo("plisio_rate_auto_ok", { source, rateTomanPerUsdt });
        return Math.max(1, rateTomanPerUsdt + extra);
    }
    catch (error) {
        if (manual > 0) {
            logError("plisio_rate_auto_failed_using_fallback", error, { fallbackTomanPerUsdt: manual });
            return Math.max(1, manual + extra);
        }
        throw error;
    }
}
export async function fetchWithTimeout(url, init, timeoutMs = 8000) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
        return await fetch(url, { ...init, signal: controller.signal });
    }
    finally {
        clearTimeout(timer);
    }
}
export function parseJsonObject(raw) {
    try {
        const parsed = JSON.parse(raw);
        if (parsed && typeof parsed === "object") {
            return parsed;
        }
        return null;
    }
    catch {
        return null;
    }
}
export function responseSnippet(raw, limit = 220) {
    const value = raw.trim().slice(0, limit);
    return value || "empty_response";
}
function extractUuidFromText(raw) {
    if (!raw)
        return null;
    const match = raw.match(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i);
    return match ? match[0] : null;
}
function parsePanelUserTelegramId(candidate) {
    const direct = Number(candidate);
    if (Number.isFinite(direct) && direct > 0)
        return Math.round(direct);
    const match = String(candidate || "").match(/telegram[:=\s]+(\d{5,})/i);
    if (match?.[1]) {
        const parsed = Number(match[1]);
        if (Number.isFinite(parsed) && parsed > 0)
            return Math.round(parsed);
    }
    return null;
}
function collectLookupCandidates(raw) {
    const candidates = new Set();
    const push = (value) => {
        const item = String(value || "").trim();
        if (!item)
            return;
        candidates.add(item);
        const lower = item.toLowerCase();
        if (lower !== item)
            candidates.add(lower);
    };
    const source = raw.trim();
    push(source);
    try {
        push(decodeURIComponent(source));
    }
    catch {
    }
    const uuid = extractUuidFromText(source);
    if (uuid)
        push(uuid);
    const emailMatches = source.match(/[a-z0-9._%+\-]+@[a-z0-9.\-]+/gi) || [];
    for (const item of emailMatches)
        push(item);
    const tokenMatches = source.match(/[a-z0-9_\-]{8,}/gi) || [];
    for (const token of tokenMatches)
        push(token);
    if (source.toLowerCase().startsWith("vmess://")) {
        const encoded = source.slice("vmess://".length).split("#")[0].trim();
        if (encoded) {
            try {
                const decoded = Buffer.from(encoded, "base64").toString("utf8");
                const vmess = parseJsonObject(decoded);
                if (vmess) {
                    push(vmess.id);
                    push(vmess.ps);
                    push(vmess.add);
                    push(vmess.host);
                    push(vmess.path);
                    push(vmess.sni);
                }
                push(decoded);
            }
            catch {
            }
        }
    }
    const urlLike = source.match(/^[a-z][a-z0-9+\-.]*:\/\//i) ? source : source.startsWith("/") ? `https://x${source}` : "";
    if (urlLike) {
        try {
            const url = new URL(urlLike);
            push(url.hostname);
            const parts = url.pathname.split("/").map((part) => part.trim()).filter(Boolean);
            for (const part of parts)
                push(part);
            for (const value of url.searchParams.values())
                push(value);
        }
        catch {
        }
    }
    return Array.from(candidates).filter((item) => item.length >= 3);
}
function extractSessionCookie(setCookieHeader) {
    if (!setCookieHeader)
        return "";
    const sessionMatch = setCookieHeader.match(/(?:^|,\s*)(session=[^;]+)/i);
    if (sessionMatch?.[1])
        return sessionMatch[1];
    return setCookieHeader.split(";")[0]?.trim() || "";
}
async function updatePanelCheckState(panelId, ok, message, meta, accessToken) {
    if (accessToken === undefined) {
        await sql `
      UPDATE panels
      SET last_check_at = NOW(),
          last_check_ok = ${ok},
          last_check_message = ${message},
          cached_meta = ${JSON.stringify(meta)}::jsonb
      WHERE id = ${panelId};
    `;
        return;
    }
    await sql `
    UPDATE panels
    SET access_token = ${accessToken},
        last_check_at = NOW(),
        last_check_ok = ${ok},
        last_check_message = ${message},
        cached_meta = ${JSON.stringify(meta)}::jsonb
    WHERE id = ${panelId};
  `;
}
export function jsonSuccess(data) {
    return data?.success === true;
}
function jsonArrayLength(data, key) {
    const value = data?.[key];
    return Array.isArray(value) ? value.length : null;
}
async function getPanelById(panelId) {
    const rows = await sql `
    SELECT
      id,
      name,
      panel_type,
      base_url,
      username,
      password,
      active,
      allow_customer_migration,
      allow_new_sales,
      last_check_at,
      last_check_ok,
      last_check_message,
      cached_meta,
      priority,
      created_at
    FROM panels
    WHERE id = ${panelId}
    LIMIT 1;
  `;
    return rows[0] || null;
}
function panelWizardPayload(mode, step, panelType, panelId, current) {
    return {
        mode,
        step,
        panelId: panelId || null,
        panelType,
        name: String(current?.name || ""),
        baseUrl: String(current?.base_url || ""),
        username: String(current?.username || ""),
        password: String(current?.password || "")
    };
}
async function promptPanelTypePicker(chatId, mode, panelId) {
    const prefix = mode === "add" ? "admin_panel_pick_type_add_" : `admin_panel_pick_type_edit_${panelId}_`;
    await tg("sendMessage", {
        chat_id: chatId,
        text: mode === "add" ? "نوع پنل جدید را انتخاب کنید:" : "نوع پنل را برای ویرایش انتخاب کنید:",
        reply_markup: {
            inline_keyboard: [
                [
                    cb("Marzban", `${prefix}marzban`, "primary"),
                    cb("Sanaei / 3x-ui", `${prefix}sanaei`, "primary")
                ],
                [backButton(panelId ? `admin_panel_open_${panelId}` : "admin_panels")]
            ]
        }
    });
}
async function promptPanelWizardStep(chatId, payload) {
    const mode = String(payload.mode || "add");
    const step = String(payload.step || "name");
    const panelId = Number(payload.panelId || 0);
    const keepHint = mode === "edit" ? "\nبرای نگه داشتن مقدار فعلی، فقط - بفرستید." : "";
    let text = "";
    if (step === "name") {
        text =
            `مرحله 1 از 4 - نام پنل\n` +
                `نوع: ${panelTypeTitle(String(payload.panelType || ""))}` +
                (mode === "edit" ? `\nمقدار فعلی: ${String(payload.name || "-")}` : "") +
                `${keepHint}\n\nنام پنل را بفرستید.`;
    }
    if (step === "base_url") {
        text =
            `مرحله 2 از 4 - آدرس پنل\n` +
                `نوع: ${panelTypeTitle(String(payload.panelType || ""))}` +
                (mode === "edit" ? `\nمقدار فعلی: ${String(payload.baseUrl || "-")}` : "") +
                `${keepHint}\n\nآدرس کامل را بفرستید.\nنمونه:\nhttps://panel.example.com`;
    }
    if (step === "username") {
        text =
            `مرحله 3 از 4 - نام کاربری\n` +
                (mode === "edit" ? `مقدار فعلی: ${String(payload.username || "-")}` : "") +
                `${keepHint}\n\nنام کاربری پنل را بفرستید.`;
    }
    if (step === "password") {
        text =
            `مرحله 4 از 4 - رمز عبور\n` +
                (mode === "edit" ? `مقدار فعلی: ${maskSecret(String(payload.password || ""))}` : "") +
                `${keepHint}\n\nرمز عبور پنل را بفرستید.`;
    }
    await tg("sendMessage", {
        chat_id: chatId,
        text,
        reply_markup: {
            inline_keyboard: [[cancelButton(panelId ? `admin_panel_wizard_cancel_${panelId}` : "admin_panel_wizard_cancel")]]
        }
    });
}
async function startPanelWizard(chatId, userId, mode, panelType, panelId) {
    let current;
    if (mode === "edit") {
        const panel = await getPanelById(Number(panelId));
        if (!panel) {
            await tg("sendMessage", { chat_id: chatId, text: "پنل پیدا نشد." });
            return;
        }
        current = panel;
    }
    const payload = panelWizardPayload(mode, "name", panelType, panelId, current);
    await setState(userId, "admin_panel_wizard", payload);
    await promptPanelWizardStep(chatId, payload);
}
async function getProductForPanelWizard(productId) {
    const rows = await sql `
    SELECT id, name, size_mb, sell_mode, panel_id, panel_sell_limit, panel_delivery_mode, panel_config
    FROM products
    WHERE id = ${productId}
    LIMIT 1;
  `;
    return rows[0] || null;
}
function productPanelWizardPayload(product) {
    const panelConfig = sanitizePanelConfig(product.panel_config);
    const protocol = String(panelConfig.protocol || "").trim() || "vless";
    return {
        step: "panel",
        productId: Number(product.id),
        productName: String(product.name || ""),
        sizeMb: Number(product.size_mb || 0),
        panelId: Number(product.panel_id || 0) || null,
        panelSellLimit: product.panel_sell_limit === null || product.panel_sell_limit === undefined ? null : Number(product.panel_sell_limit),
        panelDeliveryMode: parseDeliveryMode(String(product.panel_delivery_mode || "both")),
        inboundId: parseMaybeNumber(panelConfig.inbound_id) ?? 1,
        protocol,
        expireDays: parseMaybeNumber(panelConfig.expire_days) ?? 30,
        dataLimitMb: parseMaybeNumber(panelConfig.data_limit_mb) ?? (Number(product.size_mb || 0) || 1024)
    };
}
async function promptProductPanelWizardStep(chatId, payload) {
    const step = String(payload.step || "panel");
    const productId = Number(payload.productId || 0);
    const productName = String(payload.productName || "-");
    if (step === "panel") {
        const panels = await sql `
      SELECT id, name, active, allow_new_sales
      FROM panels
      ORDER BY active DESC, allow_new_sales DESC, priority DESC, id ASC;
    `;
        if (!panels.length) {
            await tg("sendMessage", {
                chat_id: chatId,
                text: "هیچ پنلی ثبت نشده است. اول از بخش پنل‌ها یک پنل اضافه کنید.",
                reply_markup: { inline_keyboard: [[backButton("admin_products")]] }
            });
            return;
        }
        const keyboard = panels.map((panel) => [
            cb(`${panel.name}${panel.active && panel.allow_new_sales ? "" : " ⛔"}`, `admin_product_panel_pick_${panel.id}`, "primary")
        ]);
        keyboard.push([cancelButton(`admin_product_panel_wizard_cancel_${productId}`)]);
        await tg("sendMessage", {
            chat_id: chatId,
            text: `تنظیم فروش پنل برای «${productName}»\nمرحله 1 از 2: پنل مقصد را انتخاب کنید:`,
            reply_markup: { inline_keyboard: keyboard }
        });
        return;
    }
    if (step === "mode") {
        await tg("sendMessage", {
            chat_id: chatId,
            text: `مرحله 2 از 2: نوع تنظیم را انتخاب کنید\n` +
                `سریع: بقیه موارد خودکار تنظیم می‌شود.\n` +
                `مرحله‌ای: مقادیر دلخواه را می‌پرسد.`,
            reply_markup: {
                inline_keyboard: [
                    [cb("⚡ تنظیم سریع (پیشنهادی)", "admin_product_panel_quick", "success")],
                    [cb("⚙️ تنظیم مرحله‌ای", "admin_product_panel_custom", "primary")],
                    [cancelButton(`admin_product_panel_wizard_cancel_${productId}`)]
                ]
            }
        });
        return;
    }
    if (step === "sell_limit") {
        await tg("sendMessage", {
            chat_id: chatId,
            text: `تنظیم مرحله‌ای - 1 از 5\n` +
                `سقف فروش پنل را بفرستید.\n` +
                `0 = بدون سقف\n` +
                `- = نگه داشتن مقدار فعلی\n` +
                `مقدار فعلی: ${payload.panelSellLimit === null || payload.panelSellLimit === undefined ? "بدون سقف" : payload.panelSellLimit}`,
            reply_markup: { inline_keyboard: [[cancelButton(`admin_product_panel_wizard_cancel_${productId}`)]] }
        });
        return;
    }
    if (step === "delivery") {
        await tg("sendMessage", {
            chat_id: chatId,
            text: "تنظیم مرحله‌ای - 2 از 5\nحالت تحویل را انتخاب کنید:",
            reply_markup: {
                inline_keyboard: [
                    [
                        cb("ساب + کانفیگ", "admin_product_panel_delivery_both", "primary"),
                        cb("فقط ساب", "admin_product_panel_delivery_sub", "primary"),
                        cb("فقط کانفیگ", "admin_product_panel_delivery_configs", "primary")
                    ],
                    [cancelButton(`admin_product_panel_wizard_cancel_${productId}`)]
                ]
            }
        });
        return;
    }
    if (step === "inbound_id") {
        await tg("sendMessage", {
            chat_id: chatId,
            text: `تنظیم مرحله‌ای - 3 از 5\ninbound_id را بفرستید.\n- = مقدار فعلی (${payload.inboundId || 1})`,
            reply_markup: { inline_keyboard: [[cancelButton(`admin_product_panel_wizard_cancel_${productId}`)]] }
        });
        return;
    }
    if (step === "protocol") {
        await tg("sendMessage", {
            chat_id: chatId,
            text: `تنظیم مرحله‌ای - 4 از 5\nپروتکل را انتخاب کنید یا دستی بفرستید.\nمقدار فعلی: ${String(payload.protocol || "vless")}`,
            reply_markup: {
                inline_keyboard: [
                    [
                        cb("vless", "admin_product_panel_protocol_vless", "primary"),
                        cb("vmess", "admin_product_panel_protocol_vmess", "primary"),
                        cb("trojan", "admin_product_panel_protocol_trojan", "primary")
                    ],
                    [cancelButton(`admin_product_panel_wizard_cancel_${productId}`)]
                ]
            }
        });
        return;
    }
    if (step === "expire_days") {
        await tg("sendMessage", {
            chat_id: chatId,
            text: `تنظیم مرحله‌ای - 5 از 5\nexpire_days را بفرستید.\n- = مقدار فعلی (${payload.expireDays || 30})`,
            reply_markup: { inline_keyboard: [[cancelButton(`admin_product_panel_wizard_cancel_${productId}`)]] }
        });
        return;
    }
    if (step === "data_limit_mb") {
        await tg("sendMessage", {
            chat_id: chatId,
            text: `آخرین مرحله\ndata_limit_mb را بفرستید.\n- = مقدار فعلی (${payload.dataLimitMb || 1024})`,
            reply_markup: { inline_keyboard: [[cancelButton(`admin_product_panel_wizard_cancel_${productId}`)]] }
        });
    }
}
async function saveProductPanelWizard(payload, quickMode) {
    const productId = Number(payload.productId || 0);
    const panelId = Number(payload.panelId || 0);
    if (!Number.isFinite(productId) || productId <= 0) {
        return { ok: false, message: "محصول نامعتبر است." };
    }
    if (!Number.isFinite(panelId) || panelId <= 0) {
        return { ok: false, message: "لطفاً یک پنل معتبر انتخاب کنید." };
    }
    const product = await getProductForPanelWizard(productId);
    if (!product) {
        return { ok: false, message: "محصول پیدا نشد." };
    }
    const panelRows = await sql `SELECT name FROM panels WHERE id = ${panelId} LIMIT 1;`;
    if (!panelRows.length) {
        return { ok: false, message: "پنل انتخاب‌شده پیدا نشد." };
    }
    const currentConfig = sanitizePanelConfig(product.panel_config);
    const inboundId = parseMaybeNumber(payload.inboundId) ?? parseMaybeNumber(currentConfig.inbound_id) ?? 1;
    const protocol = String(payload.protocol || currentConfig.protocol || "vless").trim() || "vless";
    const expireDays = parseMaybeNumber(payload.expireDays) ?? parseMaybeNumber(currentConfig.expire_days) ?? 30;
    const dataLimitMb = parseMaybeNumber(payload.dataLimitMb) ?? parseMaybeNumber(currentConfig.data_limit_mb) ?? (Number(product.size_mb || 0) || 1024);
    const panelSellLimit = quickMode || payload.panelSellLimit === null || payload.panelSellLimit === undefined
        ? null
        : Number(payload.panelSellLimit);
    const panelDeliveryMode = quickMode ? "both" : parseDeliveryMode(String(payload.panelDeliveryMode || "both"));
    const mergedConfig = sanitizePanelConfig(mergeDeep(currentConfig, {
        inbound_id: inboundId,
        protocol,
        expire_days: expireDays,
        data_limit_mb: dataLimitMb
    }));
    await sql `
    UPDATE products
    SET
      sell_mode = 'panel',
      is_infinite = TRUE,
      panel_id = ${panelId},
      panel_sell_limit = ${panelSellLimit},
      panel_delivery_mode = ${panelDeliveryMode},
      panel_config = ${JSON.stringify(mergedConfig)}::jsonb
    WHERE id = ${productId};
  `;
    return {
        ok: true,
        message: `تنظیم فروش پنل ذخیره شد ✅\n` +
            `محصول: ${product.name}\n` +
            `پنل: ${panelRows[0].name}\n` +
            `حالت تحویل: ${formatDeliveryModeLabel(panelDeliveryMode)}\n` +
            `سقف فروش: ${panelSellLimit === null ? "بدون سقف" : panelSellLimit}\n` +
            `protocol: ${protocol} | inbound_id: ${inboundId} | expire_days: ${expireDays} | data_limit_mb: ${dataLimitMb}`
    };
}
async function startProductWizard(chatId, userId, mode, productId) {
    let current = {};
    if (mode === "edit") {
        const id = Number(productId || 0);
        const rows = await sql `
      SELECT id, name, size_mb, price_toman, is_infinite, sell_mode, panel_id, panel_sell_limit, panel_delivery_mode, panel_config
      FROM products
      WHERE id = ${id}
      LIMIT 1;
    `;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "محصول پیدا نشد." });
            return;
        }
        current = rows[0];
    }
    const panelConfig = sanitizePanelConfig(current.panel_config);
    const productKind = parseProductKind(panelConfig.product_kind);
    const currentSizeMb = Number(current.size_mb);
    const payload = {
        mode,
        step: "name",
        productId: mode === "edit" ? Number(current.id || productId || 0) : null,
        name: String(current.name || ""),
        productKind,
        sizeMb: Number.isFinite(currentSizeMb) ? currentSizeMb : 1024,
        priceMode: "auto",
        priceToman: Number(current.price_toman || 0) || null,
        sellMode: parseSellMode(String(current.sell_mode || "manual")),
        isInfinite: Boolean(current.is_infinite),
        panelId: Number(current.panel_id || 0) || null,
        panelSellLimit: current.panel_sell_limit === null || current.panel_sell_limit === undefined ? null : Number(current.panel_sell_limit),
        panelDeliveryMode: parseDeliveryMode(String(current.panel_delivery_mode || "both")),
        inboundId: parseMaybeNumber(panelConfig.inbound_id) ?? 1,
        protocol: String(panelConfig.protocol || "vless"),
        expireDays: parseMaybeNumber(panelConfig.expire_days) ?? 30,
        dataLimitMb: parseMaybeNumber(panelConfig.data_limit_mb) ?? (Number(current.size_mb || 0) || 1024)
    };
    await setState(userId, "admin_product_wizard", payload);
    await promptProductWizardStep(chatId, payload);
}
async function promptProductWizardStep(chatId, payload) {
    const mode = String(payload.mode || "add");
    const step = String(payload.step || "name");
    const productId = Number(payload.productId || 0);
    const productKind = parseProductKind(payload.productKind);
    const keepHint = mode === "edit" ? "\nبرای نگه داشتن مقدار فعلی، - بفرستید." : "";
    if (step === "name") {
        await tg("sendMessage", {
            chat_id: chatId,
            text: `محصول ${mode === "add" ? "جدید" : "ویرایش"} - 1 از 9\n` +
                `نام محصول را بفرستید.` +
                (mode === "edit" ? `\nمقدار فعلی: ${String(payload.name || "-")}` : "") +
                keepHint,
            reply_markup: { inline_keyboard: [[cancelButton(`admin_product_wizard_cancel_${productId || 0}`)]] }
        });
        return;
    }
    if (step === "product_kind") {
        await tg("sendMessage", {
            chat_id: chatId,
            text: `محصول ${mode === "add" ? "جدید" : "ویرایش"} - 2 از 9\n` +
                `نوع محصول را انتخاب کنید:` +
                (mode === "edit" ? `\nمقدار فعلی: ${productKind === "account" ? "اکانت" : "کانفیگ V2Ray"}` : ""),
            reply_markup: {
                inline_keyboard: [
                    [cb("📶 کانفیگ V2Ray", "admin_product_wizard_kind_v2ray", "primary")],
                    [cb("👤 اکانت (VPN/وبسایت)", "admin_product_wizard_kind_account", "primary")],
                    [cancelButton(`admin_product_wizard_cancel_${productId || 0}`)]
                ]
            }
        });
        return;
    }
    if (step === "size_mb") {
        await tg("sendMessage", {
            chat_id: chatId,
            text: `محصول ${mode === "add" ? "جدید" : "ویرایش"} - 3 از 9\n` +
                `حجم را بفرستید (MB یا GB). نمونه: 2048 یا 2GB` +
                (mode === "edit" ? `\nمقدار فعلی: ${String(payload.sizeMb || "-")}` : "") +
                keepHint,
            reply_markup: { inline_keyboard: [[cancelButton(`admin_product_wizard_cancel_${productId || 0}`)]] }
        });
        return;
    }
    if (step === "price_mode") {
        if (productKind === "account") {
            await tg("sendMessage", {
                chat_id: chatId,
                text: `محصول ${mode === "add" ? "جدید" : "ویرایش"} - 4 از 9\nبرای محصول اکانتی، قیمت باید دستی ثبت شود.`,
                reply_markup: {
                    inline_keyboard: [
                        [cb("✍️ ثبت قیمت دستی", "admin_product_wizard_price_manual", "primary")],
                        [cancelButton(`admin_product_wizard_cancel_${productId || 0}`)]
                    ]
                }
            });
            return;
        }
        await tg("sendMessage", {
            chat_id: chatId,
            text: `محصول ${mode === "add" ? "جدید" : "ویرایش"} - 4 از 9\nروش قیمت‌گذاری را انتخاب کنید:`,
            reply_markup: {
                inline_keyboard: [
                    [cb("🤖 خودکار", "admin_product_wizard_price_auto", "primary")],
                    [cb("✍️ دستی", "admin_product_wizard_price_manual", "primary")],
                    [cancelButton(`admin_product_wizard_cancel_${productId || 0}`)]
                ]
            }
        });
        return;
    }
    if (step === "price_toman") {
        await tg("sendMessage", {
            chat_id: chatId,
            text: `محصول ${mode === "add" ? "جدید" : "ویرایش"} - 5 از 9\n` +
                `قیمت را به تومان بفرستید.` +
                (mode === "edit" ? `\nمقدار فعلی: ${String(payload.priceToman || "-")}` : "") +
                keepHint,
            reply_markup: { inline_keyboard: [[cancelButton(`admin_product_wizard_cancel_${productId || 0}`)]] }
        });
        return;
    }
    if (step === "sell_mode") {
        await tg("sendMessage", {
            chat_id: chatId,
            text: `محصول ${mode === "add" ? "جدید" : "ویرایش"} - 6 از 9\nحالت فروش را انتخاب کنید:`,
            reply_markup: {
                inline_keyboard: [
                    [cb("فروش دستی", "admin_product_wizard_sell_manual", "primary")],
                    [cb("فروش از پنل", "admin_product_wizard_sell_panel", "primary")],
                    [cancelButton(`admin_product_wizard_cancel_${productId || 0}`)]
                ]
            }
        });
        return;
    }
    if (step === "is_infinite") {
        await tg("sendMessage", {
            chat_id: chatId,
            text: `محصول ${mode === "add" ? "جدید" : "ویرایش"} - 7 از 9\nمحصول بی‌نهایت باشد؟`,
            reply_markup: {
                inline_keyboard: [
                    [
                        confirmButton("admin_product_wizard_infinite_yes", "✅ بله"),
                        cb("❌ خیر", "admin_product_wizard_infinite_no", "danger")
                    ],
                    [cancelButton(`admin_product_wizard_cancel_${productId || 0}`)]
                ]
            }
        });
        return;
    }
    if (step === "panel_id") {
        const panels = await sql `
      SELECT id, name, active, allow_new_sales
      FROM panels
      ORDER BY active DESC, allow_new_sales DESC, priority DESC, id ASC;
    `;
        if (!panels.length) {
            await tg("sendMessage", { chat_id: chatId, text: "هیچ پنلی ثبت نشده است. اول یک پنل اضافه کنید." });
            return;
        }
        const keyboard = panels.map((panel) => [
            cb(`${panel.name}${panel.active && panel.allow_new_sales ? "" : " ⛔"}`, `admin_product_wizard_panel_${panel.id}`, "primary")
        ]);
        keyboard.push([cancelButton(`admin_product_wizard_cancel_${productId || 0}`)]);
        await tg("sendMessage", {
            chat_id: chatId,
            text: `محصول ${mode === "add" ? "جدید" : "ویرایش"} - 7 از 9\nپنل مقصد را انتخاب کنید:`,
            reply_markup: { inline_keyboard: keyboard }
        });
        return;
    }
    if (step === "panel_sell_limit") {
        await tg("sendMessage", {
            chat_id: chatId,
            text: `محصول ${mode === "add" ? "جدید" : "ویرایش"} - 8 از 9\n` +
                `سقف فروش پنل را بفرستید.\n0 = بدون سقف` +
                (mode === "edit" ? `\nمقدار فعلی: ${payload.panelSellLimit ?? "بدون سقف"}` : "") +
                keepHint,
            reply_markup: { inline_keyboard: [[cancelButton(`admin_product_wizard_cancel_${productId || 0}`)]] }
        });
        return;
    }
    if (step === "panel_delivery_mode") {
        await tg("sendMessage", {
            chat_id: chatId,
            text: `محصول ${mode === "add" ? "جدید" : "ویرایش"} - 9 از 9\n` +
                `حالت تحویل را انتخاب کنید.\n` +
                `بعد از این مرحله، باقی تنظیمات پنل به‌صورت خودکار مثل حالت سریع ثبت می‌شود.`,
            reply_markup: {
                inline_keyboard: [
                    [
                        cb("ساب + کانفیگ", "admin_product_wizard_delivery_both", "primary"),
                        cb("فقط ساب", "admin_product_wizard_delivery_sub", "primary"),
                        cb("فقط کانفیگ", "admin_product_wizard_delivery_configs", "primary")
                    ],
                    [cancelButton(`admin_product_wizard_cancel_${productId || 0}`)]
                ]
            }
        });
        return;
    }
    if (step === "inbound_id" || step === "protocol" || step === "expire_days" || step === "data_limit_mb") {
        await tg("sendMessage", {
            chat_id: chatId,
            text: "این مرحله دیگر لازم نیست. تنظیمات پنل به‌صورت خودکار اعمال می‌شود."
        });
    }
}
async function saveProductWizard(payload) {
    const mode = String(payload.mode || "add");
    const productKind = parseProductKind(payload.productKind);
    const sellMode = parseSellMode(String(payload.sellMode || "manual"));
    const sizeMb = Number(payload.sizeMb);
    if (productKind === "v2ray" && (!Number.isFinite(sizeMb) || sizeMb <= 0))
        return { ok: false, message: "حجم محصول معتبر نیست." };
    if (productKind === "account" && sellMode === "panel")
        return { ok: false, message: "محصول اکانتی فقط با فروش دستی قابل استفاده است." };
    const name = String(payload.name || "").trim();
    if (!name)
        return { ok: false, message: "نام محصول نمی‌تواند خالی باشد." };
    const useAutoPrice = String(payload.priceMode || "auto") === "auto";
    const manualPrice = Number(payload.priceToman || 0);
    const price = useAutoPrice && productKind !== "account" ? await getProductPriceFromSizeMb(sizeMb) : manualPrice;
    if (!Number.isFinite(price) || price <= 0)
        return { ok: false, message: "قیمت محصول معتبر نیست." };
    const panelId = sellMode === "panel" ? Number(payload.panelId || 0) : null;
    if (sellMode === "panel" && (!Number.isFinite(panelId) || Number(panelId) <= 0)) {
        return { ok: false, message: "برای فروش پنل باید یک پنل انتخاب شود." };
    }
    const panelSellLimitRaw = payload.panelSellLimit;
    const panelSellLimit = sellMode !== "panel" || panelSellLimitRaw === null || panelSellLimitRaw === undefined || Number(panelSellLimitRaw) <= 0
        ? null
        : Math.round(Number(panelSellLimitRaw));
    const panelDeliveryMode = sellMode === "panel" ? parseDeliveryMode(String(payload.panelDeliveryMode || "both")) : "both";
    let currentConfig = {};
    if (mode === "edit" && Number(payload.productId || 0) > 0) {
        const rows = await sql `SELECT panel_config FROM products WHERE id = ${Number(payload.productId)} LIMIT 1;`;
        currentConfig = rows.length ? sanitizePanelConfig(rows[0].panel_config) : {};
    }
    const panelConfig = sellMode === "panel"
        ? sanitizePanelConfig(mergeDeep(currentConfig, {
            product_kind: productKind,
            inbound_id: parseMaybeNumber(payload.inboundId) ?? 1,
            protocol: String(payload.protocol || "vless").trim() || "vless",
            expire_days: parseMaybeNumber(payload.expireDays) ?? 30,
            data_limit_mb: sizeMb
        }))
        : sanitizePanelConfig(mergeDeep(currentConfig, { product_kind: productKind }));
    if (mode === "add") {
        await sql `
      INSERT INTO products (name, size_mb, price_toman, is_infinite, sell_mode, panel_id, panel_sell_limit, panel_delivery_mode, panel_config)
      VALUES (
        ${name},
        ${productKind === "account" ? 0 : sizeMb},
        ${price},
        ${sellMode === "panel" ? true : Boolean(payload.isInfinite)},
        ${sellMode},
        ${panelId},
        ${panelSellLimit},
        ${panelDeliveryMode},
        ${JSON.stringify(panelConfig)}::jsonb
      )
      ON CONFLICT (name) DO UPDATE SET
        size_mb = EXCLUDED.size_mb,
        price_toman = EXCLUDED.price_toman,
        is_active = TRUE,
        is_infinite = EXCLUDED.is_infinite,
        sell_mode = EXCLUDED.sell_mode,
        panel_id = EXCLUDED.panel_id,
        panel_sell_limit = EXCLUDED.panel_sell_limit,
        panel_delivery_mode = EXCLUDED.panel_delivery_mode,
        panel_config = EXCLUDED.panel_config;
    `;
        return {
            ok: true,
            message: `محصول ذخیره شد ✅\n` +
                `قیمت: ${formatPriceToman(price)} تومان (${useAutoPrice ? "خودکار" : "دلخواه"})\n` +
                `حالت فروش: ${sellMode === "panel" ? "از پنل" : "دستی"}\n` +
                `تحویل: ${panelDeliveryMode}`
        };
    }
    const id = Number(payload.productId || 0);
    if (!Number.isFinite(id) || id <= 0)
        return { ok: false, message: "شناسه محصول معتبر نیست." };
    await sql `
    UPDATE products
    SET
      name = ${name},
      size_mb = ${productKind === "account" ? 0 : sizeMb},
      price_toman = ${price},
      is_infinite = ${sellMode === "panel" ? true : Boolean(payload.isInfinite)},
      sell_mode = ${sellMode},
      panel_id = ${panelId},
      panel_sell_limit = ${panelSellLimit},
      panel_delivery_mode = ${panelDeliveryMode},
      panel_config = ${JSON.stringify(panelConfig)}::jsonb
    WHERE id = ${id};
  `;
    return {
        ok: true,
        message: `محصول ویرایش شد ✅\n` +
            `قیمت: ${formatPriceToman(price)} تومان (${useAutoPrice ? "خودکار" : "دلخواه"})\n` +
            `حالت فروش: ${sellMode === "panel" ? "از پنل" : "دستی"}\n` +
            `تحویل: ${panelDeliveryMode}`
    };
}
async function startCardWizard(chatId, userId, mode, cardId) {
    let current = {};
    if (mode === "edit") {
        const rows = await sql `SELECT id, label, card_number, holder_name, bank_name FROM cards WHERE id = ${Number(cardId || 0)} LIMIT 1;`;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "کارت پیدا نشد." });
            return;
        }
        current = rows[0];
    }
    const payload = {
        mode,
        step: "label",
        cardId: mode === "edit" ? Number(current.id || cardId || 0) : null,
        label: String(current.label || ""),
        cardNumber: String(current.card_number || ""),
        holderName: String(current.holder_name || ""),
        bankName: String(current.bank_name || "")
    };
    await setState(userId, "admin_card_wizard", payload);
    await promptCardWizardStep(chatId, payload);
}
async function promptCardWizardStep(chatId, payload) {
    const mode = String(payload.mode || "add");
    const step = String(payload.step || "label");
    const cardId = Number(payload.cardId || 0);
    const keepHint = mode === "edit" ? "\nبرای نگه داشتن مقدار فعلی، - بفرستید." : "";
    const cancel = { inline_keyboard: [[cancelButton(`admin_card_wizard_cancel_${cardId || 0}`)]] };
    if (step === "label") {
        await tg("sendMessage", {
            chat_id: chatId,
            text: `کارت ${mode === "add" ? "جدید" : "ویرایش"} - 1 از 4\nعنوان کارت را بفرستید.` + (mode === "edit" ? `\nفعلی: ${payload.label || "-"}` : "") + keepHint,
            reply_markup: cancel
        });
        return;
    }
    if (step === "card_number") {
        await tg("sendMessage", {
            chat_id: chatId,
            text: `کارت ${mode === "add" ? "جدید" : "ویرایش"} - 2 از 4\nشماره کارت را بفرستید.` + (mode === "edit" ? `\nفعلی: ${payload.cardNumber || "-"}` : "") + keepHint,
            reply_markup: cancel
        });
        return;
    }
    if (step === "holder_name") {
        await tg("sendMessage", {
            chat_id: chatId,
            text: `کارت ${mode === "add" ? "جدید" : "ویرایش"} - 3 از 4\nنام صاحب کارت را بفرستید.\nبرای خالی: -` + (mode === "edit" ? `\nفعلی: ${payload.holderName || "-"}` : ""),
            reply_markup: cancel
        });
        return;
    }
    await tg("sendMessage", {
        chat_id: chatId,
        text: `کارت ${mode === "add" ? "جدید" : "ویرایش"} - 4 از 4\nنام بانک را بفرستید.\nبرای خالی: -` + (mode === "edit" ? `\nفعلی: ${payload.bankName || "-"}` : ""),
        reply_markup: cancel
    });
}
async function startDiscountWizard(chatId, userId, mode, discountId) {
    let current = {};
    if (mode === "edit") {
        const rows = await sql `SELECT id, code, type, amount, usage_limit FROM discounts WHERE id = ${Number(discountId || 0)} LIMIT 1;`;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "تخفیف پیدا نشد." });
            return;
        }
        current = rows[0];
    }
    const payload = {
        mode,
        step: (mode === "add" ? "code_mode" : "type"),
        discountId: mode === "edit" ? Number(current.id || discountId || 0) : null,
        code: mode === "edit" ? String(current.code || "") : "",
        type: String(current.type || "percent"),
        amount: Number(current.amount || 0) || null,
        usageLimit: current.usage_limit === null || current.usage_limit === undefined ? null : Number(current.usage_limit)
    };
    await setState(userId, "admin_discount_wizard", payload);
    await promptDiscountWizardStep(chatId, payload);
}
async function promptDiscountWizardStep(chatId, payload) {
    const mode = String(payload.mode || "add");
    const step = String(payload.step || "code_mode");
    const discountId = Number(payload.discountId || 0);
    const keepHint = mode === "edit" ? "\nبرای نگه داشتن مقدار فعلی، - بفرستید." : "";
    if (step === "code_mode") {
        await tg("sendMessage", {
            chat_id: chatId,
            text: "تخفیف جدید - مرحله 1 از 4\nروش کد را انتخاب کنید:",
            reply_markup: {
                inline_keyboard: [
                    [cb("🎲 کد تصادفی", "admin_discount_wizard_code_random", "primary")],
                    [cb("✍️ کد دستی", "admin_discount_wizard_code_manual", "primary")],
                    [cancelButton(`admin_discount_wizard_cancel_${discountId || 0}`)]
                ]
            }
        });
        return;
    }
    if (step === "code") {
        await tg("sendMessage", {
            chat_id: chatId,
            text: "تخفیف جدید - مرحله 1 از 4\nکد تخفیف را بفرستید. مثلا: NOW10",
            reply_markup: { inline_keyboard: [[cancelButton(`admin_discount_wizard_cancel_${discountId || 0}`)]] }
        });
        return;
    }
    if (step === "type") {
        await tg("sendMessage", {
            chat_id: chatId,
            text: `مرحله ${mode === "add" ? "2" : "1"} از ${mode === "add" ? "4" : "3"}\nنوع تخفیف را انتخاب کنید:`,
            reply_markup: {
                inline_keyboard: [
                    [
                        cb("percent", "admin_discount_wizard_type_percent", "primary"),
                        cb("fixed", "admin_discount_wizard_type_fixed", "primary")
                    ],
                    [cancelButton(`admin_discount_wizard_cancel_${discountId || 0}`)]
                ]
            }
        });
        return;
    }
    if (step === "amount") {
        await tg("sendMessage", {
            chat_id: chatId,
            text: `مرحله ${mode === "add" ? "3" : "2"} از ${mode === "add" ? "4" : "3"}\n` +
                `مقدار تخفیف را بفرستید.` +
                (mode === "edit" ? `\nفعلی: ${payload.amount || "-"}` : "") +
                keepHint,
            reply_markup: { inline_keyboard: [[cancelButton(`admin_discount_wizard_cancel_${discountId || 0}`)]] }
        });
        return;
    }
    await tg("sendMessage", {
        chat_id: chatId,
        text: `مرحله ${mode === "add" ? "4" : "3"} از ${mode === "add" ? "4" : "3"}\n` +
            `سقف استفاده را بفرستید. 0 = بدون سقف` +
            (mode === "edit" ? `\nفعلی: ${payload.usageLimit ?? "بدون سقف"}` : "") +
            keepHint,
        reply_markup: { inline_keyboard: [[cancelButton(`admin_discount_wizard_cancel_${discountId || 0}`)]] }
    });
}
async function startMessageUserWizard(chatId, userId) {
    const payload = { step: "target", targetRaw: "", messageText: "" };
    await setState(userId, "admin_message_user_wizard", payload);
    await tg("sendMessage", {
        chat_id: chatId,
        text: "ارسال پیام - مرحله 1 از 2\nآیدی عددی یا یوزرنیم (با یا بدون @) را بفرستید.",
        reply_markup: { inline_keyboard: [[cancelButton("admin_message_user_wizard_cancel")]] }
    });
}
async function startDirectMigrateWizard(chatId, userId) {
    const payload = {
        step: "source_inventory_id",
        sourceInventoryId: null,
        targetPanelId: null,
        requestedFor: null,
        config: ""
    };
    await setState(userId, "admin_direct_migrate_wizard", payload);
    await tg("sendMessage", {
        chat_id: chatId,
        text: "انتقال مستقیم - مرحله 1 از 4\nشناسه inventory مبدا را بفرستید.",
        reply_markup: { inline_keyboard: [[cancelButton("admin_direct_migrate_wizard_cancel")]] }
    });
}
async function startAdminConfigBuilderWizard(chatId, userId) {
    const payload = {
        step: "target_user",
        targetUserId: null,
        targetUsername: "",
        panelId: null,
        name: "",
        dataMb: null,
        isInfinite: false,
        expiryDays: 30
    };
    await setState(userId, "admin_config_builder_wizard", payload);
    await tg("sendMessage", {
        chat_id: chatId,
        text: "ساخت کانفیگ سفارشی - مرحله 1 از 5\nآیدی عددی کاربر یا یوزرنیم (با یا بدون @) را ارسال کنید.",
        reply_markup: { inline_keyboard: [[cancelButton("admin_config_builder_cancel")]] }
    });
}
async function promptAdminConfigBuilderPanel(chatId) {
    const rows = await sql `SELECT id, name, panel_type, active, allow_new_sales FROM panels ORDER BY active DESC, allow_new_sales DESC, priority DESC, id ASC;`;
    if (!rows.length) {
        await tg("sendMessage", { chat_id: chatId, text: "هیچ پنلی ثبت نشده است." });
        return;
    }
    const keyboard = rows.map((row) => [
        cb(`${row.name} (${panelTypeTitle(String(row.panel_type || ""))})${row.active && row.allow_new_sales ? "" : " ⛔"}`, `admin_config_builder_panel_${row.id}`, "primary")
    ]);
    keyboard.push([cancelButton("admin_config_builder_cancel")]);
    await tg("sendMessage", {
        chat_id: chatId,
        text: "ساخت کانفیگ سفارشی - مرحله 2 از 5\nپنل مقصد را انتخاب کنید:",
        reply_markup: { inline_keyboard: keyboard }
    });
}
async function promptDirectMigrateTargetPanel(chatId) {
    const rows = await sql `SELECT id, name, active, allow_new_sales FROM panels ORDER BY active DESC, allow_new_sales DESC, priority DESC, id ASC;`;
    if (!rows.length) {
        await tg("sendMessage", { chat_id: chatId, text: "هیچ پنلی ثبت نشده است." });
        return;
    }
    const keyboard = rows.map((row) => [cb(`${row.name}${row.active && row.allow_new_sales ? "" : " ⛔"}`, `admin_direct_migrate_panel_${row.id}`, "primary")]);
    keyboard.push([cancelButton("admin_direct_migrate_wizard_cancel")]);
    await tg("sendMessage", {
        chat_id: chatId,
        text: "انتقال مستقیم - مرحله 2 از 4\nپنل مقصد را انتخاب کنید:",
        reply_markup: { inline_keyboard: keyboard }
    });
}
async function showPanelDetails(chatId, panelId, notice) {
    const panel = await getPanelById(panelId);
    if (!panel) {
        await tg("sendMessage", { chat_id: chatId, text: "پنل پیدا نشد." });
        return;
    }
    await tg("sendMessage", {
        chat_id: chatId,
        text: `${notice ? `${notice}\n\n` : ""}` +
            `پنل #${panel.id}\n` +
            `نام: ${panel.name}\n` +
            `نوع: ${panelTypeTitle(String(panel.panel_type))}\n` +
            `آدرس: ${panel.base_url}\n` +
            `یوزرنیم: ${panel.username || "-"}\n` +
            `وضعیت: ${panel.active ? "فعال" : "غیرفعال"}\n` +
            `فروش جدید: ${panel.allow_new_sales ? "روشن" : "خاموش"}\n` +
            `مهاجرت کاربر: ${panel.allow_customer_migration ? "روشن" : "خاموش"}\n` +
            `اولویت: ${panel.priority}\n` +
            `آخرین تست: ${panel.last_check_at || "-"}\n` +
            `نتیجه: ${panelResultLabel(panel.last_check_ok)}\n` +
            `پیام: ${panel.last_check_message || "-"}\n` +
            `meta: ${JSON.stringify(panel.cached_meta || {}, null, 2)}`,
        reply_markup: {
            inline_keyboard: [
                [
                    cb("✏️ ویرایش", `admin_panel_edit_${panel.id}`, "primary"),
                    cb("🧪 تست", `admin_panel_test_${panel.id}`, "primary")
                ],
                [
                    cb(panel.active ? "⛔ غیرفعال" : "✅ فعال", `admin_panel_toggle_${panel.id}`, panel.active ? "danger" : "success"),
                    cb(panel.allow_new_sales ? "🛑 بستن فروش" : "🟢 بازکردن فروش", `admin_panel_toggle_sales_${panel.id}`, panel.allow_new_sales ? "danger" : "success")
                ],
                [
                    cb(panel.allow_customer_migration ? "🔒 قفل مهاجرت" : "🔓 آزاد مهاجرت", `admin_panel_toggle_move_${panel.id}`, "primary"),
                    cb("🗑 حذف", `admin_panel_remove_${panel.id}`, "danger")
                ],
                [
                    cb("📋 کش", `admin_panel_cache_${panel.id}`, "primary"),
                    backButton("admin_panels", "🔙 لیست پنل‌ها")
                ]
            ]
        }
    });
}
function mainMenuMarkup(userId) {
    const rows = [
        [cb("🛍 خرید کانفیگ", "buy_menu", "primary")],
        [cb("📦 کانفیگ‌های من", "my_configs", "primary"), cb("👛 کیف پول", "wallet_menu", "success")],
        [cb("🆘 پشتیبانی", "support", "success")]
    ];
    if (isAdmin(userId)) {
        rows.push([cb("🛠 پنل ادمین", "admin_panel", "primary")]);
    }
    return { inline_keyboard: rows };
}
async function upsertUser(user) {
    await sql `
    INSERT INTO users (telegram_id, username, first_name, last_name)
    VALUES (${user.id}, ${user.username || null}, ${user.first_name || null}, ${user.last_name || null})
    ON CONFLICT (telegram_id)
    DO UPDATE SET
      username = EXCLUDED.username,
      first_name = EXCLUDED.first_name,
      last_name = EXCLUDED.last_name,
      last_seen_at = NOW();
  `;
}
async function sendMainMenu(chatId, userId, text) {
    await tg("sendMessage", {
        chat_id: chatId,
        text: text || "سلام 👋\nبه ربات فروش کانفیگ خوش آمدید.\nاز منوی زیر انتخاب کنید:",
        reply_markup: mainMenuMarkup(userId)
    });
}
async function setState(telegramId, state, payload = {}) {
    await sql `
    INSERT INTO user_states (telegram_id, state, payload)
    VALUES (${telegramId}, ${state}, ${JSON.stringify(payload)}::jsonb)
    ON CONFLICT (telegram_id)
    DO UPDATE SET state = EXCLUDED.state, payload = EXCLUDED.payload, updated_at = NOW();
  `;
}
async function clearState(telegramId) {
    await sql `DELETE FROM user_states WHERE telegram_id = ${telegramId};`;
}
async function getState(telegramId) {
    const rows = await sql `SELECT state, payload FROM user_states WHERE telegram_id = ${telegramId} LIMIT 1;`;
    if (!rows.length)
        return null;
    return { state: String(rows[0].state), payload: rows[0].payload || {} };
}
async function isBanned(userId) {
    const rows = await sql `SELECT telegram_id FROM banned_users WHERE telegram_id = ${userId} LIMIT 1;`;
    return rows.length > 0;
}
async function adminHelp(chatId) {
    await tg("sendMessage", {
        chat_id: chatId,
        text: "راهنمای ادمین:\n" +
            "/help - نمایش همین راهنما\n" +
            "/start - منوی اصلی\n" +
            "/admin - ورود سریع به پنل ادمین\n" +
            "/cancel - لغو عملیات در حال انجام\n\n" +
            "مدیریت کامل محصولات، موجودی، تخفیف‌ها، آمار و تنظیمات از پنل ادمین انجام می‌شود."
    });
}
async function sendAdminPanel(chatId) {
    await tg("sendMessage", {
        chat_id: chatId,
        text: "پنل ادمین 👇",
        reply_markup: {
            inline_keyboard: [
                [cb("📦 مدیریت محصولات", "admin_products", "primary")],
                [cb("🗂 مدیریت موجودی", "admin_inventory", "primary")],
                [cb("💳 روش‌های پرداخت", "admin_payment_methods", "primary")],
                [cb("💳 کارت‌ها", "admin_cards", "primary")],
                [cb("🎟 کد تخفیف", "admin_discounts", "primary")],
                [cb("🌐 پنل‌های V2Ray", "admin_panels", "primary")],
                [cb("👥 مدیریت کاربران", "admin_manage_users", "primary")],
                [cb("📊 آمار", "admin_stats", "primary")],
                [cb("🧰 ابزار ادمین", "admin_tools", "primary")],
                [cb("⚙️ تنظیمات", "admin_settings", "primary")],
                [homeButton()]
            ]
        }
    });
}
export function generateAdminToken(telegramId) {
    const SECRET = process.env.TELEGRAM_BOT_TOKEN || "default_secret";
    const expiresAt = Date.now() + 2 * 60 * 60 * 1000; // 2 hours
    const payload = `${telegramId}|${expiresAt}`;
    const hmac = crypto.createHmac("sha256", SECRET).update(payload).digest("hex");
    return Buffer.from(`${payload}|${hmac}`).toString("base64url");
}
export function verifyAdminToken(token) {
    const SECRET = process.env.TELEGRAM_BOT_TOKEN || "default_secret";
    try {
        const decoded = Buffer.from(token, "base64url").toString("utf-8");
        const [tgIdStr, expiresStr, signature] = decoded.split("|");
        const payload = `${tgIdStr}|${expiresStr}`;
        const expectedHmac = crypto.createHmac("sha256", SECRET).update(payload).digest("hex");
        if (signature !== expectedHmac)
            return null;
        if (Date.now() > Number(expiresStr))
            return null;
        return Number(tgIdStr);
    }
    catch {
        return null;
    }
}
async function showPanelAdminMenu(chatId, notice) {
    const rows = await sql `
    SELECT id, name, panel_type, active, allow_customer_migration, allow_new_sales, last_check_ok, last_check_message
    FROM panels
    ORDER BY priority DESC, id ASC;
  `;
    if (!rows.length) {
        await tg("sendMessage", {
            chat_id: chatId,
            text: "هنوز هیچ پنلی ثبت نشده است.",
            reply_markup: {
                inline_keyboard: [
                    [cb("➕ افزودن پنل", "admin_panel_add", "success")],
                    [cb("📥 صف انتقال‌ها", "admin_migrations", "primary")],
                    [backButton("admin_panel")]
                ]
            }
        });
        return;
    }
    const keyboard = rows.flatMap((p) => [
        [
            {
                text: `${p.name} | ${String(p.panel_type).toUpperCase()} | ${p.active ? "فعال" : "غیرفعال"}\n` +
                    `مهاجرت کاربر: ${p.allow_customer_migration ? "روشن" : "خاموش"} | فروش جدید: ${p.allow_new_sales ? "روشن" : "خاموش"}\n` +
                    `آخرین تست: ${panelResultLabel(p.last_check_ok)}${p.last_check_message ? ` | ${String(p.last_check_message).slice(0, 40)}` : ""}`,
                callback_data: `admin_panel_open_${p.id}`
            }
        ],
        [
            cb("ویرایش", `admin_panel_edit_${p.id}`, "primary"),
            cb(p.active ? "غیرفعال" : "فعال", `admin_panel_toggle_${p.id}`, p.active ? "danger" : "success"),
            cb(p.allow_customer_migration ? "قفل مهاجرت" : "آزاد مهاجرت", `admin_panel_toggle_move_${p.id}`, "primary")
        ],
        [
            cb(p.allow_new_sales ? "بستن فروش جدید" : "بازکردن فروش جدید", `admin_panel_toggle_sales_${p.id}`, p.allow_new_sales ? "danger" : "success"),
            cb("تست اتصال", `admin_panel_test_${p.id}`, "primary")
        ],
        [
            cb("وضعیت کش", `admin_panel_cache_${p.id}`, "primary"),
            cb("🗑 حذف", `admin_panel_remove_${p.id}`, "danger")
        ]
    ]);
    keyboard.push([cb("🧪 تست همه پنل‌ها", "admin_panel_test_all", "primary")]);
    keyboard.push([cb("➕ افزودن پنل", "admin_panel_add", "success")]);
    keyboard.push([cb("📥 صف انتقال‌ها", "admin_migrations", "primary")]);
    keyboard.push([backButton("admin_panel")]);
    await tg("sendMessage", {
        chat_id: chatId,
        text: `${notice ? `${notice}\n\n` : ""}مدیریت پنل‌های V2Ray:\nبرای دیدن جزئیات هر پنل، روی ردیف بالایی آن بزنید.`,
        reply_markup: { inline_keyboard: keyboard }
    });
}
export async function loginMarzbanPanel(panel) {
    const body = new URLSearchParams();
    body.set("grant_type", "password");
    body.set("username", String(panel.username || ""));
    body.set("password", String(panel.password || ""));
    const res = await fetchWithTimeout(`${normalizeBaseUrl(String(panel.base_url || ""))}/api/admin/token`, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body
    });
    const raw = await res.text();
    const data = parseJsonObject(raw);
    const token = String(data?.access_token || "");
    return { res, raw, token };
}
export async function loginSanaeiPanel(panel) {
    const body = new URLSearchParams();
    body.set("username", String(panel.username || ""));
    body.set("password", String(panel.password || ""));
    const res = await fetchWithTimeout(`${normalizeBaseUrl(String(panel.base_url || ""))}/login`, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded", Accept: "application/json" },
        body
    });
    const raw = await res.text();
    const data = parseJsonObject(raw);
    const cookie = extractSessionCookie(res.headers.get("set-cookie"));
    return { res, raw, data, cookie };
}
export async function getSanaeiInbounds(baseUrl, cookie) {
    const res = await fetchWithTimeout(`${normalizeBaseUrl(baseUrl)}/panel/api/inbounds/list`, {
        method: "GET",
        headers: { Accept: "application/json", Cookie: cookie }
    });
    const raw = await res.text();
    const data = parseJsonObject(raw);
    const items = Array.isArray(data?.obj) ? data?.obj : [];
    return { res, raw, data, items };
}
async function findSanaeiClientByIdentifier(panel, identifier) {
    const login = await loginSanaeiPanel(panel);
    if (!login.res.ok || !jsonSuccess(login.data) || !login.cookie) {
        return { ok: false, message: `Sanaei auth failed: ${login.res.status}` };
    }
    const inbounds = await getSanaeiInbounds(String(panel.base_url), login.cookie);
    if (!inbounds.res.ok || !jsonSuccess(inbounds.data)) {
        return { ok: false, message: `Sanaei list inbounds failed: ${inbounds.res.status}` };
    }
    const candidateSet = new Set(collectLookupCandidates(identifier).map((item) => item.toLowerCase()));
    for (const inbound of inbounds.items) {
        const settings = toJsonObject(parseSanaeiNested(inbound.settings)) || {};
        const clients = Array.isArray(settings.clients) ? settings.clients : [];
        for (const client of clients) {
            const id = String(client.id || "");
            const email = String(client.email || "");
            const subId = String(client.subId || "");
            const asText = JSON.stringify(client).toLowerCase();
            const matched = Array.from(candidateSet).some((candidate) => {
                if (id.toLowerCase() === candidate)
                    return true;
                if (email.toLowerCase() === candidate)
                    return true;
                if (subId.toLowerCase() === candidate)
                    return true;
                if (candidate.length >= 6 && asText.includes(candidate))
                    return true;
                return false;
            });
            if (!matched)
                continue;
            return {
                ok: true,
                loginCookie: login.cookie,
                inboundId: Number(inbound.id || 0),
                inbound,
                client,
                clientKey: id || email || subId,
                message: "ok"
            };
        }
    }
    return { ok: false, message: "client_not_found" };
}
export async function revokeSanaeiClient(panel, identifier) {
    const found = await findSanaeiClientByIdentifier(panel, identifier);
    if (!found.ok || !found.loginCookie || !found.inboundId || !found.clientKey)
        return found;
    const delRes = await fetchWithTimeout(`${normalizeBaseUrl(String(panel.base_url))}/panel/api/inbounds/delClient/${encodeURIComponent(String(found.clientKey))}`, {
        method: "POST",
        headers: {
            Accept: "application/json",
            Cookie: found.loginCookie,
            "Content-Type": "application/json"
        },
        body: JSON.stringify({ id: found.inboundId })
    });
    const delRaw = await delRes.text();
    const delData = parseJsonObject(delRaw);
    const ok = delRes.ok && (!delRaw.trim() || jsonSuccess(delData));
    if (!ok) {
        return { ok: false, message: `Sanaei revoke failed: ${delRes.status} ${responseSnippet(delRaw)}` };
    }
    return { ok: true, message: "revoked", client: found.client, inboundId: found.inboundId };
}
async function lookupMarzbanUser(panel, identifier) {
    const login = await loginMarzbanPanel(panel);
    if (!login.res.ok || !login.token) {
        return { ok: false, message: `Marzban auth failed: ${login.res.status}` };
    }
    const candidates = collectLookupCandidates(identifier).map((item) => item.toLowerCase());
    const base = normalizeBaseUrl(String(panel.base_url));
    for (const candidate of candidates) {
        const directRes = await fetchWithTimeout(`${base}/api/user/${encodeURIComponent(candidate)}`, {
            method: "GET",
            headers: { Authorization: `Bearer ${login.token}`, Accept: "application/json" }
        });
        const directRaw = await directRes.text();
        const directData = parseJsonObject(directRaw);
        if (directRes.ok && directData) {
            return { ok: true, message: "ok", token: login.token, user: directData };
        }
    }
    const limit = 200;
    for (let page = 0; page < 12; page += 1) {
        const offset = page * limit;
        const listRes = await fetchWithTimeout(`${base}/api/users?offset=${offset}&limit=${limit}`, {
            method: "GET",
            headers: { Authorization: `Bearer ${login.token}`, Accept: "application/json" }
        });
        const listRaw = await listRes.text();
        const listData = parseJsonObject(listRaw);
        if (!listRes.ok || !listData)
            break;
        const users = Array.isArray(listData.users)
            ? listData.users
            : Array.isArray(listData.items)
                ? listData.items
                : [];
        if (!users.length)
            break;
        for (const user of users) {
            const username = String(user.username || "").toLowerCase();
            const note = String(user.note || "").toLowerCase();
            const userJson = JSON.stringify(user).toLowerCase();
            const matched = candidates.some((candidate) => {
                if (username === candidate)
                    return true;
                if (note === candidate)
                    return true;
                if (candidate.length >= 6 && (username.includes(candidate) || note.includes(candidate) || userJson.includes(candidate)))
                    return true;
                return false;
            });
            if (matched)
                return { ok: true, message: "ok", token: login.token, user };
        }
        if (users.length < limit)
            break;
    }
    return { ok: false, message: "user_not_found" };
}
async function toggleMarzbanUser(panel, identifier, enable) {
    const found = await lookupMarzbanUser(panel, identifier);
    if (!found.ok || !found.token || !found.user)
        return found;
    const username = String(found.user.username || identifier).trim();
    const putRes = await fetchWithTimeout(`${normalizeBaseUrl(String(panel.base_url))}/api/user/${encodeURIComponent(username)}`, {
        method: "PUT",
        headers: {
            Authorization: `Bearer ${found.token}`,
            "Content-Type": "application/json",
            Accept: "application/json"
        },
        body: JSON.stringify({ ...found.user, status: enable ? "active" : "disabled" })
    });
    const putRaw = await putRes.text();
    if (!putRes.ok)
        return { ok: false, message: `Marzban toggle failed: ${putRes.status} ${responseSnippet(putRaw)}` };
    return { ok: true, message: enable ? "enabled" : "disabled", user: found.user };
}
async function toggleSanaeiClient(panel, identifier, enable) {
    const found = await findSanaeiClientByIdentifier(panel, identifier);
    if (!found.ok || !found.loginCookie || !found.inboundId || !found.clientKey)
        return found;
    const updateRes = await fetchWithTimeout(`${normalizeBaseUrl(String(panel.base_url))}/panel/api/inbounds/updateClient/${encodeURIComponent(String(found.clientKey))}`, {
        method: "POST",
        headers: {
            Accept: "application/json",
            Cookie: found.loginCookie,
            "Content-Type": "application/json"
        },
        body: JSON.stringify({
            id: found.inboundId,
            settings: JSON.stringify({ clients: [{ ...found.client, enable }] })
        })
    });
    const updateRaw = await updateRes.text();
    const updateData = parseJsonObject(updateRaw);
    const ok = updateRes.ok && (!updateRaw.trim() || jsonSuccess(updateData));
    if (!ok) {
        return { ok: false, message: `Sanaei toggle failed: ${updateRes.status} ${responseSnippet(updateRaw)}` };
    }
    return { ok: true, message: enable ? "enabled" : "disabled", client: found.client, inboundId: found.inboundId };
}
export async function regenerateMarzbanUserLink(panel, identifier) {
    const found = await lookupMarzbanUser(panel, identifier);
    if (!found.ok || !found.token || !found.user)
        return found;
    const username = String(found.user.username || identifier).trim();
    const postRes = await fetchWithTimeout(`${normalizeBaseUrl(String(panel.base_url))}/api/user/${encodeURIComponent(username)}/revoke_sub`, {
        method: "POST",
        headers: {
            Authorization: `Bearer ${found.token}`,
            Accept: "application/json"
        }
    });
    const postRaw = await postRes.text();
    const postData = parseJsonObject(postRaw);
    if (!postRes.ok)
        return { ok: false, message: `Marzban link regen failed: ${postRes.status} ${responseSnippet(postRaw)}` };
    return { ok: true, message: "link_regenerated", user: postData };
}
export async function regenerateSanaeiClientLink(panel, identifier) {
    const found = await findSanaeiClientByIdentifier(panel, identifier);
    if (!found.ok || !found.loginCookie || !found.inboundId || !found.clientKey)
        return found;
    // Create a new UUID
    const newUuid = crypto.randomUUID();
    const updateRes = await fetchWithTimeout(`${normalizeBaseUrl(String(panel.base_url))}/panel/api/inbounds/updateClient/${encodeURIComponent(String(found.clientKey))}`, {
        method: "POST",
        headers: {
            Accept: "application/json",
            Cookie: found.loginCookie,
            "Content-Type": "application/json"
        },
        body: JSON.stringify({
            id: found.inboundId,
            settings: JSON.stringify({ clients: [{ ...found.client, id: newUuid }] })
        })
    });
    const updateRaw = await updateRes.text();
    const updateData = parseJsonObject(updateRaw);
    const ok = updateRes.ok && (!updateRaw.trim() || jsonSuccess(updateData));
    if (!ok) {
        return { ok: false, message: `Sanaei link regen failed: ${updateRes.status} ${responseSnippet(updateRaw)}` };
    }
    return { ok: true, message: "link_regenerated", client: { ...found.client, id: newUuid }, inboundId: found.inboundId, inbound: found.inbound };
}
export async function deleteMarzbanUser(panel, identifier) {
    const found = await lookupMarzbanUser(panel, identifier);
    if (!found.ok || !found.token || !found.user)
        return found;
    const username = String(found.user.username || identifier).trim();
    const delRes = await fetchWithTimeout(`${normalizeBaseUrl(String(panel.base_url))}/api/user/${encodeURIComponent(username)}`, {
        method: "DELETE",
        headers: { Authorization: `Bearer ${found.token}`, Accept: "application/json" }
    });
    const delRaw = await delRes.text();
    if (!delRes.ok)
        return { ok: false, message: `Marzban delete failed: ${delRes.status} ${responseSnippet(delRaw)}` };
    return { ok: true, message: "deleted", user: found.user };
}
async function performRegenLink(inventoryId, actorUserId, isAdminReq, chatId) {
    const rows = await sql `
    SELECT i.id, i.panel_id, i.delivery_payload, i.owner_telegram_id, i.config_value, p.panel_config
    FROM inventory i
    LEFT JOIN products p ON p.id = i.product_id
    WHERE i.id = ${inventoryId}
    LIMIT 1;
  `;
    if (!rows.length) {
        await tg("sendMessage", { chat_id: chatId, text: "کانفیگ پیدا نشد." });
        return;
    }
    const row = rows[0];
    if (!isAdminReq && row.owner_telegram_id !== actorUserId) {
        await tg("sendMessage", { chat_id: chatId, text: "این کانفیگ متعلق به شما نیست." });
        return;
    }
    const delivery = parseDeliveryPayload(row.delivery_payload);
    const panelType = String(delivery.metadata?.panelType || "");
    const panelId = Number(row.panel_id || 0);
    const key = String(delivery.metadata?.username || delivery.metadata?.uuid || delivery.metadata?.email || delivery.metadata?.subId || "").trim();
    if (!panelId || !panelType || !key) {
        await tg("sendMessage", { chat_id: chatId, text: "این کانفیگ پنلی نیست یا شناسه معتبر ندارد." });
        return;
    }
    const panelRows = await sql `
    SELECT id, panel_type, base_url, username, password
    FROM panels
    WHERE id = ${panelId}
    LIMIT 1;
  `;
    if (!panelRows.length) {
        await tg("sendMessage", { chat_id: chatId, text: "پنل مرتبط پیدا نشد." });
        return;
    }
    let regenMessage = "عملیات انجام نشد.";
    let newUuid;
    let newConfigLinks = [];
    let newSubscriptionUrl;
    if (panelType === "marzban") {
        const result = await regenerateMarzbanUserLink(panelRows[0], key);
        if (result.ok && result.user) {
            regenMessage = "تغییر لینک با موفقیت انجام شد ✅";
            const u = result.user;
            newConfigLinks = Array.isArray(u.links) ? u.links.map((x) => String(x || "").trim()).filter(Boolean) : [];
            newSubscriptionUrl = u.subscription_url ? String(u.subscription_url) : undefined;
        }
        else {
            regenMessage = `خطا در تغییر لینک: ${result.message}`;
            await tg("sendMessage", { chat_id: chatId, text: regenMessage });
            return;
        }
    }
    else {
        const result = await regenerateSanaeiClientLink(panelRows[0], key);
        if (result.ok && result.client && result.inbound) {
            regenMessage = "تغییر لینک با موفقیت انجام شد ✅";
            newUuid = String(result.client.id || "");
            const panelConfig = (typeof row.panel_config === "string" ? parseJsonObject(row.panel_config) : row.panel_config) || {};
            newConfigLinks = buildSanaeiConfigLinks(String(panelRows[0].base_url), result.inbound, result.client, panelConfig);
        }
        else {
            regenMessage = `خطا در تغییر لینک: ${result.message}`;
            await tg("sendMessage", { chat_id: chatId, text: regenMessage });
            return;
        }
    }
    await recordInventoryForensicEvent(inventoryId, isAdminReq ? "admin_regen_link" : "customer_regen_link", { actor: actorUserId, panelResult: regenMessage });
    const previousConfigs = Array.isArray(delivery.previousConfigs) ? delivery.previousConfigs : [];
    if (row.config_value && !previousConfigs.includes(String(row.config_value))) {
        previousConfigs.push(String(row.config_value));
    }
    if (delivery.subscriptionUrl && !previousConfigs.includes(delivery.subscriptionUrl)) {
        previousConfigs.push(delivery.subscriptionUrl);
    }
    const updatedDelivery = { ...delivery, previousConfigs };
    if (newConfigLinks.length > 0)
        updatedDelivery.configLinks = newConfigLinks;
    if (newSubscriptionUrl)
        updatedDelivery.subscriptionUrl = newSubscriptionUrl;
    if (newUuid && updatedDelivery.metadata)
        updatedDelivery.metadata.uuid = newUuid;
    const newConfigValue = newSubscriptionUrl || newConfigLinks[0] || String(row.config_value);
    await sql `
    UPDATE inventory
    SET 
      config_value = ${newConfigValue},
      delivery_payload = ${JSON.stringify(updatedDelivery)}::jsonb
    WHERE id = ${inventoryId};
  `;
    let msgText = `لینک شما با موفقیت تغییر کرد ✅\n\nلینک جدید:\n${newConfigValue}`;
    if (newSubscriptionUrl && newConfigLinks.length > 0) {
        msgText = `لینک شما با موفقیت تغییر کرد ✅\n\n🔗 ساب (پیشنهادی):\n${newSubscriptionUrl}\n\nکانفیگ مستقیم:\n${newConfigLinks[0]}`;
    }
    await tg("sendMessage", {
        chat_id: chatId,
        text: msgText
    });
}
function extractPanelLookupIdentifier(raw) {
    const trimmed = raw.trim();
    const uuid = extractUuidFromText(trimmed);
    if (uuid)
        return uuid;
    const candidates = collectLookupCandidates(trimmed);
    return candidates[0] || trimmed;
}
async function lookupIdentifierInPanels(raw) {
    const identifier = raw.trim();
    if (!identifier)
        return { ok: false, message: "empty_identifier" };
    const panels = await sql `
    SELECT id, name, panel_type, base_url, username, password, active
    FROM panels
    WHERE active = TRUE
    ORDER BY priority DESC, id ASC;
  `;
    const results = await Promise.allSettled(panels.map(async (panel) => {
        const panelType = String(panel.panel_type);
        if (panelType === "marzban") {
            const found = await lookupMarzbanUser(panel, identifier);
            if (!found.ok || !found.user)
                return null;
            const ownerTg = parsePanelUserTelegramId(found.user.note);
            return {
                ok: true,
                source: "panel",
                panelId: Number(panel.id),
                panelName: String(panel.name),
                panelBaseUrl: String(panel.base_url || ""),
                panelType: "marzban",
                ownerTelegramId: ownerTg,
                panelUserKey: String(found.user.username || identifier),
                panelUser: found.user
            };
        }
        if (panelType === "sanaei") {
            const found = await findSanaeiClientByIdentifier(panel, identifier);
            if (!found.ok || !found.client)
                return null;
            const client = found.client;
            const ownerTg = parsePanelUserTelegramId(client.tgId || client.email || "");
            const panelUserKey = String(client.id || client.subId || client.email || identifier);
            return {
                ok: true,
                source: "panel",
                panelId: Number(panel.id),
                panelName: String(panel.name),
                panelBaseUrl: String(panel.base_url || ""),
                panelType: "sanaei",
                ownerTelegramId: ownerTg,
                panelUserKey,
                panelUser: client,
                inboundId: found.inboundId || null
            };
        }
        return null;
    }));
    for (const res of results) {
        if (res.status === "fulfilled" && res.value)
            return res.value;
    }
    return { ok: false, message: "not_found_in_panels" };
}
async function buildInventoryPanelRuntimeDetails(inventoryId, panelIdRaw, deliveryPayloadRaw, panelCache) {
    const panelId = Number(panelIdRaw || 0);
    const delivery = parseDeliveryPayload(deliveryPayloadRaw);
    const panelType = String(delivery.metadata?.panelType || "");
    const panelKey = String(delivery.metadata?.username || delivery.metadata?.email || delivery.metadata?.uuid || delivery.metadata?.subId || "").trim();
    if (!panelId || !panelType || !panelKey)
        return null;
    let panel = panelCache.get(panelId);
    if (!panel) {
        const rows = await sql `
      SELECT id, name, panel_type, base_url, username, password
      FROM panels
      WHERE id = ${panelId}
      LIMIT 1;
    `;
        if (!rows.length)
            return null;
        panel = rows[0];
        panelCache.set(panelId, panel);
    }
    if (panelType === "marzban") {
        const found = await lookupMarzbanUser(panel, panelKey);
        if (!found.ok || !found.user) {
            return `🖥 پنل: ${String(panel.name || "-")} (Marzban)\n📡 جزئیات لحظه‌ای: ناموفق`;
        }
        const user = found.user;
        const totalBytes = Number(user.data_limit || 0);
        const usedBytes = Number(user.used_traffic || user.usedTraffic || user.used_bytes || 0);
        const remainBytes = totalBytes > 0 ? Math.max(0, totalBytes - usedBytes) : 0;
        return (`🖥 پنل: ${String(panel.name || "-")} (Marzban)\n` +
            `🔑 user: ${String(user.username || panelKey)}\n` +
            `📶 وضعیت: ${String(user.status || "-")}\n` +
            `📊 مصرف: ${totalBytes > 0 ? `${formatBytesShort(usedBytes)} / ${formatBytesShort(totalBytes)} (باقی‌مانده: ${formatBytesShort(remainBytes)})` : "نامحدود"}\n` +
            `📅 انقضا: ${formatExpiryLabelFromSeconds(user.expire)}\n` +
            `🆔 inventory: #${inventoryId}`);
    }
    if (panelType === "sanaei") {
        const found = await findSanaeiClientByIdentifier(panel, panelKey);
        if (!found.ok || !found.client) {
            return `🖥 پنل: ${String(panel.name || "-")} (3x-ui)\n📡 جزئیات لحظه‌ای: ناموفق`;
        }
        const client = found.client;
        const totalBytes = Number(client.totalGB || 0);
        const usedBytes = Math.max(0, Number(client.up || 0) + Number(client.down || 0));
        const remainBytes = totalBytes > 0 ? Math.max(0, totalBytes - usedBytes) : 0;
        return (`🖥 پنل: ${String(panel.name || "-")} (3x-ui)\n` +
            `🔑 email: ${String(client.email || panelKey)}\n` +
            `📶 وضعیت: ${parseMaybeBoolean(client.enable) === false ? "غیرفعال" : "فعال"}\n` +
            `📊 مصرف: ${totalBytes > 0 ? `${formatBytesShort(usedBytes)} / ${formatBytesShort(totalBytes)} (باقی‌مانده: ${formatBytesShort(remainBytes)})` : "نامحدود"}\n` +
            `📅 انقضا: ${formatExpiryLabelFromMilliseconds(client.expiryTime)}\n` +
            `🧩 inbound: ${Number(found.inboundId || 0) || "-"}\n` +
            `🆔 inventory: #${inventoryId}`);
    }
    return null;
}
async function recordForensicEvent(params) {
    await sql `
    INSERT INTO config_forensics (
      inventory_id,
      owner_telegram_id,
      product_id,
      panel_id,
      panel_type,
      panel_user_key,
      uuid,
      source,
      event_type,
      config_value,
      metadata
    )
    VALUES (
      ${params.inventoryId || null},
      ${params.ownerTelegramId || null},
      ${params.productId || null},
      ${params.panelId || null},
      ${params.panelType || null},
      ${params.panelUserKey || null},
      ${params.uuid || null},
      ${params.source || "inventory"},
      ${params.eventType},
      ${params.configValue || null},
      ${JSON.stringify(params.metadata || {})}::jsonb
    );
  `;
}
async function recordInventoryForensicEvent(inventoryId, eventType, metadata) {
    const rows = await sql `
    SELECT id, product_id, panel_id, owner_telegram_id, config_value, delivery_payload
    FROM inventory
    WHERE id = ${inventoryId}
    LIMIT 1;
  `;
    if (!rows.length)
        return;
    const row = rows[0];
    const delivery = parseDeliveryPayload(row.delivery_payload);
    const panelType = delivery.metadata?.panelType ? String(delivery.metadata.panelType) : null;
    const panelUserKey = String(delivery.metadata?.username || delivery.metadata?.email || delivery.metadata?.subId || "").trim() || null;
    const uuid = String(delivery.metadata?.uuid || "").trim() ||
        extractUuidFromText(String(row.config_value || "")) ||
        extractUuidFromText((delivery.configLinks || []).join("\n")) ||
        null;
    await recordForensicEvent({
        inventoryId: Number(row.id),
        ownerTelegramId: Number(row.owner_telegram_id || 0) || null,
        productId: Number(row.product_id || 0) || null,
        panelId: Number(row.panel_id || 0) || null,
        panelType,
        panelUserKey,
        uuid,
        eventType,
        configValue: String(row.config_value || ""),
        metadata: {
            ...(metadata || {}),
            deliveryMetadata: delivery.metadata || {}
        }
    });
}
function buildQrText(primaryText, configLinks, subscriptionUrl) {
    if (primaryText && primaryText.trim())
        return primaryText.trim();
    if (configLinks.length)
        return configLinks[0];
    if (subscriptionUrl)
        return subscriptionUrl;
    return "";
}
function buildPanelTemplateContext(params) {
    return {
        purchase_id: params.purchaseId,
        telegram_id: String(params.telegramId),
        product_id: String(params.productId),
        product_name: params.productName,
        size_mb: String(params.sizeMb),
        username: params.username,
        email: params.email,
        uuid: params.uuid || "",
        password: params.password || "",
        sub_id: params.subId || "",
        data_limit_bytes: String(params.dataLimitBytes),
        expiry_time: String(params.expiryTime)
    };
}
function parseSanaeiNested(raw) {
    if (typeof raw === "string") {
        return parseJsonValue(raw);
    }
    return raw;
}
export function buildSanaeiSubscriptionUrl(baseUrl, panelConfig, subId) {
    const customPath = String(panelConfig.subscription_path || panelConfig.sub_path || "sub").replace(/^\/+|\/+$/g, "");
    return `${normalizeBaseUrl(baseUrl)}/${customPath}/${encodeURIComponent(subId)}`;
}
function extractSanaeiHost(panelBaseUrl, panelConfig, inbound) {
    const explicitHost = String(panelConfig.server_host || panelConfig.host || "").trim();
    if (explicitHost)
        return explicitHost;
    const listen = String(inbound.listen || "").trim();
    if (listen && listen !== "0.0.0.0" && listen !== "::" && listen !== "127.0.0.1")
        return listen;
    return new URL(normalizeBaseUrl(panelBaseUrl)).hostname;
}
export function buildSanaeiConfigLinks(panelBaseUrl, inbound, client, panelConfig) {
    const protocol = String(inbound.protocol || "").toLowerCase();
    const stream = toJsonObject(parseSanaeiNested(inbound.streamSettings)) || {};
    const settings = toJsonObject(parseSanaeiNested(inbound.settings)) || {};
    const network = String(stream.network || "tcp");
    const security = String(stream.security || "none");
    const host = extractSanaeiHost(panelBaseUrl, panelConfig, inbound);
    const port = Number(inbound.port || 0);
    const remark = encodeURIComponent(String(client.email || inbound.remark || "config"));
    const query = new URLSearchParams();
    if (security === "tls" || security === "reality")
        query.set("security", security);
    if (network && network !== "tcp")
        query.set("type", network);
    const tlsSettings = toJsonObject(stream.tlsSettings) || {};
    const realitySettings = toJsonObject(stream.realitySettings) || {};
    const wsSettings = toJsonObject(stream.wsSettings) || {};
    const grpcSettings = toJsonObject(stream.grpcSettings) || {};
    const httpSettings = toJsonObject(stream.httpSettings) || {};
    const tcpSettings = toJsonObject(stream.tcpSettings) || {};
    const kcpSettings = toJsonObject(stream.kcpSettings) || {};
    const splitHttpSettings = toJsonObject(stream.splitHTTPSettings || stream.splithttpSettings) || {};
    const httpUpgradeSettings = toJsonObject(stream.httpupgradeSettings || stream.httpUpgradeSettings) || {};
    const sni = String(panelConfig.sni || tlsSettings.serverName || realitySettings.serverName || "");
    if (sni)
        query.set("sni", sni);
    const fingerprint = String(panelConfig.fp || panelConfig.fingerprint || tlsSettings.fingerprint || realitySettings.fingerprint || "");
    if (fingerprint)
        query.set("fp", fingerprint);
    const alpn = Array.isArray(tlsSettings.alpn) ? tlsSettings.alpn.join(",") : String(tlsSettings.alpn || "");
    if (alpn)
        query.set("alpn", alpn);
    if (security === "reality") {
        const publicKey = String(panelConfig.pbk || realitySettings.publicKey || "");
        const shortId = String(panelConfig.sid || realitySettings.shortId || "");
        const spiderX = String(panelConfig.spx || realitySettings.spiderX || "");
        if (publicKey)
            query.set("pbk", publicKey);
        if (shortId)
            query.set("sid", shortId);
        if (spiderX)
            query.set("spx", spiderX);
    }
    const wsPath = String(panelConfig.path || wsSettings.path || httpUpgradeSettings.path || splitHttpSettings.path || "");
    const wsHost = String(panelConfig.host_header || toJsonObject(wsSettings.headers)?.Host || "");
    const serviceName = String(panelConfig.service_name || grpcSettings.serviceName || "");
    if (wsPath)
        query.set(network === "grpc" ? "serviceName" : "path", wsPath || serviceName);
    if (serviceName && network === "grpc")
        query.set("serviceName", serviceName);
    if (wsHost && (network === "ws" || network === "httpupgrade"))
        query.set("host", wsHost);
    if (network === "http") {
        const hosts = Array.isArray(httpSettings.host) ? httpSettings.host : [];
        if (hosts[0])
            query.set("host", String(hosts[0]));
        if (httpSettings.path)
            query.set("path", String(httpSettings.path));
    }
    if (network === "tcp") {
        const headerType = String(toJsonObject(tcpSettings.header)?.type || "");
        if (headerType)
            query.set("headerType", headerType);
    }
    if (network === "kcp") {
        const headerType = String(kcpSettings.headerType || "");
        if (headerType)
            query.set("headerType", headerType);
        const seed = String(kcpSettings.seed || "");
        if (seed)
            query.set("seed", seed);
    }
    const links = [];
    if (protocol === "vless") {
        query.set("encryption", "none");
        const flow = String(client.flow || panelConfig.flow || "");
        if (flow)
            query.set("flow", flow);
        links.push(`vless://${client.id}@${host}:${port}?${query.toString()}#${remark}`);
    }
    if (protocol === "vmess") {
        const vmess = {
            v: "2",
            ps: decodeURIComponent(remark),
            add: host,
            port: String(port),
            id: String(client.id || ""),
            aid: String(client.alterId || 0),
            scy: String(client.security || "auto"),
            net: network,
            type: String(toJsonObject(tcpSettings.header)?.type || "none"),
            host: query.get("host") || "",
            path: query.get("path") || "",
            tls: security === "none" ? "" : security,
            sni: query.get("sni") || "",
            alpn: query.get("alpn") || "",
            fp: query.get("fp") || ""
        };
        links.push(`vmess://${Buffer.from(JSON.stringify(vmess), "utf8").toString("base64")}`);
    }
    if (protocol === "trojan") {
        links.push(`trojan://${client.password}@${host}:${port}?${query.toString()}#${remark}`);
    }
    if (protocol === "shadowsocks") {
        const method = String(client.method || settings.method || panelConfig.method || "aes-128-gcm");
        const credentials = Buffer.from(`${method}:${client.password}`, "utf8").toString("base64");
        links.push(`ss://${credentials}@${host}:${port}#${remark}`);
    }
    return links.filter(Boolean);
}
async function provisionMarzbanSale(panel, order, panelConfig) {
    const login = await loginMarzbanPanel({
        base_url: String(panel.base_url),
        username: String(panel.username || ""),
        password: String(panel.password || "")
    });
    if (!login.res.ok || !login.token) {
        throw new Error(`Marzban auth failed: ${login.res.status} ${responseSnippet(login.raw)}`);
    }
    const days = parseMaybeNumber(panelConfig.expire_days || panelConfig.days) || 0;
    const expireTime = days > 0 ? Date.now() + days * 24 * 60 * 60 * 1000 : 0;
    const dataLimitBytes = Math.max(0, Math.round((parseMaybeNumber(panelConfig.data_limit_mb) || Number(order.size_mb || 0)) * 1024 * 1024));
    const username = String(panelConfig.username_prefix || "tg")
        .concat(`${order.telegram_id}_${Date.now()}`)
        .replace(/[^a-zA-Z0-9_]/g, "_")
        .slice(0, 32);
    const context = buildPanelTemplateContext({
        purchaseId: String(order.purchase_id),
        telegramId: Number(order.telegram_id),
        productId: Number(order.product_id),
        productName: String(order.product_name || ""),
        sizeMb: Number(order.size_mb || 0),
        username,
        email: username,
        dataLimitBytes,
        expiryTime: expireTime
    });
    const defaults = {
        username,
        proxies: { [String(panelConfig.protocol || "vless").toLowerCase()]: {} },
        inbounds: toJsonObject(panelConfig.inbounds) || {},
        expire: expireTime ? Math.floor(expireTime / 1000) : 0,
        data_limit: dataLimitBytes,
        data_limit_reset_strategy: String(panelConfig.data_limit_reset_strategy || "no_reset"),
        status: String(panelConfig.status || "active"),
        note: `order:${order.purchase_id}|telegram:${order.telegram_id}|product:${order.product_id}`
    };
    const merged = applyTemplate(mergeDeep(defaults, panelConfig.override || panelConfig.user || {}), context);
    const res = await fetchWithTimeout(`${normalizeBaseUrl(String(panel.base_url))}/api/user`, {
        method: "POST",
        headers: {
            Authorization: `Bearer ${login.token}`,
            "Content-Type": "application/json",
            Accept: "application/json"
        },
        body: JSON.stringify(merged)
    });
    const raw = await res.text();
    const data = parseJsonObject(raw);
    if (!res.ok || !data) {
        throw new Error(`Marzban create user failed: ${res.status} ${responseSnippet(raw)}`);
    }
    const links = Array.isArray(data.links) ? data.links.map((item) => String(item || "").trim()).filter(Boolean) : [];
    const subscriptionUrl = data.subscription_url ? String(data.subscription_url) : null;
    const uuid = extractUuidFromText([String(links[0] || ""), String(subscriptionUrl || "")].filter(Boolean).join("\n"));
    const deliveryMode = String(order.panel_delivery_mode || "both");
    const finalLinks = deliveryMode === "sub" ? [] : links;
    const finalSub = deliveryMode === "configs" ? null : subscriptionUrl;
    return {
        configValue: finalLinks[0] || finalSub || username,
        deliveryPayload: {
            subscriptionUrl: finalSub,
            configLinks: finalLinks,
            primaryQr: buildQrText(finalLinks[0] || null, finalLinks, finalSub),
            primaryText: finalLinks[0] || finalSub || username,
            metadata: {
                panelType: "marzban",
                username,
                uuid,
                apiResponse: data
            }
        }
    };
}
async function provisionSanaeiSale(panel, order, panelConfig) {
    const login = await loginSanaeiPanel({
        base_url: String(panel.base_url),
        username: String(panel.username || ""),
        password: String(panel.password || "")
    });
    if (!login.res.ok || !jsonSuccess(login.data) || !login.cookie) {
        throw new Error(`Sanaei auth failed: ${login.res.status} ${responseSnippet(login.raw)}`);
    }
    const inboundId = parseMaybeNumber(panelConfig.inbound_id || panelConfig.inboundId);
    if (!inboundId) {
        throw new Error("برای فروش از 3x-ui باید inbound_id در تنظیمات محصول ثبت شود.");
    }
    const inbounds = await getSanaeiInbounds(String(panel.base_url), login.cookie);
    if (!inbounds.res.ok || !jsonSuccess(inbounds.data)) {
        throw new Error(`Sanaei list inbounds failed: ${inbounds.res.status} ${responseSnippet(inbounds.raw)}`);
    }
    const inbound = inbounds.items.find((item) => Number(item.id || 0) === inboundId);
    if (!inbound) {
        throw new Error(`inbound #${inboundId} روی پنل پیدا نشد.`);
    }
    const protocol = String(inbound.protocol || "").toLowerCase();
    const sizeMbOverride = parseMaybeNumber(panelConfig.data_limit_mb) || Number(order.size_mb || 0);
    const dataLimitBytes = Math.max(0, Math.round(sizeMbOverride * 1024 * 1024));
    const days = parseMaybeNumber(panelConfig.expire_days || panelConfig.days) || 0;
    const expiryTime = days > 0 ? Date.now() + days * 24 * 60 * 60 * 1000 : 0;
    const clientId = randomUUID();
    const clientPassword = randomUUID().replaceAll("-", "");
    const subId = randomCode(16).toLowerCase();
    const email = String(panelConfig.email_prefix || "tg")
        .concat(`${order.telegram_id}_${Date.now()}`)
        .replace(/[^\w@.\-]/g, "_")
        .slice(0, 64);
    const context = buildPanelTemplateContext({
        purchaseId: String(order.purchase_id),
        telegramId: Number(order.telegram_id),
        productId: Number(order.product_id),
        productName: String(order.product_name || ""),
        sizeMb: Number(order.size_mb || 0),
        username: email,
        email,
        uuid: clientId,
        password: clientPassword,
        subId,
        dataLimitBytes,
        expiryTime
    });
    const defaultClient = {
        email,
        enable: parseMaybeBoolean(panelConfig.enable) ?? true,
        tgId: String(order.telegram_id),
        subId,
        limitIp: parseMaybeNumber(panelConfig.limit_ip || panelConfig.limitIp) || 0,
        totalGB: dataLimitBytes,
        expiryTime
    };
    if (protocol === "vless" || protocol === "vmess")
        defaultClient.id = clientId;
    if (protocol === "trojan")
        defaultClient.password = clientPassword;
    if (protocol === "shadowsocks") {
        defaultClient.password = clientPassword;
        defaultClient.method = String(panelConfig.method || "aes-128-gcm");
    }
    if (protocol === "vless") {
        const flow = String(panelConfig.flow || "");
        if (flow)
            defaultClient.flow = flow;
    }
    const client = applyTemplate(mergeDeep(defaultClient, panelConfig.client || panelConfig.override || {}), context);
    const res = await fetchWithTimeout(`${normalizeBaseUrl(String(panel.base_url))}/panel/api/inbounds/addClient`, {
        method: "POST",
        headers: {
            Accept: "application/json",
            Cookie: login.cookie,
            "Content-Type": "application/json"
        },
        body: JSON.stringify({
            id: inboundId,
            settings: JSON.stringify({ clients: [client] })
        })
    });
    const raw = await res.text();
    const ok = res.ok && (!raw.trim() || jsonSuccess(parseJsonObject(raw)));
    if (!ok) {
        throw new Error(`Sanaei create client failed: ${res.status} ${responseSnippet(raw)}`);
    }
    const configLinks = buildSanaeiConfigLinks(String(panel.base_url), inbound, toJsonObject(client) || {}, panelConfig);
    const subscriptionUrl = buildSanaeiSubscriptionUrl(String(panel.base_url), panelConfig, subId);
    const deliveryMode = String(order.panel_delivery_mode || "both");
    const finalLinks = deliveryMode === "sub" ? [] : configLinks;
    const finalSub = deliveryMode === "configs" ? null : subscriptionUrl;
    return {
        configValue: finalLinks[0] || finalSub || email,
        deliveryPayload: {
            subscriptionUrl: finalSub,
            configLinks: finalLinks,
            primaryQr: buildQrText(finalLinks[0] || null, finalLinks, finalSub),
            primaryText: finalLinks[0] || finalSub || email,
            metadata: {
                panelType: "sanaei",
                inboundId,
                protocol,
                email,
                subId,
                uuid: clientId
            }
        }
    };
}
async function testPanelConnection(panelId) {
    const rows = await sql `
    SELECT id, panel_type, base_url, username, password
    FROM panels
    WHERE id = ${panelId}
    LIMIT 1;
  `;
    if (!rows.length)
        return { ok: false, message: "پنل پیدا نشد." };
    const panel = rows[0];
    const panelType = String(panel.panel_type);
    const baseUrl = normalizeBaseUrl(String(panel.base_url));
    const username = String(panel.username || "");
    const password = String(panel.password || "");
    const startedAt = Date.now();
    try {
        if (!username || !password) {
            const detail = "نام کاربری یا رمز عبور پنل وارد نشده است.";
            await updatePanelCheckState(panelId, false, detail, {
                last_error: detail,
                last_check_ms: Date.now() - startedAt
            }, null);
            logInfo("panel_test_failed", { panelId, panelType, detail });
            return { ok: false, message: `اتصال پنل ناموفق بود.\n${detail}` };
        }
        if (panelType === "marzban") {
            const login = await loginMarzbanPanel({
                base_url: String(panel.base_url),
                username: String(panel.username || ""),
                password: String(panel.password || "")
            });
            if (!login.res.ok || !login.token) {
                const detail = `Marzban status ${login.res.status} | ${responseSnippet(login.raw)}`;
                await updatePanelCheckState(panelId, false, detail, {
                    last_error: detail,
                    last_status: login.res.status,
                    last_check_ms: Date.now() - startedAt
                }, null);
                return { ok: false, message: `اتصال Marzban ناموفق بود.\n${detail}` };
            }
            await updatePanelCheckState(panelId, true, "ok", {
                last_status: login.res.status,
                last_check_ms: Date.now() - startedAt,
                api: "marzban"
            }, login.token);
            return { ok: true, message: "اتصال Marzban موفق بود ✅" };
        }
        const login = await loginSanaeiPanel({
            base_url: String(panel.base_url),
            username: String(panel.username || ""),
            password: String(panel.password || "")
        });
        if (!login.res.ok || !jsonSuccess(login.data) || !login.cookie) {
            const detail = `Sanaei login status ${login.res.status} | ${responseSnippet(login.raw)}`;
            await updatePanelCheckState(panelId, false, detail, {
                last_error: detail,
                login_status: login.res.status,
                last_check_ms: Date.now() - startedAt
            }, null);
            return { ok: false, message: `ورود به پنل Sanaei ناموفق بود.\n${detail}` };
        }
        const inbounds = await getSanaeiInbounds(baseUrl, login.cookie);
        if (!inbounds.res.ok || !jsonSuccess(inbounds.data)) {
            const detail = `Sanaei status ${inbounds.res.status} | ${responseSnippet(inbounds.raw)}`;
            await updatePanelCheckState(panelId, false, detail, {
                last_error: detail,
                login_status: login.res.status,
                last_status: inbounds.res.status,
                last_check_ms: Date.now() - startedAt
            }, null);
            return { ok: false, message: `اتصال Sanaei ناموفق بود.\n${detail}` };
        }
        await updatePanelCheckState(panelId, true, "ok", {
            login_status: login.res.status,
            last_status: inbounds.res.status,
            inbound_count: inbounds.items.length,
            inbounds: inbounds.items.map((item) => ({
                id: item.id,
                remark: item.remark,
                protocol: item.protocol,
                port: item.port
            })),
            last_check_ms: Date.now() - startedAt
        }, null);
        return { ok: true, message: "اتصال Sanaei موفق بود ✅" };
    }
    catch (error) {
        const message = String(error.message || error);
        await updatePanelCheckState(panelId, false, message, {
            last_error: message,
            last_check_ms: Date.now() - startedAt
        }, null);
        logError("panel_test_exception", error, { panelId, baseUrl, panelType });
        return { ok: false, message: `خطا در اتصال به پنل.\n${message}` };
    }
}
async function showCustomerMigrationTargets(chatId, inventoryId, userId) {
    const ownRows = await sql `
    SELECT id FROM inventory
    WHERE id = ${inventoryId} AND owner_telegram_id = ${userId} AND status = 'sold'
    LIMIT 1;
  `;
    if (!ownRows.length) {
        await tg("sendMessage", { chat_id: chatId, text: "کانفیگ انتخاب‌شده معتبر نیست." });
        return;
    }
    const rows = await sql `
    SELECT id, name, panel_type
    FROM panels
    WHERE active = TRUE AND allow_customer_migration = TRUE
    ORDER BY priority DESC, id ASC;
  `;
    if (!rows.length) {
        await tg("sendMessage", { chat_id: chatId, text: "فعلاً مقصد فعالی برای مهاجرت آزاد نشده است." });
        return;
    }
    const keyboard = rows.map((p) => [
        { text: `${p.name} (${String(p.panel_type).toUpperCase()})`, callback_data: `migrate_pick_${inventoryId}_${p.id}` }
    ]);
    keyboard.push([homeButton()]);
    await tg("sendMessage", {
        chat_id: chatId,
        text: "پنل مقصد را انتخاب کنید:\nانتقال برای شما به‌صورت فوری انجام می‌شود ✅",
        reply_markup: { inline_keyboard: keyboard }
    });
}
async function createMigrationRequest(chatId, requestedBy, requestedFor, sourceInventoryId, targetPanelId, role) {
    const sourceRows = await sql `
    SELECT i.id, i.config_value, i.panel_id, i.migration_parent_inventory_id
    FROM inventory i
    WHERE i.id = ${sourceInventoryId} AND i.owner_telegram_id = ${requestedFor} AND i.status = 'sold'
    LIMIT 1;
  `;
    if (!sourceRows.length) {
        await tg("sendMessage", { chat_id: chatId, text: "کانفیگ مبدا معتبر نیست." });
        return false;
    }
    const targetRows = await sql `
    SELECT id, name, active, allow_customer_migration
    FROM panels
    WHERE id = ${targetPanelId}
    LIMIT 1;
  `;
    if (!targetRows.length || !targetRows[0].active) {
        await tg("sendMessage", { chat_id: chatId, text: "پنل مقصد فعال نیست." });
        return false;
    }
    if (role === "customer" && !targetRows[0].allow_customer_migration) {
        await tg("sendMessage", { chat_id: chatId, text: "ادمین مهاجرت به این پنل را برای کاربران باز نکرده است." });
        return false;
    }
    const sourcePanelId = sourceRows[0].panel_id === null ? null : Number(sourceRows[0].panel_id);
    if (sourcePanelId !== null && sourcePanelId === targetPanelId) {
        await tg("sendMessage", { chat_id: chatId, text: "کانفیگ شما همین الان روی همین پنل است." });
        return false;
    }
    const exists = await sql `
    SELECT id
    FROM panel_migrations
    WHERE source_inventory_id = ${sourceInventoryId}
      AND target_panel_id = ${targetPanelId}
      AND status IN ('pending', 'approved')
    LIMIT 1;
  `;
    if (exists.length) {
        await tg("sendMessage", { chat_id: chatId, text: "برای این مقصد قبلاً درخواست ثبت شده است." });
        return false;
    }
    const inserted = await sql `
    INSERT INTO panel_migrations (
      source_inventory_id,
      source_panel_id,
      target_panel_id,
      requested_by,
      requested_for,
      requested_by_role,
      source_config_snapshot
    )
    VALUES (
      ${sourceInventoryId},
      ${sourceRows[0].panel_id || null},
      ${targetPanelId},
      ${requestedBy},
      ${requestedFor},
      ${role},
      ${String(sourceRows[0].config_value)}
    )
    RETURNING id;
  `;
    if (role === "customer") {
        const result = await completeMigration(Number(inserted[0].id), requestedBy, null);
        if (!result.ok) {
            await sql `
        UPDATE panel_migrations
        SET status = 'failed', processed_at = NOW(), processed_by = ${requestedBy}
        WHERE id = ${inserted[0].id};
      `;
            await tg("sendMessage", { chat_id: chatId, text: "انتقال فوری انجام نشد. تیم پشتیبانی بررسی می‌کند." });
            await notifyAdmins(`⚠️ انتقال فوری ناموفق\nکد: ${inserted[0].id}\nکاربر: ${requestedFor}\nعلت: ${result.reason}`);
            return false;
        }
        const isFromManualStock = sourceRows[0].panel_id === null && sourceRows[0].migration_parent_inventory_id === null;
        if (isFromManualStock) {
            await notifyAdmins(`🔔 انتقال فوری انجام شد (منبع دستی)\nکد: ${inserted[0].id}\nکاربر: ${requestedFor}\nکانفیگ: ${sourceInventoryId}`);
        }
        return true;
    }
    await tg("sendMessage", { chat_id: chatId, text: `درخواست انتقال ثبت شد ✅\nکد درخواست: ${inserted[0].id}` });
    await notifyAdmins(`📥 درخواست انتقال جدید\nکد: ${inserted[0].id}\nکاربر: ${requestedFor}\nکانفیگ: ${sourceInventoryId}`, {
        inline_keyboard: [[{ text: "بازکردن درخواست", callback_data: `admin_migration_open_${inserted[0].id}` }]]
    });
    return true;
}
async function showMyMigrations(chatId, userId) {
    const rows = await sql `
    SELECT
      m.id,
      m.source_inventory_id,
      m.status,
      m.created_at,
      p.name AS panel_name
    FROM panel_migrations m
    INNER JOIN panels p ON p.id = m.target_panel_id
    WHERE m.requested_for = ${userId}
    ORDER BY m.id DESC
    LIMIT 20;
  `;
    if (!rows.length) {
        await tg("sendMessage", { chat_id: chatId, text: "هنوز درخواست انتقالی ندارید." });
        return;
    }
    const lines = rows.map((r) => `#${r.id} | کانفیگ ${r.source_inventory_id} → ${r.panel_name} | ${r.status} | ${r.created_at}`);
    await tg("sendMessage", {
        chat_id: chatId,
        text: `آخرین درخواست‌های انتقال شما:\n\n${lines.join("\n")}`,
        reply_markup: { inline_keyboard: [[homeButton()]] }
    });
}
async function showMyOrders(chatId, userId) {
    const rows = await sql `
    SELECT
      o.id,
      o.purchase_id,
      COALESCE(o.product_name_snapshot, p.name) AS product_name,
      o.status,
      o.payment_method,
      o.final_price,
      o.created_at
    FROM orders o
    INNER JOIN products p ON p.id = o.product_id
    WHERE o.telegram_id = ${userId}
    ORDER BY o.id DESC
    LIMIT 20;
  `;
    if (!rows.length) {
        await tg("sendMessage", { chat_id: chatId, text: "هنوز سفارشی ثبت نکرده‌ای." });
        return;
    }
    const keyboard = rows.map((o) => [
        cb(`${String(o.purchase_id)} | ${String(o.product_name)} | ${formatOrderStatusTitle(o.status)}`, `open_order_${String(o.purchase_id)}`, "primary")
    ]);
    keyboard.push([cb("🔎 پیگیری با شناسه", "order_lookup", "primary")]);
    keyboard.push([homeButton()]);
    await tg("sendMessage", {
        chat_id: chatId,
        text: "🧾 سفارش‌های اخیرت 👇",
        reply_markup: { inline_keyboard: keyboard }
    });
}
async function showOrderDetails(chatId, userId, purchaseId) {
    const rows = await sql `
    SELECT
      o.id,
      o.purchase_id,
      COALESCE(o.product_name_snapshot, p.name) AS product_name,
      o.status,
      o.payment_method,
      o.final_price,
      o.created_at,
      o.inventory_id,
      o.tronado_payment_url,
      o.plisio_invoice_url,
      o.receipt_file_id
    FROM orders o
    INNER JOIN products p ON p.id = o.product_id
    WHERE o.purchase_id = ${purchaseId} AND o.telegram_id = ${userId}
    LIMIT 1;
  `;
    if (!rows.length) {
        await tg("sendMessage", { chat_id: chatId, text: "سفارش پیدا نشد یا متعلق به تو نیست." });
        return;
    }
    const o = rows[0];
    const statusTitle = formatOrderStatusTitle(o.status);
    const methodTitle = formatPaymentMethodTitle(o.payment_method);
    const lines = [
        `🧾 جزئیات سفارش`,
        ``,
        `شناسه: ${String(o.purchase_id)}`,
        `محصول: ${String(o.product_name)}`,
        `مبلغ: ${formatPriceToman(Number(o.final_price))} تومان`,
        `روش پرداخت: ${methodTitle}`,
        `وضعیت: ${statusTitle}`,
        `زمان: ${String(o.created_at)}`
    ];
    const keyboard = [];
    const paymentUrl = String(o.plisio_invoice_url || o.tronado_payment_url || "").trim();
    if (paymentUrl && (String(o.status || "").toLowerCase() === "pending")) {
        keyboard.push([{ text: "💳 پرداخت", url: paymentUrl }]);
    }
    if (String(o.payment_method || "").toLowerCase() === "crypto") {
        keyboard.push([cb("✅ بررسی/ثبت پرداخت", `check_order_${String(o.purchase_id)}`, "success")]);
    }
    if (String(o.payment_method || "").toLowerCase() === "card2card" && String(o.status || "").toLowerCase() === "awaiting_receipt") {
        keyboard.push([cb("📷 ارسال رسید", `order_send_receipt_${Number(o.id)}`, "success")]);
    }
    if (o.inventory_id) {
        keyboard.push([cb("📦 مشاهده کانفیگ", `open_config_${Number(o.inventory_id)}`, "primary")]);
    }
    if (String(o.payment_method || "").toLowerCase() !== "wallet" && ["pending", "awaiting_receipt"].includes(String(o.status || "").toLowerCase())) {
        keyboard.push([cb("🗑 لغو سفارش", `order_cancel_${String(o.purchase_id)}`, "danger")]);
    }
    keyboard.push([backButton("my_orders")]);
    keyboard.push([homeButton()]);
    await tg("sendMessage", { chat_id: chatId, text: lines.join("\n"), reply_markup: { inline_keyboard: keyboard } });
}
async function completeMigration(migrationId, decidedBy, targetConfigValue) {
    const rows = await sql `
    SELECT
      m.id,
      m.source_inventory_id,
      m.target_panel_id,
      m.requested_for,
      i.product_id,
      i.config_value
    FROM panel_migrations m
    INNER JOIN inventory i ON i.id = m.source_inventory_id
    WHERE m.id = ${migrationId} AND m.status = 'pending'
    LIMIT 1;
  `;
    if (!rows.length)
        return { ok: false, reason: "migration_not_found" };
    const m = rows[0];
    const value = (targetConfigValue || String(m.config_value)).trim();
    if (!value)
        return { ok: false, reason: "target_config_empty" };
    const inserted = await sql `
    INSERT INTO inventory (
      product_id,
      config_value,
      status,
      owner_telegram_id,
      panel_id,
      migration_parent_inventory_id,
      sold_at
    )
    VALUES (
      ${m.product_id},
      ${value},
      'sold',
      ${m.requested_for},
      ${m.target_panel_id},
      ${m.source_inventory_id},
      NOW()
    )
    RETURNING id;
  `;
    await sql `UPDATE inventory SET migrated_to_inventory_id = ${inserted[0].id} WHERE id = ${m.source_inventory_id};`;
    await sql `
    UPDATE panel_migrations
    SET status = 'approved', target_config_value = ${value}, processed_at = NOW(), processed_by = ${decidedBy}
    WHERE id = ${migrationId};
  `;
    await tg("sendMessage", {
        chat_id: Number(m.requested_for),
        text: `درخواست انتقال #${migrationId} تایید شد ✅\nکانفیگ جدید شما:`,
    });
    await sendConfigWithQr(Number(m.requested_for), `M-${migrationId}`, value, [[homeButton()]]);
    return { ok: true, reason: "done" };
}
async function showProducts(chatId, forBuy) {
    const globalInfinite = await getBoolSetting("global_infinite_mode", false);
    const customEnabled = forBuy ? await getBoolSetting("custom_v2ray_enabled", false) : false;
    const customProductId = customEnabled ? Number((await getSetting("custom_v2ray_product_id")) || 0) : 0;
    const rows = await sql `
    SELECT
      p.id,
      p.name,
      p.size_mb,
      p.price_toman,
      p.is_infinite,
      p.sell_mode,
      p.panel_id,
      p.panel_sell_limit,
      p.panel_delivery_mode,
      pnl.name AS panel_name,
      pnl.active AS panel_active,
      pnl.allow_new_sales AS panel_allow_new_sales,
      (SELECT COUNT(*)::int FROM inventory i WHERE i.product_id = p.id AND i.status = 'available') AS stock,
      (
        SELECT COUNT(*)::int
        FROM orders o
        WHERE o.product_id = p.id
          AND o.sell_mode = 'panel'
          AND o.status NOT IN ('denied')
      ) AS panel_sales_count
    FROM products p
    LEFT JOIN panels pnl ON pnl.id = p.panel_id
    WHERE p.is_active = TRUE
    ORDER BY p.id ASC;
  `;
    if (!rows.length) {
        await tg("sendMessage", { chat_id: chatId, text: "هیچ محصول فعالی تعریف نشده است." });
        return;
    }
    const dayPrice = customEnabled ? Math.max(0, Math.round((await getNumberSetting("custom_v2ray_extra_day_toman")) || 0)) : 0;
    const pricePerGb = customEnabled
        ? normalizePricePerGb(await getSetting("product_price_per_gb_toman"), normalizePricePerGb(await getSetting("topup_price_per_gb_toman")))
        : 0;
    const minCustomPrice = customEnabled ? Math.max(1, pricePerGb + 30 * dayPrice) : 0;
    const standardRows = customEnabled && customProductId > 0 ? rows.filter((p) => Number(p.id) !== customProductId) : rows;
    const customRow = customEnabled && customProductId > 0 ? rows.find((p) => Number(p.id) === customProductId) : null;
    const keyboard = standardRows.map((p) => [
        cb(`${p.name} | ${formatPriceToman(Number(p.price_toman))} تومان`, forBuy ? `buy_product_${p.id}` : `admin_inventory_product_${p.id}`, "primary")
    ]);
    if (forBuy && customRow) {
        keyboard.push([
            cb(`🎛 سفارشی | از ${formatPriceToman(minCustomPrice)} تومان`, `buy_custom_v2ray_${customProductId}`, "success")
        ]);
    }
    keyboard.push([homeButton()]);
    await tg("sendMessage", {
        chat_id: chatId,
        text: forBuy ? "🛍 محصول موردنظر را انتخاب کنید:" : "محصول برای مدیریت موجودی:",
        reply_markup: { inline_keyboard: keyboard }
    });
}
async function listProductsForAdmin(chatId, userId) {
    const showArchived = await getBoolSetting(`admin_products_show_archived_${userId}`, false);
    const rows = await sql `
    SELECT
      p.id,
      p.name,
      p.size_mb,
      p.price_toman,
      p.is_active,
      p.is_infinite,
      p.sell_mode,
      p.panel_id,
      p.panel_sell_limit,
      p.panel_delivery_mode,
      pnl.name AS panel_name
    FROM products p
    LEFT JOIN panels pnl ON pnl.id = p.panel_id
    WHERE (${showArchived} = TRUE OR p.is_active = TRUE)
    ORDER BY p.id ASC;
  `;
    const keyboard = rows.flatMap((p) => [
        [
            {
                text: `${p.name} | ${formatPriceToman(Number(p.price_toman))} تومان`,
                callback_data: `admin_edit_product_${p.id}`
            }
        ],
        [
            cb("ویرایش", `admin_edit_product_${p.id}`, "primary"),
            cb(p.is_active ? "غیرفعال‌سازی" : "فعال‌سازی", `admin_toggle_product_${p.id}`, p.is_active ? "danger" : "success"),
            cb(parseSellMode(String(p.sell_mode || "")) === "panel" ? "فروش دستی" : "فروش از پنل", `admin_toggle_product_sell_mode_${p.id}`, "primary")
        ],
        [
            cb(p.is_infinite ? "حذف ∞" : "∞", `admin_toggle_product_infinite_${p.id}`, "primary"),
            cb("تنظیم فروش پنل", `admin_configure_product_panel_${p.id}`, "primary"),
            cb("🗑 حذف", `admin_remove_product_${p.id}`, "danger")
        ]
    ]);
    keyboard.push([cb(showArchived ? "📦 مخفی کردن آرشیو" : "📦 نمایش آرشیو", showArchived ? "admin_products_hide_archived" : "admin_products_show_archived", "primary")]);
    keyboard.push([cb("➕ افزودن محصول", "admin_add_product", "success")]);
    keyboard.push([backButton("admin_panel")]);
    await tg("sendMessage", {
        chat_id: chatId,
        text: "مدیریت محصولات:",
        reply_markup: { inline_keyboard: keyboard }
    });
}
async function showWalletUsagePrompt(chatId, userId, productId, walletBalance) {
    const productRows = await sql `SELECT price_toman FROM products WHERE id = ${productId} LIMIT 1;`;
    if (!productRows.length) {
        await tg("sendMessage", { chat_id: chatId, text: "محصول یافت نشد." });
        return;
    }
    const productPrice = Number(productRows[0].price_toman || 0);
    const maxUsable = Math.min(walletBalance, productPrice);
    if (maxUsable <= 0) {
        await showPaymentMethods(chatId, userId, productId, 0);
        return;
    }
    const keyboard = [
        [cb(`✅ استفاده از حداکثر ممکن (${formatPriceToman(maxUsable)} تومان)`, `use_wallet_${productId}_${maxUsable}`, "success")],
        [cb("✍️ ورود مبلغ دلخواه", `use_wallet_custom_${productId}`, "primary")],
        [cb("❌ بدون استفاده از کیف پول", `use_wallet_${productId}_0`, "danger")],
        [homeButton()]
    ];
    await tg("sendMessage", {
        chat_id: chatId,
        text: `شما ${formatPriceToman(walletBalance)} تومان در کیف پول خود دارید.\n\nقیمت محصول: ${formatPriceToman(productPrice)} تومان\nآیا مایلید از موجودی کیف پول خود برای پرداخت بخشی (یا تمام) هزینه استفاده کنید؟`,
        reply_markup: { inline_keyboard: keyboard }
    });
}
async function showPaymentMethods(chatId, userId, productId, walletUsed = 0) {
    const userRows = await sql `SELECT wallet_balance FROM users WHERE telegram_id = ${userId} LIMIT 1;`;
    const walletBalance = userRows.length ? Number(userRows[0].wallet_balance || 0) : 0;
    const productRows = await sql `SELECT price_toman FROM products WHERE id = ${productId} LIMIT 1;`;
    if (!productRows.length) {
        await tg("sendMessage", { chat_id: chatId, text: "محصول یافت نشد." });
        return;
    }
    const productPrice = Number(productRows[0].price_toman || 0);
    const finalPayable = Math.max(0, productPrice - walletUsed);
    const rows = await sql `SELECT code, title FROM payment_methods WHERE active = TRUE ORDER BY code ASC;`;
    if (!rows.length && walletBalance < finalPayable) {
        await tg("sendMessage", { chat_id: chatId, text: "فعلاً هیچ روش پرداخت فعالی وجود ندارد و موجودی کیف پول شما هم کافی نیست." });
        return;
    }
    const callbackBase = await getPublicBaseUrl(env.PUBLIC_BASE_URL);
    const hasCards = (await sql `SELECT 1 FROM cards WHERE active = TRUE LIMIT 1;`).length > 0;
    const hasPlisioKey = Boolean(((await getSetting("plisio_api_key")) || "").trim());
    const hasTetrapayKey = Boolean(((await getSetting("tetrapay_api_key")) || "").trim());
    const hasTronadoKey = Boolean(((await getSetting("tronado_api_key")) || "").trim());
    const hasBusinessWallet = Boolean(((await getSetting("business_wallet_address")) || env.BUSINESS_WALLET_ADDRESS || "").trim());
    const cryptoWalletRows = await sql `
    SELECT id, currency, network, address, rate_mode, rate_toman_per_unit, extra_toman_per_unit, active
    FROM crypto_wallets
    WHERE active = TRUE;
  `;
    const hasCrypto = cryptoWalletRows.map((w) => w).some(cryptoWalletReady);
    const filtered = rows.filter((m) => {
        const code = String(m.code);
        if (code === "card2card")
            return hasCards;
        if (code === "plisio")
            return Boolean(callbackBase) && hasPlisioKey;
        if (code === "tetrapay")
            return Boolean(callbackBase) && hasTetrapayKey;
        if (code === "tronado")
            return Boolean(callbackBase) && hasTronadoKey && hasBusinessWallet;
        if (code === "crypto")
            return hasCrypto;
        return true;
    });
    if (!filtered.length && walletBalance < finalPayable) {
        await tg("sendMessage", { chat_id: chatId, text: "روش پرداختی که به‌درستی تنظیم شده باشد یافت نشد." });
        return;
    }
    const keyboard = [];
    if (walletUsed >= productPrice) {
        keyboard.push([cb(`💰 پرداخت کامل با کیف پول (${formatPriceToman(productPrice)} تومان)`, `select_pay_${productId}_wallet_${walletUsed}`, "success")]);
    }
    else {
        for (const m of filtered) {
            keyboard.push([cb(String(m.title), `select_pay_${productId}_${m.code}_${walletUsed}`, "primary")]);
        }
    }
    keyboard.push([homeButton()]);
    await tg("sendMessage", {
        chat_id: chatId,
        text: walletUsed > 0 && walletUsed < productPrice
            ? `مبلغ ${formatPriceToman(walletUsed)} از کیف پول کسر خواهد شد.\nمبلغ باقیمانده برای پرداخت: ${formatPriceToman(finalPayable)} تومان\nلطفاً روش پرداخت باقیمانده را انتخاب کنید:`
            : "روش پرداخت را انتخاب کنید:",
        reply_markup: { inline_keyboard: keyboard }
    });
}
async function ensureCustomV2rayProduct() {
    const name = "سفارشی";
    const pricePerGb = normalizePricePerGb(await getSetting("product_price_per_gb_toman"), normalizePricePerGb(await getSetting("topup_price_per_gb_toman")));
    const dayPrice = Math.max(0, Math.round((await getNumberSetting("custom_v2ray_extra_day_toman")) || 0));
    const minPrice = Math.max(1, pricePerGb + 30 * dayPrice);
    const baseConfig = { product_kind: "v2ray", custom_v2ray_product: true, expire_days: 30, data_limit_mb: 1024 };
    let productId = Number((await getSetting("custom_v2ray_product_id")) || 0);
    try {
        if (Number.isFinite(productId) && productId > 0) {
            const existing = await sql `SELECT id FROM products WHERE id = ${productId} LIMIT 1;`;
            if (existing.length) {
                await sql `
          UPDATE products
          SET name = ${name},
              size_mb = 1024,
              price_toman = ${minPrice},
              is_active = TRUE,
              panel_config = COALESCE(panel_config, '{}'::jsonb) || ${JSON.stringify(baseConfig)}::jsonb
          WHERE id = ${productId};
        `;
                return { ok: true, productId };
            }
        }
        const byName = await sql `SELECT id FROM products WHERE name = ${name} LIMIT 1;`;
        if (byName.length) {
            productId = Number(byName[0].id);
            await sql `
        UPDATE products
        SET size_mb = 1024,
            price_toman = ${minPrice},
            is_active = TRUE,
            panel_config = COALESCE(panel_config, '{}'::jsonb) || ${JSON.stringify(baseConfig)}::jsonb
        WHERE id = ${productId};
      `;
            await setSetting("custom_v2ray_product_id", String(productId));
            return { ok: true, productId };
        }
        const inserted = await sql `
      INSERT INTO products (name, size_mb, price_toman, is_active, is_infinite, sell_mode, panel_config)
      VALUES (${name}, 1024, ${minPrice}, TRUE, FALSE, 'manual', ${JSON.stringify(baseConfig)}::jsonb)
      RETURNING id;
    `;
        productId = Number(inserted[0].id);
        await setSetting("custom_v2ray_product_id", String(productId));
        return { ok: true, productId };
    }
    catch (error) {
        logError("ensure_custom_v2ray_product_failed", error, { productId });
        return { ok: false, productId: 0 };
    }
}
async function startCustomV2rayWizard(chatId, userId, productId) {
    const enabled = await getBoolSetting("custom_v2ray_enabled", false);
    const selectedProductId = Number((await getSetting("custom_v2ray_product_id")) || 0);
    if (!enabled || !selectedProductId || selectedProductId !== productId) {
        await tg("sendMessage", { chat_id: chatId, text: "محصول سفارشی فعال نیست یا درست تنظیم نشده است." });
        return;
    }
    const rows = await sql `
    SELECT id, name, price_toman, size_mb, is_infinite, sell_mode, panel_id, panel_delivery_mode, panel_config
    FROM products
    WHERE id = ${productId} AND is_active = TRUE
    LIMIT 1;
  `;
    if (!rows.length) {
        await tg("sendMessage", { chat_id: chatId, text: "محصول یافت نشد." });
        return;
    }
    const product = rows[0];
    if (getV2rayProductKindFromRow(product) !== "v2ray") {
        await tg("sendMessage", { chat_id: chatId, text: "این محصول سفارشی نیست." });
        return;
    }
    const baseMb = 1024;
    const baseDays = 30;
    const pricePerGb = normalizePricePerGb(await getSetting("product_price_per_gb_toman"), normalizePricePerGb(await getSetting("topup_price_per_gb_toman")));
    const dayPrice = Math.max(0, Math.round((await getNumberSetting("custom_v2ray_extra_day_toman")) || 0));
    const statePayload = {
        productId,
        baseMb,
        baseDays,
        dataMb: baseMb,
        days: baseDays,
        pricePerGb,
        dayPrice,
        messageId: 0
    };
    await setState(userId, "custom_v2ray_wizard", statePayload);
    await renderCustomV2rayWizard(chatId, userId);
}
async function renderCustomV2rayWizard(chatId, userId, messageId) {
    const state = await getState(userId);
    if (!state || state.state !== "custom_v2ray_wizard")
        return;
    const p = state.payload || {};
    const productId = Number(p.productId);
    const baseMb = Math.max(1, Math.round(Number(p.baseMb || 0)));
    const baseDays = Math.max(30, Math.round(Number(p.baseDays || 30)));
    const dataMb = Math.max(baseMb, Math.round(Number(p.dataMb || baseMb)));
    const days = Math.max(30, Math.round(Number(p.days || baseDays)));
    const pricePerGb = Math.max(1, Math.round(Number(p.pricePerGb || 500000)));
    const dayPrice = Math.max(0, Math.round(Number(p.dayPrice || 0)));
    const gb = Math.max(1, Math.round(dataMb / 1024));
    const totalPrice = Math.max(1, gb * pricePerGb + days * dayPrice);
    const rows = await sql `SELECT name FROM products WHERE id = ${productId} LIMIT 1;`;
    const productName = rows.length ? String(rows[0].name || "-") : "-";
    const text = `🎁 فاکتور خرید [${days} روز، ${gb} گیگابایت]\n\n` +
        `🔸 محصول: ${productName}\n` +
        `🔸 حجم: ${gb} گیگابایت\n` +
        `🔸 زمان: ${days} روز\n\n` +
        `💰 مبلغ: ${formatPriceToman(totalPrice)} تومان\n\n` +
        `📌 قیمت‌ها:\n` +
        `- هر 1GB: ${formatPriceToman(pricePerGb)} تومان\n` +
        `- هر روز: ${formatPriceToman(dayPrice)} تومان\n\n` +
        `💡 نکته: بعد از پرداخت، کانفیگ بر اساس همین حجم و زمان ساخته می‌شود.`;
    const keyboard = [];
    keyboard.push([
        cb("کاهش -", "custom_v2ray_dec_data", "primary"),
        cb(`${gb} گیگابایت`, "noop_custom_gb"),
        cb("افزایش +", "custom_v2ray_inc_data", "primary")
    ]);
    keyboard.push([
        cb("کاهش -", "custom_v2ray_dec_days", "primary"),
        cb(`${days} روز`, "noop_custom_days"),
        cb("افزایش +", "custom_v2ray_inc_days", "primary")
    ]);
    keyboard.push([confirmButton(`custom_v2ray_confirm`, "✅ تایید و پرداخت")]);
    keyboard.push([backButton("buy_menu")]);
    const targetMessageId = Number(messageId || p.messageId || 0);
    if (targetMessageId > 0) {
        await tg("editMessageText", { chat_id: chatId, message_id: targetMessageId, text, reply_markup: { inline_keyboard: keyboard } }).catch((e) => {
            logError("custom_v2ray_edit_failed", e, { userId, chatId, messageId: targetMessageId });
        });
        return;
    }
    const msg = await tg("sendMessage", { chat_id: chatId, text, reply_markup: { inline_keyboard: keyboard } });
    await setState(userId, "custom_v2ray_wizard", { ...p, messageId: Number(msg?.message_id || 0), dataMb, days });
}
async function computeCustomV2rayCheckout(userId) {
    const state = await getState(userId);
    if (!state || state.state !== "custom_v2ray_wizard")
        return null;
    const p = state.payload || {};
    const baseMb = Math.max(1, Math.round(Number(p.baseMb || 0)));
    const baseDays = Math.max(30, Math.round(Number(p.baseDays || 30)));
    const dataMb = Math.max(baseMb, Math.round(Number(p.dataMb || baseMb)));
    const days = Math.max(30, Math.round(Number(p.days || baseDays)));
    const pricePerGb = Math.max(1, Math.round(Number(p.pricePerGb || 500000)));
    const dayPrice = Math.max(0, Math.round(Number(p.dayPrice || 0)));
    const gb = Math.max(1, Math.round(dataMb / 1024));
    const totalPrice = Math.max(1, gb * pricePerGb + days * dayPrice);
    return {
        productId: Number(p.productId),
        baseMb,
        baseDays,
        dataMb,
        days,
        totalPrice
    };
}
async function showCustomWalletUsagePrompt(chatId, userId, totalPrice) {
    const userRows = await sql `SELECT wallet_balance FROM users WHERE telegram_id = ${userId} LIMIT 1;`;
    const walletBalance = userRows.length ? Number(userRows[0].wallet_balance || 0) : 0;
    const maxUsable = Math.min(walletBalance, totalPrice);
    if (maxUsable <= 0) {
        await showCustomPaymentMethods(chatId, userId, totalPrice, 0);
        return;
    }
    const keyboard = [
        [cb(`✅ استفاده از حداکثر ممکن (${formatPriceToman(maxUsable)} تومان)`, `custom_v2ray_use_wallet_${maxUsable}`, "success")],
        [cb("✍️ ورود مبلغ دلخواه", `custom_v2ray_use_wallet_custom`, "primary")],
        [cb("❌ بدون استفاده از کیف پول", `custom_v2ray_use_wallet_0`, "danger")],
        [homeButton()]
    ];
    await tg("sendMessage", {
        chat_id: chatId,
        text: `موجودی کیف پول: ${formatPriceToman(walletBalance)} تومان\nچه مقدار از کیف پول کسر شود؟`,
        reply_markup: { inline_keyboard: keyboard }
    });
}
async function showCustomPaymentMethods(chatId, userId, totalPrice, walletUsed) {
    const userRows = await sql `SELECT wallet_balance FROM users WHERE telegram_id = ${userId} LIMIT 1;`;
    const walletBalance = userRows.length ? Number(userRows[0].wallet_balance || 0) : 0;
    const safeWalletUsed = Math.max(0, Math.min(walletUsed, walletBalance, totalPrice));
    const finalPayable = Math.max(0, totalPrice - safeWalletUsed);
    const rows = await sql `SELECT code, title FROM payment_methods WHERE active = TRUE ORDER BY code ASC;`;
    const callbackBase = await getPublicBaseUrl(env.PUBLIC_BASE_URL);
    const hasCards = (await sql `SELECT 1 FROM cards WHERE active = TRUE LIMIT 1;`).length > 0;
    const hasPlisioKey = Boolean(((await getSetting("plisio_api_key")) || "").trim());
    const hasTetrapayKey = Boolean(((await getSetting("tetrapay_api_key")) || "").trim());
    const hasTronadoKey = Boolean(((await getSetting("tronado_api_key")) || "").trim());
    const hasBusinessWallet = Boolean(((await getSetting("business_wallet_address")) || env.BUSINESS_WALLET_ADDRESS || "").trim());
    const cryptoWalletRows = await sql `
    SELECT id, currency, network, address, rate_mode, rate_toman_per_unit, extra_toman_per_unit, active
    FROM crypto_wallets
    WHERE active = TRUE;
  `;
    const hasCrypto = cryptoWalletRows.map((w) => w).some(cryptoWalletReady);
    const filtered = rows.filter((m) => {
        const code = String(m.code);
        if (code === "card2card")
            return hasCards;
        if (code === "plisio")
            return Boolean(callbackBase) && hasPlisioKey;
        if (code === "tetrapay")
            return Boolean(callbackBase) && hasTetrapayKey;
        if (code === "tronado")
            return Boolean(callbackBase) && hasTronadoKey && hasBusinessWallet;
        if (code === "crypto")
            return hasCrypto;
        return true;
    });
    if (!filtered.length && walletBalance < finalPayable) {
        await tg("sendMessage", { chat_id: chatId, text: "روش پرداختی که به‌درستی تنظیم شده باشد یافت نشد." });
        return;
    }
    const keyboard = [];
    if (safeWalletUsed >= totalPrice) {
        keyboard.push([cb(`💰 پرداخت کامل با کیف پول (${formatPriceToman(totalPrice)} تومان)`, `custom_v2ray_select_pay_wallet_${safeWalletUsed}`, "success")]);
    }
    else {
        for (const m of filtered) {
            keyboard.push([cb(String(m.title), `custom_v2ray_select_pay_${m.code}_${safeWalletUsed}`, "primary")]);
        }
    }
    keyboard.push([homeButton()]);
    await tg("sendMessage", {
        chat_id: chatId,
        text: safeWalletUsed > 0 && safeWalletUsed < totalPrice
            ? `مبلغ ${formatPriceToman(safeWalletUsed)} از کیف پول کسر خواهد شد.\nمبلغ باقیمانده برای پرداخت: ${formatPriceToman(finalPayable)} تومان\nلطفاً روش پرداخت باقیمانده را انتخاب کنید:`
            : "روش پرداخت را انتخاب کنید:",
        reply_markup: { inline_keyboard: keyboard }
    });
}
async function showDiscountChoiceCustom(chatId, productId, paymentMethod, walletUsed) {
    await tg("sendMessage", {
        chat_id: chatId,
        text: "کد تخفیف دارید؟",
        reply_markup: {
            inline_keyboard: [
                [confirmButton(`custom_discount_yes_${productId}_${paymentMethod}_${walletUsed}`, "✅ بله")],
                [cb("❌ ندارم", `custom_discount_no_${productId}_${paymentMethod}_${walletUsed}`, "primary")],
                [homeButton()]
            ]
        }
    });
}
async function showDiscountChoice(chatId, productId, paymentMethod, walletUsed = 0) {
    await tg("sendMessage", {
        chat_id: chatId,
        text: "کد تخفیف دارید؟",
        reply_markup: {
            inline_keyboard: [
                [confirmButton(`discount_yes_${productId}_${paymentMethod}_${walletUsed}`, "✅ بله")],
                [cb("❌ ندارم", `discount_no_${productId}_${paymentMethod}_${walletUsed}`, "primary")],
                [homeButton()]
            ]
        }
    });
}
async function parseAndApplyState(chatId, userId, text, photoFileId, stickerFileId, animationFileId, state) {
    if (state.state === "await_wallet_custom_amount") {
        const productId = Number(state.payload.productId);
        const amount = Number(text.trim());
        if (!Number.isFinite(amount) || amount <= 0) {
            await tg("sendMessage", { chat_id: chatId, text: "مبلغ وارد شده معتبر نیست." });
            return true;
        }
        const userRows = await sql `SELECT wallet_balance FROM users WHERE telegram_id = ${userId} LIMIT 1;`;
        const walletBalance = userRows.length ? Number(userRows[0].wallet_balance || 0) : 0;
        const productRows = await sql `SELECT price_toman FROM products WHERE id = ${productId} LIMIT 1;`;
        if (!productRows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "محصول یافت نشد." });
            return true;
        }
        const productPrice = Number(productRows[0].price_toman || 0);
        if (amount > walletBalance) {
            await tg("sendMessage", { chat_id: chatId, text: `موجودی شما کافی نیست (موجودی فعلی: ${formatPriceToman(walletBalance)} تومان). لطفاً مبلغ کمتری وارد کنید:` });
            return true;
        }
        if (amount > productPrice) {
            await tg("sendMessage", { chat_id: chatId, text: `مبلغ وارد شده از قیمت محصول بیشتر است. حداکثر مبلغ قابل استفاده ${formatPriceToman(productPrice)} تومان است. لطفاً مجدداً وارد کنید:` });
            return true;
        }
        await clearState(userId);
        await showPaymentMethods(chatId, userId, productId, amount);
        return true;
    }
    if (state.state === "await_custom_wallet_amount") {
        const amount = Number(text.trim());
        if (!Number.isFinite(amount) || amount < 0) {
            await tg("sendMessage", { chat_id: chatId, text: "مبلغ وارد شده معتبر نیست." });
            return true;
        }
        const checkout = sanitizePanelConfig(state.payload.checkout);
        const totalPrice = Math.max(1, Math.round(Number(checkout.totalPrice || 0)));
        const productId = Number(checkout.productId || 0);
        if (!Number.isFinite(productId) || productId <= 0) {
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "جلسه سفارش سفارشی منقضی شده. دوباره از اول شروع کن." });
            return true;
        }
        const userRows = await sql `SELECT wallet_balance FROM users WHERE telegram_id = ${userId} LIMIT 1;`;
        const walletBalance = userRows.length ? Number(userRows[0].wallet_balance || 0) : 0;
        if (amount > walletBalance) {
            await tg("sendMessage", { chat_id: chatId, text: `موجودی شما کافی نیست (موجودی فعلی: ${formatPriceToman(walletBalance)} تومان).` });
            return true;
        }
        if (amount > totalPrice) {
            await tg("sendMessage", { chat_id: chatId, text: `مبلغ وارد شده از مبلغ سفارش بیشتر است. حداکثر ${formatPriceToman(totalPrice)} تومان.` });
            return true;
        }
        await clearState(userId);
        await setState(userId, "custom_v2ray_checkout", checkout);
        await showCustomPaymentMethods(chatId, userId, totalPrice, amount);
        return true;
    }
    if (state.state === "await_wallet_charge_amount") {
        const amount = Number(text.trim());
        if (!Number.isFinite(amount) || amount < 10000) {
            await tg("sendMessage", { chat_id: chatId, text: "مبلغ وارد شده معتبر نیست. حداقل مبلغ 10,000 تومان است." });
            return true;
        }
        await setState(userId, "await_wallet_charge_method", { amount });
        const methods = await sql `SELECT code, title FROM payment_methods WHERE active = TRUE;`;
        const callbackBase = await getPublicBaseUrl(env.PUBLIC_BASE_URL);
        const hasCards = (await sql `SELECT 1 FROM cards WHERE active = TRUE LIMIT 1;`).length > 0;
        const hasPlisioKey = Boolean(((await getSetting("plisio_api_key")) || "").trim());
        const hasTetrapayKey = Boolean(((await getSetting("tetrapay_api_key")) || "").trim());
        const hasTronadoKey = Boolean(((await getSetting("tronado_api_key")) || "").trim());
        const hasBusinessWallet = Boolean(((await getSetting("business_wallet_address")) || env.BUSINESS_WALLET_ADDRESS || "").trim());
        const filtered = methods.filter((m) => {
            const code = String(m.code);
            if (code === "crypto")
                return false;
            if (code === "card2card")
                return hasCards;
            if (code === "plisio")
                return Boolean(callbackBase) && hasPlisioKey;
            if (code === "tetrapay")
                return Boolean(callbackBase) && hasTetrapayKey;
            if (code === "tronado")
                return Boolean(callbackBase) && hasTronadoKey && hasBusinessWallet;
            return true;
        });
        const buttons = filtered.map((m) => [cb(String(m.title), `wallet_charge_method_${m.code}`, "primary")]);
        buttons.push([backButton("wallet_menu", "🔙 بازگشت")]);
        await tg("sendMessage", {
            chat_id: chatId,
            text: `مبلغ ${formatPriceToman(amount)} تومان.\nلطفاً روش پرداخت را انتخاب کنید:`,
            reply_markup: { inline_keyboard: buttons }
        });
        return true;
    }
    if (state.state === "await_order_lookup") {
        const purchaseId = text.trim();
        if (!purchaseId || purchaseId.length < 4) {
            await tg("sendMessage", { chat_id: chatId, text: "شناسه سفارش معتبر نیست. دوباره ارسال کن." });
            return true;
        }
        await clearState(userId);
        await showOrderDetails(chatId, userId, purchaseId);
        return true;
    }
    if (state.state === "await_crypto_receipt") {
        if (!photoFileId) {
            await tg("sendMessage", { chat_id: chatId, text: "لطفاً اسکرین‌شات پرداخت را به صورت عکس ارسال کنید." });
            return true;
        }
        const purchaseId = String(state.payload.purchaseId || "").trim();
        if (!purchaseId)
            return true;
        const rows = await sql `
      UPDATE orders
      SET receipt_file_id = ${photoFileId}, status = 'receipt_submitted'
      WHERE purchase_id = ${purchaseId} AND telegram_id = ${userId} AND status != 'denied'
      RETURNING id, purchase_id, product_name_snapshot, final_price, crypto_currency, crypto_network, crypto_amount, crypto_address;
    `;
        await clearState(userId);
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "سفارش یافت نشد یا قابل بروزرسانی نیست." });
            return true;
        }
        const orderId = Number(rows[0].id);
        await tg("sendMessage", { chat_id: chatId, text: "اسکرین‌شات ثبت شد ✅\nبعد از بررسی ادمین، سفارش تکمیل می‌شود." });
        const profileRows = await sql `
      SELECT username, first_name, last_name
      FROM users
      WHERE telegram_id = ${userId}
      LIMIT 1;
    `;
        const tgUsername = profileRows.length && profileRows[0].username ? `@${String(profileRows[0].username)}` : "-";
        const tgFullName = [profileRows[0]?.first_name, profileRows[0]?.last_name].filter(Boolean).join(" ").trim() || "-";
        const caption = `🪙 درخواست تایید پرداخت کریپتو\n` +
            `سفارش: ${rows[0].purchase_id}\n` +
            `کاربر: ${userId}\n` +
            `یوزرنیم: ${tgUsername}\n` +
            `نام: ${tgFullName}\n` +
            `محصول: ${rows[0].product_name_snapshot || "-"}\n` +
            `مبلغ: ${formatPriceToman(Number(rows[0].final_price))} تومان\n` +
            `ارز: ${rows[0].crypto_currency || "-"}\n` +
            `شبکه: ${rows[0].crypto_network || "-"}\n` +
            `مقدار: ${rows[0].crypto_amount || "-"}\n` +
            `آدرس: ${shortAddr(String(rows[0].crypto_address || ""))}`;
        for (const adminId of adminIds) {
            await tg("sendPhoto", {
                chat_id: adminId,
                photo: photoFileId,
                caption,
                reply_markup: {
                    inline_keyboard: [
                        [confirmButton(`crypto_accept_${orderId}`, "✅ تایید")],
                        [cancelButton(`crypto_deny_${orderId}`, "❌ رد")]
                    ]
                }
            }).catch(() => { });
        }
        return true;
    }
    if (state.state === "await_wallet_receipt") {
        if (!photoFileId) {
            await tg("sendMessage", { chat_id: chatId, text: "لطفاً تصویر رسید شارژ را به صورت عکس ارسال کنید." });
            return true;
        }
        const topupId = Number(state.payload.topupId);
        const rows = await sql `
      UPDATE wallet_topups
      SET receipt_file_id = ${photoFileId}, status = 'receipt_submitted'
      WHERE id = ${topupId}
      RETURNING amount;
    `;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "درخواست شارژ یافت نشد." });
            await clearState(userId);
            return true;
        }
        const profileRows = await sql `
      SELECT username, first_name, last_name
      FROM users
      WHERE telegram_id = ${userId}
      LIMIT 1;
    `;
        const username = profileRows.length && profileRows[0].username ? `@${String(profileRows[0].username)}` : "-";
        const fullName = [profileRows[0]?.first_name, profileRows[0]?.last_name].filter(Boolean).join(" ").trim() || "-";
        await clearState(userId);
        for (const adminId of adminIds) {
            try {
                await tg("sendPhoto", {
                    chat_id: adminId,
                    photo: photoFileId,
                    caption: `رسید جدید شارژ کیف پول\n` +
                        `کاربر: ${userId}\n` +
                        `یوزرنیم: ${username}\n` +
                        `نام: ${fullName}\n` +
                        `مبلغ: ${formatPriceToman(Number(rows[0].amount))} تومان`,
                    reply_markup: {
                        inline_keyboard: [
                            [
                                confirmButton(`wallet_accept_${topupId}`, "✅ تایید"),
                                cancelButton(`wallet_deny_${topupId}`, "❌ رد")
                            ]
                        ]
                    }
                });
            }
            catch (error) {
                logError("notify_admin_wallet_receipt_failed", error, { adminId, topupId, userId });
            }
        }
        await tg("sendMessage", { chat_id: chatId, text: "رسید ارسال شد ✅\nپس از بررسی ادمین کیف پول شما شارژ خواهد شد." });
        return true;
    }
    if (state.state === "await_discount_code") {
        const productId = Number(state.payload.productId);
        const paymentMethod = String(state.payload.paymentMethod || "tronado");
        const walletUsed = Number(state.payload.walletUsed || 0);
        await clearState(userId);
        await createOrder(chatId, userId, productId, paymentMethod, text.trim() || null, walletUsed);
        return true;
    }
    if (state.state === "await_custom_discount_code") {
        const productId = Number(state.payload.productId);
        const paymentMethod = String(state.payload.paymentMethod || "tronado");
        const walletUsed = Number(state.payload.walletUsed || 0);
        const checkout = sanitizePanelConfig(state.payload.checkout);
        const totalPrice = Math.max(1, Math.round(Number(checkout.totalPrice || 0)));
        const dataMb = Math.max(1, Math.round(Number(checkout.dataMb || 0)));
        const days = Math.max(30, Math.round(Number(checkout.days || 30)));
        const gb = Math.max(1, Math.round(dataMb / 1024));
        const overrides = {
            basePriceToman: totalPrice,
            panelConfigPatch: { data_limit_mb: dataMb, expire_days: days, force_awaiting_config: true },
            productNameSuffix: `(سفارشی ${gb}GB / ${days} روز)`
        };
        await clearState(userId);
        await createOrder(chatId, userId, productId, paymentMethod, text.trim() || null, walletUsed, overrides);
        return true;
    }
    if (state.state === "await_crypto_receipt") {
        if (!photoFileId) {
            await tg("sendMessage", { chat_id: chatId, text: "لطفاً اسکرین‌شات پرداخت را به صورت عکس ارسال کن." });
            return true;
        }
        const orderId = Number(state.payload.orderId);
        const rows = await sql `
      UPDATE orders
      SET receipt_file_id = ${photoFileId}, status = 'receipt_submitted'
      WHERE id = ${orderId}
        AND telegram_id = ${userId}
        AND status NOT IN ('paid', 'denied', 'cancelled')
      RETURNING id;
    `;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "سفارش یافت نشد یا قابل ثبت رسید نیست." });
            await clearState(userId);
            return true;
        }
        const infoRows = await sql `
      SELECT
        o.id,
        o.purchase_id,
        o.final_price,
        o.wallet_used,
        o.payment_method,
        o.tron_amount,
        o.tronado_token,
        o.tronado_payment_url,
        o.plisio_txn_id,
        o.plisio_invoice_url,
        o.plisio_status,
        COALESCE(o.product_name_snapshot, p.name) AS product_name,
        u.username,
        u.first_name,
        u.last_name
      FROM orders o
      INNER JOIN products p ON p.id = o.product_id
      LEFT JOIN users u ON u.telegram_id = o.telegram_id
      WHERE o.id = ${orderId}
      LIMIT 1;
    `;
        const o = infoRows[0] || {};
        const username = o.username ? `@${String(o.username)}` : "-";
        const fullName = [o.first_name ? String(o.first_name) : "", o.last_name ? String(o.last_name) : ""].filter(Boolean).join(" ").trim() || "-";
        const method = String(o.payment_method || "-");
        const walletUsed = Number(o.wallet_used || 0);
        const extraLines = [];
        if (method === "tronado") {
            extraLines.push(`مقدار TRON: ${String(o.tron_amount || "-")}`);
            if (o.tronado_payment_url)
                extraLines.push(`لینک پرداخت: ${String(o.tronado_payment_url)}`);
        }
        else if (method === "plisio") {
            if (o.plisio_txn_id)
                extraLines.push(`txn: ${String(o.plisio_txn_id)}`);
            if (o.plisio_status)
                extraLines.push(`status: ${String(o.plisio_status)}`);
            if (o.plisio_invoice_url)
                extraLines.push(`لینک پرداخت: ${String(o.plisio_invoice_url)}`);
        }
        else if (method === "tetrapay") {
            if (o.tronado_token)
                extraLines.push(`authority: ${String(o.tronado_token)}`);
            if (o.tronado_payment_url)
                extraLines.push(`لینک پرداخت: ${String(o.tronado_payment_url)}`);
        }
        const caption = `رسید پرداخت کریپتو ارسال شد\n` +
            `سفارش: ${String(o.purchase_id || "-")}\n` +
            `کاربر: ${userId}\n` +
            `یوزرنیم: ${username}\n` +
            `نام: ${fullName}\n` +
            `محصول: ${String(o.product_name || "-")}\n` +
            `مبلغ: ${formatPriceToman(Number(o.final_price || 0))} تومان\n` +
            `روش پرداخت: ${method}` +
            (walletUsed > 0 ? `\nکسر از کیف پول: ${formatPriceToman(walletUsed)} تومان` : "") +
            (extraLines.length ? `\n${extraLines.join("\n")}` : "");
        await clearState(userId);
        for (const adminId of adminIds) {
            try {
                await tg("sendPhoto", {
                    chat_id: adminId,
                    photo: photoFileId,
                    caption,
                    reply_markup: {
                        inline_keyboard: [
                            [
                                { text: "✅ تایید", callback_data: `crypto_accept_${orderId}` },
                                { text: "❌ رد", callback_data: `crypto_deny_${orderId}` },
                                { text: "⛔ بن", callback_data: `crypto_ban_${orderId}_${userId}` }
                            ]
                        ]
                    }
                });
            }
            catch (e) {
                logError("notify_admin_crypto_receipt_failed", e, { adminId, orderId, userId });
            }
        }
        await tg("sendMessage", { chat_id: chatId, text: "اسکرین‌شات ارسال شد ✅\nبعد از بررسی ادمین نتیجه بهت خبر داده میشه." });
        return true;
    }
    if (state.state === "await_receipt") {
        if (!photoFileId) {
            await tg("sendMessage", { chat_id: chatId, text: "لطفاً تصویر رسید را به صورت عکس ارسال کنید." });
            return true;
        }
        const orderId = Number(state.payload.orderId);
        const orderRows = await sql `SELECT wallet_used, product_name_snapshot FROM orders WHERE id = ${orderId} LIMIT 1;`;
        if (!orderRows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "سفارش یافت نشد." });
            await clearState(userId);
            return true;
        }
        const walletUsed = Number(orderRows[0].wallet_used || 0);
        if (walletUsed > 0) {
            const userRows = await sql `SELECT wallet_balance FROM users WHERE telegram_id = ${userId} LIMIT 1;`;
            const walletBalance = Number(userRows[0]?.wallet_balance || 0);
            if (walletBalance < walletUsed) {
                await tg("sendMessage", { chat_id: chatId, text: "موجودی کیف پول شما برای اعمال روی این سفارش کافی نیست. لطفاً سفارش جدیدی ثبت کنید." });
                await sql `UPDATE orders SET status = 'cancelled' WHERE id = ${orderId}`;
                await clearState(userId);
                return true;
            }
        }
        let rows = [];
        try {
            if (walletUsed > 0) {
                rows = await sql `
          WITH deducted AS (
            UPDATE users SET wallet_balance = wallet_balance - ${walletUsed} WHERE telegram_id = ${userId} AND wallet_balance >= ${walletUsed}
            RETURNING telegram_id
          ),
          txn AS (
            INSERT INTO wallet_transactions (telegram_id, amount, type, description, created_at)
            SELECT telegram_id, ${-walletUsed}, 'purchase', ${`کسر بخشی از مبلغ خرید محصول ${orderRows[0].product_name_snapshot}`}, NOW()
            FROM deducted
            RETURNING id
          )
          UPDATE orders
          SET receipt_file_id = ${photoFileId}, status = 'receipt_submitted'
          WHERE id = ${orderId} AND EXISTS (SELECT 1 FROM deducted)
          RETURNING purchase_id, final_price, payment_method, wallet_used;
        `;
            }
            else {
                rows = await sql `
          UPDATE orders
          SET receipt_file_id = ${photoFileId}, status = 'receipt_submitted'
          WHERE id = ${orderId}
          RETURNING purchase_id, final_price, payment_method, wallet_used;
        `;
            }
        }
        catch (e) {
            logError("receipt_submit_transaction_failed", e, { orderId });
            await tg("sendMessage", { chat_id: chatId, text: "خطا در ثبت رسید. لطفاً دوباره تلاش کنید." });
            return true;
        }
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "سفارش یافت نشد یا موجودی کیف پول کافی نیست." });
            await clearState(userId);
            return true;
        }
        const profileRows = await sql `
      SELECT username, first_name, last_name
      FROM users
      WHERE telegram_id = ${userId}
      LIMIT 1;
    `;
        const username = profileRows.length && profileRows[0].username ? `@${String(profileRows[0].username)}` : "-";
        const firstName = profileRows.length && profileRows[0].first_name ? String(profileRows[0].first_name) : "";
        const lastName = profileRows.length && profileRows[0].last_name ? String(profileRows[0].last_name) : "";
        const fullName = [firstName, lastName].filter(Boolean).join(" ").trim() || "-";
        const actualWalletUsed = Number(rows[0].wallet_used || 0);
        const walletUsedText = actualWalletUsed > 0 ? `\nکسر از کیف پول: ${formatPriceToman(actualWalletUsed)} تومان` : "";
        await clearState(userId);
        for (const adminId of adminIds) {
            try {
                await tg("sendPhoto", {
                    chat_id: adminId,
                    photo: photoFileId,
                    caption: `رسید جدید ارسال شد\n` +
                        `سفارش: ${rows[0].purchase_id}\n` +
                        `کاربر: ${userId}\n` +
                        `یوزرنیم: ${username}\n` +
                        `نام: ${fullName}\n` +
                        `مبلغ پرداختی: ${formatPriceToman(Number(rows[0].final_price))} تومان` + walletUsedText,
                    reply_markup: {
                        inline_keyboard: [
                            [
                                confirmButton(`receipt_accept_${orderId}`, "✅ تایید"),
                                cancelButton(`receipt_deny_${orderId}`, "❌ رد"),
                                cb("⛔ بن", `receipt_ban_${orderId}_${userId}`, "danger")
                            ]
                        ]
                    }
                });
            }
            catch (error) {
                logError("notify_admin_receipt_failed", error, { adminId, orderId, userId });
                continue;
            }
        }
        await tg("sendMessage", { chat_id: chatId, text: "رسید ارسال شد ✅\nپس از بررسی ادمین نتیجه اطلاع داده می‌شود." });
        return true;
    }
    if (state.state === "await_topup_receipt") {
        if (!photoFileId) {
            await tg("sendMessage", { chat_id: chatId, text: "لطفاً تصویر رسید افزایش دیتا را به صورت عکس ارسال کنید." });
            return true;
        }
        const topupRequestId = Number(state.payload.topupRequestId);
        const rows = await sql `
      UPDATE topup_requests
      SET receipt_file_id = ${photoFileId}, status = 'receipt_submitted'
      WHERE id = ${topupRequestId}
      RETURNING purchase_id, requested_mb, final_price, inventory_id;
    `;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "درخواست افزایش دیتا یافت نشد." });
            await clearState(userId);
            return true;
        }
        const profileRows = await sql `
      SELECT username, first_name, last_name
      FROM users
      WHERE telegram_id = ${userId}
      LIMIT 1;
    `;
        const username = profileRows.length && profileRows[0].username ? `@${String(profileRows[0].username)}` : "-";
        const firstName = profileRows.length && profileRows[0].first_name ? String(profileRows[0].first_name) : "";
        const lastName = profileRows.length && profileRows[0].last_name ? String(profileRows[0].last_name) : "";
        const fullName = [firstName, lastName].filter(Boolean).join(" ").trim() || "-";
        const cfgRows = await sql `SELECT config_value FROM inventory WHERE id = ${rows[0].inventory_id} LIMIT 1;`;
        const cfgText = String(cfgRows[0]?.config_value || "-");
        await clearState(userId);
        for (const adminId of adminIds) {
            try {
                await tg("sendPhoto", {
                    chat_id: adminId,
                    photo: photoFileId,
                    caption: `رسید جدید افزایش دیتا\n` +
                        `شماره سفارش: ${rows[0].purchase_id}\n` +
                        `کاربر: ${userId}\n` +
                        `یوزرنیم: ${username}\n` +
                        `نام: ${fullName}\n` +
                        `درخواست: ${rows[0].requested_mb}MB\n` +
                        `مبلغ: ${formatPriceToman(Number(rows[0].final_price))} تومان\n` +
                        `کانفیگ:\n${cfgText}`,
                    reply_markup: {
                        inline_keyboard: [
                            [
                                confirmButton(`topup_accept_${topupRequestId}`, "✅ تایید"),
                                cancelButton(`topup_deny_${topupRequestId}`, "❌ رد"),
                                cb("⛔ بن", `topup_ban_${topupRequestId}_${userId}`, "danger")
                            ]
                        ]
                    }
                });
            }
            catch (error) {
                logError("notify_admin_topup_receipt_failed", error, { adminId, topupRequestId, userId });
                continue;
            }
        }
        await tg("sendMessage", { chat_id: chatId, text: "رسید افزایش دیتا ارسال شد ✅\nپس از بررسی ادمین اطلاع می‌دهیم." });
        return true;
    }
    if (state.state === "await_topup_custom_amount") {
        const inventoryId = Number(state.payload.inventoryId);
        const mb = parseDataAmountToMb(text);
        if (!Number.isFinite(inventoryId) || !mb || mb <= 0) {
            await tg("sendMessage", { chat_id: chatId, text: "فرمت حجم معتبر نیست. مثال: 1536 یا 1.5GB یا 800MB" });
            return true;
        }
        await clearState(userId);
        await createTopupCard2CardRequest(chatId, userId, inventoryId, mb);
        return true;
    }
    if (!isAdmin(userId))
        return false;
    if (state.state === "admin_set_start_media") {
        const kind = String(state.payload.kind || "").trim();
        const raw = text.trim();
        if (raw === "-") {
            await setSetting("start_media_kind", "none");
            await setSetting("start_media_value", "");
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "مدیای شروع پاک شد ✅" });
            return true;
        }
        if (kind === "text") {
            if (!raw) {
                await tg("sendMessage", { chat_id: chatId, text: "متن نمی‌تواند خالی باشد." });
                return true;
            }
            await setSetting("start_media_kind", "text");
            await setSetting("start_media_value", raw);
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "ذخیره شد ✅" });
            await tg("sendMessage", { chat_id: chatId, text: raw }).catch(() => { });
            return true;
        }
        if (kind === "sticker") {
            if (!stickerFileId) {
                await tg("sendMessage", { chat_id: chatId, text: "لطفاً استیکر را ارسال کن (نه عکس)." });
                return true;
            }
            await setSetting("start_media_kind", "sticker");
            await setSetting("start_media_value", stickerFileId);
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "ذخیره شد ✅" });
            await tg("sendSticker", { chat_id: chatId, sticker: stickerFileId }).catch(() => { });
            return true;
        }
        if (kind === "animation") {
            if (!animationFileId) {
                await tg("sendMessage", { chat_id: chatId, text: "لطفاً گیف را ارسال کن." });
                return true;
            }
            await setSetting("start_media_kind", "animation");
            await setSetting("start_media_value", animationFileId);
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "ذخیره شد ✅" });
            await tg("sendAnimation", { chat_id: chatId, animation: animationFileId }).catch(() => { });
            return true;
        }
        if (kind === "photo") {
            if (!photoFileId) {
                await tg("sendMessage", { chat_id: chatId, text: "لطفاً عکس را ارسال کن." });
                return true;
            }
            await setSetting("start_media_kind", "photo");
            await setSetting("start_media_value", photoFileId);
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "ذخیره شد ✅" });
            await tg("sendPhoto", { chat_id: chatId, photo: photoFileId }).catch(() => { });
            return true;
        }
        await tg("sendMessage", { chat_id: chatId, text: "نوع مدیا نامعتبر است. از تنظیمات دوباره شروع کن." });
        return true;
    }
    if (state.state === "admin_product_wizard") {
        const mode = String(state.payload.mode || "add");
        const step = String(state.payload.step || "name");
        const raw = text.trim();
        if (step === "name") {
            const name = mode === "edit" && raw === "-" ? String(state.payload.name || "") : raw;
            if (!name) {
                await tg("sendMessage", { chat_id: chatId, text: "نام محصول نمی‌تواند خالی باشد." });
                return true;
            }
            const payload = { ...state.payload, name, step: "product_kind" };
            await setState(userId, "admin_product_wizard", payload);
            await promptProductWizardStep(chatId, payload);
            return true;
        }
        if (step === "size_mb") {
            const productKind = parseProductKind(state.payload.productKind);
            if (productKind === "account") {
                const payload = { ...state.payload, sizeMb: 0, priceMode: "manual", step: "price_mode" };
                await setState(userId, "admin_product_wizard", payload);
                await promptProductWizardStep(chatId, payload);
                return true;
            }
            const sizeMbRaw = mode === "edit" && raw === "-" ? Number(state.payload.sizeMb || 0) : parseDataAmountToMb(raw);
            const sizeMb = Number(sizeMbRaw);
            if (!Number.isFinite(sizeMb) || sizeMb <= 0) {
                await tg("sendMessage", { chat_id: chatId, text: "حجم معتبر بفرستید. مثال: 2048 یا 2GB یا 800MB" });
                return true;
            }
            const payload = { ...state.payload, sizeMb: Math.round(sizeMb), step: "price_mode" };
            await setState(userId, "admin_product_wizard", payload);
            await promptProductWizardStep(chatId, payload);
            return true;
        }
        if (step === "price_toman") {
            const priceToman = mode === "edit" && raw === "-" ? Number(state.payload.priceToman || 0) : Number(raw);
            if (!Number.isFinite(priceToman) || priceToman <= 0) {
                await tg("sendMessage", { chat_id: chatId, text: "قیمت معتبر بفرستید. مثال: 450000" });
                return true;
            }
            const payload = { ...state.payload, priceToman: Math.round(priceToman), step: "sell_mode" };
            await setState(userId, "admin_product_wizard", payload);
            await promptProductWizardStep(chatId, payload);
            return true;
        }
        if (step === "panel_sell_limit") {
            let panelSellLimit = state.payload.panelSellLimit === null || state.payload.panelSellLimit === undefined ? null : Number(state.payload.panelSellLimit);
            if (!(mode === "edit" && raw === "-")) {
                if (!raw || raw === "0") {
                    panelSellLimit = null;
                }
                else {
                    const n = Number(raw);
                    if (!Number.isFinite(n) || n < 0) {
                        await tg("sendMessage", { chat_id: chatId, text: "عدد معتبر بفرستید. مثال: 50 یا 0 برای بدون سقف." });
                        return true;
                    }
                    panelSellLimit = Math.round(n);
                }
            }
            const payload = { ...state.payload, panelSellLimit, step: "panel_delivery_mode" };
            await setState(userId, "admin_product_wizard", payload);
            await promptProductWizardStep(chatId, payload);
            return true;
        }
        if (step === "inbound_id" || step === "protocol" || step === "expire_days" || step === "data_limit_mb") {
            const payload = { ...state.payload };
            const result = await saveProductWizard(payload);
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: result.message });
            if (result.ok)
                await listProductsForAdmin(chatId, userId);
            return true;
        }
        await tg("sendMessage", { chat_id: chatId, text: "برای این مرحله از دکمه‌های پیام قبلی استفاده کنید." });
        return true;
    }
    if (state.state === "admin_card_wizard") {
        const mode = String(state.payload.mode || "add");
        const step = String(state.payload.step || "label");
        const raw = text.trim();
        if (step === "label") {
            const label = mode === "edit" && raw === "-" ? String(state.payload.label || "") : raw;
            if (!label) {
                await tg("sendMessage", { chat_id: chatId, text: "عنوان کارت نمی‌تواند خالی باشد." });
                return true;
            }
            const payload = { ...state.payload, label, step: "card_number" };
            await setState(userId, "admin_card_wizard", payload);
            await promptCardWizardStep(chatId, payload);
            return true;
        }
        if (step === "card_number") {
            const cardNumber = mode === "edit" && raw === "-" ? String(state.payload.cardNumber || "") : raw;
            if (!cardNumber) {
                await tg("sendMessage", { chat_id: chatId, text: "شماره کارت نمی‌تواند خالی باشد." });
                return true;
            }
            const payload = { ...state.payload, cardNumber, step: "holder_name" };
            await setState(userId, "admin_card_wizard", payload);
            await promptCardWizardStep(chatId, payload);
            return true;
        }
        if (step === "holder_name") {
            const holderName = raw === "-" ? "" : mode === "edit" && raw === "-" ? String(state.payload.holderName || "") : raw;
            const payload = { ...state.payload, holderName, step: "bank_name" };
            await setState(userId, "admin_card_wizard", payload);
            await promptCardWizardStep(chatId, payload);
            return true;
        }
        if (step === "bank_name") {
            const bankName = raw === "-" ? "" : mode === "edit" && raw === "-" ? String(state.payload.bankName || "") : raw;
            if (mode === "add") {
                await sql `
          INSERT INTO cards (label, card_number, holder_name, bank_name)
          VALUES (${String(state.payload.label || "")}, ${String(state.payload.cardNumber || "")}, ${String(state.payload.holderName || "") || null}, ${bankName || null});
        `;
                await clearState(userId);
                await tg("sendMessage", { chat_id: chatId, text: "کارت ثبت شد ✅" });
                return true;
            }
            const cardId = Number(state.payload.cardId || 0);
            await sql `
        UPDATE cards
        SET label = ${String(state.payload.label || "")}, card_number = ${String(state.payload.cardNumber || "")}, holder_name = ${String(state.payload.holderName || "") || null}, bank_name = ${bankName || null}
        WHERE id = ${cardId};
      `;
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "کارت ویرایش شد ✅" });
            return true;
        }
        return true;
    }
    if (state.state === "admin_discount_wizard") {
        const mode = String(state.payload.mode || "add");
        const step = String(state.payload.step || "code_mode");
        const raw = text.trim();
        if (step === "code") {
            const code = raw.toUpperCase();
            if (!code) {
                await tg("sendMessage", { chat_id: chatId, text: "کد تخفیف نمی‌تواند خالی باشد." });
                return true;
            }
            const payload = { ...state.payload, code, step: "type" };
            await setState(userId, "admin_discount_wizard", payload);
            await promptDiscountWizardStep(chatId, payload);
            return true;
        }
        if (step === "amount") {
            const amount = mode === "edit" && raw === "-" ? Number(state.payload.amount || 0) : Number(raw);
            if (!Number.isFinite(amount) || amount < 0) {
                await tg("sendMessage", { chat_id: chatId, text: "مقدار معتبر بفرستید." });
                return true;
            }
            const payload = { ...state.payload, amount: Math.round(amount), step: "usage_limit" };
            await setState(userId, "admin_discount_wizard", payload);
            await promptDiscountWizardStep(chatId, payload);
            return true;
        }
        if (step === "usage_limit") {
            let usageLimit;
            if (mode === "edit" && raw === "-") {
                usageLimit = state.payload.usageLimit === null || state.payload.usageLimit === undefined ? null : Number(state.payload.usageLimit);
            }
            else if (!raw || raw === "0") {
                usageLimit = null;
            }
            else {
                const n = Number(raw);
                if (!Number.isFinite(n) || n < 0) {
                    await tg("sendMessage", { chat_id: chatId, text: "سقف مصرف معتبر بفرستید. 0 برای بدون سقف." });
                    return true;
                }
                usageLimit = Math.round(n);
            }
            const type = String(state.payload.type || "").toLowerCase();
            const amount = Number(state.payload.amount || 0);
            if (!["percent", "fixed"].includes(type) || !Number.isFinite(amount)) {
                await tg("sendMessage", { chat_id: chatId, text: "نوع یا مقدار تخفیف نامعتبر است." });
                return true;
            }
            if (mode === "add") {
                const code = String(state.payload.code || "").toUpperCase();
                if (!code) {
                    await tg("sendMessage", { chat_id: chatId, text: "کد تخفیف نامعتبر است." });
                    return true;
                }
                await sql `
          INSERT INTO discounts (code, type, amount, usage_limit)
          VALUES (${code}, ${type}, ${amount}, ${usageLimit});
        `;
                await clearState(userId);
                await tg("sendMessage", { chat_id: chatId, text: `کد تخفیف ساخته شد ✅\nکد: ${code}` });
                return true;
            }
            const id = Number(state.payload.discountId || 0);
            await sql `UPDATE discounts SET type = ${type}, amount = ${amount}, usage_limit = ${usageLimit} WHERE id = ${id};`;
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "تخفیف ویرایش شد ✅" });
            return true;
        }
        await tg("sendMessage", { chat_id: chatId, text: "برای این مرحله از دکمه‌های پیام قبلی استفاده کنید." });
        return true;
    }
    if (state.state === "admin_message_user_wizard") {
        const step = String(state.payload.step || "target");
        const raw = text.trim();
        if (step === "target") {
            if (!raw) {
                await tg("sendMessage", { chat_id: chatId, text: "مخاطب معتبر بفرستید." });
                return true;
            }
            const payload = { ...state.payload, targetRaw: raw, step: "message" };
            await setState(userId, "admin_message_user_wizard", payload);
            await tg("sendMessage", {
                chat_id: chatId,
                text: "ارسال پیام - مرحله 2 از 2\nمتن پیام را بفرستید.",
                reply_markup: { inline_keyboard: [[cancelButton("admin_message_user_wizard_cancel")]] }
            });
            return true;
        }
        const targetRaw = String(state.payload.targetRaw || "");
        const messageText = raw;
        if (!messageText) {
            await tg("sendMessage", { chat_id: chatId, text: "متن پیام نمی‌تواند خالی باشد." });
            return true;
        }
        let targetId = Number(targetRaw);
        if (!Number.isFinite(targetId)) {
            const username = targetRaw.replace("@", "").trim().toLowerCase();
            const rows = await sql `
        SELECT telegram_id
        FROM users
        WHERE LOWER(username) = ${username}
        ORDER BY last_seen_at DESC
        LIMIT 1;
      `;
            if (!rows.length) {
                await tg("sendMessage", { chat_id: chatId, text: "کاربر یافت نشد." });
                return true;
            }
            targetId = Number(rows[0].telegram_id);
        }
        try {
            await tg("sendMessage", { chat_id: targetId, text: messageText });
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "پیام ارسال شد ✅" });
        }
        catch (error) {
            logError("admin_message_user_failed", error, { fromAdminId: userId, targetId });
            await tg("sendMessage", { chat_id: chatId, text: "ارسال پیام انجام نشد. کاربر ممکن است ربات را بلاک کرده باشد." });
        }
        return true;
    }
    if (state.state === "admin_direct_migrate_wizard") {
        const step = String(state.payload.step || "source_inventory_id");
        const raw = text.trim();
        if (step === "source_inventory_id") {
            const sourceInventoryId = Number(raw);
            if (!Number.isFinite(sourceInventoryId) || sourceInventoryId <= 0) {
                await tg("sendMessage", { chat_id: chatId, text: "شناسه inventory معتبر نیست." });
                return true;
            }
            const payload = { ...state.payload, sourceInventoryId, step: "target_panel_id" };
            await setState(userId, "admin_direct_migrate_wizard", payload);
            await promptDirectMigrateTargetPanel(chatId);
            return true;
        }
        if (step === "user_telegram_id") {
            const requestedFor = Number(raw);
            if (!Number.isFinite(requestedFor) || requestedFor <= 0) {
                await tg("sendMessage", { chat_id: chatId, text: "telegram id معتبر نیست." });
                return true;
            }
            const payload = { ...state.payload, requestedFor, step: "config" };
            await setState(userId, "admin_direct_migrate_wizard", payload);
            await tg("sendMessage", {
                chat_id: chatId,
                text: "انتقال مستقیم - مرحله 4 از 4\nاگر کانفیگ جدید دارید بفرستید. برای انتقال با کانفیگ قبلی، - بفرستید.",
                reply_markup: { inline_keyboard: [[cancelButton("admin_direct_migrate_wizard_cancel")]] }
            });
            return true;
        }
        if (step === "config") {
            const sourceInventoryId = Number(state.payload.sourceInventoryId || 0);
            const targetPanelId = Number(state.payload.targetPanelId || 0);
            const requestedFor = Number(state.payload.requestedFor || 0);
            if (!Number.isFinite(sourceInventoryId) || !Number.isFinite(targetPanelId) || !Number.isFinite(requestedFor)) {
                await tg("sendMessage", { chat_id: chatId, text: "اطلاعات انتقال کامل نیست. دوباره تلاش کنید." });
                return true;
            }
            const config = raw === "-" ? "" : raw;
            const ok = await createMigrationRequest(chatId, userId, requestedFor, sourceInventoryId, targetPanelId, "admin");
            if (!ok)
                return true;
            if (config) {
                const row = await sql `
          SELECT id
          FROM panel_migrations
          WHERE source_inventory_id = ${sourceInventoryId}
            AND target_panel_id = ${targetPanelId}
            AND requested_for = ${requestedFor}
            AND status = 'pending'
          ORDER BY id DESC
          LIMIT 1;
        `;
                if (row.length) {
                    const complete = await completeMigration(Number(row[0].id), userId, config);
                    await tg("sendMessage", { chat_id: chatId, text: complete.ok ? "انتقال فوری انجام شد ✅" : `خطا: ${complete.reason}` });
                }
            }
            await clearState(userId);
            return true;
        }
        await tg("sendMessage", { chat_id: chatId, text: "برای این مرحله از دکمه‌های پیام قبلی استفاده کنید." });
        return true;
    }
    if (state.state === "admin_manage_users") {
        const rawInput = text.trim();
        let targetUserId = null;
        let userRows = [];
        // First, try if it's a numeric Telegram ID
        if (/^\d+$/.test(rawInput)) {
            targetUserId = Number(rawInput);
            if (Number.isFinite(targetUserId) && targetUserId > 0) {
                userRows = await sql `
          SELECT telegram_id, username, first_name, last_name, wallet_balance
          FROM users
          WHERE telegram_id = ${targetUserId}
          LIMIT 1;
        `;
            }
        }
        // If not found by ID, try finding by username
        if (!userRows.length) {
            const cleanUsername = rawInput.replace("@", "").toLowerCase();
            userRows = await sql `
        SELECT telegram_id, username, first_name, last_name, wallet_balance
        FROM users
        WHERE LOWER(username) = ${cleanUsername}
        LIMIT 1;
      `;
        }
        if (!userRows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "کاربر در ربات یافت نشد." });
            return true;
        }
        const u = userRows[0];
        const username = u.username ? `@${String(u.username)}` : "-";
        const fullName = [u.first_name ? String(u.first_name) : "", u.last_name ? String(u.last_name) : ""].filter(Boolean).join(" ").trim() || "-";
        const balance = Number(u.wallet_balance || 0);
        // Escape Markdown special characters to fix the "Can't parse entities" error
        const escapeMd = (str) => str.replace(/([_*\[\]()~`>#+\-=|{}.!])/g, "\\$1");
        await clearState(userId);
        await tg("sendMessage", {
            chat_id: chatId,
            text: `👤 *اطلاعات کاربر*\n\nآیدی: \`${u.telegram_id}\`\nیوزرنیم: ${escapeMd(username)}\nنام: ${escapeMd(fullName)}\n\nموجودی کیف پول: ${formatPriceToman(balance)} تومان`,
            parse_mode: "MarkdownV2",
            reply_markup: {
                inline_keyboard: [
                    [
                        cb("➕ افزایش موجودی", `admin_wallet_add_${u.telegram_id}`, "success"),
                        cb("➖ کسر موجودی", `admin_wallet_sub_${u.telegram_id}`, "danger")
                    ],
                    [backButton("admin_panel")]
                ]
            }
        });
        return true;
    }
    if (state.state === "admin_wallet_add") {
        const amount = Number(text.trim());
        const targetUserId = Number(state.payload.targetUserId);
        if (!Number.isFinite(amount) || amount <= 0) {
            await tg("sendMessage", { chat_id: chatId, text: "مبلغ نامعتبر است." });
            return true;
        }
        await sql `
      UPDATE users
      SET wallet_balance = wallet_balance + ${amount}
      WHERE telegram_id = ${targetUserId};
    `;
        await sql `
      INSERT INTO wallet_transactions (telegram_id, amount, type, description)
      VALUES (${targetUserId}, ${amount}, 'admin_add', 'افزایش موجودی توسط مدیریت');
    `;
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: `مبلغ ${formatPriceToman(amount)} تومان با موفقیت به کیف پول کاربر اضافه شد ✅` });
        try {
            await tg("sendMessage", {
                chat_id: targetUserId,
                text: `💰 مبلغ ${formatPriceToman(amount)} تومان توسط مدیریت به کیف پول شما اضافه شد.`
            });
        }
        catch (e) {
            logError("notify_user_wallet_add_failed", e, { targetUserId });
        }
        return true;
    }
    if (state.state === "admin_wallet_sub") {
        const amount = Number(text.trim());
        const targetUserId = Number(state.payload.targetUserId);
        if (!Number.isFinite(amount) || amount <= 0) {
            await tg("sendMessage", { chat_id: chatId, text: "مبلغ نامعتبر است." });
            return true;
        }
        const deducted = await sql `
      UPDATE users
      SET wallet_balance = GREATEST(0, wallet_balance - ${amount})
      WHERE telegram_id = ${targetUserId}
      RETURNING telegram_id;
    `;
        if (deducted.length) {
            await sql `
        INSERT INTO wallet_transactions (telegram_id, amount, type, description)
        VALUES (${targetUserId}, ${-amount}, 'admin_sub', 'کسر موجودی توسط مدیریت');
      `;
        }
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: `مبلغ ${formatPriceToman(amount)} تومان با موفقیت از کیف پول کاربر کسر شد ✅` });
        try {
            await tg("sendMessage", {
                chat_id: targetUserId,
                text: `💸 مبلغ ${formatPriceToman(amount)} تومان توسط مدیریت از کیف پول شما کسر شد.`
            });
        }
        catch (e) {
            logError("notify_user_wallet_sub_failed", e, { targetUserId });
        }
        return true;
    }
    if (state.state === "admin_add_product") {
        const parsed = parseProductInput(text);
        const useAutoPrice = !parsed.priceRaw || parsed.priceRaw.toLowerCase() === "auto";
        const price = useAutoPrice ? await getProductPriceFromSizeMb(parsed.sizeMb) : Number(parsed.priceRaw);
        if (!parsed.name || !Number.isFinite(parsed.sizeMb) || !Number.isFinite(price) || price <= 0) {
            await tg("sendMessage", {
                chat_id: chatId,
                text: "فرمت محصول معتبر نیست.\n" +
                    "قدیمی: نام|حجمMB|قیمت\n" +
                    "جدید:\nname: 2GB ویژه\nsize_mb: 2048\nprice_toman: auto\nsell_mode: panel\npanel_id: 1\npanel_sell_limit: 100\npanel_delivery_mode: both\npanel_config: {\"inbound_id\":1,\"protocol\":\"vless\"}"
            });
            return true;
        }
        if (parsed.sellMode === "panel" && !parsed.panelId) {
            await tg("sendMessage", { chat_id: chatId, text: "برای sell_mode: panel باید panel_id مشخص باشد." });
            return true;
        }
        await sql `
      INSERT INTO products (name, size_mb, price_toman, is_infinite, sell_mode, panel_id, panel_sell_limit, panel_delivery_mode, panel_config)
      VALUES (
        ${parsed.name},
        ${parsed.sizeMb},
        ${price},
        ${parsed.sellMode === "panel" ? true : parsed.isInfinite},
        ${parsed.sellMode},
        ${parsed.sellMode === "panel" ? parsed.panelId : null},
        ${parsed.sellMode === "panel" ? parsed.panelSellLimit : null},
        ${parsed.panelDeliveryMode},
        ${JSON.stringify(parsed.panelConfig)}::jsonb
      )
      ON CONFLICT (name) DO UPDATE SET
        size_mb = EXCLUDED.size_mb,
        price_toman = EXCLUDED.price_toman,
        is_active = TRUE,
        is_infinite = EXCLUDED.is_infinite,
        sell_mode = EXCLUDED.sell_mode,
        panel_id = EXCLUDED.panel_id,
        panel_sell_limit = EXCLUDED.panel_sell_limit,
        panel_delivery_mode = EXCLUDED.panel_delivery_mode,
        panel_config = EXCLUDED.panel_config;
    `;
        await clearState(userId);
        await tg("sendMessage", {
            chat_id: chatId,
            text: `محصول ذخیره شد ✅\n` +
                `قیمت: ${formatPriceToman(price)} تومان (${useAutoPrice ? "خودکار" : "دلخواه"})\n` +
                `حالت فروش: ${parsed.sellMode === "panel" ? "از پنل" : "دستی"}\n` +
                `تحویل: ${parsed.panelDeliveryMode}`
        });
        return true;
    }
    if (state.state === "admin_edit_product") {
        const id = Number(state.payload.productId);
        const currentRows = await sql `
      SELECT name, size_mb, price_toman, is_infinite, sell_mode, panel_id, panel_sell_limit, panel_delivery_mode, panel_config
      FROM products
      WHERE id = ${id}
      LIMIT 1;
    `;
        if (!currentRows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "محصول پیدا نشد." });
            return true;
        }
        const parsed = parseProductInput(text, currentRows[0]);
        const useAutoPrice = !parsed.priceRaw || parsed.priceRaw.toLowerCase() === "auto";
        const price = useAutoPrice ? await getProductPriceFromSizeMb(parsed.sizeMb) : Number(parsed.priceRaw || currentRows[0].price_toman);
        if (!parsed.name || !Number.isFinite(parsed.sizeMb) || !Number.isFinite(price) || price <= 0) {
            await tg("sendMessage", {
                chat_id: chatId,
                text: "فرمت محصول معتبر نیست.\n" +
                    "نمونه:\nname: 2GB ویژه\nsize_mb: 2048\nprice_toman: auto\nsell_mode: panel\npanel_id: 1\npanel_delivery_mode: both"
            });
            return true;
        }
        if (parsed.sellMode === "panel" && !parsed.panelId) {
            await tg("sendMessage", { chat_id: chatId, text: "برای sell_mode: panel باید panel_id مشخص باشد." });
            return true;
        }
        await sql `
      UPDATE products
      SET
        name = ${parsed.name},
        size_mb = ${parsed.sizeMb},
        price_toman = ${price},
        is_infinite = ${parsed.sellMode === "panel" ? true : parsed.isInfinite},
        sell_mode = ${parsed.sellMode},
        panel_id = ${parsed.sellMode === "panel" ? parsed.panelId : null},
        panel_sell_limit = ${parsed.sellMode === "panel" ? parsed.panelSellLimit : null},
        panel_delivery_mode = ${parsed.panelDeliveryMode},
        panel_config = ${JSON.stringify(parsed.panelConfig)}::jsonb
      WHERE id = ${id};
    `;
        await clearState(userId);
        await tg("sendMessage", {
            chat_id: chatId,
            text: `محصول ویرایش شد ✅\n` +
                `قیمت: ${formatPriceToman(price)} تومان (${useAutoPrice ? "خودکار" : "دلخواه"})\n` +
                `حالت فروش: ${parsed.sellMode === "panel" ? "از پنل" : "دستی"}\n` +
                `تحویل: ${parsed.panelDeliveryMode}`
        });
        return true;
    }
    if (state.state === "admin_product_panel_wizard") {
        const step = String(state.payload.step || "panel");
        const raw = text.trim();
        if (step === "sell_limit") {
            let panelSellLimit = state.payload.panelSellLimit === null || state.payload.panelSellLimit === undefined
                ? null
                : Number(state.payload.panelSellLimit);
            if (raw !== "-") {
                if (!raw || raw === "0") {
                    panelSellLimit = null;
                }
                else {
                    const n = Number(raw);
                    if (!Number.isFinite(n) || n < 0) {
                        await tg("sendMessage", { chat_id: chatId, text: "عدد معتبر بفرستید. مثلا 50 یا 0 برای بدون سقف." });
                        return true;
                    }
                    panelSellLimit = Math.round(n);
                }
            }
            const payload = { ...state.payload, panelSellLimit, step: "delivery" };
            await setState(userId, "admin_product_panel_wizard", payload);
            await promptProductPanelWizardStep(chatId, payload);
            return true;
        }
        if (step === "inbound_id") {
            let inboundId = parseMaybeNumber(state.payload.inboundId) ?? 1;
            if (raw !== "-") {
                const n = Number(raw);
                if (!Number.isFinite(n) || n <= 0) {
                    await tg("sendMessage", { chat_id: chatId, text: "inbound_id باید عدد معتبر و بزرگ‌تر از صفر باشد." });
                    return true;
                }
                inboundId = Math.round(n);
            }
            const payload = { ...state.payload, inboundId, step: "protocol" };
            await setState(userId, "admin_product_panel_wizard", payload);
            await promptProductPanelWizardStep(chatId, payload);
            return true;
        }
        if (step === "protocol") {
            const protocol = raw === "-" ? String(state.payload.protocol || "vless").trim() : raw.trim().toLowerCase();
            if (!protocol) {
                await tg("sendMessage", { chat_id: chatId, text: "پروتکل نمی‌تواند خالی باشد." });
                return true;
            }
            const payload = { ...state.payload, protocol, step: "expire_days" };
            await setState(userId, "admin_product_panel_wizard", payload);
            await promptProductPanelWizardStep(chatId, payload);
            return true;
        }
        if (step === "expire_days") {
            let expireDays = parseMaybeNumber(state.payload.expireDays) ?? 30;
            if (raw !== "-") {
                const n = Number(raw);
                if (!Number.isFinite(n) || n < 0) {
                    await tg("sendMessage", { chat_id: chatId, text: "expire_days باید عدد معتبر و صفر یا بیشتر باشد." });
                    return true;
                }
                expireDays = Math.round(n);
            }
            const payload = { ...state.payload, expireDays, step: "data_limit_mb" };
            await setState(userId, "admin_product_panel_wizard", payload);
            await promptProductPanelWizardStep(chatId, payload);
            return true;
        }
        if (step === "data_limit_mb") {
            let dataLimitMb = parseMaybeNumber(state.payload.dataLimitMb) ?? 1024;
            if (raw !== "-") {
                const mb = parseDataAmountToMb(raw);
                if (!mb || mb <= 0) {
                    await tg("sendMessage", { chat_id: chatId, text: "حجم معتبر بفرستید. مثال: 3072 یا 3GB یا 800MB" });
                    return true;
                }
                dataLimitMb = Math.round(mb);
            }
            const payload = { ...state.payload, dataLimitMb };
            const result = await saveProductPanelWizard(payload, false);
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: result.message });
            if (result.ok) {
                await listProductsForAdmin(chatId, userId);
            }
            return true;
        }
        await tg("sendMessage", { chat_id: chatId, text: "برای این مرحله از دکمه‌های پیام قبلی استفاده کنید." });
        return true;
    }
    if (state.state === "admin_add_stock") {
        const productId = Number(state.payload.productId);
        const lines = text.split("\n").map((x) => x.trim()).filter(Boolean);
        if (!lines.length) {
            await tg("sendMessage", { chat_id: chatId, text: "هیچ کانفیگی ارسال نشد." });
            return true;
        }
        for (const line of lines) {
            await sql `INSERT INTO inventory (product_id, config_value) VALUES (${productId}, ${line});`;
        }
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: `${lines.length} کانفیگ اضافه شد ✅` });
        return true;
    }
    if (state.state === "admin_add_card") {
        const parsed = parseCardInput(text);
        if (!parsed.label || !parsed.cardNumber) {
            await tg("sendMessage", {
                chat_id: chatId,
                text: "فرمت کارت معتبر نیست.\nقدیمی: کارت 1|6037...|علی رضایی|ملی\nجدید:\nlabel: کارت 1\ncard_number: 6037...\nholder_name: علی رضایی\nbank_name: ملی"
            });
            return true;
        }
        await sql `
      INSERT INTO cards (label, card_number, holder_name, bank_name)
      VALUES (${parsed.label}, ${parsed.cardNumber}, ${parsed.holderName || null}, ${parsed.bankName || null});
    `;
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: "کارت ثبت شد ✅" });
        return true;
    }
    if (state.state === "admin_edit_card") {
        const cardId = Number(state.payload.cardId);
        const parsed = parseCardInput(text);
        if (!parsed.label || !parsed.cardNumber) {
            await tg("sendMessage", {
                chat_id: chatId,
                text: "فرمت کارت معتبر نیست.\nنمونه:\nlabel: کارت 1\ncard_number: 6037...\nholder_name: علی رضایی\nbank_name: ملی"
            });
            return true;
        }
        await sql `
      UPDATE cards
      SET label = ${parsed.label}, card_number = ${parsed.cardNumber}, holder_name = ${parsed.holderName || null}, bank_name = ${parsed.bankName || null}
      WHERE id = ${cardId};
    `;
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: "کارت ویرایش شد ✅" });
        return true;
    }
    if (state.state === "admin_add_discount") {
        const parsed = parseDiscountInput(text);
        if (!parsed.code || !["percent", "fixed"].includes(parsed.type) || !Number.isFinite(parsed.amount)) {
            await tg("sendMessage", {
                chat_id: chatId,
                text: "فرمت تخفیف معتبر نیست.\n" +
                    "قدیمی: RANDOM|percent|10|100\n" +
                    "جدید:\ncode: RANDOM\ntype: percent\namount: 10\nusage_limit: 100"
            });
            return true;
        }
        await sql `
      INSERT INTO discounts (code, type, amount, usage_limit)
      VALUES (${parsed.code}, ${parsed.type}, ${parsed.amount}, ${parsed.usageLimit});
    `;
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: `کد تخفیف ساخته شد ✅\nکد: ${parsed.code}` });
        return true;
    }
    if (state.state === "admin_edit_discount") {
        const id = Number(state.payload.discountId);
        const parsed = parseDiscountInput(text, "EXISTING");
        if (!["percent", "fixed"].includes(parsed.type) || !Number.isFinite(parsed.amount)) {
            await tg("sendMessage", {
                chat_id: chatId,
                text: "فرمت تخفیف معتبر نیست.\nنمونه:\ntype: percent\namount: 10\nusage_limit: 100"
            });
            return true;
        }
        await sql `
      UPDATE discounts SET type = ${parsed.type}, amount = ${parsed.amount}, usage_limit = ${parsed.usageLimit}
      WHERE id = ${id};
    `;
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: "تخفیف ویرایش شد ✅" });
        return true;
    }
    if (state.state === "admin_set_mandatory_channels") {
        const raw = text.trim();
        if (raw.toLowerCase() === "خاموش") {
            await setSetting("mandatory_channels", "");
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "قفل کانال خاموش شد ✅" });
            return true;
        }
        const channels = raw.split(/[\n,]+/).map(c => c.trim()).filter(Boolean);
        if (!channels.length) {
            await tg("sendMessage", { chat_id: chatId, text: "لیست نامعتبر است. حداقل یک کانال وارد کنید یا بنویسید 'خاموش'." });
            return true;
        }
        await setSetting("mandatory_channels", channels.join(","));
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: `لیست کانال‌های اجباری ذخیره شد ✅\n${channels.join("\n")}` });
        return true;
    }
    if (state.state === "admin_set_support") {
        const username = text.replace("@", "").trim();
        await setSetting("support_username", username);
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: `یوزرنیم پشتیبانی ثبت شد: @${username}` });
        return true;
    }
    if (state.state === "admin_set_wallet") {
        const wallet = text.trim();
        await setSetting("business_wallet_address", wallet);
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: "آدرس کیف پول مقصد ذخیره شد ✅" });
        return true;
    }
    if (state.state === "admin_set_public_base_url") {
        const raw = text.trim();
        if (raw === "-") {
            await setSetting("public_base_url", "");
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "آدرس سایت پاک شد ✅" });
            return true;
        }
        if (!isValidHttpUrl(raw)) {
            await tg("sendMessage", { chat_id: chatId, text: "آدرس معتبر نیست. مثال: https://example.com" });
            return true;
        }
        await setSetting("public_base_url", raw);
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: "آدرس سایت ذخیره شد ✅" });
        return true;
    }
    if (state.state === "admin_set_tronado_api_key") {
        const raw = text.trim();
        await setSetting("tronado_api_key", raw === "-" ? "" : raw);
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: "کلید Tronado ذخیره شد ✅" });
        return true;
    }
    if (state.state === "admin_set_tetrapay_api_key") {
        const raw = text.trim();
        await setSetting("tetrapay_api_key", raw === "-" ? "" : raw);
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: "کلید TetraPay ذخیره شد ✅" });
        return true;
    }
    if (state.state === "admin_set_plisio_api_key") {
        const raw = text.trim();
        await setSetting("plisio_api_key", raw === "-" ? "" : raw);
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: "کلید Plisio ذخیره شد ✅" });
        return true;
    }
    if (state.state === "admin_crypto_wallet_add_other_currency") {
        const currency = text.trim().toUpperCase();
        if (!currency) {
            await tg("sendMessage", { chat_id: chatId, text: "نام ارز معتبر نیست." });
            return true;
        }
        await setState(userId, "admin_crypto_wallet_add_other_network", { currency });
        await tg("sendMessage", { chat_id: chatId, text: "شبکه/بلاکچین را ارسال کنید (مثال: BTC، TRC20، ERC20، TON):" });
        return true;
    }
    if (state.state === "admin_crypto_wallet_add_other_network") {
        const currency = String(state.payload.currency || "").toUpperCase();
        const network = text.trim().toUpperCase();
        if (!currency || !network) {
            await tg("sendMessage", { chat_id: chatId, text: "شبکه معتبر نیست." });
            return true;
        }
        const inserted = await sql `
      INSERT INTO crypto_wallets (currency, network, active)
      VALUES (${currency}, ${network}, FALSE)
      ON CONFLICT (currency, network) DO UPDATE SET currency = EXCLUDED.currency
      RETURNING id;
    `;
        const walletId = Number(inserted[0].id);
        await setState(userId, "admin_crypto_wallet_set_address", { walletId });
        await tg("sendMessage", { chat_id: chatId, text: `آدرس کیف پول ${currency} (${network}) را ارسال کنید.\nبرای پاک‌کردن: -` });
        return true;
    }
    if (state.state === "admin_crypto_wallet_set_address") {
        const walletId = Number(state.payload.walletId);
        const raw = text.trim();
        const address = raw === "-" ? "" : raw;
        await sql `UPDATE crypto_wallets SET address = ${address} WHERE id = ${walletId};`;
        const rows = await sql `SELECT currency, network FROM crypto_wallets WHERE id = ${walletId} LIMIT 1;`;
        await clearState(userId);
        if (rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: `آدرس ذخیره شد ✅\n${String(rows[0].currency)} (${String(rows[0].network)})` });
        }
        else {
            await tg("sendMessage", { chat_id: chatId, text: "آدرس ذخیره شد ✅" });
        }
        return true;
    }
    if (state.state === "admin_crypto_wallet_set_rate") {
        const walletId = Number(state.payload.walletId);
        const raw = text.trim();
        if (raw === "-") {
            await sql `UPDATE crypto_wallets SET rate_toman_per_unit = NULL, rate_mode = 'manual' WHERE id = ${walletId};`;
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "نرخ دستی پاک شد ✅" });
            return true;
        }
        const rate = Math.round(Number(raw));
        if (!Number.isFinite(rate) || rate <= 0) {
            await tg("sendMessage", { chat_id: chatId, text: "عدد معتبر بفرستید. مثال: 65000" });
            return true;
        }
        await sql `UPDATE crypto_wallets SET rate_toman_per_unit = ${rate}, rate_mode = 'manual' WHERE id = ${walletId};`;
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: `نرخ دستی ذخیره شد ✅\n${rate} تومان` });
        return true;
    }
    if (state.state === "admin_crypto_wallet_set_extra") {
        const walletId = Number(state.payload.walletId);
        const raw = text.trim();
        if (raw === "-") {
            await sql `UPDATE crypto_wallets SET extra_toman_per_unit = 0 WHERE id = ${walletId};`;
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "حاشیه پاک شد ✅" });
            return true;
        }
        const extra = Math.round(Number(raw));
        if (!Number.isFinite(extra)) {
            await tg("sendMessage", { chat_id: chatId, text: "عدد معتبر بفرستید. مثال: 2000" });
            return true;
        }
        await sql `UPDATE crypto_wallets SET extra_toman_per_unit = ${extra} WHERE id = ${walletId};`;
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: `حاشیه ذخیره شد ✅\n${extra} تومان` });
        return true;
    }
    if (state.state === "admin_set_plisio_extra_toman") {
        const raw = text.trim();
        if (raw === "-") {
            await setSetting("plisio_usdt_extra_toman", "");
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "حاشیه پاک شد ✅" });
            return true;
        }
        const n = Math.round(Number(raw));
        if (!Number.isFinite(n)) {
            await tg("sendMessage", { chat_id: chatId, text: "عدد معتبر بفرستید. مثال: 2000" });
            return true;
        }
        await setSetting("plisio_usdt_extra_toman", String(n));
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: `حاشیه ذخیره شد ✅\n${n} تومان` });
        return true;
    }
    if (state.state === "admin_set_plisio_fallback_rate") {
        const raw = text.trim();
        if (raw === "-") {
            await setSetting("plisio_usdt_rate_fallback_toman", "");
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "نرخ دستی (fallback) پاک شد ✅" });
            return true;
        }
        const rate = Math.round(Number(raw));
        if (!Number.isFinite(rate) || rate <= 0) {
            await tg("sendMessage", { chat_id: chatId, text: "عدد معتبر بفرستید. مثال: 65000" });
            return true;
        }
        await setSetting("plisio_usdt_rate_fallback_toman", String(rate));
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: `نرخ دستی (fallback) ذخیره شد ✅\n${rate} تومان` });
        return true;
    }
    if (state.state === "admin_set_plisio_usd_rate") {
        const raw = text.trim();
        if (raw === "-") {
            await setSetting("plisio_usd_rate_toman", "");
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "نرخ دلار پاک شد ✅" });
            return true;
        }
        const rate = Math.round(Number(raw));
        if (!Number.isFinite(rate) || rate <= 0) {
            await tg("sendMessage", { chat_id: chatId, text: "عدد معتبر بفرستید. مثال: 60000" });
            return true;
        }
        await setSetting("plisio_usd_rate_toman", String(rate));
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: `نرخ دلار Plisio ذخیره شد ✅\n${rate} تومان` });
        return true;
    }
    if (state.state === "admin_set_topup_price") {
        const pricePerGb = normalizePricePerGb(text.trim());
        if (!Number.isFinite(pricePerGb) || pricePerGb <= 0) {
            await tg("sendMessage", { chat_id: chatId, text: "عدد معتبر بفرستید. مثال: 500000" });
            return true;
        }
        await setSetting("topup_price_per_gb_toman", String(Math.round(pricePerGb)));
        await clearState(userId);
        await tg("sendMessage", {
            chat_id: chatId,
            text: `قیمت افزایش دیتا ثبت شد ✅\nهر 1GB = ${formatPriceToman(Math.round(pricePerGb))} تومان`
        });
        return true;
    }
    if (state.state === "admin_set_product_price") {
        const pricePerGb = normalizePricePerGb(text.trim());
        if (!Number.isFinite(pricePerGb) || pricePerGb <= 0) {
            await tg("sendMessage", { chat_id: chatId, text: "عدد معتبر بفرستید. مثال: 500000" });
            return true;
        }
        await setSetting("product_price_per_gb_toman", String(Math.round(pricePerGb)));
        await clearState(userId);
        await tg("sendMessage", {
            chat_id: chatId,
            text: `قیمت پیشفرض محصولات ثبت شد ✅\nهر 1GB = ${formatPriceToman(Math.round(pricePerGb))} تومان`
        });
        return true;
    }
    if (state.state === "admin_set_custom_v2ray_extra_day") {
        const raw = text.trim();
        const n = Math.round(Number(raw));
        if (!Number.isFinite(n) || n < 0) {
            await tg("sendMessage", { chat_id: chatId, text: "عدد معتبر بفرستید. مثال: 10000\nبرای خاموش: 0" });
            return true;
        }
        await setSetting("custom_v2ray_extra_day_toman", String(n));
        const enabled = await getBoolSetting("custom_v2ray_enabled", false);
        const productId = Number((await getSetting("custom_v2ray_product_id")) || 0);
        if (enabled && Number.isFinite(productId) && productId > 0) {
            const pricePerGb = normalizePricePerGb(await getSetting("product_price_per_gb_toman"), normalizePricePerGb(await getSetting("topup_price_per_gb_toman")));
            const minPrice = Math.max(1, pricePerGb + 30 * Math.max(0, n));
            await sql `UPDATE products SET price_toman = ${minPrice} WHERE id = ${productId};`;
        }
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: `ذخیره شد ✅\nقیمت هر روز: ${formatPriceToman(n)} تومان` });
        return true;
    }
    if (state.state === "admin_ban_username") {
        const username = text.replace("@", "").trim().toLowerCase();
        if (!username) {
            await tg("sendMessage", { chat_id: chatId, text: "یوزرنیم معتبر بفرستید." });
            return true;
        }
        const rows = await sql `
      SELECT telegram_id
      FROM users
      WHERE LOWER(username) = ${username}
      ORDER BY last_seen_at DESC
      LIMIT 1;
    `;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "کاربری با این یوزرنیم پیدا نشد." });
            return true;
        }
        await sql `
      INSERT INTO banned_users (telegram_id, reason, banned_by)
      VALUES (${rows[0].telegram_id}, 'manual_username_ban', ${userId})
      ON CONFLICT (telegram_id) DO UPDATE SET reason = EXCLUDED.reason, banned_by = EXCLUDED.banned_by;
    `;
        try {
            await tg("sendMessage", { chat_id: Number(rows[0].telegram_id), text: "دسترسی شما به دلیل تخلف/سوءاستفاده مسدود شد." });
        }
        catch (error) {
            logError("ban_user_notify_failed", error, { targetUserId: Number(rows[0].telegram_id), by: userId, mode: "username" });
        }
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: `کاربر @${username} بن شد ✅` });
        return true;
    }
    if (state.state === "admin_message_user") {
        const { targetRaw, messageText } = parseAdminMessageInput(text);
        if (!targetRaw || !messageText) {
            await tg("sendMessage", {
                chat_id: chatId,
                text: "فرمت پیام معتبر نیست.\nقدیمی: telegram_id|متن پیام\nجدید:\ntarget: 123456\nmessage: سلام"
            });
            return true;
        }
        if (!messageText) {
            await tg("sendMessage", { chat_id: chatId, text: "متن پیام نمی‌تواند خالی باشد." });
            return true;
        }
        let targetId = Number(targetRaw);
        if (!Number.isFinite(targetId)) {
            const username = targetRaw.replace("@", "").trim().toLowerCase();
            const rows = await sql `
        SELECT telegram_id
        FROM users
        WHERE LOWER(username) = ${username}
        ORDER BY last_seen_at DESC
        LIMIT 1;
      `;
            if (!rows.length) {
                await tg("sendMessage", { chat_id: chatId, text: "کاربر یافت نشد." });
                return true;
            }
            targetId = Number(rows[0].telegram_id);
        }
        try {
            await tg("sendMessage", { chat_id: targetId, text: messageText });
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "پیام ارسال شد ✅" });
        }
        catch (error) {
            logError("admin_message_user_failed", error, { fromAdminId: userId, targetId });
            await tg("sendMessage", { chat_id: chatId, text: "ارسال پیام انجام نشد. کاربر ممکن است ربات را بلاک کرده باشد." });
        }
        return true;
    }
    if (state.state === "admin_lookup_purchase") {
        const purchaseId = text.trim();
        if (!purchaseId) {
            await tg("sendMessage", { chat_id: chatId, text: "شماره سفارش را ارسال کنید." });
            return true;
        }
        await sendPurchaseLookupResult(chatId, purchaseId);
        await clearState(userId);
        return true;
    }
    if (state.state === "admin_lookup_config") {
        const raw = text.trim();
        if (!raw) {
            await tg("sendMessage", { chat_id: chatId, text: "کانفیگ کامل، UUID، نام کاربر (تلگرام یا پنل) یا نام محصول را ارسال کنید." });
            return true;
        }
        const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
        const isUuid = uuidRegex.test(raw);
        const matches = await sql `
      SELECT
        i.id,
        i.panel_id,
        i.owner_telegram_id,
        i.status,
        i.config_value,
        i.delivery_payload,
        p.name AS product_name,
        o.purchase_id,
        u.username AS tg_username,
        u.first_name AS tg_first_name,
        u.last_name AS tg_last_name
      FROM inventory i
      LEFT JOIN products p ON p.id = i.product_id
      LEFT JOIN orders o ON o.id = i.sold_order_id
      LEFT JOIN users u ON u.telegram_id = i.owner_telegram_id
      WHERE (
        (${isUuid} = TRUE AND (i.delivery_payload->'metadata'->>'uuid') = ${raw})
        OR (${isUuid} = FALSE AND (i.config_value = ${raw} OR i.config_value ILIKE ${"%" + raw + "%"}))
        OR (i.config_value ILIKE ${"%" + raw + "%"})
        OR ((i.delivery_payload->>'subscriptionUrl') ILIKE ${"%" + raw + "%"})
        OR (i.delivery_payload::text ILIKE ${"%" + raw + "%"})
        OR (u.username ILIKE ${"%" + raw + "%"})
        OR (u.first_name ILIKE ${"%" + raw + "%"})
        OR (u.last_name ILIKE ${"%" + raw + "%"})
        OR (p.name ILIKE ${"%" + raw + "%"})
      )
      ORDER BY i.id DESC
      LIMIT 10;
    `;
        if (matches.length) {
            const uniqueOwners = Array.from(new Set(matches.map((m) => Number(m.owner_telegram_id || 0)).filter((x) => x > 0)));
            if (uniqueOwners.length === 1) {
                const targetUser = uniqueOwners[0];
                const userRows = await sql `SELECT telegram_id, username, first_name, last_name FROM users WHERE telegram_id = ${targetUser} LIMIT 1;`;
                const u = userRows.length ? userRows[0] : { telegram_id: targetUser, username: null, first_name: null, last_name: null };
                const usernameLine = u.username ? `@${String(u.username)}` : "-";
                const fullName = [u.first_name ? String(u.first_name) : "", u.last_name ? String(u.last_name) : ""].filter(Boolean).join(" ").trim() || "-";
                await tg("sendMessage", {
                    chat_id: chatId,
                    text: `✅ پیدا شد\n` +
                        `👤 کاربر: ${targetUser}\n` +
                        `🆔 یوزرنیم: ${usernameLine}\n` +
                        `📛 نام: ${fullName}\n` +
                        `📦 تعداد مچ: ${matches.length}`,
                    reply_markup: {
                        inline_keyboard: [[{ text: "⛔ بن کاربر", callback_data: `admin_lookup_ban_${targetUser}` }]]
                    }
                });
            }
            else {
                const lines = matches.map((m) => {
                    const owner = Number(m.owner_telegram_id || 0) || "-";
                    const pid = String(m.purchase_id || "-");
                    return `#${m.id} | owner:${owner} | order:${pid} | ${String(m.product_name || "-")}`;
                });
                await tg("sendMessage", {
                    chat_id: chatId,
                    text: `✅ ${matches.length} نتیجه پیدا شد\n` +
                        `مالک یکسان نیست یا تعیین نشده:\n\n${lines.join("\n")}\n\n` +
                        `جزئیات و ابزار هر مورد در پیام‌های بعدی آمده است.`
                });
            }
            const panelCache = new Map();
            for (const row of matches) {
                let payload = parseDeliveryPayload(row.delivery_payload);
                let isPanelConfig = Boolean(payload.metadata?.panelType) && Number(row.panel_id || 0) > 0;
                if (!isPanelConfig && row.config_value) {
                    const foundOnPanel = await lookupIdentifierInPanels(row.config_value);
                    if (foundOnPanel.ok && foundOnPanel.source === "panel") {
                        row.panel_id = foundOnPanel.panelId;
                        payload.metadata = payload.metadata || {};
                        payload.metadata.panelType = foundOnPanel.panelType;
                        if (foundOnPanel.panelType === "marzban") {
                            payload.metadata.username = foundOnPanel.panelUserKey;
                            const userRec = foundOnPanel.panelUser;
                            if (userRec.links && Array.isArray(userRec.links) && userRec.links.length > 0 && !payload.subscriptionUrl) {
                                payload.subscriptionUrl = String(userRec.links[0]);
                            }
                        }
                        else if (foundOnPanel.panelType === "sanaei") {
                            payload.metadata.email = foundOnPanel.panelUserKey;
                            payload.metadata.inboundId = foundOnPanel.inboundId;
                            payload.metadata.uuid = extractUuidFromText(row.config_value);
                        }
                        await sql `
              UPDATE inventory 
              SET panel_id = ${foundOnPanel.panelId}, delivery_payload = ${JSON.stringify(payload)}::jsonb
              WHERE id = ${row.id}
            `;
                        row.delivery_payload = JSON.stringify(payload);
                        isPanelConfig = true;
                    }
                }
                const revoked = payload.metadata?.revoked === true;
                const panelDetails = await buildInventoryPanelRuntimeDetails(Number(row.id), row.panel_id, row.delivery_payload, panelCache);
                const ownerLabel = Number(row.owner_telegram_id || 0) > 0 ? String(Number(row.owner_telegram_id)) : "-";
                const keyboard = [
                    [
                        revoked
                            ? confirmButton(`admin_lookup_toggle_inv_${row.id}`, "✅ فعال‌سازی")
                            : cb("🚫 غیرفعال‌سازی", `admin_lookup_toggle_inv_${row.id}`, "danger"),
                        cb("🗑 حذف کامل", `admin_lookup_delete_inv_${row.id}`, "danger")
                    ],
                    [
                        cb("🔄 بازسازی لینک", `admin_lookup_regen_link_${row.id}`, "primary")
                    ]
                ];
                if (isPanelConfig) {
                    keyboard.push([
                        cb("➕ افزودن دیتا", `admin_lookup_add_data_${row.id}`, "primary"),
                        cb("✏️ تنظیم سقف دیتا", `admin_lookup_set_data_${row.id}`, "primary")
                    ]);
                    keyboard.push([
                        cb("♻️ ریست مصرف", `admin_lookup_reset_data_${row.id}`, "primary"),
                        cb("🔗 لینک‌های مستقیم", `admin_lookup_direct_links_${row.id}`, "primary")
                    ]);
                    keyboard.push([
                        cb("📅 تنظیم انقضا", `admin_lookup_set_expiry_${row.id}`, "primary"),
                        cb("♾️ بدون انقضا", `admin_lookup_set_expiry_${row.id}_0`, "primary")
                    ]);
                }
                let prevConfigsText = "";
                if (payload.previousConfigs && payload.previousConfigs.length > 0) {
                    prevConfigsText = `\n\n🕒 کانفیگ‌های قبلی:\n${payload.previousConfigs.map((c) => escapeHtml(responseSnippet(c, 100))).join("\n")}`;
                }
                await tg("sendMessage", {
                    chat_id: chatId,
                    parse_mode: "HTML",
                    text: `🧾 #${row.id} | ${row.product_name || "-"} | order:${row.purchase_id || "-"}${revoked ? " | 🚫" : ""}\n` +
                        `👤 owner: ${ownerLabel} | وضعیت: ${row.status || "-"}\n` +
                        `${panelDetails ? `${escapeHtml(panelDetails)}\n` : "🖥 پنل: نامشخص\n"}` +
                        `\n${isPanelConfig
                            ? `🔗 ساب:\n${payload.subscriptionUrl ? escapeHtml(String(payload.subscriptionUrl)) : "-"}`
                            : escapeHtml(responseSnippet(String(row.config_value || ""), 220))}${prevConfigsText}`,
                    reply_markup: {
                        inline_keyboard: keyboard
                    }
                });
            }
            await clearState(userId);
            return true;
        }
        const forensicMatches = await sql `
      SELECT
        id,
        inventory_id,
        owner_telegram_id,
        panel_id,
        panel_type,
        panel_user_key,
        uuid,
        event_type,
        config_value,
        created_at
      FROM config_forensics
      WHERE
        (${isUuid} = TRUE AND uuid = ${raw})
        OR (config_value ILIKE ${"%" + raw + "%"})
        OR (panel_user_key ILIKE ${"%" + raw + "%"})
        OR (metadata::text ILIKE ${"%" + raw + "%"})
      ORDER BY created_at DESC
      LIMIT 5;
    `;
        if (forensicMatches.length) {
            const lines = forensicMatches.map((m) => {
                const owner = Number(m.owner_telegram_id || 0) || "-";
                const dateStr = m.created_at ? new Date(m.created_at).toLocaleDateString("fa-IR") : "-";
                return `🔹 رویداد: ${m.event_type} | مالک: ${owner} | تاریخ: ${dateStr}`;
            });
            await tg("sendMessage", {
                chat_id: chatId,
                text: `🔎 سوابق یافت شده در آرشیو:\n\n${lines.join("\n")}\n\n⏳ در حال استعلام مستقیم از پنل...`
            });
        }
        const panelMatch = await lookupIdentifierInPanels(raw);
        if (!panelMatch.ok) {
            await tg("sendMessage", { chat_id: chatId, text: "هیچ موردی در لیست فروش، آرشیو یا پنل پیدا نشد." });
            await clearState(userId);
            return true;
        }
        const targetUser = Number(panelMatch.ownerTelegramId || 0) || null;
        const userRows = targetUser
            ? await sql `SELECT telegram_id, username, first_name, last_name FROM users WHERE telegram_id = ${targetUser} LIMIT 1;`
            : [];
        const u = userRows.length ? userRows[0] : { telegram_id: targetUser, username: null, first_name: null, last_name: null };
        await recordForensicEvent({
            inventoryId: null,
            ownerTelegramId: targetUser,
            productId: null,
            panelId: Number(panelMatch.panelId || 0) || null,
            panelType: String(panelMatch.panelType || ""),
            panelUserKey: String(panelMatch.panelUserKey || ""),
            uuid: extractUuidFromText(raw),
            source: "panel_lookup",
            eventType: "admin_lookup_panel_hit",
            configValue: raw,
            metadata: { panelName: panelMatch.panelName || "", actorAdmin: userId }
        });
        const banBtn = targetUser && Number.isFinite(targetUser)
            ? [{ text: "⛔ بن کاربر", callback_data: `admin_lookup_ban_${targetUser}` }]
            : [{ text: "ℹ️ شناسه کاربر نامشخص", callback_data: "noop_lookup_user_unknown" }];
        const panelKey = encodeURIComponent(String(panelMatch.panelUserKey || ""));
        const panelUser = toJsonObject(panelMatch.panelUser) || {};
        const panelSubscriptionUrl = String(panelUser.subscription_url || panelUser.subscriptionUrl || "").trim() ||
            (String(panelMatch.panelType || "") === "sanaei" && panelUser.subId && panelMatch.panelBaseUrl
                ? buildSanaeiSubscriptionUrl(String(panelMatch.panelBaseUrl), {}, String(panelUser.subId))
                : "");
        const panelRuntimeLine = String(panelMatch.panelType || "") === "marzban"
            ? `📊 مصرف: ${Number(panelUser.data_limit || 0) > 0
                ? `${formatBytesShort(panelUser.used_traffic || panelUser.usedTraffic || 0)} / ${formatBytesShort(panelUser.data_limit)}`
                : "نامحدود"}\n📅 انقضا: ${formatExpiryLabelFromSeconds(panelUser.expire)}`
            : `📊 مصرف: ${Number(panelUser.totalGB || 0) > 0
                ? `${formatBytesShort((Number(panelUser.up || 0) + Number(panelUser.down || 0)) || 0)} / ${formatBytesShort(panelUser.totalGB)}`
                : "نامحدود"}\n📅 انقضا: ${formatExpiryLabelFromMilliseconds(panelUser.expiryTime)}`;
        const panelRevoked = (String(panelMatch.panelType || "") === "marzban" && panelUser.status === "disabled") ||
            (String(panelMatch.panelType || "") === "sanaei" && panelUser.enable === false);
        await tg("sendMessage", {
            chat_id: chatId,
            text: `✅ پیدا شد (Panel Fallback)\n` +
                `🖥 پنل: ${String(panelMatch.panelName || "-")} (${String(panelMatch.panelType || "-")})\n` +
                `🔑 کلید کاربر پنل: ${String(panelMatch.panelUserKey || "-")}\n` +
                `🔗 ساب: ${panelSubscriptionUrl || "-"}\n` +
                `${panelRuntimeLine}\n` +
                `👤 تلگرام: ${targetUser || "-"}\n` +
                `🆔 یوزرنیم: ${u.username ? `@${String(u.username)}` : "-"}\n` +
                `📛 نام: ${[u.first_name ? String(u.first_name) : "", u.last_name ? String(u.last_name) : ""].filter(Boolean).join(" ").trim() || "-"}`,
            reply_markup: {
                inline_keyboard: [
                    banBtn,
                    [
                        panelRevoked
                            ? confirmButton(`admin_panel_toggle_${panelMatch.panelId}_${panelKey}`, "✅ فعال‌سازی")
                            : cb("🚫 غیرفعال‌سازی", `admin_panel_toggle_${panelMatch.panelId}_${panelKey}`, "danger"),
                        cb("🗑 حذف کامل از پنل", `admin_panel_del_${panelMatch.panelId}_${panelKey}`, "danger")
                    ],
                    [
                        cb("🔄 بازسازی لینک", `admin_panel_rv_${panelMatch.panelId}_${panelKey}`, "primary")
                    ],
                    [
                        cb("➕ افزودن دیتا", `admin_panel_add_data_${panelMatch.panelId}_${panelKey}`, "primary"),
                        cb("✏️ تنظیم سقف دیتا", `admin_panel_set_data_${panelMatch.panelId}_${panelKey}`, "primary")
                    ],
                    [cb("♻️ ریست مصرف", `admin_panel_reset_data_${panelMatch.panelId}_${panelKey}`, "primary")],
                    [
                        cb("📅 تنظیم انقضا", `admin_panel_set_expiry_${panelMatch.panelId}_${panelKey}`, "primary"),
                        cb("♾️ بدون انقضا", `admin_panel_set_expiry_${panelMatch.panelId}_${panelKey}_days_0`, "primary")
                    ]
                ]
            }
        });
        await clearState(userId);
        return true;
    }
    if (state.state === "admin_lookup_add_data") {
        const inventoryId = Number(state.payload.inventoryId || 0);
        const addMb = parseDataAmountToMb(text);
        if (!Number.isFinite(inventoryId) || inventoryId <= 0) {
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "شناسه کانفیگ نامعتبر شد." });
            return true;
        }
        if (!addMb || addMb <= 0 || addMb > 1000000) {
            await tg("sendMessage", { chat_id: chatId, text: "مقدار معتبر ارسال کنید. (حداکثر ۱۰۰۰ گیگابایت)" });
            return true;
        }
        const rows = await sql `
      SELECT i.id, i.panel_id, i.delivery_payload
      FROM inventory i
      WHERE i.id = ${inventoryId}
      LIMIT 1;
    `;
        if (!rows.length) {
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "کانفیگ پیدا نشد." });
            return true;
        }
        const row = rows[0];
        const delivery = parseDeliveryPayload(row.delivery_payload);
        const panelType = String(delivery.metadata?.panelType || "");
        const panelId = Number(row.panel_id || 0);
        if (!panelId || !panelType) {
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "این کانفیگ پنلی نیست." });
            return true;
        }
        const panelRows = await sql `
      SELECT id, panel_type, base_url, username, password
      FROM panels
      WHERE id = ${panelId}
      LIMIT 1;
    `;
        if (!panelRows.length) {
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "پنل مرتبط پیدا نشد." });
            return true;
        }
        const addBytes = Math.max(0, Math.round(addMb * 1024 * 1024));
        let result = { ok: false, message: "پنل پشتیبانی نمی‌شود." };
        if (panelType === "marzban") {
            const username = String(delivery.metadata?.username || "").trim();
            if (!username) {
                await clearState(userId);
                await tg("sendMessage", { chat_id: chatId, text: "username پنل در متادیتا پیدا نشد." });
                return true;
            }
            result = await applyTopupOnMarzban(panelRows[0], username, addBytes);
        }
        else if (panelType === "sanaei") {
            const inboundId = parseMaybeNumber(delivery.metadata?.inboundId);
            const email = String(delivery.metadata?.email || "").trim();
            if (!inboundId || !email) {
                await clearState(userId);
                await tg("sendMessage", { chat_id: chatId, text: "inbound/email در متادیتا کانفیگ ناقص است." });
                return true;
            }
            result = await applyTopupOnSanaei(panelRows[0], inboundId, email, addBytes);
        }
        await clearState(userId);
        if (!result.ok) {
            await tg("sendMessage", { chat_id: chatId, text: `افزایش دیتا انجام نشد.\n${result.message}` });
            return true;
        }
        await recordInventoryForensicEvent(inventoryId, "admin_lookup_add_data", { adminId: userId, addMb, panelResult: result.message });
        await tg("sendMessage", { chat_id: chatId, text: `افزایش دیتا انجام شد ✅\nمقدار: ${addMb}MB\n${result.message}` });
        return true;
    }
    if (state.state === "admin_lookup_set_data") {
        const inventoryId = Number(state.payload.inventoryId || 0);
        const raw = text.trim();
        const isInfinite = raw === "0" || parseInfiniteDataFlag(raw);
        const targetMb = isInfinite ? 0 : parseDataAmountToMb(raw);
        if (!Number.isFinite(inventoryId) || inventoryId <= 0) {
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "شناسه کانفیگ نامعتبر شد." });
            return true;
        }
        if (!isInfinite && (!targetMb || targetMb <= 0 || targetMb > 1000000)) {
            await tg("sendMessage", { chat_id: chatId, text: "حجم جدید معتبر نیست. (حداکثر ۱۰۰۰ گیگابایت یا unlimited)" });
            return true;
        }
        const rows = await sql `
      SELECT i.id, i.panel_id, i.delivery_payload
      FROM inventory i
      WHERE i.id = ${inventoryId}
      LIMIT 1;
    `;
        if (!rows.length) {
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "کانفیگ پیدا نشد." });
            return true;
        }
        const row = rows[0];
        const delivery = parseDeliveryPayload(row.delivery_payload);
        const panelType = String(delivery.metadata?.panelType || "");
        const panelId = Number(row.panel_id || 0);
        if (!panelId || !panelType) {
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "این کانفیگ پنلی نیست." });
            return true;
        }
        const panelRows = await sql `
      SELECT id, panel_type, base_url, username, password
      FROM panels
      WHERE id = ${panelId}
      LIMIT 1;
    `;
        if (!panelRows.length) {
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "پنل مرتبط پیدا نشد." });
            return true;
        }
        const targetBytes = isInfinite ? 0 : Math.max(0, Math.round(Number(targetMb || 0) * 1024 * 1024));
        let result = { ok: false, message: "پنل پشتیبانی نمی‌شود." };
        if (panelType === "marzban") {
            const username = String(delivery.metadata?.username || "").trim();
            if (!username) {
                await clearState(userId);
                await tg("sendMessage", { chat_id: chatId, text: "username پنل در متادیتا پیدا نشد." });
                return true;
            }
            result = await applyAdminSetLimitOnlyOnMarzban(panelRows[0], username, targetBytes);
        }
        else if (panelType === "sanaei") {
            const inboundId = parseMaybeNumber(delivery.metadata?.inboundId);
            const email = String(delivery.metadata?.email || "").trim();
            if (!inboundId || !email) {
                await clearState(userId);
                await tg("sendMessage", { chat_id: chatId, text: "inbound/email در متادیتا کانفیگ ناقص است." });
                return true;
            }
            result = await applyAdminSetLimitOnlyOnSanaei(panelRows[0], inboundId, email, targetBytes);
        }
        await clearState(userId);
        if (!result.ok) {
            await tg("sendMessage", { chat_id: chatId, text: `تنظیم سقف دیتا انجام نشد.\n${result.message}` });
            return true;
        }
        await recordInventoryForensicEvent(inventoryId, "admin_lookup_set_data_limit", {
            adminId: userId,
            targetMb: isInfinite ? 0 : targetMb,
            isInfinite,
            panelResult: result.message
        });
        await tg("sendMessage", {
            chat_id: chatId,
            text: `سقف دیتای کانفیگ تنظیم شد ✅\nسقف جدید: ${isInfinite ? "نامحدود" : `${targetMb}MB`}\n${result.message}`
        });
        return true;
    }
    if (state.state === "admin_lookup_set_expiry") {
        const inventoryId = Number(state.payload.inventoryId || 0);
        const days = Math.round(Number(text.trim()));
        if (!Number.isFinite(inventoryId) || inventoryId <= 0) {
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "شناسه کانفیگ نامعتبر شد." });
            return true;
        }
        if (!Number.isFinite(days) || days < 0 || days > 3650) {
            await tg("sendMessage", { chat_id: chatId, text: "عدد معتبر بفرستید. (۰ برای بدون انقضا، حداکثر ۳۶۵۰ روز)" });
            return true;
        }
        const rows = await sql `
      SELECT i.id, i.panel_id, i.delivery_payload
      FROM inventory i
      WHERE i.id = ${inventoryId}
      LIMIT 1;
    `;
        if (!rows.length) {
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "کانفیگ پیدا نشد." });
            return true;
        }
        const row = rows[0];
        const delivery = parseDeliveryPayload(row.delivery_payload);
        const panelType = String(delivery.metadata?.panelType || "");
        const panelId = Number(row.panel_id || 0);
        if (!panelId || !panelType) {
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "این کانفیگ پنلی نیست." });
            return true;
        }
        const panelRows = await sql `
      SELECT id, panel_type, base_url, username, password
      FROM panels
      WHERE id = ${panelId}
      LIMIT 1;
    `;
        if (!panelRows.length) {
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "پنل مرتبط پیدا نشد." });
            return true;
        }
        const expiryTimeMs = days > 0 ? Date.now() + days * 24 * 60 * 60 * 1000 : 0;
        let result = { ok: false, message: "پنل پشتیبانی نمی‌شود." };
        if (panelType === "marzban") {
            const username = String(delivery.metadata?.username || "").trim();
            if (!username) {
                await clearState(userId);
                await tg("sendMessage", { chat_id: chatId, text: "username پنل در متادیتا پیدا نشد." });
                return true;
            }
            result = await applyAdminSetExpiryOnMarzban(panelRows[0], username, expiryTimeMs);
        }
        else if (panelType === "sanaei") {
            const inboundId = parseMaybeNumber(delivery.metadata?.inboundId);
            const email = String(delivery.metadata?.email || "").trim();
            if (!inboundId || !email) {
                await clearState(userId);
                await tg("sendMessage", { chat_id: chatId, text: "inbound/email در متادیتا کانفیگ ناقص است." });
                return true;
            }
            result = await applyAdminSetExpiryOnSanaei(panelRows[0], inboundId, email, expiryTimeMs);
        }
        await clearState(userId);
        if (!result.ok) {
            await tg("sendMessage", { chat_id: chatId, text: `تنظیم انقضا انجام نشد.\n${result.message}` });
            return true;
        }
        await recordInventoryForensicEvent(inventoryId, "admin_lookup_set_expiry", { adminId: userId, days, panelResult: result.message });
        await tg("sendMessage", { chat_id: chatId, text: days > 0 ? `انقضا روی ${days} روز تنظیم شد ✅` : "انقضا حذف شد ✅" });
        return true;
    }
    if (state.state === "admin_panel_add_data") {
        const panelId = Number(state.payload.panelId || 0);
        const panelKey = String(state.payload.panelKey || "").trim();
        const addMb = parseDataAmountToMb(text);
        if (!Number.isFinite(panelId) || panelId <= 0 || !panelKey) {
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "اطلاعات پنل نامعتبر شد." });
            return true;
        }
        if (!addMb || addMb <= 0 || addMb > 1000000) {
            await tg("sendMessage", { chat_id: chatId, text: "مقدار معتبر ارسال کنید. (حداکثر ۱۰۰۰ گیگابایت)" });
            return true;
        }
        const panelRows = await sql `
      SELECT id, panel_type, base_url, username, password
      FROM panels
      WHERE id = ${panelId}
      LIMIT 1;
    `;
        if (!panelRows.length) {
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "پنل مرتبط پیدا نشد." });
            return true;
        }
        const panel = panelRows[0];
        const panelType = String(panel.panel_type || "");
        const addBytes = Math.max(0, Math.round(addMb * 1024 * 1024));
        let result = { ok: false, message: "پنل پشتیبانی نمی‌شود." };
        if (panelType === "marzban") {
            const found = await lookupMarzbanUser(panel, panelKey);
            if (!found.ok || !found.user) {
                await clearState(userId);
                await tg("sendMessage", { chat_id: chatId, text: "کاربر روی پنل پیدا نشد." });
                return true;
            }
            const username = String(found.user.username || panelKey).trim();
            result = await applyTopupOnMarzban(panel, username, addBytes);
        }
        else if (panelType === "sanaei") {
            const found = await findSanaeiClientByIdentifier(panel, panelKey);
            if (!found.ok || !found.client || !found.inboundId) {
                await clearState(userId);
                await tg("sendMessage", { chat_id: chatId, text: "کلاینت روی پنل پیدا نشد." });
                return true;
            }
            const email = String(found.client.email || "").trim();
            if (!email) {
                await clearState(userId);
                await tg("sendMessage", { chat_id: chatId, text: "email کلاینت روی پنل پیدا نشد." });
                return true;
            }
            result = await applyTopupOnSanaei(panel, Number(found.inboundId), email, addBytes);
        }
        await clearState(userId);
        if (!result.ok) {
            await tg("sendMessage", { chat_id: chatId, text: `افزایش دیتا انجام نشد.\n${result.message}` });
            return true;
        }
        await recordForensicEvent({
            inventoryId: null,
            ownerTelegramId: null,
            productId: null,
            panelId,
            panelType,
            panelUserKey: panelKey,
            uuid: extractUuidFromText(panelKey),
            source: "panel_action",
            eventType: "admin_panel_add_data",
            configValue: null,
            metadata: { adminId: userId, addMb, panelResult: result.message }
        });
        await tg("sendMessage", { chat_id: chatId, text: `افزایش دیتا انجام شد ✅\nمقدار: ${addMb}MB` });
        return true;
    }
    if (state.state === "admin_panel_set_data") {
        const panelId = Number(state.payload.panelId || 0);
        const panelKey = String(state.payload.panelKey || "").trim();
        const raw = text.trim();
        const isInfinite = raw === "0" || parseInfiniteDataFlag(raw);
        const targetMb = isInfinite ? 0 : parseDataAmountToMb(raw);
        if (!Number.isFinite(panelId) || panelId <= 0 || !panelKey) {
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "اطلاعات پنل نامعتبر شد." });
            return true;
        }
        if (!isInfinite && (!targetMb || targetMb <= 0 || targetMb > 1000000)) {
            await tg("sendMessage", { chat_id: chatId, text: "حجم جدید معتبر نیست. (حداکثر ۱۰۰۰ گیگابایت یا unlimited)" });
            return true;
        }
        const panelRows = await sql `
      SELECT id, panel_type, base_url, username, password
      FROM panels
      WHERE id = ${panelId}
      LIMIT 1;
    `;
        if (!panelRows.length) {
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "پنل مرتبط پیدا نشد." });
            return true;
        }
        const panel = panelRows[0];
        const panelType = String(panel.panel_type || "");
        const targetBytes = isInfinite ? 0 : Math.max(0, Math.round(Number(targetMb || 0) * 1024 * 1024));
        let result = { ok: false, message: "پنل پشتیبانی نمی‌شود." };
        if (panelType === "marzban") {
            const found = await lookupMarzbanUser(panel, panelKey);
            if (!found.ok || !found.user) {
                await clearState(userId);
                await tg("sendMessage", { chat_id: chatId, text: "کاربر روی پنل پیدا نشد." });
                return true;
            }
            const username = String(found.user.username || panelKey).trim();
            result = await applyAdminSetLimitOnlyOnMarzban(panel, username, targetBytes);
        }
        else if (panelType === "sanaei") {
            const found = await findSanaeiClientByIdentifier(panel, panelKey);
            if (!found.ok || !found.client || !found.inboundId) {
                await clearState(userId);
                await tg("sendMessage", { chat_id: chatId, text: "کلاینت روی پنل پیدا نشد." });
                return true;
            }
            const email = String(found.client.email || "").trim();
            if (!email) {
                await clearState(userId);
                await tg("sendMessage", { chat_id: chatId, text: "email کلاینت روی پنل پیدا نشد." });
                return true;
            }
            result = await applyAdminSetLimitOnlyOnSanaei(panel, Number(found.inboundId), email, targetBytes);
        }
        await clearState(userId);
        if (!result.ok) {
            await tg("sendMessage", { chat_id: chatId, text: `تنظیم سقف دیتا انجام نشد.\n${result.message}` });
            return true;
        }
        await recordForensicEvent({
            inventoryId: null,
            ownerTelegramId: null,
            productId: null,
            panelId,
            panelType,
            panelUserKey: panelKey,
            uuid: extractUuidFromText(panelKey),
            source: "panel_action",
            eventType: "admin_panel_set_data_limit",
            configValue: null,
            metadata: { adminId: userId, targetMb: isInfinite ? 0 : targetMb, isInfinite, panelResult: result.message }
        });
        await tg("sendMessage", { chat_id: chatId, text: `سقف دیتای کاربر تنظیم شد ✅\nسقف جدید: ${isInfinite ? "نامحدود" : `${targetMb}MB`}` });
        return true;
    }
    if (state.state === "admin_panel_set_expiry") {
        const panelId = Number(state.payload.panelId || 0);
        const panelKey = String(state.payload.panelKey || "").trim();
        const days = Math.round(Number(text.trim()));
        if (!Number.isFinite(panelId) || panelId <= 0 || !panelKey) {
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "اطلاعات پنل نامعتبر شد." });
            return true;
        }
        if (!Number.isFinite(days) || days < 0 || days > 3650) {
            await tg("sendMessage", { chat_id: chatId, text: "عدد معتبر بفرستید. (۰ برای بدون انقضا، حداکثر ۳۶۵۰ روز)" });
            return true;
        }
        const panelRows = await sql `
      SELECT id, panel_type, base_url, username, password
      FROM panels
      WHERE id = ${panelId}
      LIMIT 1;
    `;
        if (!panelRows.length) {
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "پنل مرتبط پیدا نشد." });
            return true;
        }
        const panel = panelRows[0];
        const panelType = String(panel.panel_type || "");
        const expiryTimeMs = days > 0 ? Date.now() + days * 24 * 60 * 60 * 1000 : 0;
        let result = { ok: false, message: "پنل پشتیبانی نمی‌شود." };
        if (panelType === "marzban") {
            const found = await lookupMarzbanUser(panel, panelKey);
            if (!found.ok || !found.user) {
                await clearState(userId);
                await tg("sendMessage", { chat_id: chatId, text: "کاربر روی پنل پیدا نشد." });
                return true;
            }
            const username = String(found.user.username || panelKey).trim();
            result = await applyAdminSetExpiryOnMarzban(panel, username, expiryTimeMs);
        }
        else if (panelType === "sanaei") {
            const found = await findSanaeiClientByIdentifier(panel, panelKey);
            if (!found.ok || !found.client || !found.inboundId) {
                await clearState(userId);
                await tg("sendMessage", { chat_id: chatId, text: "کلاینت روی پنل پیدا نشد." });
                return true;
            }
            const email = String(found.client.email || "").trim();
            if (!email) {
                await clearState(userId);
                await tg("sendMessage", { chat_id: chatId, text: "email کلاینت روی پنل پیدا نشد." });
                return true;
            }
            result = await applyAdminSetExpiryOnSanaei(panel, Number(found.inboundId), email, expiryTimeMs);
        }
        await clearState(userId);
        if (!result.ok) {
            await tg("sendMessage", { chat_id: chatId, text: `تنظیم انقضا انجام نشد.\n${result.message}` });
            return true;
        }
        await recordForensicEvent({
            inventoryId: null,
            ownerTelegramId: null,
            productId: null,
            panelId,
            panelType,
            panelUserKey: panelKey,
            uuid: extractUuidFromText(panelKey),
            source: "panel_action",
            eventType: "admin_panel_set_expiry",
            configValue: null,
            metadata: { adminId: userId, days, panelResult: result.message }
        });
        await tg("sendMessage", { chat_id: chatId, text: days > 0 ? `انقضا روی ${days} روز تنظیم شد ✅` : "انقضا حذف شد ✅" });
        return true;
    }
    if (state.state === "admin_config_builder_wizard") {
        const step = String(state.payload.step || "target_user");
        const raw = text.trim();
        if (step === "target_user") {
            const target = await resolveTelegramTargetId(raw);
            if (!target.ok) {
                await tg("sendMessage", { chat_id: chatId, text: target.reason });
                return true;
            }
            const payload = {
                ...state.payload,
                step: "panel",
                targetUserId: target.telegramId,
                targetUsername: target.username || ""
            };
            await setState(userId, "admin_config_builder_wizard", payload);
            await promptAdminConfigBuilderPanel(chatId);
            return true;
        }
        if (step === "name") {
            const payload = {
                ...state.payload,
                step: "data",
                name: raw === "-" ? "" : raw
            };
            await setState(userId, "admin_config_builder_wizard", payload);
            await tg("sendMessage", {
                chat_id: chatId,
                text: "ساخت کانفیگ سفارشی - مرحله 4 از 5\nحجم دیتا را بفرستید (مثال: 2GB یا 2048MB).\nبرای نامحدود: unlimited",
                reply_markup: { inline_keyboard: [[cancelButton("admin_config_builder_cancel")]] }
            });
            return true;
        }
        if (step === "data") {
            const isInfinite = parseInfiniteDataFlag(raw);
            const dataMb = isInfinite ? 0 : parseDataAmountToMb(raw);
            if (!isInfinite && (!dataMb || dataMb <= 0 || dataMb > 1000000)) {
                await tg("sendMessage", { chat_id: chatId, text: "مقدار معتبر ارسال کنید. (حداکثر ۱۰۰۰ گیگابایت یا unlimited)" });
                return true;
            }
            const payload = {
                ...state.payload,
                step: "expiry",
                isInfinite,
                dataMb: isInfinite ? 0 : dataMb
            };
            await setState(userId, "admin_config_builder_wizard", payload);
            await tg("sendMessage", {
                chat_id: chatId,
                text: "ساخت کانفیگ سفارشی - مرحله 5 از 5\nتعداد روز انقضا را بفرستید.\n0 = بدون انقضا",
                reply_markup: { inline_keyboard: [[cancelButton("admin_config_builder_cancel")]] }
            });
            return true;
        }
        if (step === "expiry") {
            const days = Math.round(Number(raw));
            const targetUserId = Number(state.payload.targetUserId || 0);
            const panelId = Number(state.payload.panelId || 0);
            const configName = String(state.payload.name || "");
            const isInfinite = Boolean(state.payload.isInfinite);
            const dataMb = Number(state.payload.dataMb || 0);
            if (!Number.isFinite(days) || days < 0 || days > 3650) {
                await tg("sendMessage", { chat_id: chatId, text: "عدد معتبر بفرستید. (۰ برای بدون انقضا، حداکثر ۳۶۵۰ روز)" });
                return true;
            }
            if (!Number.isFinite(targetUserId) || targetUserId <= 0 || !Number.isFinite(panelId) || panelId <= 0) {
                await clearState(userId);
                await tg("sendMessage", { chat_id: chatId, text: "اطلاعات ساخت ناقص است. دوباره از منوی ابزار شروع کنید." });
                return true;
            }
            const panelRows = await sql `
        SELECT id, panel_type, base_url, username, password
        FROM panels
        WHERE id = ${panelId}
        LIMIT 1;
      `;
            if (!panelRows.length) {
                await clearState(userId);
                await tg("sendMessage", { chat_id: chatId, text: "پنل انتخاب‌شده پیدا نشد." });
                return true;
            }
            const panel = panelRows[0];
            const panelType = String(panel.panel_type || "");
            const effectiveDataMb = isInfinite ? 0 : Math.max(1, Math.round(dataMb || 0));
            const panelConfig = {
                expire_days: days,
                data_limit_mb: effectiveDataMb,
                protocol: "vless",
                username_prefix: "adm",
                email_prefix: "adm"
            };
            if (panelType === "sanaei") {
                const inbound = await resolveFirstSanaeiInboundId(panel);
                if (!inbound.ok) {
                    await clearState(userId);
                    await tg("sendMessage", { chat_id: chatId, text: inbound.reason });
                    return true;
                }
                panelConfig.inbound_id = inbound.inboundId;
                panelConfig.protocol = inbound.protocol;
            }
            const productId = await ensureAdminCustomProductId();
            const purchaseId = `A${Date.now()}${Math.floor(Math.random() * 1000).toString().padStart(3, "0")}`;
            const pseudoOrder = {
                purchase_id: purchaseId,
                telegram_id: targetUserId,
                product_id: productId,
                product_name: configName || "کانفیگ سفارشی ادمین",
                size_mb: effectiveDataMb
            };
            let provision;
            try {
                provision =
                    panelType === "marzban"
                        ? await provisionMarzbanSale(panel, pseudoOrder, panelConfig)
                        : await provisionSanaeiSale(panel, pseudoOrder, panelConfig);
            }
            catch (error) {
                await clearState(userId);
                await tg("sendMessage", {
                    chat_id: chatId,
                    text: `ساخت کانفیگ روی پنل انجام نشد.\n${String(error.message || error)}`
                });
                return true;
            }
            const delivery = parseDeliveryPayload(provision.deliveryPayload);
            const metadata = {
                ...(delivery.metadata || {}),
                label: configName || "",
                isAdminCustom: true,
                customDataMb: effectiveDataMb,
                customInfinite: isInfinite,
                expire_days: days,
                createdByAdmin: userId
            };
            const meta = delivery.metadata;
            const panelUserKey = String(meta?.username || meta?.email || meta?.subId || meta?.uuid || "").trim() || null;
            const inserted = await sql `
        INSERT INTO inventory (product_id, panel_user_key, config_value, delivery_payload, status, owner_telegram_id, panel_id, sold_at)
        VALUES (${productId}, ${panelUserKey}, ${provision.configValue}, ${serializeDeliveryPayload({ ...delivery, metadata })}::jsonb, 'sold', ${targetUserId}, ${panelId}, NOW())
        RETURNING id;
      `;
            await recordInventoryForensicEvent(Number(inserted[0].id), "admin_custom_config_created", {
                adminId: userId,
                targetUserId,
                panelId,
                dataMb: effectiveDataMb,
                isInfinite,
                expireDays: days,
                label: configName || ""
            });
            await clearState(userId);
            try {
                await sendDeliveryPackage(targetUserId, purchaseId, provision.configValue, { ...delivery, metadata }, [[homeButton()]], "🎁 یک کانفیگ جدید برای شما صادر شد.");
            }
            catch (error) {
                logError("admin_custom_config_send_failed", error, { targetUserId, by: userId, inventoryId: Number(inserted[0].id) });
            }
            await tg("sendMessage", {
                chat_id: chatId,
                text: `کانفیگ سفارشی ساخته شد ✅\n` +
                    `شناسه inventory: ${inserted[0].id}\n` +
                    `کاربر: ${targetUserId}\n` +
                    `پنل: #${panelId}\n` +
                    `حجم: ${isInfinite ? "نامحدود" : `${effectiveDataMb}MB`}\n` +
                    `انقضا: ${days > 0 ? `${days} روز` : "بدون انقضا"}`
            });
            return true;
        }
        await tg("sendMessage", { chat_id: chatId, text: "برای این مرحله از دکمه‌های پیام قبلی استفاده کنید." });
        return true;
    }
    if (state.state === "admin_unban_user") {
        const target = Math.round(Number(text.trim()));
        if (!Number.isFinite(target) || target <= 0) {
            await tg("sendMessage", { chat_id: chatId, text: "telegram_id معتبر بفرستید." });
            return true;
        }
        const deleted = await sql `DELETE FROM banned_users WHERE telegram_id = ${target} RETURNING telegram_id;`;
        await clearState(userId);
        if (!deleted.length) {
            await tg("sendMessage", { chat_id: chatId, text: "این کاربر در لیست بن‌شده‌ها نیست." });
            return true;
        }
        try {
            await tg("sendMessage", { chat_id: target, text: "دسترسی شما رفع مسدودیت شد ✅" });
        }
        catch (error) {
            logError("unban_user_notify_failed", error, { targetUserId: target, by: userId });
        }
        await tg("sendMessage", { chat_id: chatId, text: `کاربر ${target} آنبن شد ✅` });
        return true;
    }
    if (state.state === "admin_inv_rename") {
        const inventoryId = Number(state.payload.inventoryId);
        const name = text.trim();
        if (!Number.isFinite(inventoryId) || inventoryId <= 0) {
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "شناسه کانفیگ نامعتبر شد." });
            return true;
        }
        const label = name === "-" ? "" : name;
        await sql `
      UPDATE inventory
      SET delivery_payload = jsonb_set(
        jsonb_set(COALESCE(delivery_payload, '{}'::jsonb), '{metadata}', COALESCE(delivery_payload->'metadata', '{}'::jsonb), true),
        '{metadata,label}',
        to_jsonb(${label}::text),
        true
      )
      WHERE id = ${inventoryId};
    `;
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: "نام ذخیره شد ✅" });
        return true;
    }
    if (state.state === "admin_provide_config") {
        const orderId = Number(state.payload.orderId);
        const orderRows = await sql `
      SELECT id, purchase_id, telegram_id, product_id
      FROM orders
      WHERE id = ${orderId}
      LIMIT 1;
    `;
        if (!orderRows.length) {
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "سفارش یافت نشد." });
            return true;
        }
        const order = orderRows[0];
        const inserted = await sql `
      INSERT INTO inventory (product_id, config_value, status, owner_telegram_id, sold_order_id, sold_at)
      VALUES (${order.product_id}, ${text}, 'sold', ${order.telegram_id}, ${order.id}, NOW())
      RETURNING id;
    `;
        await sql `
      UPDATE orders
      SET status = 'paid', paid_at = COALESCE(paid_at, NOW()), inventory_id = ${inserted[0].id}
      WHERE id = ${order.id};
    `;
        await recordInventoryForensicEvent(Number(inserted[0].id), "sale_delivered_manual", {
            purchaseId: String(order.purchase_id),
            by: userId
        });
        await clearState(userId);
        const profile = await getTelegramProfileText(Number(order.telegram_id));
        const productRows = await sql `SELECT name FROM products WHERE id = ${Number(order.product_id)} LIMIT 1;`;
        const productName = productRows.length ? String(productRows[0].name || `#${Number(order.product_id)}`) : `#${Number(order.product_id)}`;
        await sendDeliveryPackage(Number(order.telegram_id), String(order.purchase_id), String(text), { configLinks: [String(text)] }, [[homeButton()]]);
        await notifyAdmins(buildAdminDeliverySummary({
            purchaseId: String(order.purchase_id),
            userId: Number(order.telegram_id),
            telegramUsername: profile.username,
            telegramFullName: profile.fullName,
            productName,
            deliveryPayload: {}
        }), { inline_keyboard: [[{ text: "🔎 باز کردن سفارش", callback_data: `admin_open_purchase_${String(order.purchase_id)}` }]] });
        await tg("sendMessage", { chat_id: chatId, text: "کانفیگ برای کاربر ارسال شد ✅" });
        return true;
    }
    if (state.state === "admin_panel_wizard") {
        const mode = String(state.payload.mode || "add");
        const step = String(state.payload.step || "name");
        const panelId = Number(state.payload.panelId || 0);
        const panelType = parsePanelType(String(state.payload.panelType || ""));
        const currentName = String(state.payload.name || "");
        const currentBaseUrl = String(state.payload.baseUrl || "");
        const currentUsername = String(state.payload.username || "");
        const currentPassword = String(state.payload.password || "");
        if (!panelType) {
            await clearState(userId);
            await tg("sendMessage", { chat_id: chatId, text: "نوع پنل نامعتبر شد. دوباره تلاش کنید." });
            return true;
        }
        const raw = text.trim();
        if (step === "name") {
            const name = mode === "edit" && raw === "-" ? currentName : raw;
            if (!name) {
                await tg("sendMessage", { chat_id: chatId, text: "نام پنل نمی‌تواند خالی باشد." });
                return true;
            }
            const payload = {
                ...state.payload,
                step: "base_url",
                name
            };
            await setState(userId, "admin_panel_wizard", payload);
            await promptPanelWizardStep(chatId, payload);
            return true;
        }
        if (step === "base_url") {
            const baseUrl = mode === "edit" && raw === "-" ? currentBaseUrl : normalizeBaseUrl(raw);
            if (!baseUrl || !isValidHttpUrl(baseUrl)) {
                await tg("sendMessage", { chat_id: chatId, text: "آدرس پنل معتبر نیست. نمونه: https://panel.example.com" });
                return true;
            }
            const payload = {
                ...state.payload,
                step: "username",
                baseUrl
            };
            await setState(userId, "admin_panel_wizard", payload);
            await promptPanelWizardStep(chatId, payload);
            return true;
        }
        if (step === "username") {
            const username = mode === "edit" && raw === "-" ? currentUsername : raw;
            if (!username) {
                await tg("sendMessage", { chat_id: chatId, text: "نام کاربری پنل الزامی است." });
                return true;
            }
            const payload = {
                ...state.payload,
                step: "password",
                username
            };
            await setState(userId, "admin_panel_wizard", payload);
            await promptPanelWizardStep(chatId, payload);
            return true;
        }
        if (step === "password") {
            const password = mode === "edit" && raw === "-" ? currentPassword : raw;
            const name = String(state.payload.name || "");
            const baseUrl = String(state.payload.baseUrl || "");
            const username = String(state.payload.username || "");
            if (!password) {
                await tg("sendMessage", { chat_id: chatId, text: "رمز عبور پنل الزامی است." });
                return true;
            }
            try {
                if (mode === "add") {
                    await sql `
            INSERT INTO panels (name, panel_type, base_url, username, password)
            VALUES (${name}, ${panelType}, ${baseUrl}, ${username}, ${password})
            ON CONFLICT (name) DO UPDATE
            SET panel_type = EXCLUDED.panel_type, base_url = EXCLUDED.base_url, username = EXCLUDED.username, password = EXCLUDED.password;
          `;
                    const idRows = await sql `SELECT id FROM panels WHERE name = ${name} LIMIT 1;`;
                    await clearState(userId);
                    if (!idRows.length) {
                        await tg("sendMessage", { chat_id: chatId, text: "پنل ذخیره شد ✅" });
                        return true;
                    }
                    const savedPanelId = Number(idRows[0].id);
                    const test = await testPanelConnection(savedPanelId);
                    logInfo("panel_saved", { panelId: savedPanelId, panelType, name, baseUrl, testOk: test.ok });
                    await showPanelDetails(chatId, savedPanelId, `پنل ذخیره شد ✅\n${test.message}`);
                    return true;
                }
                if (!Number.isFinite(panelId) || panelId <= 0) {
                    await clearState(userId);
                    await tg("sendMessage", { chat_id: chatId, text: "شناسه پنل معتبر نیست." });
                    return true;
                }
                await sql `
          UPDATE panels
          SET name = ${name}, panel_type = ${panelType}, base_url = ${baseUrl}, username = ${username}, password = ${password}
          WHERE id = ${panelId};
        `;
                await clearState(userId);
                const test = await testPanelConnection(panelId);
                logInfo("panel_updated", { panelId, panelType, name, baseUrl, testOk: test.ok });
                await showPanelDetails(chatId, panelId, `اطلاعات پنل بروزرسانی شد ✅\n${test.message}`);
                return true;
            }
            catch (error) {
                await clearState(userId);
                if (mode === "add") {
                    logError("panel_save_failed", error, { panelType, name, baseUrl, userId });
                    await tg("sendMessage", {
                        chat_id: chatId,
                        text: `ذخیره پنل انجام نشد.\n${String(error.message || error)}`
                    });
                    return true;
                }
                logError("panel_update_failed", error, { panelId, panelType, name, baseUrl, userId });
                await tg("sendMessage", {
                    chat_id: chatId,
                    text: `بروزرسانی پنل انجام نشد.\n${String(error.message || error)}`
                });
                return true;
            }
        }
        return true;
    }
    if (state.state === "admin_panel_add") {
        const [typeRaw, nameRaw, baseUrlRaw, usernameRaw, passwordRaw] = text.split("|").map((x) => x.trim());
        const panelType = parsePanelType(typeRaw || "");
        const name = nameRaw || "";
        const baseUrl = normalizeBaseUrl(baseUrlRaw || "");
        const username = usernameRaw || "";
        const password = passwordRaw || "";
        if (!panelType || !name || !baseUrl || !isValidHttpUrl(baseUrl)) {
            await tg("sendMessage", {
                chat_id: chatId,
                text: "فرمت صحیح نیست. نمونه:\nmarzban|Main Panel|https://panel.example.com|admin|pass"
            });
            return true;
        }
        if (!username || !password) {
            await tg("sendMessage", {
                chat_id: chatId,
                text: "برای افزودن پنل، نام کاربری و رمز عبور الزامی است."
            });
            return true;
        }
        try {
            await sql `
        INSERT INTO panels (name, panel_type, base_url, username, password)
        VALUES (${name}, ${panelType}, ${baseUrl}, ${username}, ${password})
        ON CONFLICT (name) DO UPDATE
        SET panel_type = EXCLUDED.panel_type, base_url = EXCLUDED.base_url, username = EXCLUDED.username, password = EXCLUDED.password;
      `;
            const idRows = await sql `SELECT id FROM panels WHERE name = ${name} LIMIT 1;`;
            await clearState(userId);
            if (idRows.length) {
                const panelId = Number(idRows[0].id);
                const test = await testPanelConnection(panelId);
                await tg("sendMessage", { chat_id: chatId, text: `پنل ذخیره شد ✅\n${test.message}` });
                logInfo("panel_saved", { panelId, panelType, name, baseUrl, testOk: test.ok });
                return true;
            }
            await tg("sendMessage", { chat_id: chatId, text: "پنل ذخیره شد ✅" });
            return true;
        }
        catch (error) {
            logError("panel_save_failed", error, { panelType, name, baseUrl, userId });
            await tg("sendMessage", {
                chat_id: chatId,
                text: `ذخیره پنل انجام نشد.\n${String(error.message || error)}`
            });
            return true;
        }
    }
    if (state.state === "admin_panel_edit") {
        const panelId = Number(state.payload.panelId);
        const [nameRaw, baseUrlRaw, usernameRaw, passwordRaw] = text.split("|").map((x) => x.trim());
        const name = nameRaw || "";
        const baseUrl = normalizeBaseUrl(baseUrlRaw || "");
        const username = usernameRaw || "";
        const password = passwordRaw || "";
        if (!Number.isFinite(panelId) || panelId <= 0 || !name || !baseUrl || !isValidHttpUrl(baseUrl)) {
            await tg("sendMessage", { chat_id: chatId, text: "فرمت صحیح: نام|base_url|username|password" });
            return true;
        }
        if (!username || !password) {
            await tg("sendMessage", { chat_id: chatId, text: "نام کاربری و رمز عبور پنل الزامی است." });
            return true;
        }
        try {
            await sql `
        UPDATE panels
        SET name = ${name}, base_url = ${baseUrl}, username = ${username}, password = ${password}
        WHERE id = ${panelId};
      `;
            await clearState(userId);
            const test = await testPanelConnection(panelId);
            await tg("sendMessage", { chat_id: chatId, text: `اطلاعات پنل بروزرسانی شد ✅\n${test.message}` });
            logInfo("panel_updated", { panelId, name, baseUrl, testOk: test.ok });
            return true;
        }
        catch (error) {
            logError("panel_update_failed", error, { panelId, name, baseUrl, userId });
            await tg("sendMessage", {
                chat_id: chatId,
                text: `بروزرسانی پنل انجام نشد.\n${String(error.message || error)}`
            });
            return true;
        }
    }
    if (state.state === "admin_complete_migration_config") {
        const migrationId = Number(state.payload.migrationId);
        const result = await completeMigration(migrationId, userId, text.trim());
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: result.ok ? "انتقال تکمیل شد ✅" : `خطا: ${result.reason}` });
        return true;
    }
    if (state.state === "admin_direct_migrate") {
        const { sourceInventoryId, targetPanelId, requestedFor, config } = parseDirectMigrateInput(text);
        if (!Number.isFinite(sourceInventoryId) || !Number.isFinite(targetPanelId) || !Number.isFinite(requestedFor)) {
            await tg("sendMessage", {
                chat_id: chatId,
                text: "فرمت مهاجرت مستقیم معتبر نیست.\n" +
                    "قدیمی: inventory_id|target_panel_id|user_telegram_id|config\n" +
                    "جدید:\nsource_inventory_id: 12\ntarget_panel_id: 3\nuser_telegram_id: 123456\nconfig: optional"
            });
            return true;
        }
        const ok = await createMigrationRequest(chatId, userId, requestedFor, sourceInventoryId, targetPanelId, "admin");
        if (!ok)
            return true;
        if (config) {
            const row = await sql `
        SELECT id
        FROM panel_migrations
        WHERE source_inventory_id = ${sourceInventoryId}
          AND target_panel_id = ${targetPanelId}
          AND requested_for = ${requestedFor}
          AND status = 'pending'
        ORDER BY id DESC
        LIMIT 1;
      `;
            if (row.length) {
                const complete = await completeMigration(Number(row[0].id), userId, config);
                await tg("sendMessage", { chat_id: chatId, text: complete.ok ? "انتقال فوری انجام شد ✅" : `خطا: ${complete.reason}` });
            }
        }
        await clearState(userId);
        return true;
    }
    return false;
}
async function resolveDiscount(code, basePrice) {
    if (!code)
        return { discountAmount: 0, discountCode: null };
    const rows = await sql `
    SELECT id, code, type, amount, usage_limit, used_count, active
    FROM discounts
    WHERE code = ${code.toUpperCase()} AND active = TRUE
    LIMIT 1;
  `;
    if (!rows.length)
        return { discountAmount: 0, discountCode: null };
    const d = rows[0];
    if (d.usage_limit !== null && Number(d.used_count) >= Number(d.usage_limit)) {
        return { discountAmount: 0, discountCode: null };
    }
    const discountAmount = d.type === "percent" ? Math.floor((basePrice * Number(d.amount)) / 100) : Number(d.amount);
    return { discountAmount: Math.max(0, Math.min(discountAmount, basePrice)), discountCode: String(d.code) };
}
async function getProductPriceFromSizeMb(sizeMb) {
    const productRateRaw = await getSetting("product_price_per_gb_toman");
    const fallbackRateRaw = await getSetting("topup_price_per_gb_toman");
    const rate = normalizePricePerGb(productRateRaw || fallbackRateRaw || "500000");
    return Math.max(1, Math.ceil((sizeMb / 1024) * rate));
}
async function sendConfigWithQr(chatId, purchaseId, configValue, keyboard, prefixText) {
    const captionLines = [
        prefixText ? prefixText : null,
        `شناسه خرید: ${purchaseId}`,
        `کانفیگ:\n${configValue}`
    ].filter(Boolean);
    await tg("sendPhoto", {
        chat_id: chatId,
        photo: qrCodeUrl(configValue),
        parse_mode: "HTML",
        caption: escapeHtml(truncateText(captionLines.join("\n\n"), 900)),
        reply_markup: { inline_keyboard: keyboard }
    });
}
async function sendDeliveryPackage(chatId, purchaseId, fallbackConfigValue, deliveryPayload, keyboard, prefixText) {
    const configLinks = deliveryPayload.configLinks || [];
    const hasManyConfigs = configLinks.length > 1;
    const firstConfig = configLinks.length ? configLinks[0] : fallbackConfigValue || "";
    const finalKeyboard = keyboard.map((row) => [...row]);
    if (hasManyConfigs && purchaseId && purchaseId !== "-") {
        finalKeyboard.unshift([{ text: "📃 نمایش بقیه کانفیگ‌ها", callback_data: `show_configs_${purchaseId}_1` }]);
    }
    const captionLines = [
        prefixText ? prefixText : null,
        `شناسه خرید: ${purchaseId}`,
        deliveryPayload.subscriptionUrl ? `لینک ساب:\n${deliveryPayload.subscriptionUrl}` : null,
        firstConfig ? `کانفیگ:\n${firstConfig}` : null,
        hasManyConfigs ? `(${configLinks.length - 1} کانفیگ دیگر هم موجود است)` : null
    ].filter(Boolean);
    const qrText = String(firstConfig || deliveryPayload.subscriptionUrl || "").trim();
    if (!qrText) {
        await tg("sendMessage", {
            chat_id: chatId,
            parse_mode: "HTML",
            text: escapeHtml(captionLines.join("\n\n")),
            reply_markup: { inline_keyboard: finalKeyboard }
        });
        return;
    }
    await tg("sendPhoto", {
        chat_id: chatId,
        photo: qrCodeUrl(qrText),
        parse_mode: "HTML",
        caption: escapeHtml(truncateText(captionLines.join("\n\n"), 900)),
        reply_markup: { inline_keyboard: finalKeyboard }
    });
}
function buildAdminDeliverySummary(params) {
    const meta = params.deliveryPayload.metadata || {};
    const username = typeof meta.username === "string" ? meta.username : null;
    const email = typeof meta.email === "string" ? meta.email : null;
    const uuid = typeof meta.uuid === "string" ? meta.uuid : null;
    const days = typeof meta.expire_days === "number" ? meta.expire_days : null;
    const lines = [
        "✅ سفارش تحویل شد",
        `شناسه خرید: ${params.purchaseId}`,
        `کاربر: ${params.userId}`,
        `یوزرنیم: ${params.telegramUsername}`,
        `نام: ${params.telegramFullName}`,
        `محصول: ${params.productName}`,
        params.walletUsed ? `کسر از کیف پول: ${formatPriceToman(params.walletUsed)} تومان` : null,
        params.deliveryPayload.subscriptionUrl ? `لینک ساب:\n${params.deliveryPayload.subscriptionUrl}` : null,
        username ? `username: ${username}` : null,
        email ? `email: ${email}` : null,
        uuid ? `uuid: ${uuid}` : null,
        days !== null ? `expire_days: ${days}` : null
    ].filter(Boolean);
    return lines.join("\n\n");
}
async function createTopupCard2CardRequest(chatId, userId, inventoryId, mb) {
    const ownRows = await sql `
    SELECT i.id, i.config_value, p.price_toman, p.size_mb
    FROM inventory i
    INNER JOIN products p ON p.id = i.product_id
    WHERE i.id = ${inventoryId} AND i.owner_telegram_id = ${userId} AND i.status = 'sold'
    LIMIT 1;
  `;
    if (!ownRows.length || !Number.isFinite(mb) || mb <= 0) {
        await tg("sendMessage", { chat_id: chatId, text: "درخواست معتبر نیست." });
        return;
    }
    const rateSetting = await getSetting("topup_price_per_gb_toman");
    const defaultRate = Math.max(1, Math.round((Number(ownRows[0].price_toman || 500000) * 1024) / Math.max(1, Number(ownRows[0].size_mb || 1024))));
    const rate = normalizePricePerGb(rateSetting ?? defaultRate, defaultRate);
    const finalPrice = Math.max(1, Math.ceil((mb / 1024) * rate));
    const cards = await sql `SELECT id, label, card_number, holder_name, bank_name FROM cards WHERE active = TRUE ORDER BY id ASC;`;
    if (!cards.length) {
        await tg("sendMessage", { chat_id: chatId, text: "فعلاً کارت فعالی برای پرداخت کارت‌به‌کارت ثبت نشده است." });
        return;
    }
    const randomMode = await getBoolSetting("random_card_distribution", false);
    const mainCardRaw = await getSetting("main_card_id");
    const mainCardId = mainCardRaw ? Number(mainCardRaw) : NaN;
    const preferred = Number.isFinite(mainCardId) ? cards.find((c) => Number(c.id) === mainCardId) : null;
    const selected = randomMode ? cards[Math.floor(Math.random() * cards.length)] : preferred || cards[0];
    const purchaseId = `T${Date.now()}${Math.floor(Math.random() * 10000).toString().padStart(4, "0")}`;
    const inserted = await sql `
    INSERT INTO topup_requests (purchase_id, telegram_id, inventory_id, requested_mb, payment_method, card_id, final_price, status)
    VALUES (${purchaseId}, ${userId}, ${inventoryId}, ${mb}, 'card2card', ${selected.id}, ${finalPrice}, 'awaiting_receipt')
    RETURNING id;
  `;
    await setState(userId, "await_topup_receipt", { topupRequestId: inserted[0].id });
    await tg("sendMessage", {
        chat_id: chatId,
        text: `درخواست افزایش دیتا ساخته شد ✅\n` +
            `شماره سفارش: ${purchaseId}\n` +
            `مقدار: ${mb}MB\n` +
            `مبلغ: ${formatPriceToman(finalPrice)} تومان\n\n` +
            `کارت مقصد:\n` +
            `${selected.label}\n` +
            `شماره کارت: ${selected.card_number}\n` +
            `${selected.holder_name ? `صاحب کارت: ${selected.holder_name}\n` : ""}` +
            `${selected.bank_name ? `بانک: ${selected.bank_name}\n` : ""}\n` +
            `پس از پرداخت، عکس رسید را ارسال کنید.`,
        reply_markup: { inline_keyboard: [[homeButton()]] }
    });
}
function extractSanaeiClients(inbound) {
    const settings = toJsonObject(parseSanaeiNested(inbound.settings)) || {};
    const clientsRaw = Array.isArray(settings.clients) ? settings.clients : [];
    return clientsRaw
        .map((item) => toJsonObject(item))
        .filter((item) => Boolean(item));
}
async function applyTopupOnMarzban(panel, username, addBytes) {
    const login = await loginMarzbanPanel({
        base_url: String(panel.base_url),
        username: String(panel.username || ""),
        password: String(panel.password || "")
    });
    if (!login.res.ok || !login.token) {
        return { ok: false, message: `Marzban auth failed: ${login.res.status}` };
    }
    const baseUrl = normalizeBaseUrl(String(panel.base_url));
    const getRes = await fetchWithTimeout(`${baseUrl}/api/user/${encodeURIComponent(username)}`, {
        method: "GET",
        headers: {
            Authorization: `Bearer ${login.token}`,
            Accept: "application/json"
        }
    });
    const getRaw = await getRes.text();
    const getData = parseJsonObject(getRaw);
    if (!getRes.ok || !getData) {
        return { ok: false, message: `Marzban user lookup failed: ${getRes.status} ${responseSnippet(getRaw)}` };
    }
    const currentLimit = Number(getData.data_limit || 0);
    if (!Number.isFinite(currentLimit) || currentLimit <= 0) {
        return { ok: false, message: "Marzban user has no finite data limit." };
    }
    const targetLimit = Math.max(0, Math.round(currentLimit + addBytes));
    const payload = {
        ...getData,
        data_limit: targetLimit
    };
    const putRes = await fetchWithTimeout(`${baseUrl}/api/user/${encodeURIComponent(username)}`, {
        method: "PUT",
        headers: {
            Authorization: `Bearer ${login.token}`,
            "Content-Type": "application/json",
            Accept: "application/json"
        },
        body: JSON.stringify(payload)
    });
    const putRaw = await putRes.text();
    if (!putRes.ok) {
        return { ok: false, message: `Marzban topup failed: ${putRes.status} ${responseSnippet(putRaw)}` };
    }
    return { ok: true, message: `Marzban data_limit ${currentLimit} -> ${targetLimit}` };
}
async function applyTopupOnSanaei(panel, inboundId, email, addBytes) {
    const login = await loginSanaeiPanel({
        base_url: String(panel.base_url),
        username: String(panel.username || ""),
        password: String(panel.password || "")
    });
    if (!login.res.ok || !jsonSuccess(login.data) || !login.cookie) {
        return { ok: false, message: `Sanaei auth failed: ${login.res.status}` };
    }
    const inbounds = await getSanaeiInbounds(String(panel.base_url), login.cookie);
    if (!inbounds.res.ok || !jsonSuccess(inbounds.data)) {
        return { ok: false, message: `Sanaei list inbounds failed: ${inbounds.res.status}` };
    }
    const inbound = inbounds.items.find((item) => Number(item.id || 0) === inboundId);
    if (!inbound) {
        return { ok: false, message: `inbound #${inboundId} not found` };
    }
    const clients = extractSanaeiClients(inbound);
    const client = clients.find((item) => String(item.email || "").toLowerCase() === email.toLowerCase());
    if (!client) {
        return { ok: false, message: `client email not found: ${email}` };
    }
    const currentTotalGb = Number(client.totalGB || 0);
    if (!Number.isFinite(currentTotalGb) || currentTotalGb <= 0) {
        return { ok: false, message: "Sanaei client has no finite totalGB." };
    }
    const targetTotalGb = Math.max(0, Math.round(currentTotalGb + addBytes));
    const updatedClient = {
        ...client,
        totalGB: targetTotalGb
    };
    const candidateIds = Array.from(new Set([String(client.id || ""), String(client.password || ""), String(client.email || "")].filter(Boolean)));
    let lastFail = "update endpoint failed";
    for (const candidateId of candidateIds) {
        const res = await fetchWithTimeout(`${normalizeBaseUrl(String(panel.base_url))}/panel/api/inbounds/updateClient/${encodeURIComponent(candidateId)}`, {
            method: "POST",
            headers: {
                Accept: "application/json",
                Cookie: login.cookie,
                "Content-Type": "application/json"
            },
            body: JSON.stringify({
                id: inboundId,
                settings: JSON.stringify({ clients: [updatedClient] })
            })
        });
        const raw = await res.text();
        const parsed = parseJsonObject(raw);
        if (res.ok && (!raw.trim() || jsonSuccess(parsed))) {
            return { ok: true, message: `Sanaei totalGB ${currentTotalGb} -> ${targetTotalGb}` };
        }
        lastFail = `${res.status} ${responseSnippet(raw)}`;
    }
    return { ok: false, message: `Sanaei topup failed: ${lastFail}` };
}
async function resolveTelegramTargetId(raw) {
    const normalized = raw.trim();
    const direct = Number(normalized);
    if (Number.isFinite(direct) && direct > 0) {
        return { ok: true, telegramId: Math.round(direct), username: "" };
    }
    const username = normalized.replace("@", "").trim().toLowerCase();
    if (!username) {
        return { ok: false, reason: "شناسه کاربر معتبر نیست." };
    }
    const rows = await sql `
    SELECT telegram_id, username
    FROM users
    WHERE LOWER(username) = ${username}
    ORDER BY last_seen_at DESC
    LIMIT 1;
  `;
    if (!rows.length) {
        return { ok: false, reason: "کاربری با این یوزرنیم پیدا نشد." };
    }
    return { ok: true, telegramId: Number(rows[0].telegram_id), username: String(rows[0].username || username) };
}
async function ensureAdminCustomProductId() {
    const name = "__ADMIN_CUSTOM_CONFIG__";
    await sql `
    INSERT INTO products (name, size_mb, price_toman, is_active, is_infinite, sell_mode, panel_delivery_mode, panel_config)
    VALUES (${name}, 1024, 1, FALSE, FALSE, 'panel', 'both', '{}'::jsonb)
    ON CONFLICT (name) DO NOTHING;
  `;
    const rows = await sql `SELECT id FROM products WHERE name = ${name} LIMIT 1;`;
    if (!rows.length) {
        throw new Error("محصول سیستمی برای کانفیگ سفارشی پیدا نشد.");
    }
    return Number(rows[0].id);
}
async function resolveFirstSanaeiInboundId(panel) {
    const login = await loginSanaeiPanel({
        base_url: String(panel.base_url),
        username: String(panel.username || ""),
        password: String(panel.password || "")
    });
    if (!login.res.ok || !jsonSuccess(login.data) || !login.cookie) {
        return { ok: false, reason: `ورود به پنل سانایی ناموفق: ${login.res.status}` };
    }
    const inbounds = await getSanaeiInbounds(String(panel.base_url), login.cookie);
    if (!inbounds.res.ok || !jsonSuccess(inbounds.data) || !inbounds.items.length) {
        return { ok: false, reason: "هیچ inbound فعالی روی پنل سانایی پیدا نشد." };
    }
    const first = inbounds.items[0];
    const inboundId = Number(first.id || 0);
    if (!Number.isFinite(inboundId) || inboundId <= 0) {
        return { ok: false, reason: "شناسه inbound معتبر نیست." };
    }
    return { ok: true, inboundId, protocol: String(first.protocol || "vless") };
}
export async function applyAdminResetUsageOnMarzban(panel, username) {
    const login = await loginMarzbanPanel({
        base_url: String(panel.base_url),
        username: String(panel.username || ""),
        password: String(panel.password || "")
    });
    if (!login.res.ok || !login.token) {
        return { ok: false, message: `Marzban auth failed: ${login.res.status}` };
    }
    const baseUrl = normalizeBaseUrl(String(panel.base_url));
    const resetRes = await fetchWithTimeout(`${baseUrl}/api/user/${encodeURIComponent(username)}/reset`, {
        method: "POST",
        headers: { Authorization: `Bearer ${login.token}`, Accept: "application/json" }
    });
    if (!resetRes.ok) {
        const resetRaw = await resetRes.text();
        return { ok: false, message: `Marzban reset traffic failed: ${resetRes.status} ${responseSnippet(resetRaw)}` };
    }
    return { ok: true, message: "Marzban usage reset." };
}
export async function applyAdminResetUsageOnSanaei(panel, inboundId, email) {
    const login = await loginSanaeiPanel({
        base_url: String(panel.base_url),
        username: String(panel.username || ""),
        password: String(panel.password || "")
    });
    if (!login.res.ok || !jsonSuccess(login.data) || !login.cookie) {
        return { ok: false, message: `Sanaei auth failed: ${login.res.status}` };
    }
    const baseUrl = normalizeBaseUrl(String(panel.base_url));
    const resetRes = await fetchWithTimeout(`${baseUrl}/panel/api/inbounds/${inboundId}/resetClientTraffic/${encodeURIComponent(email)}`, {
        method: "POST",
        headers: { Accept: "application/json", Cookie: login.cookie }
    });
    if (!resetRes.ok) {
        const resetRaw = await resetRes.text();
        return { ok: false, message: `Sanaei reset traffic failed: ${resetRes.status} ${responseSnippet(resetRaw)}` };
    }
    // Sanaei does not automatically re-enable the client after resetting traffic. We must explicitly enable them.
    const enableRes = await updateSanaeiClient(panel, inboundId, email, (client) => ({
        ...client,
        enable: true
    }));
    if (!enableRes.ok) {
        return { ok: true, message: `Sanaei usage reset but enable failed: ${enableRes.message}` };
    }
    return { ok: true, message: "Sanaei usage reset and client enabled." };
}
async function applyAdminSetLimitOnlyOnMarzban(panel, username, targetBytes) {
    const login = await loginMarzbanPanel({
        base_url: String(panel.base_url),
        username: String(panel.username || ""),
        password: String(panel.password || "")
    });
    if (!login.res.ok || !login.token) {
        return { ok: false, message: `Marzban auth failed: ${login.res.status}` };
    }
    const baseUrl = normalizeBaseUrl(String(panel.base_url));
    const getRes = await fetchWithTimeout(`${baseUrl}/api/user/${encodeURIComponent(username)}`, {
        method: "GET",
        headers: { Authorization: `Bearer ${login.token}`, Accept: "application/json" }
    });
    const getRaw = await getRes.text();
    const getData = parseJsonObject(getRaw);
    if (!getRes.ok || !getData) {
        return { ok: false, message: `Marzban user lookup failed: ${getRes.status} ${responseSnippet(getRaw)}` };
    }
    const currentLimit = Number(getData.data_limit || 0);
    const newLimit = Math.max(0, Math.round(targetBytes));
    if (currentLimit === newLimit)
        return { ok: true, message: "Marzban data limit unchanged." };
    const payload = { ...getData, data_limit: newLimit };
    const putRes = await fetchWithTimeout(`${baseUrl}/api/user/${encodeURIComponent(username)}`, {
        method: "PUT",
        headers: {
            Authorization: `Bearer ${login.token}`,
            "Content-Type": "application/json",
            Accept: "application/json"
        },
        body: JSON.stringify(payload)
    });
    const putRaw = await putRes.text();
    if (!putRes.ok) {
        return { ok: false, message: `Marzban limit update failed: ${putRes.status} ${responseSnippet(putRaw)}` };
    }
    return { ok: true, message: "Marzban data limit updated." };
}
async function applyAdminSetLimitOnlyOnSanaei(panel, inboundId, email, targetBytes) {
    const res = await updateSanaeiClient(panel, inboundId, email, (client) => ({
        ...client,
        totalGB: Math.max(0, Math.round(targetBytes))
    }));
    if (!res.ok)
        return res;
    return { ok: true, message: "Sanaei data limit updated." };
}
export async function applyAdminSetDataLimitOnMarzban(panel, username, targetBytes) {
    const login = await loginMarzbanPanel({
        base_url: String(panel.base_url),
        username: String(panel.username || ""),
        password: String(panel.password || "")
    });
    if (!login.res.ok || !login.token) {
        return { ok: false, message: `Marzban auth failed: ${login.res.status}` };
    }
    const baseUrl = normalizeBaseUrl(String(panel.base_url));
    const resetRes = await fetchWithTimeout(`${baseUrl}/api/user/${encodeURIComponent(username)}/reset`, {
        method: "POST",
        headers: { Authorization: `Bearer ${login.token}`, Accept: "application/json" }
    });
    if (!resetRes.ok) {
        const resetRaw = await resetRes.text();
        return { ok: false, message: `Marzban reset traffic failed: ${resetRes.status} ${responseSnippet(resetRaw)}` };
    }
    const getRes = await fetchWithTimeout(`${baseUrl}/api/user/${encodeURIComponent(username)}`, {
        method: "GET",
        headers: { Authorization: `Bearer ${login.token}`, Accept: "application/json" }
    });
    const getRaw = await getRes.text();
    const getData = parseJsonObject(getRaw);
    if (!getRes.ok || !getData) {
        return { ok: false, message: `Marzban user lookup failed: ${getRes.status} ${responseSnippet(getRaw)}` };
    }
    const currentLimit = Number(getData.data_limit || 0);
    const newLimit = Math.max(0, Math.round(targetBytes));
    if (currentLimit === newLimit) {
        return { ok: true, message: "Marzban data limit and usage reset." };
    }
    const payload = { ...getData, data_limit: newLimit };
    const putRes = await fetchWithTimeout(`${baseUrl}/api/user/${encodeURIComponent(username)}`, {
        method: "PUT",
        headers: {
            Authorization: `Bearer ${login.token}`,
            "Content-Type": "application/json",
            Accept: "application/json"
        },
        body: JSON.stringify(payload)
    });
    const putRaw = await putRes.text();
    if (!putRes.ok) {
        return { ok: false, message: `Marzban limit update failed: ${putRes.status} ${responseSnippet(putRaw)}` };
    }
    return { ok: true, message: "Marzban data limit and usage reset." };
}
export async function applyAdminSetExpiryOnMarzban(panel, username, expiryTimeMs) {
    const login = await loginMarzbanPanel({
        base_url: String(panel.base_url),
        username: String(panel.username || ""),
        password: String(panel.password || "")
    });
    if (!login.res.ok || !login.token) {
        return { ok: false, message: `Marzban auth failed: ${login.res.status}` };
    }
    const baseUrl = normalizeBaseUrl(String(panel.base_url));
    const getRes = await fetchWithTimeout(`${baseUrl}/api/user/${encodeURIComponent(username)}`, {
        method: "GET",
        headers: { Authorization: `Bearer ${login.token}`, Accept: "application/json" }
    });
    const getRaw = await getRes.text();
    const getData = parseJsonObject(getRaw);
    if (!getRes.ok || !getData) {
        return { ok: false, message: `Marzban user lookup failed: ${getRes.status} ${responseSnippet(getRaw)}` };
    }
    const user = getData;
    const statusRaw = user.status;
    const statusCandidate = typeof statusRaw === "string"
        ? statusRaw
        : statusRaw && typeof statusRaw === "object"
            ? String(statusRaw.status || statusRaw.value || statusRaw.name || "")
            : "";
    const status = ["active", "disabled", "on_hold"].includes(statusCandidate) ? statusCandidate : "active";
    const payload = {
        username: String(user.username || username),
        proxies: user.proxies || {},
        inbounds: user.inbounds || {},
        expire: expiryTimeMs > 0 ? Math.floor(expiryTimeMs / 1000) : 0,
        data_limit: Number(user.data_limit || 0),
        data_limit_reset_strategy: String(user.data_limit_reset_strategy || "no_reset"),
        status,
        note: String(user.note || "")
    };
    const putRes = await fetchWithTimeout(`${baseUrl}/api/user/${encodeURIComponent(username)}`, {
        method: "PUT",
        headers: {
            Authorization: `Bearer ${login.token}`,
            "Content-Type": "application/json",
            Accept: "application/json"
        },
        body: JSON.stringify(payload)
    });
    const putRaw = await putRes.text();
    if (!putRes.ok) {
        return { ok: false, message: `Marzban expiry update failed: ${putRes.status} ${responseSnippet(putRaw)}` };
    }
    return { ok: true, message: "Marzban expiry updated." };
}
async function updateSanaeiClient(panel, inboundId, email, updater) {
    const login = await loginSanaeiPanel({
        base_url: String(panel.base_url),
        username: String(panel.username || ""),
        password: String(panel.password || "")
    });
    if (!login.res.ok || !jsonSuccess(login.data) || !login.cookie) {
        return { ok: false, message: `Sanaei auth failed: ${login.res.status}` };
    }
    const inbounds = await getSanaeiInbounds(String(panel.base_url), login.cookie);
    if (!inbounds.res.ok || !jsonSuccess(inbounds.data)) {
        return { ok: false, message: `Sanaei list inbounds failed: ${inbounds.res.status}` };
    }
    const inbound = inbounds.items.find((item) => Number(item.id || 0) === inboundId);
    if (!inbound)
        return { ok: false, message: `inbound #${inboundId} not found` };
    const clients = extractSanaeiClients(inbound);
    const client = clients.find((item) => String(item.email || "").toLowerCase() === email.toLowerCase());
    if (!client)
        return { ok: false, message: `client email not found: ${email}` };
    const updatedClient = updater(client);
    const candidateIds = Array.from(new Set([String(client.id || ""), String(client.password || ""), String(client.email || "")].filter(Boolean)));
    let lastFail = "update endpoint failed";
    for (const candidateId of candidateIds) {
        const res = await fetchWithTimeout(`${normalizeBaseUrl(String(panel.base_url))}/panel/api/inbounds/updateClient/${encodeURIComponent(candidateId)}`, {
            method: "POST",
            headers: {
                Accept: "application/json",
                Cookie: login.cookie,
                "Content-Type": "application/json"
            },
            body: JSON.stringify({
                id: inboundId,
                settings: JSON.stringify({ clients: [updatedClient] })
            })
        });
        const raw = await res.text();
        const parsed = parseJsonObject(raw);
        if (res.ok && (!raw.trim() || jsonSuccess(parsed)))
            return { ok: true, message: "Sanaei client updated." };
        lastFail = `${res.status} ${responseSnippet(raw)}`;
    }
    return { ok: false, message: `Sanaei update failed: ${lastFail}` };
}
export async function applyAdminSetDataLimitOnSanaei(panel, inboundId, email, targetBytes) {
    const login = await loginSanaeiPanel({
        base_url: String(panel.base_url),
        username: String(panel.username || ""),
        password: String(panel.password || "")
    });
    if (!login.res.ok || !jsonSuccess(login.data) || !login.cookie) {
        return { ok: false, message: `Sanaei auth failed: ${login.res.status}` };
    }
    const baseUrl = normalizeBaseUrl(String(panel.base_url));
    const resetRes = await fetchWithTimeout(`${baseUrl}/panel/api/inbounds/${inboundId}/resetClientTraffic/${encodeURIComponent(email)}`, {
        method: "POST",
        headers: { Accept: "application/json", Cookie: login.cookie }
    });
    const resetOk = resetRes.ok;
    const limitRes = await updateSanaeiClient(panel, inboundId, email, (client) => ({
        ...client,
        totalGB: Math.max(0, Math.round(targetBytes)),
        up: 0,
        down: 0
    }));
    if (!limitRes.ok)
        return limitRes;
    return {
        ok: true,
        message: resetOk ? "Sanaei data limit and usage reset." : "Sanaei data limit updated (fallback reset)."
    };
}
export async function applyAdminSetExpiryOnSanaei(panel, inboundId, email, expiryTimeMs) {
    return updateSanaeiClient(panel, inboundId, email, (client) => ({
        ...client,
        expiryTime: Math.max(0, Math.round(expiryTimeMs))
    }));
}
async function tryAutoApplyPanelTopup(topupRequestId, doneBy) {
    const rows = await sql `
    SELECT
      tr.id,
      tr.telegram_id,
      tr.inventory_id,
      tr.requested_mb,
      tr.purchase_id,
      tr.status,
      i.panel_id,
      i.delivery_payload,
      p.panel_type,
      p.base_url,
      p.username,
      p.password
    FROM topup_requests tr
    INNER JOIN inventory i ON i.id = tr.inventory_id
    LEFT JOIN panels p ON p.id = i.panel_id
    WHERE tr.id = ${topupRequestId}
    LIMIT 1;
  `;
    if (!rows.length) {
        return { ok: false, message: "Topup request not found." };
    }
    const row = rows[0];
    if (String(row.status) !== "paid") {
        return { ok: false, message: "Topup request is not in paid state." };
    }
    if (!row.panel_id || !row.panel_type) {
        return { ok: false, message: "Inventory is not a panel-issued config." };
    }
    const payload = parseDeliveryPayload(row.delivery_payload);
    const metadata = payload.metadata || {};
    const addBytes = Math.max(0, Math.round(Number(row.requested_mb || 0) * 1024 * 1024));
    if (!addBytes) {
        return { ok: false, message: "Requested data amount is invalid." };
    }
    const panel = {
        base_url: row.base_url,
        username: row.username,
        password: row.password
    };
    let result = { ok: false, message: "Unsupported panel type." };
    if (String(row.panel_type) === "marzban") {
        const username = String(metadata.username || "").trim();
        if (!username) {
            return { ok: false, message: "Missing panel username in delivery metadata." };
        }
        result = await applyTopupOnMarzban(panel, username, addBytes);
    }
    else if (String(row.panel_type) === "sanaei") {
        const inboundId = parseMaybeNumber(metadata.inboundId);
        const email = String(metadata.email || "").trim();
        if (!inboundId || !email) {
            return { ok: false, message: "Missing inboundId/email in delivery metadata." };
        }
        result = await applyTopupOnSanaei(panel, inboundId, email, addBytes);
    }
    if (!result.ok) {
        return result;
    }
    const doneRows = await sql `
    UPDATE topup_requests
    SET status = 'done', done_at = NOW(), done_by = ${doneBy}
    WHERE id = ${topupRequestId} AND status = 'paid'
    RETURNING telegram_id, inventory_id, requested_mb, purchase_id;
  `;
    if (!doneRows.length) {
        return { ok: false, message: "Topup status changed before auto completion." };
    }
    const cfg = await sql `SELECT config_value FROM inventory WHERE id = ${doneRows[0].inventory_id} LIMIT 1;`;
    await tg("sendMessage", {
        chat_id: Number(doneRows[0].telegram_id),
        text: `درخواست افزایش ${doneRows[0].requested_mb}MB شما به‌صورت خودکار انجام شد ✅\n` +
            `شماره سفارش: ${doneRows[0].purchase_id}\n` +
            `کانفیگ:\n${String(cfg[0]?.config_value || "-")}`
    });
    return { ok: true, message: result.message };
}
async function createOrder(chatId, userId, productId, paymentMethod, discountInput, walletUsedParam = 0, overrides = null) {
    const globalInfinite = await getBoolSetting("global_infinite_mode", false);
    const rows = await sql `
    SELECT
      p.id,
      p.name,
      p.price_toman,
      p.size_mb,
      p.is_infinite,
      p.sell_mode,
      p.panel_id,
      p.panel_sell_limit,
      p.panel_delivery_mode,
      p.panel_config,
      pnl.active AS panel_active,
      pnl.allow_new_sales AS panel_allow_new_sales,
      (
        SELECT COUNT(*)::int
        FROM inventory i
        WHERE i.product_id = p.id AND i.status = 'available'
      ) AS stock,
      (
        SELECT COUNT(*)::int
        FROM orders o
        WHERE o.product_id = p.id
          AND o.sell_mode = 'panel'
          AND o.status NOT IN ('denied')
      ) AS panel_sales_count
    FROM products p
    LEFT JOIN panels pnl ON pnl.id = p.panel_id
    WHERE p.id = ${productId} AND p.is_active = TRUE
    LIMIT 1;
  `;
    if (!rows.length) {
        await tg("sendMessage", { chat_id: chatId, text: "محصول یافت نشد." });
        return;
    }
    const product = rows[0];
    const sellMode = parseSellMode(String(product.sell_mode || ""));
    const panelRemaining = Number(product.panel_sell_limit || 0) > 0 ? Math.max(0, Number(product.panel_sell_limit) - Number(product.panel_sales_count || 0)) : Infinity;
    if (sellMode === "panel" &&
        (!product.panel_id || !product.panel_active || !product.panel_allow_new_sales || panelRemaining <= 0)) {
        await tg("sendMessage", { chat_id: chatId, text: "فروش از پنل برای این محصول فعلاً در دسترس نیست." });
        return;
    }
    const allowNoStock = sanitizePanelConfig(overrides?.panelConfigPatch).force_awaiting_config === true;
    if (sellMode !== "panel" && !globalInfinite && !product.is_infinite && Number(product.stock) <= 0 && !allowNoStock) {
        await tg("sendMessage", { chat_id: chatId, text: "موجودی این محصول تمام شده است." });
        return;
    }
    const basePriceToman = Math.max(1, Math.round(Number(overrides?.basePriceToman ?? product.price_toman)));
    const basePanelConfig = sanitizePanelConfig(product.panel_config);
    const panelConfigSnapshot = overrides?.panelConfigPatch ? { ...basePanelConfig, ...sanitizePanelConfig(overrides.panelConfigPatch) } : basePanelConfig;
    const productNameSnapshot = `${String(product.name || "")}${overrides?.productNameSuffix ? ` ${overrides.productNameSuffix}` : ""}`.trim();
    const { discountAmount, discountCode } = await resolveDiscount(discountInput, basePriceToman);
    let walletUsed = 0;
    let finalPrice = Math.max(1, basePriceToman - discountAmount);
    if (paymentMethod === "wallet") {
        const userRows = await sql `SELECT wallet_balance FROM users WHERE telegram_id = ${userId} LIMIT 1;`;
        const walletBalance = userRows.length ? Number(userRows[0].wallet_balance || 0) : 0;
        if (walletBalance < finalPrice) {
            await tg("sendMessage", { chat_id: chatId, text: "موجودی کیف پول شما کافی نیست." });
            return;
        }
        walletUsed = finalPrice;
        finalPrice = 0;
    }
    else if (walletUsedParam > 0) {
        const userRows = await sql `SELECT wallet_balance FROM users WHERE telegram_id = ${userId} LIMIT 1;`;
        const walletBalance = userRows.length ? Number(userRows[0].wallet_balance || 0) : 0;
        walletUsed = Math.min(walletUsedParam, walletBalance, Math.max(0, basePriceToman - discountAmount));
        finalPrice = Math.max(1, basePriceToman - discountAmount - walletUsed);
    }
    let cryptoWalletId = null;
    if (paymentMethod.startsWith("crypto_")) {
        const parsed = Number(paymentMethod.replace("crypto_", ""));
        cryptoWalletId = Number.isFinite(parsed) ? parsed : null;
        paymentMethod = "crypto";
    }
    if (paymentMethod === "crypto" && cryptoWalletId === null) {
        const wallets = await sql `
      SELECT id, currency, network, address, rate_mode, rate_toman_per_unit, extra_toman_per_unit, active
      FROM crypto_wallets
      WHERE active = TRUE
      ORDER BY currency ASC, network ASC, id ASC;
    `;
        const ready = wallets.map((w) => w).filter(cryptoWalletReady);
        if (!ready.length) {
            await tg("sendMessage", { chat_id: chatId, text: "هیچ کیف پول کریپتوی فعالی برای پرداخت تنظیم نشده است." });
            return;
        }
        if (ready.length > 1) {
            await setState(userId, "await_crypto_wallet_select", { productId, discountInput, walletUsedParam, overrides });
            await tg("sendMessage", {
                chat_id: chatId,
                text: "کدام کیف پول را برای پرداخت انتخاب می‌کنید؟",
                reply_markup: { inline_keyboard: ready.slice(0, 12).map((w) => [{ text: cryptoWalletTitle(w), callback_data: `select_crypto_wallet_${w.id}` }]).concat([[homeButton()]]) }
            });
            return;
        }
        cryptoWalletId = ready[0].id;
    }
    const purchaseId = `P${Date.now()}${Math.floor(Math.random() * 10000).toString().padStart(4, "0")}`;
    if (discountCode) {
        await sql `UPDATE discounts SET used_count = used_count + 1 WHERE code = ${discountCode};`;
    }
    if (paymentMethod === "wallet") {
        // Atomic deduction and order insertion to prevent negative balance exploits
        const inserted = await sql `
      WITH deducted AS (
        UPDATE users
        SET wallet_balance = wallet_balance - ${walletUsed}
        WHERE telegram_id = ${userId} AND wallet_balance >= ${walletUsed}
        RETURNING telegram_id
      )
      INSERT INTO orders
      (
        purchase_id, telegram_id, product_id, product_name_snapshot, sell_mode, source_panel_id, panel_delivery_mode, panel_config_snapshot,
        payment_method, discount_code, discount_amount, final_price, tron_amount, status, wallet_used
      )
      SELECT
        ${purchaseId}, ${userId}, ${product.id}, ${productNameSnapshot}, ${sellMode}, ${product.panel_id || null}, ${parseDeliveryMode(String(product.panel_delivery_mode || ""))},
        ${JSON.stringify(panelConfigSnapshot)}::jsonb,
        'wallet', ${discountCode}, ${discountAmount}, 0, 0, 'pending', ${walletUsed}
      FROM deducted
      RETURNING id;
    `;
        if (!inserted.length) {
            await tg("sendMessage", { chat_id: chatId, text: "موجودی کیف پول شما کافی نیست یا خطایی رخ داده است." });
            return;
        }
        const negativeWalletUsed = -walletUsed;
        await sql `
      INSERT INTO wallet_transactions (telegram_id, amount, type, description, created_at)
      VALUES (${userId}, ${negativeWalletUsed}, 'purchase', ${`خرید محصول ${productNameSnapshot} (سفارش ${purchaseId})`}, NOW());
    `;
        const orderId = Number(inserted[0].id);
        await tg("sendMessage", {
            chat_id: chatId,
            text: `✅ مبلغ ${formatPriceToman(walletUsed)} تومان از کیف پول شما کسر شد و سفارش ثبت گردید.\nدرحال آماده‌سازی محصول...`
        });
        const fulfill = await finalizeOrder(orderId, null);
        if (!fulfill.ok && fulfill.reason === "stock_empty") {
            await tg("sendMessage", { chat_id: chatId, text: "موجودی صفر است. ادمین پیگیری می‌کند." });
        }
        return;
    }
    if (paymentMethod === "card2card") {
        const cards = await sql `SELECT id, label, card_number, holder_name, bank_name FROM cards WHERE active = TRUE ORDER BY id ASC;`;
        if (!cards.length) {
            await tg("sendMessage", { chat_id: chatId, text: "فعلاً کارت فعالی برای پرداخت کارت‌به‌کارت ثبت نشده است." });
            return;
        }
        const randomMode = await getBoolSetting("random_card_distribution", false);
        const mainCardRaw = await getSetting("main_card_id");
        const mainCardId = mainCardRaw ? Number(mainCardRaw) : NaN;
        const preferred = Number.isFinite(mainCardId) ? cards.find((c) => Number(c.id) === mainCardId) : null;
        const selected = randomMode ? cards[Math.floor(Math.random() * cards.length)] : preferred || cards[0];
        const inserted = await sql `
      INSERT INTO orders
      (
        purchase_id, telegram_id, product_id, product_name_snapshot, sell_mode, source_panel_id, panel_delivery_mode, panel_config_snapshot,
        payment_method, card_id, discount_code, discount_amount, final_price, tron_amount, status, wallet_used
      )
      VALUES
      (
        ${purchaseId}, ${userId}, ${product.id}, ${productNameSnapshot}, ${sellMode}, ${product.panel_id || null}, ${parseDeliveryMode(String(product.panel_delivery_mode || ""))},
        ${JSON.stringify(panelConfigSnapshot)}::jsonb,
        'card2card', ${selected.id}, ${discountCode}, ${discountAmount}, ${finalPrice}, 0, 'awaiting_receipt', ${walletUsed}
      )
      RETURNING id;
    `;
        await setState(userId, "await_receipt", { orderId: inserted[0].id });
        await tg("sendMessage", {
            chat_id: chatId,
            text: `سفارش شما ساخته شد ✅\n` +
                `شناسه خرید: ${purchaseId}\n` +
                `محصول: ${productNameSnapshot}\n` +
                `مبلغ: ${formatPriceToman(finalPrice)} تومان\n\n` +
                `کارت مقصد:\n` +
                `${selected.label}\n` +
                `شماره کارت: ${selected.card_number}\n` +
                `${selected.holder_name ? `صاحب کارت: ${selected.holder_name}\n` : ""}` +
                `${selected.bank_name ? `بانک: ${selected.bank_name}\n` : ""}\n` +
                `بعد از انتقال، اسکرین‌شات رسید را به صورت عکس ارسال کنید.`,
            reply_markup: {
                inline_keyboard: [[homeButton()]]
            }
        });
        return;
    }
    if (paymentMethod === "crypto") {
        if (!cryptoWalletId) {
            await tg("sendMessage", { chat_id: chatId, text: "کیف پول کریپتو انتخاب نشده است." });
            return;
        }
        const walletRows = await sql `
      SELECT id, currency, network, address, rate_mode, rate_toman_per_unit, extra_toman_per_unit, active
      FROM crypto_wallets
      WHERE id = ${cryptoWalletId}
      LIMIT 1;
    `;
        if (!walletRows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "کیف پول کریپتو یافت نشد." });
            return;
        }
        const w = walletRows[0];
        if (!cryptoWalletReady(w)) {
            await tg("sendMessage", { chat_id: chatId, text: "کیف پول کریپتو به‌درستی تنظیم نشده یا غیرفعال است." });
            return;
        }
        const expiresAt = new Date(Date.now() + 20 * 60 * 1000);
        let tomanPerUnit = 0;
        if (w.rate_mode === "auto") {
            const base = await getCryptoTomanPerUnitCached(String(w.currency || ""));
            tomanPerUnit = base + Number(w.extra_toman_per_unit || 0);
        }
        else {
            tomanPerUnit = Number(w.rate_toman_per_unit || 0) + Number(w.extra_toman_per_unit || 0);
        }
        if (!Number.isFinite(tomanPerUnit) || tomanPerUnit <= 0) {
            await tg("sendMessage", { chat_id: chatId, text: "نرخ کیف پول کریپتو معتبر نیست." });
            return;
        }
        const decimals = String(w.currency).toUpperCase() === "USDT" ? 2 : 5;
        const cryptoAmount = Number((finalPrice / tomanPerUnit).toFixed(decimals));
        if (!Number.isFinite(cryptoAmount) || cryptoAmount <= 0) {
            await tg("sendMessage", { chat_id: chatId, text: "مبلغ کریپتو معتبر نیست." });
            return;
        }
        await sql `
      INSERT INTO orders
      (
        purchase_id, telegram_id, product_id, product_name_snapshot, sell_mode, source_panel_id, panel_delivery_mode, panel_config_snapshot,
        payment_method, discount_code, discount_amount, final_price, tron_amount, status, wallet_used,
        crypto_wallet_id, crypto_currency, crypto_network, crypto_address, crypto_amount, crypto_expires_at
      )
      VALUES
      (
        ${purchaseId}, ${userId}, ${product.id}, ${productNameSnapshot}, ${sellMode}, ${product.panel_id || null}, ${parseDeliveryMode(String(product.panel_delivery_mode || ""))},
        ${JSON.stringify(panelConfigSnapshot)}::jsonb,
        'crypto', ${discountCode}, ${discountAmount}, ${finalPrice}, 0, 'pending', ${walletUsed},
        ${w.id}, ${w.currency}, ${w.network}, ${String(w.address || "")}, ${cryptoAmount}, ${expiresAt.toISOString()}
      );
    `;
        const cryptoText = `سفارش شما ساخته شد ✅\n` +
            `شناسه خرید: ${purchaseId}\n` +
            `محصول: ${productNameSnapshot}\n` +
            `مبلغ: ${formatPriceToman(finalPrice)} تومان\n\n` +
            `⏰ مهلت پرداخت: 20 دقیقه\n` +
            `🪙 ارز: ${String(w.currency)}\n` +
            `🌐 شبکه: ${String(w.network)}\n` +
            `☑️ مبلغ پرداختی: ${cryptoAmount}\n\n` +
            `📱 آدرس کیف پول:\n\n${String(w.address || "-")}\n\n` +
            `بعد از پرداخت روی «بررسی پرداخت» بزنید و اسکرین‌شات پرداخت را ارسال کنید.`;
        await tg("sendMessage", {
            chat_id: chatId,
            text: cryptoText,
            reply_markup: {
                inline_keyboard: [
                    [cb("✅ بررسی پرداخت", `check_order_${purchaseId}`, "success")],
                    [homeButton()]
                ]
            }
        });
        return;
    }
    if (paymentMethod === "tetrapay") {
        const callbackBase = await getPublicBaseUrl(env.PUBLIC_BASE_URL);
        if (!callbackBase) {
            await tg("sendMessage", { chat_id: chatId, text: "آدرس سایت برای Callback تنظیم نشده است. لطفاً به پشتیبانی پیام دهید." });
            await notifyAdmins(`⚠️ تنظیمات Callback Base ناقص است (تتراپی)\nسفارش: ${purchaseId}`, {
                inline_keyboard: [[{ text: "🔎 باز کردن سفارش", callback_data: `admin_open_purchase_${purchaseId}` }]]
            });
            return;
        }
        const tetrapayApiKey = ((await getSetting("tetrapay_api_key")) || "").trim();
        if (!tetrapayApiKey) {
            await tg("sendMessage", { chat_id: chatId, text: "کلید تتراپی تنظیم نشده است. لطفاً به پشتیبانی پیام دهید." });
            await notifyAdmins(`⚠️ کلید تتراپی تنظیم نشده است\nسفارش: ${purchaseId}`, {
                inline_keyboard: [[{ text: "🔎 باز کردن سفارش", callback_data: `admin_open_purchase_${purchaseId}` }]]
            });
            return;
        }
        try {
            const { createTetrapayOrder } = await import("./tetrapay.js");
            const orderRes = await createTetrapayOrder({
                purchaseId,
                amountToman: finalPrice,
                description: `خرید محصول ${productNameSnapshot}`,
                callbackUrl: `${callbackBase}/api/tetrapay-callback`,
                apiKey: tetrapayApiKey
            });
            if (!orderRes.ok) {
                await tg("sendMessage", { chat_id: chatId, text: `خطا در ارتباط با درگاه تتراپی: ${orderRes.message}` });
                return;
            }
            await sql `
        INSERT INTO orders
        (
          purchase_id, telegram_id, product_id, product_name_snapshot, sell_mode, source_panel_id, panel_delivery_mode, panel_config_snapshot,
          payment_method, discount_code, discount_amount, final_price, tron_amount, status, tronado_token, tronado_payment_url, wallet_used
        )
        VALUES
        (
          ${purchaseId}, ${userId}, ${product.id}, ${productNameSnapshot}, ${sellMode}, ${product.panel_id || null}, ${parseDeliveryMode(String(product.panel_delivery_mode || ""))},
          ${JSON.stringify(panelConfigSnapshot)}::jsonb,
          'tetrapay', ${discountCode}, ${discountAmount}, ${finalPrice}, 0, 'pending', ${orderRes.authority}, ${orderRes.paymentUrlBot}, ${walletUsed}
        );
      `;
            await tg("sendMessage", {
                chat_id: chatId,
                text: `سفارش شما ساخته شد ✅\n` +
                    `شناسه خرید: ${purchaseId}\n` +
                    `محصول: ${productNameSnapshot}\n` +
                    `مبلغ: ${formatPriceToman(finalPrice)} تومان\n\n` +
                    `برای پرداخت روی دکمه زیر کلیک کنید.`,
                reply_markup: {
                    inline_keyboard: [
                        [{ text: "💳 پرداخت با تتراپی", url: orderRes.paymentUrlBot }],
                        [homeButton()]
                    ]
                }
            });
        }
        catch (error) {
            logError("create_tetrapay_order_failed", error, { chatId, userId, productId });
            await tg("sendMessage", { chat_id: chatId, text: `ساخت سفارش با خطا مواجه شد: ${String(error.message || error)}` });
        }
        return;
    }
    if (paymentMethod === "plisio") {
        const callbackBase = await getPublicBaseUrl(env.PUBLIC_BASE_URL);
        if (!callbackBase) {
            await tg("sendMessage", { chat_id: chatId, text: "آدرس سایت برای Callback تنظیم نشده است. لطفاً به پشتیبانی پیام دهید." });
            await notifyAdmins(`⚠️ تنظیمات Callback Base ناقص است (Plisio)\nسفارش: ${purchaseId}`, {
                inline_keyboard: [[{ text: "🔎 باز کردن سفارش", callback_data: `admin_open_purchase_${purchaseId}` }]]
            });
            return;
        }
        const plisioApiKey = ((await getSetting("plisio_api_key")) || "").trim();
        if (!plisioApiKey) {
            await tg("sendMessage", { chat_id: chatId, text: "تنظیمات Plisio کامل نیست. لطفاً به پشتیبانی پیام دهید." });
            await notifyAdmins(`⚠️ تنظیمات Plisio ناقص است\nسفارش: ${purchaseId}\nکلید: ${plisioApiKey ? "ok" : "missing"}`, { inline_keyboard: [[{ text: "🔎 باز کردن سفارش", callback_data: `admin_open_purchase_${purchaseId}` }]] });
            return;
        }
        try {
            const tomanPerUsdt = await getPlisioTomanPerUsdt();
            const usdtAmount = Math.max(0.01, Number((finalPrice / tomanPerUsdt).toFixed(2)));
            const { createPlisioInvoice } = await import("./plisio.js");
            const invoice = await createPlisioInvoice({
                apiKey: plisioApiKey,
                orderNumber: purchaseId.slice(1),
                orderName: purchaseId,
                sourceCurrency: "USD",
                sourceAmount: usdtAmount,
                callbackUrl: `${callbackBase}/api/plisio-callback?json=true`
            });
            await sql `
        INSERT INTO orders
        (
          purchase_id, telegram_id, product_id, product_name_snapshot, sell_mode, source_panel_id, panel_delivery_mode, panel_config_snapshot,
          payment_method, discount_code, discount_amount, final_price, tron_amount, status, wallet_used,
          plisio_txn_id, plisio_invoice_url, plisio_status
        )
        VALUES
        (
          ${purchaseId}, ${userId}, ${product.id}, ${productNameSnapshot}, ${sellMode}, ${product.panel_id || null}, ${parseDeliveryMode(String(product.panel_delivery_mode || ""))},
          ${JSON.stringify(panelConfigSnapshot)}::jsonb,
          'plisio', ${discountCode}, ${discountAmount}, ${finalPrice}, 0, 'pending', ${walletUsed},
          ${invoice.txnId}, ${invoice.invoiceUrl}, 'new'
        );
      `;
            await tg("sendMessage", {
                chat_id: chatId,
                text: `سفارش شما ساخته شد ✅\n` +
                    `شناسه خرید: ${purchaseId}\n` +
                    `محصول: ${productNameSnapshot}\n` +
                    `مبلغ: ${formatPriceToman(finalPrice)} تومان\n` +
                    `معادل تقریبی: ${usdtAmount} USDT\n\n` +
                    `بعد از پرداخت، روی دکمه «بررسی پرداخت» بزنید.`,
                reply_markup: {
                    inline_keyboard: [
                        [{ text: "💳 پرداخت با Plisio", url: invoice.invoiceUrl }],
                        [cb("✅ بررسی پرداخت", `check_order_${purchaseId}`, "success")],
                        [homeButton()]
                    ]
                }
            });
        }
        catch (error) {
            logError("create_plisio_invoice_failed", error, { chatId, userId, productId, purchaseId });
            await notifyAdmins(`❌ خطا در ساخت فاکتور Plisio\nسفارش: ${purchaseId}\nعلت: ${error.message || String(error)}`, {
                inline_keyboard: [[{ text: "🔎 باز کردن سفارش", callback_data: `admin_open_purchase_${purchaseId}` }]]
            });
            await tg("sendMessage", { chat_id: chatId, text: "ساخت لینک پرداخت با خطا مواجه شد. لطفاً کمی بعد دوباره تلاش کنید یا به پشتیبانی پیام دهید." });
        }
        return;
    }
    try {
        const walletFromSetting = await getSetting("business_wallet_address");
        const walletAddress = walletFromSetting || env.BUSINESS_WALLET_ADDRESS;
        if (!walletAddress) {
            await tg("sendMessage", { chat_id: chatId, text: "تنظیمات کیف پول کامل نیست. لطفاً به پشتیبانی پیام دهید." });
            return;
        }
        const tronadoApiKey = ((await getSetting("tronado_api_key")) || "").trim();
        const tronPrice = await getTronPriceToman(tronadoApiKey || undefined);
        const tronAmount = Number((finalPrice / tronPrice).toFixed(6));
        const callbackBase = await getPublicBaseUrl(env.PUBLIC_BASE_URL);
        if (!callbackBase) {
            await tg("sendMessage", { chat_id: chatId, text: "آدرس سایت برای Callback تنظیم نشده است. لطفاً به پشتیبانی پیام دهید." });
            await notifyAdmins(`⚠️ تنظیمات Callback Base ناقص است (Tronado)\nسفارش: ${purchaseId}`, {
                inline_keyboard: [[{ text: "🔎 باز کردن سفارش", callback_data: `admin_open_purchase_${purchaseId}` }]]
            });
            return;
        }
        const token = await getOrderToken({
            paymentId: purchaseId,
            walletAddress,
            tronAmount: Math.max(0.1, tronAmount),
            callbackUrl: `${callbackBase}/api/tronado-callback`,
            apiKey: tronadoApiKey || undefined
        });
        await sql `
      INSERT INTO orders
      (
        purchase_id, telegram_id, product_id, product_name_snapshot, sell_mode, source_panel_id, panel_delivery_mode, panel_config_snapshot,
        payment_method, discount_code, discount_amount, final_price, tron_amount, status, tronado_token, tronado_payment_url, wallet_used
      )
      VALUES
      (
        ${purchaseId}, ${userId}, ${product.id}, ${productNameSnapshot}, ${sellMode}, ${product.panel_id || null}, ${parseDeliveryMode(String(product.panel_delivery_mode || ""))},
        ${JSON.stringify(panelConfigSnapshot)}::jsonb,
        'tronado', ${discountCode}, ${discountAmount}, ${finalPrice}, ${Math.max(0.1, tronAmount)}, 'pending', ${token.token}, ${token.paymentUrl}, ${walletUsed}
      );
    `;
        await tg("sendMessage", {
            chat_id: chatId,
            text: `سفارش شما ساخته شد ✅\n` +
                `شناسه خرید: ${purchaseId}\n` +
                `محصول: ${productNameSnapshot}\n` +
                `مبلغ: ${formatPriceToman(finalPrice)} تومان\n` +
                `مقدار TRON: ${Math.max(0.1, tronAmount)}\n\n` +
                `بعد از پرداخت، روی دکمه «بررسی پرداخت» بزنید.`,
            reply_markup: {
                inline_keyboard: [
                    [{ text: "💳 پرداخت", url: token.paymentUrl }],
                    [cb("✅ بررسی پرداخت", `check_order_${purchaseId}`, "success")],
                    [homeButton()]
                ]
            }
        });
    }
    catch (error) {
        logError("create_order_failed", error, { chatId, userId, productId, paymentMethod });
        await tg("sendMessage", { chat_id: chatId, text: `ساخت سفارش با خطا مواجه شد: ${String(error.message || error)}` });
    }
}
async function showMyConfigs(chatId, userId, forTopupFlow) {
    const rows = await sql `
    SELECT i.id, i.config_value, i.delivery_payload, p.name, o.purchase_id
    FROM inventory i
    INNER JOIN products p ON p.id = i.product_id
    LEFT JOIN orders o ON o.id = i.sold_order_id
    WHERE i.owner_telegram_id = ${userId} AND i.status = 'sold'
    ORDER BY i.id DESC
    LIMIT 30;
  `;
    if (!rows.length) {
        await tg("sendMessage", { chat_id: chatId, text: "شما هنوز کانفیگی خریداری نکرده‌اید." });
        return;
    }
    const keyboard = rows.map((row) => [
        {
            text: (() => {
                const payload = parseDeliveryPayload(row.delivery_payload);
                const label = payload.metadata?.label ? String(payload.metadata.label) : "";
                const revoked = payload.metadata?.revoked === true;
                const title = label ? `${label} (${row.name})` : String(row.name);
                return `🔹 ${title}${revoked ? " 🚫" : ""} | سفارش ${row.purchase_id || "#-"} | ${configSummaryLine(payload)}`;
            })(),
            callback_data: `open_config_${row.id}${forTopupFlow ? "_t" : ""}`
        }
    ]);
    if (!forTopupFlow) {
        keyboard.push([cb("🧾 سفارش‌های من", "my_orders", "primary"), cb("🔎 پیگیری سفارش", "order_lookup", "primary")]);
        keyboard.push([cb("➕ افزایش دیتا", "topup_menu", "primary"), cb("📜 درخواست‌های انتقال", "my_migrations", "primary")]);
    }
    keyboard.push([homeButton()]);
    await tg("sendMessage", {
        chat_id: chatId,
        text: forTopupFlow
            ? "کانفیگ موردنظر برای افزایش دیتا را انتخاب کنید:"
            : "کانفیگ‌های خریداری‌شده شما 👇\nبرای دیدن جزئیات و QR روی هر کانفیگ بزنید:",
        reply_markup: { inline_keyboard: keyboard }
    });
}
async function openMyConfig(chatId, userId, inventoryId, fromTopupFlow) {
    const rows = await sql `
    SELECT i.id, i.config_value, i.delivery_payload, i.panel_id, p.name, p.panel_config, o.purchase_id
    FROM inventory i
    INNER JOIN products p ON p.id = i.product_id
    LEFT JOIN orders o ON o.id = i.sold_order_id
    WHERE i.id = ${inventoryId} AND i.owner_telegram_id = ${userId} AND i.status = 'sold'
    LIMIT 1;
  `;
    if (!rows.length) {
        await tg("sendMessage", { chat_id: chatId, text: "این کانفیگ برای شما نیست یا یافت نشد." });
        return;
    }
    const row = rows[0];
    const delivery = parseDeliveryPayload(row.delivery_payload);
    const revoked = delivery.metadata?.revoked === true;
    const isPanelConfig = Boolean(delivery.metadata?.panelType) && String(delivery.metadata?.panelType || "") !== "manual";
    const panelId = Number(row.panel_id || 0);
    // Validate panel config link matches
    if (isPanelConfig && panelId > 0) {
        const panelRows = await sql `SELECT * FROM panels WHERE id = ${panelId} LIMIT 1;`;
        if (panelRows.length > 0) {
            const panel = panelRows[0];
            const panelType = String(delivery.metadata?.panelType || panel.panel_type);
            const identifier = String(delivery.metadata?.username || delivery.metadata?.uuid || delivery.metadata?.email || delivery.metadata?.subId || "").trim();
            const userSubLink = String(delivery.subscriptionUrl || "").trim();
            let panelSubLink = "";
            let foundOnPanel = false;
            let panelError = false;
            if (panelType === "marzban") {
                const found = await lookupMarzbanUser(panel, identifier);
                if (found.ok && found.user) {
                    foundOnPanel = true;
                    panelSubLink = String(found.user.subscription_url || "").trim();
                }
                else if (found.message !== "user_not_found") {
                    panelError = true;
                }
            }
            else if (panelType === "sanaei") {
                const found = await findSanaeiClientByIdentifier(panel, identifier);
                if (found.ok && found.client) {
                    foundOnPanel = true;
                    const subId = String(found.client.subId || "");
                    const panelConfig = typeof row.panel_config === "string" ? parseJsonObject(row.panel_config) : row.panel_config;
                    if (subId) {
                        panelSubLink = buildSanaeiSubscriptionUrl(String(panel.base_url), panelConfig || {}, subId).trim();
                    }
                }
                else if (found.message !== "client_not_found") {
                    panelError = true;
                }
            }
            if (!panelError) {
                const mismatch = !foundOnPanel || (userSubLink && panelSubLink && userSubLink !== panelSubLink) || (!panelSubLink && userSubLink);
                if (mismatch) {
                    await sql `
            WITH
            nullify_orders AS (
              UPDATE orders SET inventory_id = NULL WHERE inventory_id = ${row.id}
            ),
            deleted_forensics AS (
              DELETE FROM config_forensics WHERE inventory_id = ${row.id}
            ),
            deleted_topups AS (
              DELETE FROM topup_requests WHERE inventory_id = ${row.id}
            ),
            deleted_migrations AS (
              DELETE FROM panel_migrations WHERE source_inventory_id = ${row.id}
            )
            DELETE FROM inventory WHERE id = ${row.id};
          `;
                    await tg("sendMessage", { chat_id: chatId, text: "کانفیگ در پنل یافت نشد یا لینک آن تغییر کرده است. این کانفیگ از لیست شما حذف شد." });
                    return;
                }
            }
        }
    }
    const keyboard = [
        [{ text: "➕ درخواست افزایش دیتا", callback_data: `request_topup_${row.id}` }],
        [{ text: "🔁 انتقال به پنل جدید", callback_data: `config_migrate_targets_${row.id}` }],
        [{ text: "🧹 حذف از لیست من", callback_data: `customer_remove_cfg_${row.id}` }],
        ...(isPanelConfig ? [[{ text: "🔄 بازسازی لینک", callback_data: `customer_revoke_cfg_${row.id}` }]] : []),
        [{ text: "📦 بازگشت به لیست کانفیگ‌ها", callback_data: fromTopupFlow ? "topup_menu" : "my_configs" }],
        [homeButton()]
    ];
    if (revoked) {
        await tg("sendMessage", { chat_id: chatId, text: "⚠️ این کانفیگ توسط ادمین غیرفعال شده است." });
    }
    await sendDeliveryPackage(chatId, String(row.purchase_id || "-"), String(row.config_value), delivery, keyboard, `محصول: ${row.name}`);
}
async function notifyAdmins(text, replyMarkup) {
    for (const adminId of adminIds) {
        try {
            await tg("sendMessage", { chat_id: adminId, text, reply_markup: replyMarkup });
        }
        catch (error) {
            logError("notify_admin_generic_failed", error, { adminId });
            continue;
        }
    }
}
async function getTelegramProfileText(userId) {
    const rows = await sql `
    SELECT username, first_name, last_name
    FROM users
    WHERE telegram_id = ${userId}
    LIMIT 1;
  `;
    const username = rows.length && rows[0].username ? `@${String(rows[0].username)}` : "-";
    const fullName = [rows[0]?.first_name ? String(rows[0].first_name) : "", rows[0]?.last_name ? String(rows[0].last_name) : ""].filter(Boolean).join(" ").trim() || "-";
    return { username, fullName };
}
async function sendPurchaseLookupResult(chatId, purchaseId) {
    const orderRows = await sql `
    SELECT
      o.purchase_id,
      o.telegram_id,
      o.product_id,
      o.status,
      o.final_price,
      o.wallet_used,
      o.payment_method,
      o.created_at,
      COALESCE(o.product_name_snapshot, p.name) AS product_name,
      u.username,
      u.first_name,
      u.last_name
    FROM orders o
    LEFT JOIN products p ON p.id = o.product_id
    LEFT JOIN users u ON u.telegram_id = o.telegram_id
    WHERE o.purchase_id = ${purchaseId}
    LIMIT 1;
  `;
    if (orderRows.length) {
        const row = orderRows[0];
        const username = row.username ? `@${String(row.username)}` : "-";
        const fullName = [row.first_name ? String(row.first_name) : "", row.last_name ? String(row.last_name) : ""].filter(Boolean).join(" ").trim() || "-";
        const actualWalletUsed = Number(row.wallet_used || 0);
        const walletUsedText = actualWalletUsed > 0 ? `\nکسر از کیف پول: ${formatPriceToman(actualWalletUsed)} تومان` : "";
        await tg("sendMessage", {
            chat_id: chatId,
            text: `جزئیات سفارش:\n` +
                `شماره سفارش: ${row.purchase_id}\n` +
                `نوع: خرید محصول\n` +
                `کاربر: ${row.telegram_id}\n` +
                `یوزرنیم: ${username}\n` +
                `نام: ${fullName}\n` +
                `محصول: ${row.product_name || row.product_id}\n` +
                `مبلغ پرداختی: ${formatPriceToman(Number(row.final_price))} تومان` + walletUsedText + `\n` +
                `روش پرداخت: ${row.payment_method}\n` +
                `وضعیت: ${row.status}\n` +
                `زمان: ${row.created_at}`
        });
        return true;
    }
    const topupRows = await sql `
    SELECT
      t.purchase_id,
      t.telegram_id,
      t.inventory_id,
      t.requested_mb,
      t.status,
      t.final_price,
      t.payment_method,
      t.created_at,
      u.username,
      u.first_name,
      u.last_name
    FROM topup_requests t
    LEFT JOIN users u ON u.telegram_id = t.telegram_id
    WHERE t.purchase_id = ${purchaseId}
    LIMIT 1;
  `;
    if (!topupRows.length) {
        await tg("sendMessage", { chat_id: chatId, text: "شماره سفارش پیدا نشد." });
        return false;
    }
    const row = topupRows[0];
    const username = row.username ? `@${String(row.username)}` : "-";
    const fullName = [row.first_name ? String(row.first_name) : "", row.last_name ? String(row.last_name) : ""].filter(Boolean).join(" ").trim() || "-";
    await tg("sendMessage", {
        chat_id: chatId,
        text: `جزئیات سفارش:\n` +
            `شماره سفارش: ${row.purchase_id}\n` +
            `نوع: افزایش دیتا\n` +
            `کاربر: ${row.telegram_id}\n` +
            `یوزرنیم: ${username}\n` +
            `نام: ${fullName}\n` +
            `کانفیگ: ${row.inventory_id}\n` +
            `حجم درخواستی: ${row.requested_mb}MB\n` +
            `مبلغ: ${formatPriceToman(Number(row.final_price))} تومان\n` +
            `روش پرداخت: ${row.payment_method}\n` +
            `وضعیت: ${row.status}\n` +
            `زمان: ${row.created_at}`
    });
    return true;
}
const rateLimitMap = new Map();
async function isRateLimited(userId, key, windowMs) {
    const mapKey = `rl_${key}_${userId}`;
    const now = Date.now();
    const last = rateLimitMap.get(mapKey) || 0;
    if (now - last < windowMs)
        return true;
    rateLimitMap.set(mapKey, now);
    // Clean up old entries occasionally to prevent memory leak
    if (rateLimitMap.size > 1000) {
        const cutoff = now - Math.max(windowMs, 60000);
        for (const [k, v] of rateLimitMap.entries()) {
            if (v < cutoff)
                rateLimitMap.delete(k);
        }
    }
    return false;
}
export async function fulfillOrderByPaymentId(paymentId) {
    await ensureSchema();
    const topupRows = await sql `
    SELECT id, telegram_id, amount, status, payment_method
    FROM wallet_topups
    WHERE receipt_file_id = ${paymentId}
    LIMIT 1;
  `;
    if (topupRows.length) {
        const topup = topupRows[0];
        if (topup.status === 'paid')
            return { ok: false, reason: "already_paid" };
        await sql `
      UPDATE wallet_topups
      SET status = 'paid', done_at = NOW()
      WHERE id = ${topup.id};
    `;
        await sql `
      UPDATE users
      SET wallet_balance = wallet_balance + ${topup.amount}
      WHERE telegram_id = ${topup.telegram_id};
    `;
        const paymentMethod = String(topup.payment_method || "");
        const paymentLabel = paymentMethod === "tronado"
            ? "Tronado"
            : paymentMethod === "tetrapay"
                ? "تتراپی"
                : paymentMethod === "plisio"
                    ? "Plisio"
                    : paymentMethod === "crypto"
                        ? "کریپتو"
                        : paymentMethod || "-";
        await sql `
      INSERT INTO wallet_transactions (telegram_id, amount, type, description)
      VALUES (${topup.telegram_id}, ${topup.amount}, 'charge', ${`شارژ از طریق ${paymentLabel}`});
    `;
        try {
            await tg("sendMessage", {
                chat_id: Number(topup.telegram_id),
                text: `✅ پرداخت شما با موفقیت انجام شد و مبلغ ${formatPriceToman(Number(topup.amount))} تومان به کیف پول شما اضافه شد.`
            });
            for (const adminId of adminIds) {
                await tg("sendMessage", {
                    chat_id: adminId,
                    text: `💰 کاربر ${topup.telegram_id} مبلغ ${formatPriceToman(Number(topup.amount))} تومان از طریق ${paymentLabel} کیف پول خود را شارژ کرد.`
                }).catch(() => { });
            }
        }
        catch (e) {
            logError("notify_wallet_charge_success_failed", e, { topupId: topup.id });
        }
        return { ok: true, reason: "wallet_charged" };
    }
    const rows = await sql `
    SELECT id, purchase_id, telegram_id, product_id, status
    FROM orders
    WHERE purchase_id = ${paymentId}
    LIMIT 1;
  `;
    if (!rows.length) {
        return { ok: false, reason: "order_not_found" };
    }
    return await finalizeOrder(Number(rows[0].id), null);
}
async function finalizeOrder(orderId, decidedBy) {
    const locked = await sql `
    UPDATE orders
    SET status = 'fulfilling'
    WHERE id = ${orderId}
      AND status IN ('pending', 'receipt_submitted', 'awaiting_receipt')
    RETURNING id;
  `;
    if (!locked.length) {
        const s = await sql `SELECT status FROM orders WHERE id = ${orderId} LIMIT 1;`;
        const status = s.length ? String(s[0].status) : "";
        if (status === "paid")
            return { ok: true, reason: "already_paid" };
        if (status === "fulfilling")
            return { ok: true, reason: "already_processing" };
        if (status === "denied")
            return { ok: false, reason: "denied" };
        return { ok: false, reason: "order_not_found" };
    }
    const rows = await sql `
    SELECT
      o.id,
      o.purchase_id,
      o.telegram_id,
      o.product_id,
      o.status,
      o.sell_mode,
      o.source_panel_id,
      o.panel_delivery_mode,
      o.panel_config_snapshot,
      o.wallet_used,
      o.payment_method,
      COALESCE(o.product_name_snapshot, p.name) AS product_name,
      p.size_mb,
      p.is_infinite
    FROM orders o
    INNER JOIN products p ON p.id = o.product_id
    WHERE o.id = ${orderId}
    LIMIT 1;
  `;
    if (!rows.length)
        return { ok: false, reason: "order_not_found" };
    const order = rows[0];
    const profile = await getTelegramProfileText(Number(order.telegram_id));
    if (order.payment_method === 'tronado' || order.payment_method === 'tetrapay' || order.payment_method === 'plisio') {
        const walletUsed = Number(order.wallet_used || 0);
        if (walletUsed > 0) {
            const negativeWalletUsed = -walletUsed;
            await sql `
          WITH deducted AS (
            UPDATE users SET wallet_balance = wallet_balance - ${walletUsed} WHERE telegram_id = ${order.telegram_id}
            RETURNING telegram_id
          )
          INSERT INTO wallet_transactions (telegram_id, amount, type, description, created_at)
          SELECT telegram_id, ${negativeWalletUsed}, 'purchase', ${`کسر بخشی از مبلغ خرید محصول ${order.product_name}`}, NOW()
          FROM deducted;
        `;
        }
    }
    if (parseSellMode(String(order.sell_mode || "")) === "panel") {
        const panelRows = await sql `
      SELECT id, panel_type, base_url, username, password, active, allow_new_sales
      FROM panels
      WHERE id = ${order.source_panel_id}
      LIMIT 1;
    `;
        if (!panelRows.length || !panelRows[0].active || !panelRows[0].allow_new_sales) {
            await sql `UPDATE orders SET status = 'receipt_submitted' WHERE id = ${order.id} AND status = 'fulfilling';`;
            return { ok: false, reason: "panel_unavailable" };
        }
        const panel = panelRows[0];
        const panelConfig = sanitizePanelConfig(order.panel_config_snapshot);
        let provision;
        try {
            provision =
                String(panel.panel_type) === "marzban"
                    ? await provisionMarzbanSale(panel, order, panelConfig)
                    : await provisionSanaeiSale(panel, order, panelConfig);
        }
        catch (err) {
            logError("provision_failed", err, { orderId });
            // If panel fails, we don't refund. The user paid (crypto or wallet or card).
            // We mark it as 'awaiting_config' and alert the admin to provide it manually.
            await sql `
        UPDATE orders
        SET status = 'awaiting_config', paid_at = NOW(), admin_decision_by = ${decidedBy}
        WHERE id = ${order.id} AND status = 'fulfilling';
      `;
            await tg("sendMessage", {
                chat_id: Number(order.telegram_id),
                text: `پرداخت شما تایید شد ✅\nمتاسفانه ارتباط با سرور برای ساخت اتوماتیک کانفیگ شما (سفارش ${order.purchase_id}) با خطا مواجه شد.\nادمین به زودی کانفیگ شما را به صورت دستی تحویل خواهد داد.`
            }).catch(() => { });
            await notifyAdmins(`❌ خطای ساخت کانفیگ روی پنل برای سفارش ${order.purchase_id}:\n${err.message || "Unknown error"}\nسفارش در وضعیت «نیازمند کانفیگ دستی» قرار گرفت.`, {
                inline_keyboard: [
                    [{ text: "ارسال کانفیگ دستی", callback_data: `admin_provide_config_${order.id}` }],
                    [{ text: "🔎 بررسی سفارش", callback_data: `admin_open_purchase_${String(order.purchase_id)}` }]
                ]
            });
            return { ok: false, reason: "provision_failed" };
        }
        const delivered = parseDeliveryPayload(provision.deliveryPayload);
        const panelUserKey = String(delivered.metadata?.username || delivered.metadata?.email || delivered.metadata?.subId || delivered.metadata?.uuid || "").trim() || null;
        const inserted = await sql `
      INSERT INTO inventory (
        product_id, panel_user_key, config_value, delivery_payload, status, owner_telegram_id, sold_order_id, panel_id, sold_at
      )
      VALUES (
        ${order.product_id},
        ${panelUserKey},
        ${provision.configValue},
        ${serializeDeliveryPayload(provision.deliveryPayload)}::jsonb,
        'sold',
        ${order.telegram_id},
        ${order.id},
        ${order.source_panel_id},
        NOW()
      )
      RETURNING id;
    `;
        await sql `
      UPDATE orders
      SET status = 'paid', paid_at = NOW(), inventory_id = ${inserted[0].id}, admin_decision_by = ${decidedBy}
      WHERE id = ${order.id};
    `;
        await recordInventoryForensicEvent(Number(inserted[0].id), "sale_delivered", {
            purchaseId: String(order.purchase_id),
            by: decidedBy
        });
        await tg("sendMessage", {
            chat_id: Number(order.telegram_id),
            text: "پرداخت شما تایید شد ✅"
        }).catch(() => { });
        await sendDeliveryPackage(Number(order.telegram_id), String(order.purchase_id), String(provision.configValue), provision.deliveryPayload, [
            [{ text: "➕ درخواست افزایش دیتا", callback_data: "topup_menu" }],
            [homeButton()]
        ]).catch((e) => logError("delivery_package_failed", e, { orderId: order.id }));
        await notifyAdmins(buildAdminDeliverySummary({
            purchaseId: String(order.purchase_id),
            userId: Number(order.telegram_id),
            telegramUsername: profile.username,
            telegramFullName: profile.fullName,
            productName: String(order.product_name || "-"),
            deliveryPayload: provision.deliveryPayload,
            walletUsed: Number(order.wallet_used || 0)
        }), { inline_keyboard: [[{ text: "🔎 باز کردن سفارش", callback_data: `admin_open_purchase_${String(order.purchase_id)}` }]] });
        return { ok: true, reason: "fulfilled" };
    }
    const globalInfinite = await getBoolSetting("global_infinite_mode", false);
    const allocated = await sql `
    UPDATE inventory
    SET status = 'sold', owner_telegram_id = ${order.telegram_id}, sold_order_id = ${order.id}, sold_at = NOW()
    WHERE id = (
      SELECT id FROM inventory
      WHERE product_id = ${order.product_id} AND status = 'available'
      ORDER BY id ASC
      LIMIT 1
      FOR UPDATE SKIP LOCKED
    )
    RETURNING id, config_value;
  `;
    if (!allocated.length) {
        const panelConfig = sanitizePanelConfig(order.panel_config_snapshot);
        const forceAwaitingConfig = panelConfig.force_awaiting_config === true;
        if (globalInfinite || order.is_infinite || forceAwaitingConfig) {
            await sql `
        UPDATE orders
        SET status = 'awaiting_config', paid_at = NOW(), admin_decision_by = ${decidedBy}
        WHERE id = ${order.id};
      `;
            await tg("sendMessage", {
                chat_id: Number(order.telegram_id),
                text: `پرداخت شما تایید شد ✅\nشناسه خرید: ${order.purchase_id}\nدر حال آماده‌سازی کانفیگ هستیم.`
            }).catch(() => { });
            const extraLines = [];
            if (typeof panelConfig.data_limit_mb === "number")
                extraLines.push(`حجم: ${Math.max(1, Math.round(Number(panelConfig.data_limit_mb) / 1024))} گیگابایت`);
            if (typeof panelConfig.expire_days === "number")
                extraLines.push(`زمان: ${Math.max(1, Math.round(Number(panelConfig.expire_days)))} روز`);
            await notifyAdmins(`🛠 سفارش ${order.purchase_id} نیاز به ساخت کانفیگ دستی دارد.${extraLines.length ? `\n${extraLines.join("\n")}` : ""}`, {
                inline_keyboard: [[{ text: "ارسال کانفیگ", callback_data: `admin_provide_config_${order.id}` }]]
            });
            return { ok: true, reason: "awaiting_config" };
        }
        await sql `UPDATE orders SET status = 'receipt_submitted' WHERE id = ${order.id} AND status = 'fulfilling';`;
        await notifyAdmins(`⚠️ سفارش ${order.purchase_id} پرداخت شد اما موجودی این محصول تمام شده است.`);
        return { ok: false, reason: "stock_empty" };
    }
    await sql `
    UPDATE orders
    SET status = 'paid', paid_at = NOW(), inventory_id = ${allocated[0].id}, admin_decision_by = ${decidedBy}
    WHERE id = ${order.id};
  `;
    await recordInventoryForensicEvent(Number(allocated[0].id), "sale_delivered", {
        purchaseId: String(order.purchase_id),
        by: decidedBy
    });
    await tg("sendMessage", {
        chat_id: Number(order.telegram_id),
        text: "پرداخت شما تایید شد ✅"
    }).catch(() => { });
    await sendDeliveryPackage(Number(order.telegram_id), String(order.purchase_id), String(allocated[0].config_value), { configLinks: [String(allocated[0].config_value)] }, [
        [{ text: "➕ درخواست افزایش دیتا", callback_data: "topup_menu" }],
        [homeButton()]
    ]).catch((e) => logError("delivery_package_failed", e, { orderId: order.id }));
    await notifyAdmins(buildAdminDeliverySummary({
        purchaseId: String(order.purchase_id),
        userId: Number(order.telegram_id),
        telegramUsername: profile.username,
        telegramFullName: profile.fullName,
        productName: String(order.product_name || "-"),
        deliveryPayload: { configLinks: [String(allocated[0].config_value)] },
        walletUsed: Number(order.wallet_used || 0)
    }), { inline_keyboard: [[{ text: "🔎 باز کردن سفارش", callback_data: `admin_open_purchase_${String(order.purchase_id)}` }]] });
    return { ok: true, reason: "fulfilled" };
}
async function handleCallback(update) {
    if (!update?.data || !update.message)
        return;
    const data = update.data;
    const userId = update.from.id;
    const chatId = update.message.chat.id;
    await upsertUser(update.from);
    if (data !== "check_membership") {
        await tg("answerCallbackQuery", { callback_query_id: update.id }).catch(() => { });
    }
    if (data.startsWith("noop_")) {
        return;
    }
    if (await isBanned(userId)) {
        await tg("sendMessage", { chat_id: chatId, text: "دسترسی شما به دلیل تخلف مسدود شده است." });
        return;
    }
    if (data !== "check_membership" && !(await checkMandatoryChannels(userId, chatId))) {
        return;
    }
    if (data === "check_membership") {
        const isMember = await checkMandatoryChannels(userId, chatId, true);
        if (isMember) {
            const msgId = update.message?.message_id || 0;
            if (msgId) {
                const deleted = await tg("deleteMessage", { chat_id: chatId, message_id: msgId }).catch(() => null);
                if (!deleted || !deleted.ok) {
                    await tg("editMessageText", { chat_id: chatId, message_id: msgId, text: "عضویت شما تایید شد ✅" }).catch(() => { });
                }
            }
            await sendMainMenu(chatId, userId, "عضویت شما تایید شد ✅");
        }
        else {
            await tg("answerCallbackQuery", { callback_query_id: update.id, text: "هنوز در همه کانال‌ها عضو نشده‌اید!", show_alert: true }).catch(() => { });
        }
        return;
    }
    if (data === "home") {
        await clearState(userId);
        await sendMainMenu(chatId, userId);
        return;
    }
    if (data === "wallet_menu") {
        await clearState(userId);
        const userRows = await sql `SELECT wallet_balance FROM users WHERE telegram_id = ${userId} LIMIT 1;`;
        const balance = userRows.length ? Number(userRows[0].wallet_balance || 0) : 0;
        await tg("sendMessage", {
            chat_id: chatId,
            text: `👛 کیف پول\n\nموجودی فعلی: ${formatPriceToman(balance)} تومان`,
            reply_markup: {
                inline_keyboard: [
                    [cb("➕ شارژ موجودی", "wallet_charge", "success")],
                    [homeButton()]
                ]
            }
        });
        return;
    }
    if (data === "wallet_charge") {
        await setState(userId, "await_wallet_charge_amount");
        await tg("sendMessage", {
            chat_id: chatId,
            text: "مبلغ شارژ را به تومان ارسال کنید.\nمثال: 50000",
            reply_markup: { inline_keyboard: [[backButton("wallet_menu")]] }
        });
        return;
    }
    if (data.startsWith("wallet_charge_method_")) {
        const method = data.replace("wallet_charge_method_", "");
        const state = await getState(userId);
        if (!state || state.state !== "await_wallet_charge_method")
            return;
        const amount = Number(state.payload.amount);
        if (method === "tronado") {
            const rows = await sql `
        INSERT INTO wallet_topups (telegram_id, amount, payment_method)
        VALUES (${userId}, ${amount}, 'tronado')
        RETURNING id;
      `;
            const topupId = Number(rows[0].id);
            try {
                const walletFromSetting = await getSetting("business_wallet_address");
                const walletAddress = walletFromSetting || env.BUSINESS_WALLET_ADDRESS;
                if (!walletAddress) {
                    await tg("sendMessage", { chat_id: chatId, text: "تنظیمات کیف پول کامل نیست. لطفاً به پشتیبانی پیام دهید." });
                    return;
                }
                const tronadoApiKey = ((await getSetting("tronado_api_key")) || "").trim();
                const tronPrice = await getTronPriceToman(tronadoApiKey || undefined);
                const tronAmount = Number((amount / tronPrice).toFixed(6));
                const callbackBase = await getPublicBaseUrl(env.PUBLIC_BASE_URL);
                if (!callbackBase) {
                    await tg("sendMessage", { chat_id: chatId, text: "آدرس سایت برای Callback تنظیم نشده است. لطفاً به پشتیبانی پیام دهید." });
                    return;
                }
                const paymentId = `W${Date.now()}${Math.floor(Math.random() * 10000).toString().padStart(4, "0")}`;
                const tokenData = await getOrderToken({
                    paymentId,
                    walletAddress,
                    tronAmount: Math.max(0.1, tronAmount),
                    callbackUrl: `${callbackBase}/api/tronado-callback`,
                    apiKey: tronadoApiKey || undefined
                });
                await sql `UPDATE wallet_topups SET receipt_file_id = ${paymentId} WHERE id = ${topupId}`;
                await tg("sendMessage", {
                    chat_id: chatId,
                    text: `لینک پرداخت ترونادو برای شارژ کیف پول آماده است:\nمبلغ: ${formatPriceToman(amount)} تومان`,
                    reply_markup: { inline_keyboard: [[{ text: "💳 پرداخت با Tronado", url: tokenData.paymentUrl }]] }
                });
                await clearState(userId);
            }
            catch (error) {
                logError("create_wallet_tronado_failed", error, { userId, amount });
                await tg("sendMessage", { chat_id: chatId, text: "خطا در ایجاد لینک پرداخت." });
            }
        }
        else if (method === "tetrapay") {
            try {
                const callbackBase = await getPublicBaseUrl(env.PUBLIC_BASE_URL);
                if (!callbackBase) {
                    await tg("sendMessage", { chat_id: chatId, text: "آدرس سایت برای Callback تنظیم نشده است. لطفاً به پشتیبانی پیام دهید." });
                    return;
                }
                const tetrapayApiKey = ((await getSetting("tetrapay_api_key")) || "").trim();
                if (!tetrapayApiKey) {
                    await tg("sendMessage", { chat_id: chatId, text: "کلید تتراپی تنظیم نشده است. لطفاً به پشتیبانی پیام دهید." });
                    return;
                }
                const paymentId = `W${Date.now()}${Math.floor(Math.random() * 10000).toString().padStart(4, "0")}`;
                const { createTetrapayOrder } = await import("./tetrapay.js");
                const orderRes = await createTetrapayOrder({
                    purchaseId: paymentId,
                    amountToman: amount,
                    description: `شارژ کیف پول`,
                    callbackUrl: `${callbackBase}/api/tetrapay-callback`,
                    apiKey: tetrapayApiKey
                });
                if (!orderRes.ok) {
                    await tg("sendMessage", { chat_id: chatId, text: `خطا در ارتباط با درگاه تتراپی: ${orderRes.message}` });
                    return;
                }
                await sql `
          INSERT INTO wallet_topups (telegram_id, amount, payment_method, receipt_file_id)
          VALUES (${userId}, ${amount}, 'tetrapay', ${paymentId});
        `;
                await tg("sendMessage", {
                    chat_id: chatId,
                    text: `لینک پرداخت تتراپی برای شارژ کیف پول آماده است:\nمبلغ: ${formatPriceToman(amount)} تومان`,
                    reply_markup: {
                        inline_keyboard: [
                            [{ text: "💳 پرداخت با تتراپی", url: orderRes.paymentUrlBot }],
                            [homeButton()]
                        ]
                    }
                });
                await clearState(userId);
            }
            catch (error) {
                logError("create_wallet_tetrapay_failed", error, { userId, amount });
                await tg("sendMessage", { chat_id: chatId, text: "خطا در ایجاد لینک پرداخت." });
            }
        }
        else if (method === "plisio") {
            try {
                const callbackBase = await getPublicBaseUrl(env.PUBLIC_BASE_URL);
                if (!callbackBase) {
                    await tg("sendMessage", { chat_id: chatId, text: "آدرس سایت برای Callback تنظیم نشده است. لطفاً به پشتیبانی پیام دهید." });
                    return;
                }
                const plisioApiKey = ((await getSetting("plisio_api_key")) || "").trim();
                if (!plisioApiKey) {
                    await tg("sendMessage", { chat_id: chatId, text: "تنظیمات Plisio کامل نیست. لطفاً به پشتیبانی پیام دهید." });
                    return;
                }
                const paymentId = `W${Date.now()}${Math.floor(Math.random() * 10000).toString().padStart(4, "0")}`;
                const tomanPerUsdt = await getPlisioTomanPerUsdt();
                const usdtAmount = Math.max(0.01, Number((amount / tomanPerUsdt).toFixed(2)));
                const { createPlisioInvoice } = await import("./plisio.js");
                const invoice = await createPlisioInvoice({
                    apiKey: plisioApiKey,
                    orderNumber: paymentId.slice(1),
                    orderName: paymentId,
                    sourceCurrency: "USD",
                    sourceAmount: usdtAmount,
                    callbackUrl: `${callbackBase}/api/plisio-callback?json=true`
                });
                await sql `
          INSERT INTO wallet_topups (telegram_id, amount, payment_method, receipt_file_id)
          VALUES (${userId}, ${amount}, 'plisio', ${paymentId});
        `;
                await tg("sendMessage", {
                    chat_id: chatId,
                    text: `لینک پرداخت Plisio برای شارژ کیف پول آماده است:\nمبلغ: ${formatPriceToman(amount)} تومان\nمعادل تقریبی: ${usdtAmount} USDT`,
                    reply_markup: { inline_keyboard: [[{ text: "💳 پرداخت با Plisio", url: invoice.invoiceUrl }], [homeButton()]] }
                });
                await clearState(userId);
            }
            catch (error) {
                logError("create_wallet_plisio_failed", error, { userId, amount });
                await tg("sendMessage", { chat_id: chatId, text: "خطا در ایجاد لینک پرداخت." });
            }
        }
        else if (method === "card2card") {
            const cards = await sql `SELECT card_number, holder_name, bank_name FROM cards WHERE active = TRUE;`;
            if (!cards.length) {
                await tg("sendMessage", { chat_id: chatId, text: "هیچ کارتی برای کارت‌به‌کارت تنظیم نشده است." });
                return;
            }
            const rows = await sql `
        INSERT INTO wallet_topups (telegram_id, amount, payment_method)
        VALUES (${userId}, ${amount}, 'card2card')
        RETURNING id;
      `;
            const topupId = Number(rows[0].id);
            await setState(userId, "await_wallet_receipt", { topupId });
            const cardsText = cards
                .map(c => `💳 ${c.card_number}\n👤 ${c.holder_name || "نامشخص"} (${c.bank_name || "نامشخص"})`)
                .join("\n\n");
            await tg("sendMessage", {
                chat_id: chatId,
                text: `مبلغ: ${formatPriceToman(amount)} تومان\n\nلطفاً مبلغ را به یکی از کارت‌های زیر واریز کنید:\n\n${cardsText}\n\nسپس تصویر رسید را همینجا ارسال کنید.`
            });
        }
        else {
            await tg("sendMessage", { chat_id: chatId, text: "این روش پرداخت برای شارژ کیف پول پشتیبانی نمی‌شود." });
        }
        return;
    }
    if (data === "buy_menu") {
        await showProducts(chatId, true);
        return;
    }
    if (data.startsWith("buy_custom_v2ray_")) {
        const productId = Number(data.replace("buy_custom_v2ray_", ""));
        await clearState(userId);
        await startCustomV2rayWizard(chatId, userId, productId);
        return;
    }
    if (data.startsWith("buy_product_")) {
        const productId = Number(data.replace("buy_product_", ""));
        const userRows = await sql `SELECT wallet_balance FROM users WHERE telegram_id = ${userId} LIMIT 1;`;
        const walletBalance = userRows.length ? Number(userRows[0].wallet_balance || 0) : 0;
        if (walletBalance > 0) {
            await showWalletUsagePrompt(chatId, userId, productId, walletBalance);
        }
        else {
            await showPaymentMethods(chatId, userId, productId, 0);
        }
        return;
    }
    if (data === "custom_v2ray_inc_data" || data === "custom_v2ray_dec_data" || data === "custom_v2ray_inc_days" || data === "custom_v2ray_dec_days") {
        try {
            const state = await getState(userId);
            if (!state || state.state !== "custom_v2ray_wizard")
                return;
            const p = state.payload || {};
            const baseMb = Math.max(1, Math.round(Number(p.baseMb || 0)));
            const baseDays = Math.max(30, Math.round(Number(p.baseDays || 30)));
            const stepMb = 1024;
            const stepDays = 7;
            const curMb = Math.max(baseMb, Math.round(Number(p.dataMb || baseMb)));
            const curDays = Math.max(baseDays, Math.round(Number(p.days || baseDays)));
            let nextMb = curMb;
            let nextDays = curDays;
            if (data === "custom_v2ray_inc_data")
                nextMb = curMb + stepMb;
            if (data === "custom_v2ray_dec_data")
                nextMb = Math.max(baseMb, curMb - stepMb);
            if (data === "custom_v2ray_inc_days")
                nextDays = curDays + stepDays;
            if (data === "custom_v2ray_dec_days")
                nextDays = Math.max(baseDays, curDays - stepDays);
            await setState(userId, "custom_v2ray_wizard", { ...p, dataMb: nextMb, days: nextDays, messageId: Number(p.messageId || 0) });
            await renderCustomV2rayWizard(chatId, userId, update.message.message_id);
        }
        catch (e) {
            logError("custom_v2ray_adjust_failed", e, { userId, chatId, data });
            await tg("sendMessage", { chat_id: chatId, text: "خطا در بروزرسانی فاکتور." });
        }
        return;
    }
    if (data === "custom_v2ray_confirm") {
        try {
            const checkout = await computeCustomV2rayCheckout(userId);
            if (!checkout)
                return;
            await clearState(userId);
            await setState(userId, "custom_v2ray_checkout", checkout);
            await showCustomWalletUsagePrompt(chatId, userId, checkout.totalPrice);
        }
        catch (e) {
            logError("custom_v2ray_confirm_failed", e, { userId, chatId });
            await tg("sendMessage", { chat_id: chatId, text: "خطا در ادامه پرداخت." });
        }
        return;
    }
    if (data === "custom_v2ray_use_wallet_custom") {
        try {
            const state = await getState(userId);
            if (!state || state.state !== "custom_v2ray_checkout") {
                await tg("sendMessage", { chat_id: chatId, text: "جلسه سفارش سفارشی منقضی شده. دوباره از اول شروع کن." });
                return;
            }
            await setState(userId, "await_custom_wallet_amount", { checkout: state.payload });
            await tg("sendMessage", { chat_id: chatId, text: "مبلغی که می‌خواهی از کیف پول کسر شود را به تومان وارد کن (فقط عدد):" });
        }
        catch (e) {
            logError("custom_v2ray_wallet_custom_failed", e, { userId, chatId });
            await tg("sendMessage", { chat_id: chatId, text: "خطا در انتخاب کیف پول." });
        }
        return;
    }
    if (data.startsWith("custom_v2ray_use_wallet_")) {
        try {
            const amount = Number(data.replace("custom_v2ray_use_wallet_", ""));
            const state = await getState(userId);
            if (!state || state.state !== "custom_v2ray_checkout") {
                await tg("sendMessage", { chat_id: chatId, text: "جلسه سفارش سفارشی منقضی شده. دوباره از اول شروع کن." });
                return;
            }
            const totalPrice = Math.max(1, Math.round(Number(state.payload.totalPrice || 0)));
            await showCustomPaymentMethods(chatId, userId, totalPrice, Math.max(0, Math.round(amount)));
        }
        catch (e) {
            logError("custom_v2ray_wallet_pick_failed", e, { userId, chatId });
            await tg("sendMessage", { chat_id: chatId, text: "خطا در انتخاب کیف پول." });
        }
        return;
    }
    if (data.startsWith("custom_v2ray_select_pay_")) {
        try {
            const payload = data.replace("custom_v2ray_select_pay_", "");
            const parts = payload.split("_");
            const method = parts[0];
            const walletUsed = Math.max(0, Math.round(Number(parts[1] || 0)));
            const state = await getState(userId);
            if (!state || state.state !== "custom_v2ray_checkout") {
                await tg("sendMessage", { chat_id: chatId, text: "جلسه سفارش سفارشی منقضی شده. دوباره از اول شروع کن." });
                return;
            }
            await showDiscountChoiceCustom(chatId, Number(state.payload.productId || 0), method, walletUsed);
        }
        catch (e) {
            logError("custom_v2ray_select_pay_failed", e, { userId, chatId, data });
            await tg("sendMessage", { chat_id: chatId, text: "خطا در انتخاب روش پرداخت." });
        }
        return;
    }
    if (data.startsWith("custom_discount_yes_")) {
        try {
            const payload = data.replace("custom_discount_yes_", "");
            const parts = payload.split("_");
            const productId = Number(parts[0]);
            let walletUsed = 0;
            if (parts.length >= 3 && !isNaN(Number(parts[parts.length - 1]))) {
                walletUsed = Number(parts.pop());
            }
            const paymentMethod = parts.slice(1).join("_");
            const state = await getState(userId);
            if (!state || state.state !== "custom_v2ray_checkout") {
                await tg("sendMessage", { chat_id: chatId, text: "جلسه سفارش سفارشی منقضی شده. دوباره از اول شروع کن." });
                return;
            }
            await setState(userId, "await_custom_discount_code", { productId, paymentMethod, walletUsed, checkout: state.payload });
            await tg("sendMessage", { chat_id: chatId, text: "کد تخفیف را ارسال کنید:" });
        }
        catch (e) {
            logError("custom_v2ray_discount_yes_failed", e, { userId, chatId, data });
            await tg("sendMessage", { chat_id: chatId, text: "خطا در مرحله تخفیف." });
        }
        return;
    }
    if (data.startsWith("custom_discount_no_")) {
        try {
            const payload = data.replace("custom_discount_no_", "");
            const parts = payload.split("_");
            const productId = Number(parts[0]);
            let walletUsed = 0;
            if (parts.length >= 3 && !isNaN(Number(parts[parts.length - 1]))) {
                walletUsed = Number(parts.pop());
            }
            const paymentMethod = parts.slice(1).join("_");
            const state = await getState(userId);
            if (!state || state.state !== "custom_v2ray_checkout") {
                await tg("sendMessage", { chat_id: chatId, text: "جلسه سفارش سفارشی منقضی شده. دوباره از اول شروع کن." });
                return;
            }
            const checkout = state.payload || {};
            const totalPrice = Math.max(1, Math.round(Number(checkout.totalPrice || 0)));
            const dataMb = Math.max(1, Math.round(Number(checkout.dataMb || 0)));
            const days = Math.max(30, Math.round(Number(checkout.days || 30)));
            const gb = Math.max(1, Math.round(dataMb / 1024));
            const overrides = {
                basePriceToman: totalPrice,
                panelConfigPatch: { data_limit_mb: dataMb, expire_days: days, force_awaiting_config: true },
                productNameSuffix: `(سفارشی ${gb}GB / ${days} روز)`
            };
            await clearState(userId);
            await createOrder(chatId, userId, productId, paymentMethod, null, paymentMethod === "wallet" ? 0 : walletUsed, overrides);
        }
        catch (e) {
            logError("custom_v2ray_discount_no_failed", e, { userId, chatId, data });
            await tg("sendMessage", { chat_id: chatId, text: "خطا در ثبت سفارش." });
        }
        return;
    }
    if (data.startsWith("use_wallet_custom_")) {
        const productId = Number(data.replace("use_wallet_custom_", ""));
        await setState(userId, "await_wallet_custom_amount", { productId });
        await tg("sendMessage", { chat_id: chatId, text: "لطفاً مبلغی که می‌خواهید از کیف پول کسر شود را به تومان وارد کنید (فقط عدد):" });
        return;
    }
    if (data.startsWith("use_wallet_")) {
        const parts = data.replace("use_wallet_", "").split("_");
        const productId = Number(parts[0]);
        const amount = Number(parts[1]);
        await showPaymentMethods(chatId, userId, productId, amount);
        return;
    }
    if (data.startsWith("select_pay_")) {
        const payload = data.replace("select_pay_", "");
        const parts = payload.split("_");
        const productId = Number(parts[0]);
        let walletUsed = 0;
        if (parts.length >= 3 && !isNaN(Number(parts[parts.length - 1]))) {
            walletUsed = Number(parts.pop());
        }
        const paymentMethod = parts.slice(1).join("_");
        await showDiscountChoice(chatId, productId, paymentMethod, walletUsed);
        return;
    }
    if (data.startsWith("select_crypto_wallet_")) {
        const walletId = Number(data.replace("select_crypto_wallet_", ""));
        const state = await getState(userId);
        if (!state || state.state !== "await_crypto_wallet_select")
            return;
        const productId = Number(state.payload.productId);
        const discountInput = state.payload.discountInput ? String(state.payload.discountInput) : null;
        const walletUsedParam = Number(state.payload.walletUsedParam || 0);
        const overrides = state.payload.overrides ? state.payload.overrides : null;
        await clearState(userId);
        await createOrder(chatId, userId, productId, `crypto_${walletId}`, discountInput, walletUsedParam, overrides);
        return;
    }
    if (data.startsWith("discount_yes_")) {
        const payload = data.replace("discount_yes_", "");
        const parts = payload.split("_");
        const productId = Number(parts[0]);
        let walletUsed = 0;
        if (parts.length >= 3 && !isNaN(Number(parts[parts.length - 1]))) {
            walletUsed = Number(parts.pop());
        }
        const paymentMethod = parts.slice(1).join("_");
        await setState(userId, "await_discount_code", { productId, paymentMethod, walletUsed });
        await tg("sendMessage", { chat_id: chatId, text: "کد تخفیف را ارسال کنید:" });
        return;
    }
    if (data.startsWith("discount_no_")) {
        const payload = data.replace("discount_no_", "");
        const parts = payload.split("_");
        const productId = Number(parts[0]);
        let walletUsed = 0;
        if (parts.length >= 3 && !isNaN(Number(parts[parts.length - 1]))) {
            walletUsed = Number(parts.pop());
        }
        const paymentMethod = parts.slice(1).join("_");
        await clearState(userId);
        await createOrder(chatId, userId, productId, paymentMethod, null, walletUsed);
        return;
    }
    if (data.startsWith("check_order_")) {
        const purchaseId = data.replace("check_order_", "");
        if (await isRateLimited(userId, "check_order", 10_000)) {
            await tg("sendMessage", { chat_id: chatId, text: "کمی صبر کنید و دوباره تلاش کنید." });
            return;
        }
        try {
            const orderRows = await sql `
        SELECT payment_method, plisio_txn_id, receipt_file_id
        FROM orders
        WHERE purchase_id = ${purchaseId}
        LIMIT 1;
      `;
            if (!orderRows.length) {
                await tg("sendMessage", { chat_id: chatId, text: "سفارش یافت نشد." });
                return;
            }
            const paymentMethod = orderRows[0].payment_method;
            let isAccepted = false;
            if (paymentMethod === "tetrapay") {
                await tg("sendMessage", {
                    chat_id: chatId,
                    text: "بررسی وضعیت پرداخت تتراپی معمولاً به صورت خودکار انجام می‌شود.\nاگر پرداخت کرده‌ای ولی تایید نمی‌شود، اسکرین‌شات پرداخت را ارسال کن تا ادمین بررسی کند.",
                    reply_markup: {
                        inline_keyboard: [
                            [{ text: "📷 ارسال اسکرین‌شات پرداخت", callback_data: `crypto_receipt_${purchaseId}` }],
                            [{ text: "🏠 منوی اصلی", callback_data: "home" }]
                        ]
                    }
                });
                return;
            }
            else if (paymentMethod === "tronado") {
                const tronadoApiKey = ((await getSetting("tronado_api_key")) || "").trim();
                const result = await getStatusByPaymentId(purchaseId, tronadoApiKey || undefined);
                const orderStatusTitle = result?.OrderStatusTitle || result?.Data?.OrderStatusTitle || result?.orderStatusTitle || result?.Data?.orderStatusTitle;
                const isPaid = result?.IsPaid === true || result?.Data?.IsPaid === true || result?.isPaid === true || result?.Data?.isPaid === true;
                isAccepted = orderStatusTitle === "PaymentAccepted" || isPaid;
            }
            else if (paymentMethod === "plisio") {
                const txnId = String(orderRows[0].plisio_txn_id || "").trim();
                if (!txnId) {
                    await tg("sendMessage", { chat_id: chatId, text: "اطلاعات پرداخت Plisio ناقص است. لطفاً به پشتیبانی پیام دهید." });
                    await notifyAdmins(`⚠️ Plisio txn_id برای سفارش ثبت نشده است\nسفارش: ${purchaseId}`, {
                        inline_keyboard: [[{ text: "🔎 باز کردن سفارش", callback_data: `admin_open_purchase_${purchaseId}` }]]
                    });
                    return;
                }
                const plisioApiKey = ((await getSetting("plisio_api_key")) || "").trim();
                if (!plisioApiKey) {
                    await tg("sendMessage", { chat_id: chatId, text: "تنظیمات Plisio کامل نیست. لطفاً به پشتیبانی پیام دهید." });
                    await notifyAdmins(`⚠️ کلید Plisio تنظیم نشده است\nسفارش: ${purchaseId}`, {
                        inline_keyboard: [[{ text: "🔎 باز کردن سفارش", callback_data: `admin_open_purchase_${purchaseId}` }]]
                    });
                    return;
                }
                const { getPlisioOperation } = await import("./plisio.js");
                const op = await getPlisioOperation({ apiKey: plisioApiKey, operationId: txnId });
                const s = String(op?.status || "").toLowerCase().trim();
                await sql `UPDATE orders SET plisio_status = ${s} WHERE purchase_id = ${purchaseId};`;
                if (s === "expired" || s === "cancelled" || s === "error" || s === "cancelled duplicate") {
                    await tg("sendMessage", { chat_id: chatId, text: `وضعیت پرداخت Plisio: ${s}\nاگر پرداخت کرده‌اید ولی ثبت نشده، به پشتیبانی پیام دهید.` });
                    await notifyAdmins(`⚠️ وضعیت ناموفق Plisio\nسفارش: ${purchaseId}\nstatus: ${s}\ntxn: ${txnId}`, {
                        inline_keyboard: [[{ text: "🔎 باز کردن سفارش", callback_data: `admin_open_purchase_${purchaseId}` }]]
                    });
                    return;
                }
                isAccepted = s === "completed" || s === "mismatch";
            }
            else if (paymentMethod === "crypto") {
                const existingReceipt = String(orderRows[0].receipt_file_id || "").trim() || "";
                if (existingReceipt) {
                    await tg("sendMessage", { chat_id: chatId, text: "قبلاً برای این سفارش اطلاعات پرداخت ثبت شده و در انتظار تایید ادمین است." });
                    return;
                }
                await setState(userId, "await_crypto_receipt", { purchaseId });
                await tg("sendMessage", { chat_id: chatId, text: "لطفاً اسکرین‌شات پرداخت را به صورت عکس ارسال کنید:" });
                return;
            }
            if (isAccepted) {
                const fulfill = await fulfillOrderByPaymentId(purchaseId);
                if (!fulfill.ok && fulfill.reason === "stock_empty") {
                    await tg("sendMessage", { chat_id: chatId, text: "پرداخت ثبت شد ولی موجودی صفر است. ادمین پیگیری می‌کند." });
                }
            }
            else {
                const allowManual = paymentMethod === "tronado" || paymentMethod === "plisio" || paymentMethod === "tetrapay";
                await tg("sendMessage", {
                    chat_id: chatId,
                    text: "هنوز پرداخت تایید نشده است.\nاگر پرداخت کرده‌ای ولی تایید نمی‌شود، اسکرین‌شات پرداخت را ارسال کن تا ادمین بررسی کند.",
                    ...(allowManual
                        ? {
                            reply_markup: {
                                inline_keyboard: [
                                    [{ text: "📷 ارسال اسکرین‌شات پرداخت", callback_data: `crypto_receipt_${purchaseId}` }],
                                    [{ text: "🏠 منوی اصلی", callback_data: "home" }]
                                ]
                            }
                        }
                        : {})
                });
            }
        }
        catch (error) {
            logError("check_order_status_failed", error, { purchaseId, userId, chatId });
            await tg("sendMessage", { chat_id: chatId, text: "خطا در بررسی وضعیت پرداخت." });
        }
        return;
    }
    if (data.startsWith("crypto_receipt_")) {
        const purchaseId = data.replace("crypto_receipt_", "").trim();
        if (!purchaseId)
            return;
        const rows = await sql `
      SELECT id, status, payment_method
      FROM orders
      WHERE purchase_id = ${purchaseId} AND telegram_id = ${userId}
      LIMIT 1;
    `;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "سفارش پیدا نشد." });
            return;
        }
        const order = rows[0];
        const method = String(order.payment_method || "").toLowerCase();
        if (!(method === "tronado" || method === "plisio" || method === "tetrapay")) {
            await tg("sendMessage", { chat_id: chatId, text: "این سفارش نیازی به ارسال اسکرین‌شات ندارد." });
            return;
        }
        const status = String(order.status || "").toLowerCase();
        if (status === "paid") {
            await tg("sendMessage", { chat_id: chatId, text: "این سفارش قبلاً پرداخت شده است ✅" });
            return;
        }
        if (status === "denied" || status === "cancelled") {
            await tg("sendMessage", { chat_id: chatId, text: "این سفارش بسته شده است." });
            return;
        }
        await setState(userId, "await_crypto_receipt", { orderId: Number(order.id) });
        await tg("sendMessage", { chat_id: chatId, text: "لطفاً اسکرین‌شات پرداخت را به صورت عکس ارسال کن:" });
        return;
    }
    if (data.startsWith("show_configs_")) {
        const payload = data.replace("show_configs_", "");
        const parts = payload.split("_");
        const purchaseId = parts[0];
        const page = Math.max(1, Math.round(Number(parts[1] || 1)));
        const rows = await sql `
      SELECT i.id, i.delivery_payload, p.name
      FROM inventory i
      INNER JOIN products p ON p.id = i.product_id
      LEFT JOIN orders o ON o.id = i.sold_order_id
      WHERE i.owner_telegram_id = ${userId}
        AND i.status = 'sold'
        AND o.purchase_id = ${purchaseId}
      LIMIT 1;
    `;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "این سفارش برای شما نیست یا یافت نشد." });
            return;
        }
        const inv = rows[0];
        const payloadObj = parseDeliveryPayload(inv.delivery_payload);
        const links = payloadObj.configLinks || [];
        if (links.length <= 1) {
            await tg("sendMessage", { chat_id: chatId, text: "برای این سفارش کانفیگ اضافی وجود ندارد." });
            return;
        }
        const pageSize = 5;
        const totalPages = Math.max(1, Math.ceil(links.length / pageSize));
        const safePage = Math.min(totalPages, Math.max(1, page));
        const start = (safePage - 1) * pageSize;
        const slice = links.slice(start, start + pageSize);
        const text = `محصول: ${String(inv.name || "-")}\n` +
            `شناسه خرید: ${purchaseId}\n` +
            `کانفیگ‌ها (صفحه ${safePage}/${totalPages}):\n\n` +
            slice.map((item, idx) => `${start + idx + 1}) ${item}`).join("\n");
        const navRow = [];
        if (safePage > 1)
            navRow.push({ text: "⬅️ قبلی", callback_data: `show_configs_${purchaseId}_${safePage - 1}` });
        if (safePage < totalPages)
            navRow.push({ text: "بعدی ➡️", callback_data: `show_configs_${purchaseId}_${safePage + 1}` });
        const keyboard = [];
        if (navRow.length)
            keyboard.push(navRow);
        keyboard.push([{ text: "📦 کانفیگ‌های من", callback_data: "my_configs" }]);
        keyboard.push([homeButton()]);
        await tg("sendMessage", { chat_id: chatId, text, reply_markup: { inline_keyboard: keyboard } });
        return;
    }
    if (data === "my_configs") {
        await showMyConfigs(chatId, userId, false);
        return;
    }
    if (data === "my_orders") {
        await showMyOrders(chatId, userId);
        return;
    }
    if (data === "order_lookup") {
        await setState(userId, "await_order_lookup");
        await tg("sendMessage", {
            chat_id: chatId,
            text: "شناسه سفارش را ارسال کن (مثال: P1712345678901234):",
            reply_markup: { inline_keyboard: [[backButton("my_orders")], [homeButton()]] }
        });
        return;
    }
    if (data.startsWith("open_order_")) {
        const purchaseId = data.replace("open_order_", "").trim();
        if (!purchaseId)
            return;
        await showOrderDetails(chatId, userId, purchaseId);
        return;
    }
    if (data.startsWith("order_send_receipt_")) {
        const orderId = Number(data.replace("order_send_receipt_", ""));
        if (!Number.isFinite(orderId) || orderId <= 0)
            return;
        const rows = await sql `SELECT id, status, purchase_id FROM orders WHERE id = ${orderId} AND telegram_id = ${userId} LIMIT 1;`;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "سفارش پیدا نشد." });
            return;
        }
        if (String(rows[0].status || "").toLowerCase() !== "awaiting_receipt") {
            await tg("sendMessage", { chat_id: chatId, text: "برای این سفارش نیازی به ارسال رسید نیست." });
            return;
        }
        await setState(userId, "await_receipt", { orderId });
        const purchaseId = String(rows[0].purchase_id || "").trim();
        await tg("sendMessage", {
            chat_id: chatId,
            text: "لطفاً تصویر رسید را به صورت عکس ارسال کن:",
            reply_markup: { inline_keyboard: [[backButton(`open_order_${purchaseId}`)], [homeButton()]] }
        });
        return;
    }
    if (data.startsWith("order_cancel_")) {
        const purchaseId = data.replace("order_cancel_", "").trim();
        if (!purchaseId)
            return;
        const rows = await sql `
      UPDATE orders
      SET status = 'cancelled'
      WHERE purchase_id = ${purchaseId}
        AND telegram_id = ${userId}
        AND status IN ('pending', 'awaiting_receipt')
      RETURNING purchase_id;
    `;
        await tg("sendMessage", { chat_id: chatId, text: rows.length ? "سفارش لغو شد ✅" : "امکان لغو این سفارش وجود ندارد." });
        if (rows.length) {
            await showOrderDetails(chatId, userId, purchaseId);
        }
        return;
    }
    if (data === "my_migrations") {
        await showMyMigrations(chatId, userId);
        return;
    }
    if (data === "topup_menu") {
        await showMyConfigs(chatId, userId, true);
        return;
    }
    if (data.startsWith("open_config_")) {
        const payload = data.replace("open_config_", "");
        const fromTopupFlow = payload.endsWith("_t");
        const inventoryId = Number(fromTopupFlow ? payload.slice(0, -2) : payload);
        if (!Number.isFinite(inventoryId) || inventoryId <= 0) {
            await tg("sendMessage", { chat_id: chatId, text: "کانفیگ انتخاب‌شده معتبر نیست." });
            return;
        }
        await openMyConfig(chatId, userId, inventoryId, fromTopupFlow);
        return;
    }
    if (data.startsWith("request_topup_")) {
        const inventoryId = Number(data.replace("request_topup_", ""));
        const ownRows = await sql `
      SELECT id, config_value FROM inventory
      WHERE id = ${inventoryId} AND owner_telegram_id = ${userId} AND status = 'sold'
      LIMIT 1;
    `;
        if (!ownRows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "این کانفیگ برای شما نیست یا یافت نشد." });
            return;
        }
        await tg("sendMessage", {
            chat_id: chatId,
            text: "مقدار افزایش دیتا را انتخاب کنید:\n" +
                "500MB = نیم گیگابایت\n" +
                "1024MB = یک گیگابایت\n" +
                "2048MB = دو گیگابایت",
            reply_markup: {
                inline_keyboard: [
                    [
                        { text: "500MB", callback_data: `topup_amount_${inventoryId}_500` },
                        { text: "1GB (1024MB)", callback_data: `topup_amount_${inventoryId}_1024` }
                    ],
                    [{ text: "2GB (2048MB)", callback_data: `topup_amount_${inventoryId}_2048` }],
                    [{ text: "✍️ مقدار دلخواه", callback_data: `topup_custom_${inventoryId}` }],
                    [homeButton()]
                ]
            }
        });
        return;
    }
    if (data.startsWith("config_migrate_targets_")) {
        const inventoryId = Number(data.replace("config_migrate_targets_", ""));
        await showCustomerMigrationTargets(chatId, inventoryId, userId);
        return;
    }
    if (data.startsWith("migrate_pick_")) {
        const payload = data.replace("migrate_pick_", "");
        const [inventoryRaw, panelRaw] = payload.split("_");
        const inventoryId = Number(inventoryRaw);
        const panelId = Number(panelRaw);
        await createMigrationRequest(chatId, userId, userId, inventoryId, panelId, "customer");
        return;
    }
    if (data.startsWith("topup_custom_")) {
        const inventoryId = Number(data.replace("topup_custom_", ""));
        await setState(userId, "await_topup_custom_amount", { inventoryId });
        await tg("sendMessage", {
            chat_id: chatId,
            text: "مقدار دلخواه را بفرستید.\nنمونه: 1536 یا 1.5GB یا 800MB"
        });
        return;
    }
    if (data.startsWith("topup_amount_")) {
        const payload = data.replace("topup_amount_", "");
        const [inventoryIdRaw, mbRaw] = payload.split("_");
        const inventoryId = Number(inventoryIdRaw);
        const mb = Number(mbRaw);
        await createTopupCard2CardRequest(chatId, userId, inventoryId, mb);
        return;
    }
    if (data.startsWith("customer_remove_cfg_")) {
        const inventoryId = Number(data.replace("customer_remove_cfg_", ""));
        const rows = await sql `
      SELECT id, owner_telegram_id, delivery_payload
      FROM inventory
      WHERE id = ${inventoryId} AND owner_telegram_id = ${userId} AND status = 'sold'
      LIMIT 1;
    `;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "این کانفیگ پیدا نشد یا متعلق به شما نیست." });
            return;
        }
        await recordInventoryForensicEvent(inventoryId, "customer_removed_from_inventory", { actorUser: userId });
        await sql `
      UPDATE inventory
      SET
        owner_telegram_id = NULL,
        delivery_payload = jsonb_set(
          jsonb_set(
            jsonb_set(COALESCE(delivery_payload, '{}'::jsonb), '{metadata}', COALESCE(delivery_payload->'metadata', '{}'::jsonb), true),
            '{metadata,removed_by_owner}',
            to_jsonb(TRUE),
            true
          ),
          '{metadata,removed_at}',
          to_jsonb(NOW()::text),
          true
        )
      WHERE id = ${inventoryId};
    `;
        await tg("sendMessage", { chat_id: chatId, text: "کانفیگ از لیست شما حذف شد ✅\nاطلاعات برای پیگیری امنیتی ذخیره شد." });
        return;
    }
    if (data.startsWith("customer_revoke_cfg_")) {
        const inventoryId = Number(data.replace("customer_revoke_cfg_", ""));
        await performRegenLink(inventoryId, userId, false, chatId);
        return;
    }
    if (data === "support") {
        const support = await getSetting("support_username");
        if (!support) {
            await tg("sendMessage", { chat_id: chatId, text: "پشتیبانی هنوز تنظیم نشده است." });
            return;
        }
        await tg("sendMessage", {
            chat_id: chatId,
            text: `🆘 پشتیبانی\n\nبرای ارتباط با پشتیبانی روی دکمه زیر بزنید یا پیام دهید:\n@${support}`,
            reply_markup: {
                inline_keyboard: [
                    [{ text: "💬 چت با پشتیبانی", url: `https://t.me/${support}` }],
                    [homeButton()]
                ]
            }
        });
        return;
    }
    if (!isAdmin(userId))
        return;
    if (data.startsWith("admin_lookup_ban_")) {
        const targetUser = Number(data.replace("admin_lookup_ban_", ""));
        if (!Number.isFinite(targetUser) || targetUser <= 0) {
            await tg("sendMessage", { chat_id: chatId, text: "شناسه کاربر نامعتبر است." });
            return;
        }
        await sql `
      INSERT INTO banned_users (telegram_id, reason, banned_by)
      VALUES (${targetUser}, 'lookup_abuse', ${userId})
      ON CONFLICT (telegram_id) DO UPDATE SET reason = EXCLUDED.reason, banned_by = EXCLUDED.banned_by;
    `;
        await tg("sendMessage", { chat_id: chatId, text: `کاربر ${targetUser} بن شد ✅` });
        return;
    }
    if (data.startsWith("admin_lookup_toggle_inv_")) {
        const inventoryId = Number(data.replace("admin_lookup_toggle_inv_", ""));
        if (!Number.isFinite(inventoryId))
            return;
        const rows = await sql `
      SELECT i.id, i.panel_id, i.delivery_payload, i.owner_telegram_id
      FROM inventory i
      WHERE i.id = ${inventoryId}
      LIMIT 1;
    `;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "کانفیگ پیدا نشد." });
            return;
        }
        const row = rows[0];
        const delivery = parseDeliveryPayload(row.delivery_payload);
        const currentlyRevoked = !!delivery.metadata?.revoked;
        const willEnable = currentlyRevoked;
        const panelType = String(delivery.metadata?.panelType || "");
        const panelId = Number(row.panel_id || 0);
        const key = String(delivery.metadata?.username || delivery.metadata?.uuid || delivery.metadata?.email || delivery.metadata?.subId || "").trim();
        let panelToggleMessage = "عملیات روی پنل انجام نشد.";
        if (panelId && panelType && key) {
            const panelRows = await sql `
        SELECT id, panel_type, base_url, username, password
        FROM panels
        WHERE id = ${panelId}
        LIMIT 1;
      `;
            if (panelRows.length) {
                const result = panelType === "marzban" ? await toggleMarzbanUser(panelRows[0], key, willEnable) : await toggleSanaeiClient(panelRows[0], key, willEnable);
                panelToggleMessage = result.ok ? "عملیات پنل موفق ✅" : `عملیات پنل ناموفق: ${result.message}`;
            }
        }
        await recordInventoryForensicEvent(inventoryId, willEnable ? "admin_enable" : "admin_disable", { adminId: userId, panelResult: panelToggleMessage });
        await sql `
      UPDATE inventory
      SET delivery_payload = jsonb_set(
        jsonb_set(COALESCE(delivery_payload, '{}'::jsonb), '{metadata}', COALESCE(delivery_payload->'metadata', '{}'::jsonb), true),
        '{metadata,revoked}',
        to_jsonb(${!willEnable}::boolean),
        true
      )
      WHERE id = ${inventoryId};
    `;
        await tg("sendMessage", { chat_id: chatId, text: `وضعیت کانفیگ تغییر یافت (${willEnable ? 'فعال' : 'غیرفعال'}) ✅\n${panelToggleMessage}` });
        return;
    }
    if (data.startsWith("admin_lookup_regen_link_")) {
        const inventoryId = Number(data.replace("admin_lookup_regen_link_", ""));
        if (!Number.isFinite(inventoryId))
            return;
        await performRegenLink(inventoryId, userId, true, chatId);
        return;
    }
    if (data.startsWith("admin_lookup_revoke_inv_")) {
        let inventoryIdRaw = data.replace("admin_lookup_revoke_inv_", "");
        const isConfirmed = inventoryIdRaw.endsWith("_confirm");
        if (isConfirmed) {
            inventoryIdRaw = inventoryIdRaw.replace("_confirm", "");
        }
        const inventoryId = Number(inventoryIdRaw);
        if (!isConfirmed) {
            await tg("sendMessage", {
                chat_id: chatId,
                text: `⚠️ آیا از لغو دسترسی کانفیگ #${inventoryId} اطمینان دارید؟\nاین عمل دسترسی کاربر را در پنل و دیتابیس غیرفعال می‌کند.`,
                reply_markup: {
                    inline_keyboard: [
                        [
                            cb("✅ تایید", `admin_lookup_revoke_inv_${inventoryId}_confirm`, "danger"),
                            cb("❌ انصراف", "admin_lookup_action_cancel", "primary")
                        ]
                    ]
                }
            });
            return;
        }
        const inventoryIdFinal = inventoryId;
        const rows = await sql `
      SELECT i.id, i.panel_id, i.delivery_payload, i.owner_telegram_id
      FROM inventory i
      WHERE i.id = ${inventoryIdFinal}
      LIMIT 1;
    `;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "کانفیگ پیدا نشد." });
            return;
        }
        const row = rows[0];
        const delivery = parseDeliveryPayload(row.delivery_payload);
        const panelType = String(delivery.metadata?.panelType || "");
        const panelId = Number(row.panel_id || 0);
        const key = String(delivery.metadata?.username || delivery.metadata?.uuid || delivery.metadata?.email || delivery.metadata?.subId || "").trim();
        let panelRevokeMessage = "لغو دسترسی در پنل انجام نشد.";
        if (panelId && panelType && key) {
            const panelRows = await sql `
        SELECT id, panel_type, base_url, username, password
        FROM panels
        WHERE id = ${panelId}
        LIMIT 1;
      `;
            if (panelRows.length) {
                const result = panelType === "marzban" ? await toggleMarzbanUser(panelRows[0], key, false) : await toggleSanaeiClient(panelRows[0], key, false);
                panelRevokeMessage = result.ok ? "لغو دسترسی در پنل موفق ✅" : `لغو دسترسی در پنل ناموفق: ${result.message}`;
            }
        }
        await recordInventoryForensicEvent(inventoryId, "admin_revoke", { adminId: userId, panelResult: panelRevokeMessage });
        await sql `
      UPDATE inventory
      SET delivery_payload = jsonb_set(
        jsonb_set(COALESCE(delivery_payload, '{}'::jsonb), '{metadata}', COALESCE(delivery_payload->'metadata', '{}'::jsonb), true),
        '{metadata,revoked}',
        to_jsonb(TRUE),
        true
      )
      WHERE id = ${inventoryId};
    `;
        await tg("sendMessage", { chat_id: chatId, text: `دسترسی کانفیگ قطع شد ✅\n${panelRevokeMessage}` });
        return;
    }
    if (data.startsWith("admin_lookup_direct_links_")) {
        const inventoryId = Number(data.replace("admin_lookup_direct_links_", ""));
        if (!Number.isFinite(inventoryId))
            return;
        const rows = await sql `
      SELECT id, delivery_payload, config_value
      FROM inventory
      WHERE id = ${inventoryId}
      LIMIT 1;
    `;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "کانفیگ پیدا نشد." });
            return;
        }
        const row = rows[0];
        const delivery = parseDeliveryPayload(row.delivery_payload);
        const links = delivery.configLinks || [];
        if (links.length === 0) {
            // Fallback to raw config value if no links array is present
            if (row.config_value && String(row.config_value).includes("://")) {
                links.push(String(row.config_value));
            }
            else {
                await tg("sendMessage", { chat_id: chatId, text: "لینک مستقیمی برای این کانفیگ یافت نشد." });
                return;
            }
        }
        // Send the links to the admin (using chunks to avoid Telegram's character limits for large link arrays)
        const chunkSize = 10;
        for (let i = 0; i < links.length; i += chunkSize) {
            const chunk = links.slice(i, i + chunkSize);
            const chunkText = chunk.map(l => `<code>${escapeHtml(l)}</code>`).join("\n\n");
            const msgText = i === 0 ? `🔗 لینک‌های مستقیم (تعداد کل: ${links.length}):\n\n${chunkText}` : chunkText;
            await tg("sendMessage", {
                chat_id: chatId,
                text: msgText,
                parse_mode: "HTML"
            });
        }
        return;
    }
    if (data.startsWith("admin_lookup_delete_inv_")) {
        let inventoryIdRaw = data.replace("admin_lookup_delete_inv_", "");
        const isConfirmed = inventoryIdRaw.endsWith("_confirm");
        if (isConfirmed) {
            inventoryIdRaw = inventoryIdRaw.replace("_confirm", "");
        }
        const inventoryId = Number(inventoryIdRaw);
        if (!Number.isFinite(inventoryId)) {
            await tg("sendMessage", { chat_id: chatId, text: "شناسه کانفیگ نامعتبر است." });
            return;
        }
        if (!isConfirmed) {
            await tg("sendMessage", {
                chat_id: chatId,
                text: `🧨 خطر! آیا از حذف کامل کانفیگ #${inventoryId} اطمینان دارید؟\nاین عمل قابل بازگشت نیست و کاربر از دیتابیس و پنل حذف می‌شود.`,
                reply_markup: {
                    inline_keyboard: [
                        [
                            cb("🔥 حذف کامل", `admin_lookup_delete_inv_${inventoryId}_confirm`, "danger"),
                            cb("❌ انصراف", "admin_lookup_action_cancel", "primary")
                        ]
                    ]
                }
            });
            return;
        }
        const inventoryIdFinal = inventoryId;
        const rows = await sql `
      SELECT i.id, i.panel_id, i.delivery_payload
      FROM inventory i
      WHERE i.id = ${inventoryIdFinal}
      LIMIT 1;
    `;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "کانفیگ پیدا نشد." });
            return;
        }
        const row = rows[0];
        const delivery = parseDeliveryPayload(row.delivery_payload);
        const panelType = String(delivery.metadata?.panelType || "");
        const panelId = Number(row.panel_id || 0);
        const key = String(delivery.metadata?.username || delivery.metadata?.uuid || delivery.metadata?.email || delivery.metadata?.subId || "").trim();
        let panelDeleteMessage = "حذف پنل انجام نشد.";
        if (panelId && panelType && key) {
            const panelRows = await sql `
        SELECT id, panel_type, base_url, username, password
        FROM panels
        WHERE id = ${panelId}
        LIMIT 1;
      `;
            if (panelRows.length) {
                const result = panelType === "marzban"
                    ? await deleteMarzbanUser(panelRows[0], key)
                    : await revokeSanaeiClient(panelRows[0], key);
                panelDeleteMessage = result.ok ? "حذف/غیرفعالسازی در پنل موفق ✅" : `اقدام پنل ناموفق: ${result.message}`;
            }
        }
        await recordInventoryForensicEvent(inventoryId, "admin_permanent_delete", { adminId: userId, panelResult: panelDeleteMessage });
        try {
            // Because `inventory` has multiple dependent tables without ON DELETE CASCADE,
            // we must manually delete dependent rows first to prevent foreign key violations.
            await sql `
        WITH deleted_forensics AS (
          DELETE FROM config_forensics WHERE inventory_id = ${inventoryId}
        ),
        deleted_topups AS (
          DELETE FROM topup_requests WHERE inventory_id = ${inventoryId}
        ),
        deleted_migrations AS (
          DELETE FROM panel_migrations WHERE source_inventory_id = ${inventoryId}
        )
        DELETE FROM inventory WHERE id = ${inventoryId};
      `;
            // Also nullify references in orders to prevent violating orders_inventory_id_fkey
            await sql `UPDATE orders SET inventory_id = NULL WHERE inventory_id = ${inventoryId}`;
            await tg("sendMessage", { chat_id: chatId, text: `کانفیگ از دیتابیس حذف شد ✅\n${panelDeleteMessage}` });
        }
        catch (err) {
            logError("admin_inventory_delete_failed", err, { inventoryId, adminId: userId });
            await tg("sendMessage", { chat_id: chatId, text: `❌ حذف کانفیگ از دیتابیس با خطا مواجه شد.\n${err.message}` });
        }
        return;
    }
    if (data.startsWith("admin_lookup_add_data_")) {
        const inventoryId = Number(data.replace("admin_lookup_add_data_", ""));
        if (!Number.isFinite(inventoryId) || inventoryId <= 0) {
            await tg("sendMessage", { chat_id: chatId, text: "شناسه کانفیگ نامعتبر است." });
            return;
        }
        await setState(userId, "admin_lookup_add_data", { inventoryId });
        await tg("sendMessage", {
            chat_id: chatId,
            text: "مقدار دیتای اضافه را ارسال کنید.\nمثال: 500MB یا 2GB",
            reply_markup: { inline_keyboard: [[cancelButton("admin_lookup_action_cancel")]] }
        });
        return;
    }
    if (data.startsWith("admin_lookup_set_data_")) {
        const inventoryId = Number(data.replace("admin_lookup_set_data_", ""));
        if (!Number.isFinite(inventoryId) || inventoryId <= 0) {
            await tg("sendMessage", { chat_id: chatId, text: "شناسه کانفیگ نامعتبر است." });
            return;
        }
        await setState(userId, "admin_lookup_set_data", { inventoryId });
        await tg("sendMessage", {
            chat_id: chatId,
            text: "سقف دیتای جدید را ارسال کنید.\nمثال: 50GB یا 102400MB یا unlimited\nبرای نامحدود: unlimited یا 0",
            reply_markup: { inline_keyboard: [[cancelButton("admin_lookup_action_cancel")]] }
        });
        return;
    }
    if (data.startsWith("admin_lookup_reset_data_")) {
        let inventoryIdRaw = data.replace("admin_lookup_reset_data_", "");
        const isConfirmed = inventoryIdRaw.endsWith("_confirm");
        if (isConfirmed) {
            inventoryIdRaw = inventoryIdRaw.replace("_confirm", "");
        }
        const inventoryId = Number(inventoryIdRaw);
        if (!Number.isFinite(inventoryId)) {
            await tg("sendMessage", { chat_id: chatId, text: "شناسه کانفیگ نامعتبر است." });
            return;
        }
        if (!isConfirmed) {
            await tg("sendMessage", {
                chat_id: chatId,
                text: `⚠️ آیا از ریست کردن مصرف کانفیگ #${inventoryId} اطمینان دارید؟\nاین عمل فقط مصرف کاربر را صفر می‌کند و سقف دیتا را تغییر نمی‌دهد.`,
                reply_markup: {
                    inline_keyboard: [
                        [
                            confirmButton(`admin_lookup_reset_data_${inventoryId}_confirm`, "✅ ریست شود"),
                            cb("❌ انصراف", "admin_lookup_action_cancel", "primary")
                        ]
                    ]
                }
            });
            return;
        }
        const inventoryIdFinal = inventoryId;
        if (!Number.isFinite(inventoryIdFinal) || inventoryIdFinal <= 0) {
            await tg("sendMessage", { chat_id: chatId, text: "شناسه کانفیگ نامعتبر است." });
            return;
        }
        const rows = await sql `
      SELECT i.id, i.panel_id, i.delivery_payload, p.size_mb, p.is_infinite
      FROM inventory i
      INNER JOIN products p ON p.id = i.product_id
      WHERE i.id = ${inventoryIdFinal}
      LIMIT 1;
    `;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "کانفیگ پیدا نشد." });
            return;
        }
        const row = rows[0];
        const delivery = parseDeliveryPayload(row.delivery_payload);
        const panelType = String(delivery.metadata?.panelType || "");
        const panelId = Number(row.panel_id || 0);
        if (!panelId || !panelType) {
            await tg("sendMessage", { chat_id: chatId, text: "این کانفیگ پنلی نیست." });
            return;
        }
        const panelRows = await sql `
      SELECT id, panel_type, base_url, username, password
      FROM panels
      WHERE id = ${panelId}
      LIMIT 1;
    `;
        if (!panelRows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "پنل مرتبط پیدا نشد." });
            return;
        }
        let result = { ok: false, message: "پنل پشتیبانی نمی‌شود." };
        if (panelType === "marzban") {
            const username = String(delivery.metadata?.username || "").trim();
            if (!username) {
                await tg("sendMessage", { chat_id: chatId, text: "username پنل در متادیتا پیدا نشد." });
                return;
            }
            result = await applyAdminResetUsageOnMarzban(panelRows[0], username);
        }
        else if (panelType === "sanaei") {
            const inboundId = parseMaybeNumber(delivery.metadata?.inboundId);
            const email = String(delivery.metadata?.email || "").trim();
            if (!inboundId || !email) {
                await tg("sendMessage", { chat_id: chatId, text: "inbound/email در متادیتا کانفیگ ناقص است." });
                return;
            }
            result = await applyAdminResetUsageOnSanaei(panelRows[0], inboundId, email);
        }
        if (!result.ok) {
            await tg("sendMessage", { chat_id: chatId, text: `ریست دیتا انجام نشد.\n${result.message}` });
            return;
        }
        await recordInventoryForensicEvent(inventoryId, "admin_lookup_reset_data", {
            adminId: userId,
            panelResult: result.message
        });
        await tg("sendMessage", {
            chat_id: chatId,
            text: `مصرف دیتای کانفیگ صفر شد ✅\n${result.message}`
        });
        return;
    }
    if (data.startsWith("admin_lookup_set_expiry_")) {
        const payload = data.replace("admin_lookup_set_expiry_", "");
        const [inventoryRaw, daysRaw] = payload.split("_");
        const inventoryId = Number(inventoryRaw);
        const forcedDays = daysRaw !== undefined ? Number(daysRaw) : NaN;
        if (!Number.isFinite(inventoryId) || inventoryId <= 0) {
            await tg("sendMessage", { chat_id: chatId, text: "شناسه کانفیگ نامعتبر است." });
            return;
        }
        if (Number.isFinite(forcedDays) && forcedDays >= 0) {
            await parseAndApplyState(chatId, userId, String(Math.round(forcedDays)), null, null, null, {
                state: "admin_lookup_set_expiry",
                payload: { inventoryId }
            });
            return;
        }
        await setState(userId, "admin_lookup_set_expiry", { inventoryId });
        await tg("sendMessage", {
            chat_id: chatId,
            text: "چند روز انقضا تنظیم شود؟\n0 = بدون انقضا",
            reply_markup: { inline_keyboard: [[cancelButton("admin_lookup_action_cancel")]] }
        });
        return;
    }
    if (data === "admin_lookup_action_cancel") {
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: "عملیات ابزار کانفیگ لغو شد." });
        return;
    }
    if (data.startsWith("admin_panel_add_data_")) {
        const payload = data.replace("admin_panel_add_data_", "");
        const firstUnderscore = payload.indexOf("_");
        const panelId = Number(firstUnderscore >= 0 ? payload.slice(0, firstUnderscore) : "0");
        const panelKey = decodeURIComponent(firstUnderscore >= 0 ? payload.slice(firstUnderscore + 1) : "");
        if (!Number.isFinite(panelId) || panelId <= 0 || !panelKey) {
            await tg("sendMessage", { chat_id: chatId, text: "ورودی نامعتبر برای افزودن دیتا." });
            return;
        }
        await setState(userId, "admin_panel_add_data", { panelId, panelKey });
        await tg("sendMessage", {
            chat_id: chatId,
            text: "مقدار دیتای اضافه را ارسال کنید.\nمثال: 500MB یا 2GB",
            reply_markup: { inline_keyboard: [[cancelButton("admin_lookup_action_cancel")]] }
        });
        return;
    }
    if (data.startsWith("admin_panel_set_data_")) {
        const payload = data.replace("admin_panel_set_data_", "");
        const firstUnderscore = payload.indexOf("_");
        const panelId = Number(firstUnderscore >= 0 ? payload.slice(0, firstUnderscore) : "0");
        const panelKey = decodeURIComponent(firstUnderscore >= 0 ? payload.slice(firstUnderscore + 1) : "");
        if (!Number.isFinite(panelId) || panelId <= 0 || !panelKey) {
            await tg("sendMessage", { chat_id: chatId, text: "ورودی نامعتبر برای تنظیم سقف دیتا." });
            return;
        }
        await setState(userId, "admin_panel_set_data", { panelId, panelKey });
        await tg("sendMessage", {
            chat_id: chatId,
            text: "سقف دیتای جدید را ارسال کنید.\nمثال: 50GB یا 102400MB یا unlimited\nبرای نامحدود: unlimited یا 0",
            reply_markup: { inline_keyboard: [[cancelButton("admin_lookup_action_cancel")]] }
        });
        return;
    }
    if (data.startsWith("admin_panel_reset_data_")) {
        const payload = data.replace("admin_panel_reset_data_", "");
        const firstUnderscore = payload.indexOf("_");
        const panelId = Number(firstUnderscore >= 0 ? payload.slice(0, firstUnderscore) : "0");
        const panelKey = decodeURIComponent(firstUnderscore >= 0 ? payload.slice(firstUnderscore + 1) : "");
        if (!Number.isFinite(panelId) || panelId <= 0 || !panelKey) {
            await tg("sendMessage", { chat_id: chatId, text: "ورودی نامعتبر برای ریست دیتا." });
            return;
        }
        const panelRows = await sql `
      SELECT id, panel_type, base_url, username, password
      FROM panels
      WHERE id = ${panelId}
      LIMIT 1;
    `;
        if (!panelRows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "پنل مرتبط پیدا نشد." });
            return;
        }
        const panel = panelRows[0];
        const panelType = String(panel.panel_type || "");
        let result = { ok: false, message: "پنل پشتیبانی نمی‌شود." };
        let limitBytes = 0;
        if (panelType === "marzban") {
            const found = await lookupMarzbanUser(panel, panelKey);
            if (!found.ok || !found.user) {
                await tg("sendMessage", { chat_id: chatId, text: "کاربر روی پنل پیدا نشد." });
                return;
            }
            limitBytes = Math.max(0, Math.round(Number(found.user.data_limit || 0)));
            const username = String(found.user.username || panelKey).trim();
            result = await applyAdminResetUsageOnMarzban(panel, username);
        }
        else if (panelType === "sanaei") {
            const found = await findSanaeiClientByIdentifier(panel, panelKey);
            if (!found.ok || !found.client || !found.inboundId) {
                await tg("sendMessage", { chat_id: chatId, text: "کلاینت روی پنل پیدا نشد." });
                return;
            }
            const email = String(found.client.email || "").trim();
            if (!email) {
                await tg("sendMessage", { chat_id: chatId, text: "email کلاینت روی پنل پیدا نشد." });
                return;
            }
            limitBytes = Math.max(0, Math.round(Number(found.client.totalGB || 0)));
            result = await applyAdminResetUsageOnSanaei(panel, Number(found.inboundId), email);
        }
        if (!result.ok) {
            await tg("sendMessage", { chat_id: chatId, text: `ریست دیتا انجام نشد.\n${result.message}` });
            return;
        }
        await recordForensicEvent({
            inventoryId: null,
            ownerTelegramId: null,
            productId: null,
            panelId,
            panelType,
            panelUserKey: panelKey,
            uuid: extractUuidFromText(panelKey),
            source: "panel_action",
            eventType: "admin_panel_reset_data",
            configValue: null,
            metadata: { adminId: userId, resetBytes: limitBytes, panelResult: result.message }
        });
        await tg("sendMessage", {
            chat_id: chatId,
            text: `مصرف کاربر ریست شد ✅\nسقف فعلی: ${limitBytes > 0 ? formatBytesShort(limitBytes) : "نامحدود"}`
        });
        return;
    }
    if (data.startsWith("admin_panel_set_expiry_")) {
        const payload = data.replace("admin_panel_set_expiry_", "");
        const firstUnderscore = payload.indexOf("_");
        const panelId = Number(firstUnderscore >= 0 ? payload.slice(0, firstUnderscore) : "0");
        const rest = firstUnderscore >= 0 ? payload.slice(firstUnderscore + 1) : "";
        const marker = "_days_";
        const markerIndex = rest.lastIndexOf(marker);
        const maybeDaysRaw = markerIndex >= 0 ? rest.slice(markerIndex + marker.length) : "";
        const maybeDays = Number(maybeDaysRaw);
        const panelKeyEncoded = markerIndex >= 0 && Number.isFinite(maybeDays) ? rest.slice(0, markerIndex) : rest;
        const panelKey = decodeURIComponent(panelKeyEncoded);
        if (!Number.isFinite(panelId) || panelId <= 0 || !panelKey) {
            await tg("sendMessage", { chat_id: chatId, text: "ورودی نامعتبر برای تنظیم انقضا." });
            return;
        }
        if (Number.isFinite(maybeDays) && maybeDays >= 0) {
            await parseAndApplyState(chatId, userId, String(Math.round(maybeDays)), null, null, null, {
                state: "admin_panel_set_expiry",
                payload: { panelId, panelKey }
            });
            return;
        }
        await setState(userId, "admin_panel_set_expiry", { panelId, panelKey });
        await tg("sendMessage", {
            chat_id: chatId,
            text: "چند روز انقضا تنظیم شود؟\n0 = بدون انقضا",
            reply_markup: { inline_keyboard: [[cancelButton("admin_lookup_action_cancel")]] }
        });
        return;
    }
    if (data.startsWith("admin_panel_toggle_")) {
        const payload = data.replace("admin_panel_toggle_", "");
        const firstUnderscore = payload.indexOf("_");
        const panelId = Number(firstUnderscore >= 0 ? payload.slice(0, firstUnderscore) : "0");
        const key = decodeURIComponent(firstUnderscore >= 0 ? payload.slice(firstUnderscore + 1) : "");
        const rows = await sql `
      SELECT id, panel_type, base_url, username, password
      FROM panels
      WHERE id = ${panelId}
      LIMIT 1;
    `;
        if (!rows.length || !key) {
            await tg("sendMessage", { chat_id: chatId, text: "ورودی نامعتبر برای تغییر وضعیت پنل." });
            return;
        }
        const panelType = String(rows[0].panel_type || "");
        let willEnable = true;
        if (panelType === "marzban") {
            const found = await lookupMarzbanUser(rows[0], key);
            if (!found.ok || !found.user) {
                await tg("sendMessage", { chat_id: chatId, text: `پیدا نشد: ${found.message}` });
                return;
            }
            willEnable = found.user.status === "disabled";
            const result = await toggleMarzbanUser(rows[0], key, willEnable);
            if (!result.ok) {
                await tg("sendMessage", { chat_id: chatId, text: `عملیات پنل ناموفق: ${result.message}` });
                return;
            }
        }
        else {
            const found = await findSanaeiClientByIdentifier(rows[0], key);
            if (!found.ok || !found.client) {
                await tg("sendMessage", { chat_id: chatId, text: `پیدا نشد: ${found.message}` });
                return;
            }
            willEnable = found.client.enable === false;
            const result = await toggleSanaeiClient(rows[0], key, willEnable);
            if (!result.ok) {
                await tg("sendMessage", { chat_id: chatId, text: `عملیات پنل ناموفق: ${result.message}` });
                return;
            }
        }
        await recordForensicEvent({
            inventoryId: null,
            ownerTelegramId: null,
            productId: null,
            panelId,
            panelType,
            panelUserKey: key,
            uuid: extractUuidFromText(key),
            source: "panel_action",
            eventType: willEnable ? "admin_enable_panel_only" : "admin_disable_panel_only",
            configValue: null,
            metadata: { adminId: userId }
        });
        await tg("sendMessage", { chat_id: chatId, text: `وضعیت کاربر در پنل تغییر یافت (${willEnable ? 'فعال' : 'غیرفعال'}) ✅` });
        return;
    }
    if (data.startsWith("admin_panel_rv_")) {
        const isConfirmed = data.includes("_confirm");
        const payloadRaw = isConfirmed ? data.replace("admin_panel_rv_", "").replace("_confirm", "") : data.replace("admin_panel_rv_", "");
        if (!isConfirmed) {
            await tg("sendMessage", {
                chat_id: chatId,
                text: `⚠️ آیا از بازسازی لینک این کاربر روی پنل اطمینان دارید؟`,
                reply_markup: {
                    inline_keyboard: [
                        [
                            cb("✅ تایید", `admin_panel_rv_${payloadRaw}_confirm`, "primary"),
                            cb("❌ انصراف", "admin_lookup_action_cancel", "danger")
                        ]
                    ]
                }
            });
            return;
        }
        const payload = payloadRaw;
        const firstUnderscore = payload.indexOf("_");
        const panelId = Number(firstUnderscore >= 0 ? payload.slice(0, firstUnderscore) : "0");
        const key = decodeURIComponent(firstUnderscore >= 0 ? payload.slice(firstUnderscore + 1) : "");
        const rows = await sql `
      SELECT id, panel_type, base_url, username, password
      FROM panels
      WHERE id = ${panelId}
      LIMIT 1;
    `;
        if (!rows.length || !key) {
            await tg("sendMessage", { chat_id: chatId, text: "ورودی نامعتبر برای بازسازی لینک پنل." });
            return;
        }
        const panelType = String(rows[0].panel_type || "");
        const result = panelType === "marzban" ? await regenerateMarzbanUserLink(rows[0], key) : await regenerateSanaeiClientLink(rows[0], key);
        if (!result.ok) {
            await tg("sendMessage", { chat_id: chatId, text: `بازسازی لینک پنل ناموفق: ${result.message}` });
            return;
        }
        let newLinkMsg = "";
        if (panelType === "marzban") {
            const u = result.user;
            const links = Array.isArray(u.links) ? u.links.map((x) => String(x || "").trim()).filter(Boolean) : [];
            const subUrl = u.subscription_url ? String(u.subscription_url) : "";
            newLinkMsg = subUrl || links[0] || "";
        }
        else {
            const panelConfigRows = await sql `
        SELECT p.panel_config 
        FROM products p
        JOIN inventory i ON i.product_id = p.id
        WHERE i.panel_id = ${panelId} AND (i.delivery_payload->'metadata'->>'uuid' = ${key} OR i.delivery_payload->'metadata'->>'email' = ${key} OR i.delivery_payload->'metadata'->>'subId' = ${key} OR i.config_value ILIKE ${'%' + key + '%'})
        LIMIT 1;
      `;
            const panelConfig = panelConfigRows.length ? (typeof panelConfigRows[0].panel_config === "string" ? parseJsonObject(panelConfigRows[0].panel_config) : panelConfigRows[0].panel_config) || {} : {};
            const newConfigLinks = buildSanaeiConfigLinks(String(rows[0].base_url), result.inbound, result.client, panelConfig);
            const subId = String(result.client?.subId || "");
            const subUrl = subId ? buildSanaeiSubscriptionUrl(String(rows[0].base_url), panelConfig, subId) : "";
            newLinkMsg = subUrl || newConfigLinks[0] || "";
        }
        await recordForensicEvent({
            inventoryId: null,
            ownerTelegramId: null,
            productId: null,
            panelId,
            panelType,
            panelUserKey: key,
            uuid: extractUuidFromText(key),
            source: "panel_action",
            eventType: "admin_revoke_panel_only",
            configValue: null,
            metadata: { adminId: userId }
        });
        await tg("sendMessage", { chat_id: chatId, text: `لینک جدید ساخته شد ✅\n\n${newLinkMsg}` });
        return;
    }
    if (data.startsWith("admin_panel_del_")) {
        const isConfirmed = data.includes("_confirm");
        const payloadRaw = isConfirmed ? data.replace("admin_panel_del_", "").replace("_confirm", "") : data.replace("admin_panel_del_", "");
        if (!isConfirmed) {
            await tg("sendMessage", {
                chat_id: chatId,
                text: `🧨 خطر! آیا از حذف کامل این کاربر از پنل اطمینان دارید؟\nاین عمل قابل بازگشت نیست.`,
                reply_markup: {
                    inline_keyboard: [
                        [
                            cb("🔥 حذف کامل", `admin_panel_del_${payloadRaw}_confirm`, "danger"),
                            cb("❌ انصراف", "admin_lookup_action_cancel", "primary")
                        ]
                    ]
                }
            });
            return;
        }
        const payload = payloadRaw;
        const firstUnderscore = payload.indexOf("_");
        const panelId = Number(firstUnderscore >= 0 ? payload.slice(0, firstUnderscore) : "0");
        const key = decodeURIComponent(firstUnderscore >= 0 ? payload.slice(firstUnderscore + 1) : "");
        const rows = await sql `
      SELECT id, panel_type, base_url, username, password
      FROM panels
      WHERE id = ${panelId}
      LIMIT 1;
    `;
        if (!rows.length || !key) {
            await tg("sendMessage", { chat_id: chatId, text: "ورودی نامعتبر برای حذف پنل." });
            return;
        }
        const panelType = String(rows[0].panel_type || "");
        const result = panelType === "marzban" ? await deleteMarzbanUser(rows[0], key) : await revokeSanaeiClient(rows[0], key);
        if (!result.ok) {
            await tg("sendMessage", { chat_id: chatId, text: `حذف در پنل ناموفق: ${result.message}` });
            return;
        }
        await recordForensicEvent({
            inventoryId: null,
            ownerTelegramId: null,
            productId: null,
            panelId,
            panelType,
            panelUserKey: key,
            uuid: extractUuidFromText(key),
            source: "panel_action",
            eventType: "admin_delete_panel_only",
            configValue: null,
            metadata: { adminId: userId }
        });
        await tg("sendMessage", { chat_id: chatId, text: "حذف/غیرفعالسازی در پنل انجام شد ✅" });
        return;
    }
    if (data === "admin_panel") {
        await sendAdminPanel(chatId);
        return;
    }
    if (data === "admin_panels") {
        await showPanelAdminMenu(chatId);
        return;
    }
    if (data === "admin_panel_add") {
        await promptPanelTypePicker(chatId, "add");
        return;
    }
    if (/^noop_panel_\d+$/.test(data) || data.startsWith("admin_panel_open_")) {
        const panelId = Number((data.match(/\d+$/) || ["0"])[0]);
        await showPanelDetails(chatId, panelId);
        return;
    }
    if (data.startsWith("admin_panel_edit_")) {
        const panelId = Number(data.replace("admin_panel_edit_", ""));
        const panel = await getPanelById(panelId);
        if (!panel) {
            await tg("sendMessage", { chat_id: chatId, text: "پنل پیدا نشد." });
            return;
        }
        await promptPanelTypePicker(chatId, "edit", panelId);
        return;
    }
    if (data === "admin_panel_wizard_cancel") {
        await clearState(userId);
        await showPanelAdminMenu(chatId, "ثبت پنل لغو شد.");
        return;
    }
    if (data.startsWith("admin_panel_wizard_cancel_")) {
        const panelId = Number(data.replace("admin_panel_wizard_cancel_", ""));
        await clearState(userId);
        await showPanelDetails(chatId, panelId, "ویرایش پنل لغو شد.");
        return;
    }
    if (data.startsWith("admin_panel_pick_type_add_")) {
        const panelType = parsePanelType(data.replace("admin_panel_pick_type_add_", ""));
        if (!panelType) {
            await tg("sendMessage", { chat_id: chatId, text: "نوع پنل نامعتبر است." });
            return;
        }
        await startPanelWizard(chatId, userId, "add", panelType);
        return;
    }
    if (data.startsWith("admin_panel_pick_type_edit_")) {
        const payload = data.replace("admin_panel_pick_type_edit_", "");
        const [panelIdRaw, panelTypeRaw] = payload.split("_");
        const panelId = Number(panelIdRaw);
        const panelType = parsePanelType(panelTypeRaw || "");
        if (!Number.isFinite(panelId) || panelId <= 0 || !panelType) {
            await tg("sendMessage", { chat_id: chatId, text: "اطلاعات ویرایش پنل نامعتبر است." });
            return;
        }
        await startPanelWizard(chatId, userId, "edit", panelType, panelId);
        return;
    }
    if (/^admin_panel_toggle_\d+$/.test(data)) {
        const panelId = Number(data.replace("admin_panel_toggle_", ""));
        await sql `UPDATE panels SET active = NOT active WHERE id = ${panelId};`;
        await showPanelDetails(chatId, panelId, "وضعیت پنل تغییر کرد ✅");
        return;
    }
    if (data.startsWith("admin_panel_toggle_move_")) {
        const panelId = Number(data.replace("admin_panel_toggle_move_", ""));
        await sql `UPDATE panels SET allow_customer_migration = NOT allow_customer_migration WHERE id = ${panelId};`;
        await showPanelDetails(chatId, panelId, "وضعیت مهاجرت کاربر تغییر کرد ✅");
        return;
    }
    if (data.startsWith("admin_panel_toggle_sales_")) {
        const panelId = Number(data.replace("admin_panel_toggle_sales_", ""));
        await sql `UPDATE panels SET allow_new_sales = NOT allow_new_sales WHERE id = ${panelId};`;
        await showPanelDetails(chatId, panelId, "وضعیت فروش جدید این پنل تغییر کرد ✅");
        return;
    }
    if (data === "admin_panel_test_all") {
        const rows = await sql `
      SELECT id, name
      FROM panels
      ORDER BY priority DESC, id ASC;
    `;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "هنوز هیچ پنلی ثبت نشده است." });
            return;
        }
        const results = [];
        let okCount = 0;
        for (const row of rows) {
            const result = await testPanelConnection(Number(row.id));
            if (result.ok)
                okCount += 1;
            results.push(`${row.name}: ${result.ok ? "✅" : "❌"}`);
        }
        await showPanelAdminMenu(chatId, `تست همه پنل‌ها انجام شد.\nموفق: ${okCount}/${rows.length}\n${results.join("\n")}`);
        return;
    }
    if (data.startsWith("admin_panel_test_")) {
        const panelId = Number(data.replace("admin_panel_test_", ""));
        const result = await testPanelConnection(panelId);
        await showPanelDetails(chatId, panelId, result.message);
        return;
    }
    if (data.startsWith("admin_panel_cache_")) {
        const panelId = Number(data.replace("admin_panel_cache_", ""));
        const rows = await sql `
      SELECT name, last_check_at, last_check_ok, last_check_message, cached_meta
      FROM panels
      WHERE id = ${panelId}
      LIMIT 1;
    `;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "پنل پیدا نشد." });
            return;
        }
        const p = rows[0];
        await tg("sendMessage", {
            chat_id: chatId,
            text: `کش پنل: ${p.name}\n` +
                `آخرین تست: ${p.last_check_at || "-"}\n` +
                `نتیجه: ${panelResultLabel(p.last_check_ok)}\n` +
                `پیام: ${p.last_check_message || "-"}\n` +
                `meta: ${JSON.stringify(p.cached_meta || {}, null, 2)}`,
            reply_markup: {
                inline_keyboard: [[backButton(`admin_panel_open_${panelId}`, "🔙 بازگشت به پنل")]]
            }
        });
        return;
    }
    if (data.startsWith("admin_panel_remove_yes_")) {
        const panelId = Number(data.replace("admin_panel_remove_yes_", ""));
        try {
            await sql `DELETE FROM panels WHERE id = ${panelId};`;
            await showPanelAdminMenu(chatId, "پنل حذف شد ✅");
        }
        catch (err) {
            logError("admin_panel_delete_failed", err, { panelId, adminId: userId });
            await tg("sendMessage", { chat_id: chatId, text: `❌ حذف پنل با خطا مواجه شد. ممکن است کانفیگ‌ها یا محصولاتی به آن متصل باشند.\n${err.message}` });
        }
        return;
    }
    if (data.startsWith("admin_panel_remove_")) {
        const panelId = Number(data.replace("admin_panel_remove_", ""));
        await tg("sendMessage", {
            chat_id: chatId,
            text: "از حذف این پنل مطمئن هستید؟",
            reply_markup: {
                inline_keyboard: [
                    [
                        cb("🗑 حذف", `admin_panel_remove_yes_${panelId}`, "danger"),
                        cb("❌ خیر", `admin_panel_open_${panelId}`, "primary")
                    ]
                ]
            }
        });
        return;
    }
    if (data === "admin_dead_configs") {
        const token = generateAdminToken(userId);
        const domain = process.env.VERCEL_PROJECT_PRODUCTION_URL || process.env.VERCEL_URL || "localhost:3000";
        const protocol = domain.includes("localhost") ? "http" : "https";
        const link = `${protocol}://${domain}/cleanup.html?token=${token}`;
        await tg("sendMessage", {
            chat_id: chatId,
            text: `🔎 یافتن کانفیگ‌های مرده\n\n` +
                `این ابزار فقط کانفیگ‌ها را در پنل‌های شما جستجو می‌کند و تغییری ایجاد نمی‌کند.\n` +
                `برای جلوگیری از تایم‌اوت، اسکن از طریق مرورگر انجام می‌شود.\n` +
                `لینک زیر فقط تا ۲ ساعت اعتبار دارد:\n\n` +
                `<code>${escapeHtml(link)}</code>`,
            parse_mode: "HTML"
        });
        return;
    }
    if (data === "admin_migrations") {
        const rows = await sql `
      SELECT m.id, m.requested_for, m.source_inventory_id, p.name AS target_panel_name, m.requested_by_role, m.status
      FROM panel_migrations m
      INNER JOIN panels p ON p.id = m.target_panel_id
      WHERE m.status = 'pending'
      ORDER BY m.id DESC
      LIMIT 30;
    `;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "درخواست انتقال باز وجود ندارد." });
            return;
        }
        const keyboard = rows.map((m) => [
            {
                text: `#${m.id} | کاربر ${m.requested_for} | ${m.target_panel_name} | ${m.requested_by_role}`,
                callback_data: `admin_migration_open_${m.id}`
            }
        ]);
        keyboard.push([backButton("admin_panels")]);
        await tg("sendMessage", { chat_id: chatId, text: "صف انتقال‌ها:", reply_markup: { inline_keyboard: keyboard } });
        return;
    }
    if (data.startsWith("admin_migration_open_")) {
        const migrationId = Number(data.replace("admin_migration_open_", ""));
        const rows = await sql `
      SELECT
        m.id,
        m.status,
        m.requested_for,
        m.source_inventory_id,
        m.source_config_snapshot,
        p.name AS target_panel_name
      FROM panel_migrations m
      INNER JOIN panels p ON p.id = m.target_panel_id
      WHERE m.id = ${migrationId}
      LIMIT 1;
    `;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "درخواست یافت نشد." });
            return;
        }
        const r = rows[0];
        await tg("sendMessage", {
            chat_id: chatId,
            parse_mode: "HTML",
            text: `درخواست #${r.id}\n` +
                `وضعیت: ${r.status}\n` +
                `کاربر: ${r.requested_for}\n` +
                `کانفیگ مبدا: ${r.source_inventory_id}\n` +
                `پنل مقصد: ${r.target_panel_name}\n\n` +
                `${escapeHtml(String(r.source_config_snapshot || "-"))}`,
            reply_markup: {
                inline_keyboard: [
                    [cb("⚡ انتقال با همان کانفیگ", `admin_migration_auto_${r.id}`, "success")],
                    [cb("✍️ ثبت کانفیگ جدید", `admin_migration_manual_${r.id}`, "primary")],
                    [cb("❌ رد درخواست", `admin_migration_reject_${r.id}`, "danger")],
                    [backButton("admin_migrations")]
                ]
            }
        });
        return;
    }
    if (data.startsWith("admin_migration_auto_")) {
        const migrationId = Number(data.replace("admin_migration_auto_", ""));
        const result = await completeMigration(migrationId, userId, null);
        await tg("sendMessage", { chat_id: chatId, text: result.ok ? "انتقال انجام شد ✅" : `خطا: ${result.reason}` });
        return;
    }
    if (data.startsWith("admin_migration_manual_")) {
        const migrationId = Number(data.replace("admin_migration_manual_", ""));
        await setState(userId, "admin_complete_migration_config", { migrationId });
        await tg("sendMessage", { chat_id: chatId, text: "کانفیگ جدید مقصد را ارسال کنید." });
        return;
    }
    if (data.startsWith("admin_migration_reject_")) {
        const migrationId = Number(data.replace("admin_migration_reject_", ""));
        const rows = await sql `
      UPDATE panel_migrations
      SET status = 'rejected', processed_at = NOW(), processed_by = ${userId}
      WHERE id = ${migrationId} AND status = 'pending'
      RETURNING requested_for;
    `;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "این درخواست قابل رد نیست." });
            return;
        }
        await tg("sendMessage", { chat_id: Number(rows[0].requested_for), text: `درخواست انتقال #${migrationId} رد شد ❌` });
        await tg("sendMessage", { chat_id: chatId, text: "درخواست رد شد ✅" });
        return;
    }
    if (data === "admin_products") {
        await listProductsForAdmin(chatId, userId);
        return;
    }
    if (data === "admin_products_show_archived") {
        await setSetting(`admin_products_show_archived_${userId}`, "true");
        await listProductsForAdmin(chatId, userId);
        return;
    }
    if (data === "admin_products_hide_archived") {
        await setSetting(`admin_products_show_archived_${userId}`, "false");
        await listProductsForAdmin(chatId, userId);
        return;
    }
    if (data === "admin_add_product") {
        await startProductWizard(chatId, userId, "add");
        return;
    }
    if (data.startsWith("admin_edit_product_")) {
        const productId = Number(data.replace("admin_edit_product_", ""));
        await startProductWizard(chatId, userId, "edit", productId);
        return;
    }
    if (data.startsWith("admin_product_wizard_cancel_")) {
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: "ثبت/ویرایش محصول لغو شد." });
        await listProductsForAdmin(chatId, userId);
        return;
    }
    if (data === "admin_product_wizard_price_auto") {
        const state = await getState(userId);
        if (!state || state.state !== "admin_product_wizard")
            return;
        if (parseProductKind(state.payload.productKind) === "account") {
            const payload = { ...state.payload, priceMode: "manual", step: "price_toman" };
            await setState(userId, "admin_product_wizard", payload);
            await promptProductWizardStep(chatId, payload);
            return;
        }
        const payload = { ...state.payload, priceMode: "auto", step: "sell_mode" };
        await setState(userId, "admin_product_wizard", payload);
        await promptProductWizardStep(chatId, payload);
        return;
    }
    if (data === "admin_product_wizard_price_manual") {
        const state = await getState(userId);
        if (!state || state.state !== "admin_product_wizard")
            return;
        const payload = { ...state.payload, priceMode: "manual", step: "price_toman" };
        await setState(userId, "admin_product_wizard", payload);
        await promptProductWizardStep(chatId, payload);
        return;
    }
    if (data === "admin_product_wizard_sell_manual") {
        const state = await getState(userId);
        if (!state || state.state !== "admin_product_wizard")
            return;
        const payload = { ...state.payload, sellMode: "manual", step: "is_infinite" };
        await setState(userId, "admin_product_wizard", payload);
        await promptProductWizardStep(chatId, payload);
        return;
    }
    if (data === "admin_product_wizard_sell_panel") {
        const state = await getState(userId);
        if (!state || state.state !== "admin_product_wizard")
            return;
        if (parseProductKind(state.payload.productKind) === "account") {
            await tg("sendMessage", { chat_id: chatId, text: "برای محصول اکانتی، فروش از پنل غیرفعال است و فقط فروش دستی قابل انتخاب است." });
            return;
        }
        const payload = { ...state.payload, sellMode: "panel", step: "panel_id" };
        await setState(userId, "admin_product_wizard", payload);
        await promptProductWizardStep(chatId, payload);
        return;
    }
    if (data === "admin_product_wizard_kind_v2ray" || data === "admin_product_wizard_kind_account") {
        const state = await getState(userId);
        if (!state || state.state !== "admin_product_wizard")
            return;
        const productKind = data === "admin_product_wizard_kind_account" ? "account" : "v2ray";
        const payload = productKind === "account"
            ? { ...state.payload, productKind, sizeMb: 0, priceMode: "manual", step: "price_mode" }
            : { ...state.payload, productKind, step: "size_mb" };
        await setState(userId, "admin_product_wizard", payload);
        await promptProductWizardStep(chatId, payload);
        return;
    }
    if (data === "admin_product_wizard_infinite_yes" || data === "admin_product_wizard_infinite_no") {
        const state = await getState(userId);
        if (!state || state.state !== "admin_product_wizard")
            return;
        const isInfinite = data === "admin_product_wizard_infinite_yes";
        const payload = { ...state.payload, isInfinite };
        const result = await saveProductWizard(payload);
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: result.message });
        if (result.ok)
            await listProductsForAdmin(chatId, userId);
        return;
    }
    if (data.startsWith("admin_product_wizard_panel_")) {
        const panelId = Number(data.replace("admin_product_wizard_panel_", ""));
        const state = await getState(userId);
        if (!state || state.state !== "admin_product_wizard")
            return;
        const payload = { ...state.payload, panelId, step: "panel_sell_limit" };
        await setState(userId, "admin_product_wizard", payload);
        await promptProductWizardStep(chatId, payload);
        return;
    }
    if (data.startsWith("admin_product_wizard_delivery_")) {
        const panelDeliveryMode = parseDeliveryMode(data.replace("admin_product_wizard_delivery_", ""));
        const state = await getState(userId);
        if (!state || state.state !== "admin_product_wizard") {
            await tg("sendMessage", { chat_id: chatId, text: "جلسه افزودن/ویرایش محصول منقضی شده. دوباره از اول شروع کنید." });
            return;
        }
        const payload = { ...state.payload, panelDeliveryMode };
        const result = await saveProductWizard(payload);
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: result.message });
        if (result.ok)
            await listProductsForAdmin(chatId, userId);
        return;
    }
    if (data.startsWith("admin_product_wizard_protocol_")) {
        const protocol = data.replace("admin_product_wizard_protocol_", "").trim().toLowerCase();
        const state = await getState(userId);
        if (!state || state.state !== "admin_product_wizard")
            return;
        const payload = { ...state.payload, protocol };
        const result = await saveProductWizard(payload);
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: result.message });
        if (result.ok)
            await listProductsForAdmin(chatId, userId);
        return;
    }
    if (data.startsWith("admin_toggle_product_infinite_")) {
        const productId = Number(data.replace("admin_toggle_product_infinite_", ""));
        await sql `UPDATE products SET is_infinite = NOT is_infinite WHERE id = ${productId};`;
        await tg("sendMessage", { chat_id: chatId, text: "حالت بینهایت محصول تغییر کرد ✅" });
        return;
    }
    if (data.startsWith("admin_toggle_product_sell_mode_")) {
        const productId = Number(data.replace("admin_toggle_product_sell_mode_", ""));
        const rows = await sql `
      UPDATE products
      SET sell_mode = CASE WHEN sell_mode = 'panel' THEN 'manual' ELSE 'panel' END,
          is_infinite = CASE WHEN sell_mode = 'panel' THEN FALSE ELSE TRUE END
      WHERE id = ${productId}
      RETURNING sell_mode;
    `;
        await tg("sendMessage", {
            chat_id: chatId,
            text: rows.length ? `حالت فروش محصول روی ${rows[0].sell_mode === "panel" ? "فروش از پنل" : "فروش دستی"} قرار گرفت ✅` : "محصول پیدا نشد."
        });
        return;
    }
    if (data.startsWith("admin_configure_product_panel_")) {
        const productId = Number(data.replace("admin_configure_product_panel_", ""));
        const product = await getProductForPanelWizard(productId);
        if (!product) {
            await tg("sendMessage", { chat_id: chatId, text: "محصول پیدا نشد." });
            return;
        }
        const payload = productPanelWizardPayload(product);
        await setState(userId, "admin_product_panel_wizard", payload);
        await promptProductPanelWizardStep(chatId, payload);
        return;
    }
    if (data.startsWith("admin_product_panel_wizard_cancel_")) {
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: "تنظیم فروش پنل لغو شد." });
        await listProductsForAdmin(chatId, userId);
        return;
    }
    if (data.startsWith("admin_product_panel_pick_")) {
        const panelId = Number(data.replace("admin_product_panel_pick_", ""));
        const state = await getState(userId);
        if (!state || state.state !== "admin_product_panel_wizard") {
            await tg("sendMessage", { chat_id: chatId, text: "جلسه تنظیم منقضی شده. دوباره از لیست محصولات شروع کنید." });
            return;
        }
        if (!Number.isFinite(panelId) || panelId <= 0) {
            await tg("sendMessage", { chat_id: chatId, text: "پنل نامعتبر است." });
            return;
        }
        const payload = { ...state.payload, panelId, step: "mode" };
        await setState(userId, "admin_product_panel_wizard", payload);
        await promptProductPanelWizardStep(chatId, payload);
        return;
    }
    if (data === "admin_product_panel_quick") {
        const state = await getState(userId);
        if (!state || state.state !== "admin_product_panel_wizard") {
            await tg("sendMessage", { chat_id: chatId, text: "جلسه تنظیم منقضی شده. دوباره تلاش کنید." });
            return;
        }
        const result = await saveProductPanelWizard(state.payload, true);
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: result.message });
        if (result.ok) {
            await listProductsForAdmin(chatId, userId);
        }
        return;
    }
    if (data === "admin_product_panel_custom") {
        const state = await getState(userId);
        if (!state || state.state !== "admin_product_panel_wizard") {
            await tg("sendMessage", { chat_id: chatId, text: "جلسه تنظیم منقضی شده. دوباره تلاش کنید." });
            return;
        }
        const payload = { ...state.payload, step: "sell_limit" };
        await setState(userId, "admin_product_panel_wizard", payload);
        await promptProductPanelWizardStep(chatId, payload);
        return;
    }
    if (data.startsWith("admin_product_panel_delivery_")) {
        const mode = data.replace("admin_product_panel_delivery_", "");
        const state = await getState(userId);
        if (!state || state.state !== "admin_product_panel_wizard") {
            await tg("sendMessage", { chat_id: chatId, text: "جلسه تنظیم منقضی شده. دوباره تلاش کنید." });
            return;
        }
        const panelDeliveryMode = parseDeliveryMode(mode);
        const payload = { ...state.payload, panelDeliveryMode, step: "inbound_id" };
        await setState(userId, "admin_product_panel_wizard", payload);
        await promptProductPanelWizardStep(chatId, payload);
        return;
    }
    if (data.startsWith("admin_product_panel_protocol_")) {
        const protocol = data.replace("admin_product_panel_protocol_", "").trim().toLowerCase();
        const state = await getState(userId);
        if (!state || state.state !== "admin_product_panel_wizard") {
            await tg("sendMessage", { chat_id: chatId, text: "جلسه تنظیم منقضی شده. دوباره تلاش کنید." });
            return;
        }
        if (!protocol) {
            await tg("sendMessage", { chat_id: chatId, text: "پروتکل نامعتبر است." });
            return;
        }
        const payload = { ...state.payload, protocol, step: "expire_days" };
        await setState(userId, "admin_product_panel_wizard", payload);
        await promptProductPanelWizardStep(chatId, payload);
        return;
    }
    if (data.startsWith("admin_remove_product_yes_")) {
        const productId = Number(data.replace("admin_remove_product_yes_", ""));
        const refRows = await sql `
      SELECT
        (SELECT COUNT(*)::int FROM inventory WHERE product_id = ${productId}) AS inventory_count,
        (SELECT COUNT(*)::int FROM orders WHERE product_id = ${productId}) AS orders_count;
    `;
        const inventoryCount = Number(refRows[0]?.inventory_count || 0);
        const ordersCount = Number(refRows[0]?.orders_count || 0);
        if (inventoryCount > 0 || ordersCount > 0) {
            const archived = await sql `
        UPDATE products
        SET is_active = FALSE,
            name = (name || ' [archived#' || id::text || ']')
        WHERE id = ${productId}
        RETURNING name;
      `;
            await tg("sendMessage", {
                chat_id: chatId,
                text: "این محصول قبلاً فروش داشته و حذف کامل باعث از دست رفتن اتصال به سوابق می‌شود.\n" +
                    "پس به‌صورت خودکار آرشیو/غیرفعال شد تا کاربران کانفیگ‌های فروخته‌شده را از دست ندهند ✅\n" +
                    `inventory: ${inventoryCount}\n` +
                    `orders: ${ordersCount}\n` +
                    (archived.length ? `نام جدید: ${archived[0].name}` : "")
            });
            await listProductsForAdmin(chatId, userId);
            return;
        }
        try {
            const deleted = await sql `
        DELETE FROM products
        WHERE id = ${productId}
        RETURNING name;
      `;
            if (!deleted.length) {
                await tg("sendMessage", { chat_id: chatId, text: "محصول پیدا نشد یا قبلاً حذف شده است." });
                return;
            }
            await tg("sendMessage", { chat_id: chatId, text: `محصول «${deleted[0].name}» حذف شد ✅` });
            await listProductsForAdmin(chatId, userId);
        }
        catch (err) {
            logError("admin_delete_product_failed", err, { productId, adminId: userId });
            await tg("sendMessage", { chat_id: chatId, text: `❌ حذف محصول با خطا مواجه شد. ممکن است دیتایی به آن وابسته باشد.\n${err.message}` });
        }
        return;
    }
    if (data.startsWith("admin_remove_product_")) {
        const productId = Number(data.replace("admin_remove_product_", ""));
        const rows = await sql `SELECT name FROM products WHERE id = ${productId} LIMIT 1;`;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "محصول پیدا نشد." });
            return;
        }
        await tg("sendMessage", {
            chat_id: chatId,
            text: `از حذف محصول «${rows[0].name}» مطمئن هستید؟`,
            reply_markup: {
                inline_keyboard: [
                    [
                        cb("🗑 حذف", `admin_remove_product_yes_${productId}`, "danger"),
                        cb("❌ خیر", "admin_products", "primary")
                    ]
                ]
            }
        });
        return;
    }
    if (data.startsWith("admin_toggle_product_")) {
        const productId = Number(data.replace("admin_toggle_product_", ""));
        await sql `UPDATE products SET is_active = NOT is_active WHERE id = ${productId};`;
        await tg("sendMessage", { chat_id: chatId, text: "وضعیت محصول تغییر کرد ✅" });
        return;
    }
    if (data === "admin_inventory") {
        await showProducts(chatId, false);
        return;
    }
    if (data.startsWith("admin_inventory_product_")) {
        const productId = Number(data.replace("admin_inventory_product_", ""));
        const countRows = await sql `
      SELECT
        SUM(CASE WHEN status = 'available' THEN 1 ELSE 0 END)::int AS available_count,
        SUM(CASE WHEN status = 'sold' THEN 1 ELSE 0 END)::int AS sold_count
      FROM inventory
      WHERE product_id = ${productId};
    `;
        const availableCount = Number(countRows[0].available_count || 0);
        const soldCount = Number(countRows[0].sold_count || 0);
        await tg("sendMessage", {
            chat_id: chatId,
            text: `موجودی محصول:\nآزاد: ${availableCount}\nفروخته‌شده: ${soldCount}`,
            reply_markup: {
                inline_keyboard: [
                    [cb("➕ افزودن کانفیگ", `admin_add_stock_${productId}`, "success")],
                    [cb("🗑 لیست قابل حذف", `admin_available_list_${productId}`, "primary")],
                    [cb("📦 لیست فروخته‌شده‌ها", `admin_sold_list_${productId}`, "primary")],
                    [backButton("admin_inventory")]
                ]
            }
        });
        return;
    }
    if (data.startsWith("admin_add_stock_")) {
        const productId = Number(data.replace("admin_add_stock_", ""));
        await setState(userId, "admin_add_stock", { productId });
        await tg("sendMessage", { chat_id: chatId, text: "هر کانفیگ را در یک خط بفرستید." });
        return;
    }
    if (data.startsWith("admin_sold_list_")) {
        const productId = Number(data.replace("admin_sold_list_", ""));
        const rows = await sql `
      SELECT i.id, i.owner_telegram_id, i.config_value, i.delivery_payload, o.purchase_id
      FROM inventory i
      LEFT JOIN orders o ON o.id = i.sold_order_id
      WHERE i.product_id = ${productId} AND i.status = 'sold'
      ORDER BY i.sold_at DESC
      LIMIT 20;
    `;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "لیست فروش خالی است." });
            return;
        }
        for (const row of rows) {
            const payload = parseDeliveryPayload(row.delivery_payload);
            const revoked = payload.metadata?.revoked === true;
            await tg("sendMessage", {
                chat_id: chatId,
                parse_mode: "HTML",
                text: `#${row.id} | کاربر: ${row.owner_telegram_id || "-"} | خرید: ${row.purchase_id || "-"}${revoked ? " | 🚫" : ""}\n` +
                    `${escapeHtml(responseSnippet(String(row.config_value), 450))}`,
                reply_markup: {
                    inline_keyboard: [
                        [
                            revoked
                                ? confirmButton(`admin_inv_revoke_${row.id}`, "✅ فعال")
                                : cb("🚫 غیرفعال", `admin_inv_revoke_${row.id}`, "danger"),
                            cb("✏️ نام", `admin_inv_rename_${row.id}`, "primary")
                        ]
                    ]
                }
            });
        }
        return;
    }
    if (data.startsWith("admin_inv_revoke_")) {
        const inventoryId = Number(data.replace("admin_inv_revoke_", ""));
        const rows = await sql `SELECT delivery_payload, owner_telegram_id FROM inventory WHERE id = ${inventoryId} LIMIT 1;`;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "کانفیگ پیدا نشد." });
            return;
        }
        const payload = parseDeliveryPayload(rows[0].delivery_payload);
        const revoked = payload.metadata?.revoked === true;
        await sql `
      UPDATE inventory
      SET delivery_payload = jsonb_set(
        jsonb_set(COALESCE(delivery_payload, '{}'::jsonb), '{metadata}', COALESCE(delivery_payload->'metadata', '{}'::jsonb), true),
        '{metadata,revoked}',
        to_jsonb(${!revoked}::boolean),
        true
      )
      WHERE id = ${inventoryId};
    `;
        try {
            const owner = Number(rows[0].owner_telegram_id || 0);
            if (owner) {
                await tg("sendMessage", { chat_id: owner, text: !revoked ? "کانفیگ شما توسط ادمین غیرفعال شد." : "کانفیگ شما دوباره فعال شد ✅" });
            }
        }
        catch (error) {
            logError("inventory_revoke_notify_failed", error, { inventoryId });
        }
        await tg("sendMessage", { chat_id: chatId, text: !revoked ? "غیرفعال شد ✅" : "فعال شد ✅" });
        return;
    }
    if (data.startsWith("admin_inv_rename_")) {
        const inventoryId = Number(data.replace("admin_inv_rename_", ""));
        await setState(userId, "admin_inv_rename", { inventoryId });
        await tg("sendMessage", { chat_id: chatId, text: "نام جدید کانفیگ را ارسال کنید. (برای حذف نام: -)" });
        return;
    }
    if (data.startsWith("admin_available_list_")) {
        const productId = Number(data.replace("admin_available_list_", ""));
        const rows = await sql `
      SELECT id, config_value
      FROM inventory
      WHERE product_id = ${productId} AND status = 'available'
      ORDER BY id DESC
      LIMIT 30;
    `;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "موردی برای حذف وجود ندارد." });
            return;
        }
        await tg("sendMessage", { chat_id: chatId, text: "برای حذف هر کانفیگ روی دکمه «حذف» بزنید:" });
        for (const row of rows) {
            await tg("sendMessage", {
                chat_id: chatId,
                parse_mode: "HTML",
                text: `#${row.id}\n${escapeHtml(String(row.config_value))}`,
                reply_markup: {
                    inline_keyboard: [[cb("🗑 حذف", `admin_delete_inventory_${row.id}`, "danger")]]
                }
            });
        }
        return;
    }
    if (data.startsWith("admin_delete_inventory_")) {
        const inventoryId = Number(data.replace("admin_delete_inventory_", ""));
        try {
            await sql `
        WITH deleted_forensics AS (
          DELETE FROM config_forensics WHERE inventory_id = ${inventoryId}
        ),
        deleted_topups AS (
          DELETE FROM topup_requests WHERE inventory_id = ${inventoryId}
        ),
        deleted_migrations AS (
          DELETE FROM panel_migrations WHERE source_inventory_id = ${inventoryId}
        )
        DELETE FROM inventory
        WHERE id = ${inventoryId} AND status = 'available'
        RETURNING product_id;
      `;
            // Also nullify references in orders just in case an available config somehow ended up in an order
            await sql `UPDATE orders SET inventory_id = NULL WHERE inventory_id = ${inventoryId}`;
            await tg("sendMessage", {
                chat_id: chatId,
                text: "کانفیگ حذف شد ✅",
            });
        }
        catch (err) {
            logError("admin_delete_available_inventory_failed", err, { inventoryId, adminId: userId });
            await tg("sendMessage", { chat_id: chatId, text: `❌ حذف کانفیگ با خطا مواجه شد.\n${err.message}` });
        }
        return;
    }
    if (data === "admin_discounts") {
        const rows = await sql `SELECT id, code, type, amount, active, usage_limit, used_count FROM discounts ORDER BY id DESC LIMIT 30;`;
        const keyboard = rows.flatMap((d) => [
            [cb(`${d.code} | ${d.type} ${d.amount} | مصرف ${d.used_count}/${d.usage_limit ?? "∞"}`, `admin_edit_discount_${d.id}`, "primary")],
            [
                cb("ویرایش", `admin_edit_discount_${d.id}`, "primary"),
                cb(d.active ? "غیرفعال" : "فعال", `admin_toggle_discount_${d.id}`, d.active ? "danger" : "success"),
                cb("🗑 حذف", `admin_delete_discount_${d.id}`, "danger")
            ]
        ]);
        keyboard.push([cb("➕ افزودن تخفیف", "admin_add_discount", "success")]);
        keyboard.push([backButton("admin_panel")]);
        await tg("sendMessage", { chat_id: chatId, text: "مدیریت تخفیف:", reply_markup: { inline_keyboard: keyboard } });
        return;
    }
    if (data === "admin_add_discount") {
        await startDiscountWizard(chatId, userId, "add");
        return;
    }
    if (data.startsWith("admin_edit_discount_")) {
        const discountId = Number(data.replace("admin_edit_discount_", ""));
        await startDiscountWizard(chatId, userId, "edit", discountId);
        return;
    }
    if (data.startsWith("admin_discount_wizard_cancel_")) {
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: "ثبت/ویرایش تخفیف لغو شد." });
        return;
    }
    if (data === "admin_discount_wizard_code_random") {
        const state = await getState(userId);
        if (!state || state.state !== "admin_discount_wizard")
            return;
        const payload = { ...state.payload, code: randomCode(10), step: "type" };
        await setState(userId, "admin_discount_wizard", payload);
        await promptDiscountWizardStep(chatId, payload);
        return;
    }
    if (data === "admin_discount_wizard_code_manual") {
        const state = await getState(userId);
        if (!state || state.state !== "admin_discount_wizard")
            return;
        const payload = { ...state.payload, step: "code" };
        await setState(userId, "admin_discount_wizard", payload);
        await promptDiscountWizardStep(chatId, payload);
        return;
    }
    if (data === "admin_discount_wizard_type_percent" || data === "admin_discount_wizard_type_fixed") {
        const state = await getState(userId);
        if (!state || state.state !== "admin_discount_wizard")
            return;
        const type = data.endsWith("_percent") ? "percent" : "fixed";
        const payload = { ...state.payload, type, step: "amount" };
        await setState(userId, "admin_discount_wizard", payload);
        await promptDiscountWizardStep(chatId, payload);
        return;
    }
    if (data.startsWith("admin_delete_discount_")) {
        const discountId = Number(data.replace("admin_delete_discount_", ""));
        try {
            await sql `DELETE FROM discounts WHERE id = ${discountId};`;
            await tg("sendMessage", { chat_id: chatId, text: "تخفیف حذف شد ✅" });
        }
        catch (err) {
            logError("admin_delete_discount_failed", err, { discountId, adminId: userId });
            await tg("sendMessage", { chat_id: chatId, text: `❌ حذف تخفیف با خطا مواجه شد.\n${err.message}` });
        }
        return;
    }
    if (data.startsWith("admin_toggle_discount_")) {
        const discountId = Number(data.replace("admin_toggle_discount_", ""));
        await sql `UPDATE discounts SET active = NOT active WHERE id = ${discountId};`;
        await tg("sendMessage", { chat_id: chatId, text: "وضعیت تخفیف تغییر کرد ✅" });
        return;
    }
    if (data === "admin_payment_methods") {
        const rows = await sql `SELECT code, title, active FROM payment_methods ORDER BY code ASC;`;
        const keyboard = rows.map((m) => [cb(`${m.title} | ${m.active ? "فعال" : "غیرفعال"}`, `admin_toggle_method_${m.code}`, m.active ? "danger" : "success")]);
        keyboard.push([backButton("admin_panel")]);
        await tg("sendMessage", { chat_id: chatId, text: "مدیریت روش‌های پرداخت:", reply_markup: { inline_keyboard: keyboard } });
        return;
    }
    if (data.startsWith("admin_toggle_method_")) {
        const code = data.replace("admin_toggle_method_", "");
        await sql `UPDATE payment_methods SET active = NOT active WHERE code = ${code};`;
        await tg("sendMessage", { chat_id: chatId, text: "روش پرداخت بروزرسانی شد ✅" });
        return;
    }
    if (data === "admin_cards") {
        const randomMode = await getBoolSetting("random_card_distribution", false);
        const mainCardRaw = await getSetting("main_card_id");
        const mainCardId = mainCardRaw ? Number(mainCardRaw) : NaN;
        const rows = await sql `
      SELECT
        c.id,
        c.label,
        c.card_number,
        c.active,
        (SELECT COUNT(*)::int FROM orders o WHERE o.card_id = c.id) AS total_orders,
        (
          SELECT COUNT(*)::int
          FROM orders o
          WHERE o.card_id = c.id AND (o.status = 'paid' OR o.status = 'awaiting_config')
        ) AS sold_count
      FROM cards c
      ORDER BY c.id ASC;
    `;
        const keyboard = rows.flatMap((c) => [
            [
                {
                    text: `${Number(c.id) === mainCardId ? "⭐ " : ""}${c.label} | ${c.card_number} | ${c.active ? "فعال" : "غیرفعال"}\n` +
                        `فروش: ${Number(c.sold_count || 0)} | کل سفارش: ${Number(c.total_orders || 0)}`,
                    callback_data: `admin_edit_card_${c.id}`,
                    style: "primary"
                }
            ],
            [
                cb("ویرایش", `admin_edit_card_${c.id}`, "primary"),
                cb(c.active ? "غیرفعال" : "فعال", `admin_toggle_card_${c.id}`, c.active ? "danger" : "success"),
                cb("⭐ کارت اصلی", `admin_set_main_card_${c.id}`, "success"),
                cb("🗑 حذف", `admin_remove_card_${c.id}`, "danger")
            ]
        ]);
        keyboard.push([cb("➕ افزودن کارت", "admin_add_card", "success")]);
        keyboard.push([cb(randomMode ? "🎲 پخش رندوم: روشن" : "🎲 پخش رندوم: خاموش", "admin_toggle_random_cards", randomMode ? "success" : "primary")]);
        keyboard.push([backButton("admin_panel")]);
        await tg("sendMessage", { chat_id: chatId, text: "مدیریت کارت‌ها:", reply_markup: { inline_keyboard: keyboard } });
        return;
    }
    if (data === "admin_add_card") {
        await startCardWizard(chatId, userId, "add");
        return;
    }
    if (data.startsWith("admin_edit_card_")) {
        const cardId = Number(data.replace("admin_edit_card_", ""));
        await startCardWizard(chatId, userId, "edit", cardId);
        return;
    }
    if (data.startsWith("admin_card_wizard_cancel_")) {
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: "ثبت/ویرایش کارت لغو شد." });
        return;
    }
    if (data.startsWith("admin_toggle_card_")) {
        const cardId = Number(data.replace("admin_toggle_card_", ""));
        await sql `UPDATE cards SET active = NOT active WHERE id = ${cardId};`;
        await tg("sendMessage", { chat_id: chatId, text: "وضعیت کارت تغییر کرد ✅" });
        return;
    }
    if (data.startsWith("admin_set_main_card_")) {
        const cardId = Number(data.replace("admin_set_main_card_", ""));
        const rows = await sql `SELECT id FROM cards WHERE id = ${cardId} AND active = TRUE LIMIT 1;`;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "فقط کارت فعال می‌تواند کارت اصلی باشد." });
            return;
        }
        await setSetting("main_card_id", String(cardId));
        await tg("sendMessage", { chat_id: chatId, text: "کارت اصلی تعیین شد ✅" });
        return;
    }
    if (data.startsWith("admin_remove_card_")) {
        const cardId = Number(data.replace("admin_remove_card_", ""));
        try {
            await sql `DELETE FROM cards WHERE id = ${cardId};`;
            await tg("sendMessage", { chat_id: chatId, text: "کارت حذف شد ✅" });
        }
        catch (err) {
            logError("admin_remove_card_failed", err, { cardId, adminId: userId });
            await tg("sendMessage", { chat_id: chatId, text: `❌ حذف کارت با خطا مواجه شد.\n${err.message}` });
        }
        return;
    }
    if (data === "admin_toggle_random_cards") {
        const current = await getBoolSetting("random_card_distribution", false);
        await setSetting("random_card_distribution", (!current).toString());
        await tg("sendMessage", { chat_id: chatId, text: `پخش رندوم کارت ${!current ? "فعال" : "غیرفعال"} شد ✅` });
        return;
    }
    if (data.startsWith("wallet_accept_")) {
        const topupId = Number(data.replace("wallet_accept_", ""));
        const rows = await sql `
      UPDATE wallet_topups
      SET status = 'paid', done_at = NOW(), admin_decision_by = ${userId}
      WHERE id = ${topupId} AND status = 'receipt_submitted'
      RETURNING telegram_id, amount;
    `;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "این درخواست قابل تایید نیست یا قبلاً بررسی شده است." });
            return;
        }
        await sql `
      UPDATE users
      SET wallet_balance = wallet_balance + ${rows[0].amount}
      WHERE telegram_id = ${rows[0].telegram_id};
    `;
        await sql `
      INSERT INTO wallet_transactions (telegram_id, amount, type, description)
      VALUES (${rows[0].telegram_id}, ${rows[0].amount}, 'charge', 'شارژ از طریق کارت‌به‌کارت');
    `;
        await tg("sendMessage", {
            chat_id: Number(rows[0].telegram_id),
            text: `رسید شارژ کیف پول تایید شد ✅\nمبلغ ${formatPriceToman(Number(rows[0].amount))} تومان به کیف پول شما اضافه شد.`
        });
        await tg("sendMessage", { chat_id: chatId, text: "رسید تایید شد و کیف پول کاربر شارژ شد ✅" });
        return;
    }
    if (data.startsWith("wallet_deny_")) {
        const topupId = Number(data.replace("wallet_deny_", ""));
        const rows = await sql `
      UPDATE wallet_topups
      SET status = 'denied', admin_decision_by = ${userId}
      WHERE id = ${topupId} AND status = 'receipt_submitted'
      RETURNING telegram_id;
    `;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "این درخواست قابل رد نیست یا قبلاً بررسی شده است." });
            return;
        }
        await tg("sendMessage", { chat_id: Number(rows[0].telegram_id), text: `رسید شارژ کیف پول شما رد شد ❌` });
        await tg("sendMessage", { chat_id: chatId, text: "رد شد ✅" });
        return;
    }
    if (data === "admin_manage_users") {
        await setState(userId, "admin_manage_users");
        await tg("sendMessage", {
            chat_id: chatId,
            text: "لطفاً آیدی عددی (Telegram ID) یا یوزرنیم (با @ یا بدون @) کاربر موردنظر را ارسال کنید:",
            reply_markup: { inline_keyboard: [[backButton("admin_panel")]] }
        });
        return;
    }
    if (data.startsWith("admin_wallet_add_")) {
        const targetUserId = Number(data.replace("admin_wallet_add_", ""));
        await setState(userId, "admin_wallet_add", { targetUserId });
        await tg("sendMessage", {
            chat_id: chatId,
            text: "مبلغی که می‌خواهید به کیف پول این کاربر اضافه کنید را به تومان وارد کنید:",
            reply_markup: { inline_keyboard: [[backButton("admin_manage_users")]] }
        });
        return;
    }
    if (data.startsWith("admin_wallet_sub_")) {
        const targetUserId = Number(data.replace("admin_wallet_sub_", ""));
        await setState(userId, "admin_wallet_sub", { targetUserId });
        await tg("sendMessage", {
            chat_id: chatId,
            text: "مبلغی که می‌خواهید از کیف پول این کاربر کم کنید را به تومان وارد کنید:",
            reply_markup: { inline_keyboard: [[backButton("admin_manage_users")]] }
        });
        return;
    }
    if (data === "admin_stats") {
        const m1 = await sql `
      SELECT
        COALESCE(
          SUM(
            CASE
              WHEN COALESCE(p.panel_config->>'product_kind', 'v2ray') = 'account' THEN 0
              ELSE p.size_mb
            END
          ),
          0
        )::int AS sold_mb
      FROM orders o
      INNER JOIN products p ON p.id = o.product_id
      WHERE o.status = 'paid' OR o.status = 'awaiting_config';
    `;
        const m2 = await sql `SELECT COALESCE(SUM(requested_mb), 0)::int AS topup_mb FROM topup_requests WHERE status = 'done';`;
        const m3 = await sql `SELECT COUNT(*)::int AS total_users FROM users;`;
        const m4 = await sql `
      SELECT COUNT(DISTINCT telegram_id)::int AS customers
      FROM orders
      WHERE status = 'paid' OR status = 'awaiting_config';
    `;
        const m5 = await sql `
      SELECT
        COUNT(*) FILTER (WHERE status = 'approved')::int AS migrations_done,
        COUNT(*) FILTER (WHERE status = 'pending')::int AS migrations_pending
      FROM panel_migrations;
    `;
        const soldMb = Number(m1[0].sold_mb || 0);
        const totalMb = soldMb + Number(m2[0].topup_mb || 0);
        const totalGb = (totalMb / 1024).toFixed(2);
        const soldGb = soldMb / 1024;
        const productRateRaw = await getSetting("product_price_per_gb_toman");
        const fallbackRateRaw = await getSetting("topup_price_per_gb_toman");
        const productRate = normalizePricePerGb(productRateRaw || fallbackRateRaw || "500000");
        const totalEarning = Math.max(0, Math.round(soldGb * productRate));
        await tg("sendMessage", {
            chat_id: chatId,
            text: `آمار کلی:\n` +
                `دیتای فروخته‌شده: ${totalMb}MB (${totalGb}GB)\n` +
                `درآمد کل: ${formatPriceToman(totalEarning)} تومان\n` +
                `کل کاربران: ${Number(m3[0].total_users || 0)}\n` +
                `تعداد مشتریان: ${Number(m4[0].customers || 0)}\n` +
                `انتقال‌های انجام‌شده: ${Number(m5[0].migrations_done || 0)}\n` +
                `انتقال‌های در صف: ${Number(m5[0].migrations_pending || 0)}`,
            reply_markup: {
                inline_keyboard: [
                    [cb("👥 مشتریان هر محصول", "admin_stats_buyers", "primary")],
                    [backButton("admin_panel")]
                ]
            }
        });
        return;
    }
    if (data === "admin_stats_buyers") {
        const rows = await sql `
      SELECT p.id, p.name, COUNT(DISTINCT o.telegram_id)::int AS buyers
      FROM products p
      LEFT JOIN orders o
        ON o.product_id = p.id
       AND (o.status = 'paid' OR o.status = 'awaiting_config')
      GROUP BY p.id, p.name
      ORDER BY p.id ASC;
    `;
        const keyboard = rows.map((p) => [cb(`${p.name} | مشتری: ${Number(p.buyers || 0)}`, `admin_stats_buyers_product_${p.id}`, "primary")]);
        keyboard.push([backButton("admin_stats", "🔙 بازگشت به آمار")]);
        await tg("sendMessage", {
            chat_id: chatId,
            text: "یک محصول را انتخاب کنید:",
            reply_markup: { inline_keyboard: keyboard }
        });
        return;
    }
    if (data.startsWith("admin_stats_buyers_product_")) {
        const productId = Number(data.replace("admin_stats_buyers_product_", ""));
        const productRows = await sql `SELECT name FROM products WHERE id = ${productId} LIMIT 1;`;
        if (!productRows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "محصول یافت نشد." });
            return;
        }
        const rows = await sql `
      SELECT
        o.telegram_id,
        u.username,
        u.first_name,
        u.last_name,
        COUNT(*)::int AS buy_count
      FROM orders o
      LEFT JOIN users u ON u.telegram_id = o.telegram_id
      WHERE o.product_id = ${productId}
        AND (o.status = 'paid' OR o.status = 'awaiting_config')
      GROUP BY o.telegram_id, u.username, u.first_name, u.last_name
      ORDER BY buy_count DESC, o.telegram_id DESC
      LIMIT 100;
    `;
        if (!rows.length) {
            await tg("sendMessage", {
                chat_id: chatId,
                text: `برای محصول «${productRows[0].name}» هنوز مشتری ثبت نشده است.`,
                reply_markup: { inline_keyboard: [[backButton("admin_stats_buyers")]] }
            });
            return;
        }
        const lines = rows.map((r, idx) => {
            const username = r.username ? `@${String(r.username)}` : "-";
            const fullName = [r.first_name ? String(r.first_name) : "", r.last_name ? String(r.last_name) : ""].filter(Boolean).join(" ").trim() || "-";
            return `${idx + 1}) ID: ${r.telegram_id} | ${username} | ${fullName} | خرید: ${Number(r.buy_count || 0)}`;
        });
        await tg("sendMessage", {
            chat_id: chatId,
            text: `مشتریان محصول: ${productRows[0].name}\n\n${lines.join("\n")}`,
            reply_markup: { inline_keyboard: [[backButton("admin_stats_buyers")]] }
        });
        return;
    }
    if (data === "admin_tools") {
        await tg("sendMessage", {
            chat_id: chatId,
            text: "ابزارهای سریع ادمین:",
            reply_markup: {
                inline_keyboard: [
                    [cb("⛔ بن با یوزرنیم", "admin_tool_ban_username", "danger")],
                    [cb("✉️ ارسال پیام به کاربر", "admin_tool_message_user", "primary")],
                    [cb("🔎 جستجوی شماره سفارش", "admin_tool_lookup_purchase", "primary")],
                    [cb("🧾 جستجوی کانفیگ/UUID", "admin_tool_lookup_config", "primary")],
                    [cb("🛠 ساخت کانفیگ سفارشی", "admin_tool_create_config", "primary")],
                    [cb("🔎 یافتن کانفیگ‌های مرده", "admin_dead_configs", "primary")],
                    [cb("🚫 لیست بن‌شده‌ها", "admin_banned_list_1", "primary")],
                    [cb("🔁 انتقال مستقیم کانفیگ", "admin_tool_direct_migrate", "primary")],
                    [backButton("admin_panel")]
                ]
            }
        });
        return;
    }
    if (data === "admin_tool_ban_username") {
        await setState(userId, "admin_ban_username");
        await tg("sendMessage", { chat_id: chatId, text: "یوزرنیم را با یا بدون @ ارسال کنید." });
        return;
    }
    if (data === "admin_tool_message_user") {
        await startMessageUserWizard(chatId, userId);
        return;
    }
    if (data === "admin_message_user_wizard_cancel") {
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: "ارسال پیام لغو شد." });
        return;
    }
    if (data === "admin_tool_lookup_purchase") {
        await setState(userId, "admin_lookup_purchase");
        await tg("sendMessage", { chat_id: chatId, text: "شماره سفارش را ارسال کنید. مثال: P123... یا T123..." });
        return;
    }
    if (data === "admin_tool_lookup_config") {
        await setState(userId, "admin_lookup_config");
        await tg("sendMessage", {
            chat_id: chatId,
            text: "کانفیگ کامل، UUID، نام کاربر (تلگرام یا پنل) یا نام محصول را ارسال کنید.\n" +
                "بعد از پیدا شدن نتیجه می‌توانید از همان پیام:\n" +
                "➕ افزودن دیتا | ♻️ ریست دیتا | 📅 تنظیم/حذف انقضا | 🚫 لغو دسترسی | 🗑 حذف کامل"
        });
        return;
    }
    if (data === "admin_tool_create_config") {
        await startAdminConfigBuilderWizard(chatId, userId);
        return;
    }
    if (data === "admin_config_builder_cancel") {
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: "ساخت کانفیگ سفارشی لغو شد." });
        return;
    }
    if (data.startsWith("admin_config_builder_panel_")) {
        const panelId = Number(data.replace("admin_config_builder_panel_", ""));
        const state = await getState(userId);
        if (!state || state.state !== "admin_config_builder_wizard")
            return;
        if (!Number.isFinite(panelId) || panelId <= 0) {
            await tg("sendMessage", { chat_id: chatId, text: "پنل نامعتبر است." });
            return;
        }
        const payload = {
            ...state.payload,
            panelId,
            step: "name"
        };
        await setState(userId, "admin_config_builder_wizard", payload);
        await tg("sendMessage", {
            chat_id: chatId,
            text: "ساخت کانفیگ سفارشی - مرحله 3 از 5\nنام کانفیگ را بفرستید. (اختیاری)\nبرای ردشدن: -",
            reply_markup: { inline_keyboard: [[cancelButton("admin_config_builder_cancel")]] }
        });
        return;
    }
    if (data.startsWith("admin_open_purchase_")) {
        const purchaseId = data.replace("admin_open_purchase_", "").trim();
        if (!purchaseId) {
            await tg("sendMessage", { chat_id: chatId, text: "شماره سفارش نامعتبر است." });
            return;
        }
        await sendPurchaseLookupResult(chatId, purchaseId);
        return;
    }
    if (data.startsWith("admin_banned_list_")) {
        const page = Math.max(1, Math.round(Number(data.replace("admin_banned_list_", "")) || 1));
        const pageSize = 20;
        const offset = (page - 1) * pageSize;
        const rows = await sql `
      SELECT b.telegram_id, b.reason, b.banned_by, b.created_at, u.username, u.first_name, u.last_name
      FROM banned_users b
      LEFT JOIN users u ON u.telegram_id = b.telegram_id
      ORDER BY b.created_at DESC
      OFFSET ${offset}
      LIMIT ${pageSize};
    `;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "لیست بن‌شده‌ها خالی است.", reply_markup: { inline_keyboard: [[backButton("admin_tools")]] } });
            return;
        }
        const lines = rows.map((r) => {
            const uname = r.username ? `@${String(r.username)}` : "-";
            const fullName = [r.first_name ? String(r.first_name) : "", r.last_name ? String(r.last_name) : ""].filter(Boolean).join(" ").trim() || "-";
            return `${r.telegram_id} | ${uname} | ${fullName} | reason:${String(r.reason || "-")}`;
        });
        const keyboard = [];
        keyboard.push([
            cb("⬅️ قبلی", `admin_banned_list_${Math.max(1, page - 1)}`, "primary"),
            cb("بعدی ➡️", `admin_banned_list_${page + 1}`, "primary")
        ]);
        keyboard.push([cb("🔓 آنبن کاربر", "admin_unban_prompt", "success")]);
        keyboard.push([backButton("admin_tools")]);
        await tg("sendMessage", { chat_id: chatId, text: `لیست بن‌شده‌ها (صفحه ${page})\n\n${lines.join("\n")}`, reply_markup: { inline_keyboard: keyboard } });
        return;
    }
    if (data === "admin_unban_prompt") {
        await setState(userId, "admin_unban_user");
        await tg("sendMessage", { chat_id: chatId, text: "telegram_id کاربر را برای آنبن ارسال کنید." });
        return;
    }
    if (data === "admin_tool_direct_migrate") {
        await startDirectMigrateWizard(chatId, userId);
        return;
    }
    if (data === "admin_direct_migrate_wizard_cancel") {
        await clearState(userId);
        await tg("sendMessage", { chat_id: chatId, text: "انتقال مستقیم لغو شد." });
        return;
    }
    if (data.startsWith("admin_direct_migrate_panel_")) {
        const targetPanelId = Number(data.replace("admin_direct_migrate_panel_", ""));
        const state = await getState(userId);
        if (!state || state.state !== "admin_direct_migrate_wizard")
            return;
        const payload = { ...state.payload, targetPanelId, step: "user_telegram_id" };
        await setState(userId, "admin_direct_migrate_wizard", payload);
        await tg("sendMessage", {
            chat_id: chatId,
            text: "انتقال مستقیم - مرحله 3 از 4\ntelegram id کاربر مقصد را بفرستید.",
            reply_markup: { inline_keyboard: [[cancelButton("admin_direct_migrate_wizard_cancel")]] }
        });
        return;
    }
    if (data === "admin_settings") {
        const support = await getSetting("support_username");
        const wallet = (await getSetting("business_wallet_address")) || env.BUSINESS_WALLET_ADDRESS || "تنظیم نشده";
        const infiniteMode = await getBoolSetting("global_infinite_mode", false);
        const topupPricePerGb = normalizePricePerGb(await getSetting("topup_price_per_gb_toman"));
        const productPricePerGb = normalizePricePerGb(await getSetting("product_price_per_gb_toman"), topupPricePerGb);
        const customExtraDayPrice = Math.max(0, Math.round((await getNumberSetting("custom_v2ray_extra_day_toman")) || 0));
        const publicBaseUrl = await getPublicBaseUrl(env.PUBLIC_BASE_URL);
        const tronadoKeyMasked = maskSecret((await getSetting("tronado_api_key")) || "");
        const tetrapayKeyMasked = maskSecret((await getSetting("tetrapay_api_key")) || "");
        const plisioKeyMasked = maskSecret((await getSetting("plisio_api_key")) || "");
        const plisioAutoRate = await getBoolSetting("plisio_auto_rate", true);
        const plisioExtra = (await getSetting("plisio_usdt_extra_toman")) || "0";
        const plisioFallback = (await getSetting("plisio_usdt_rate_fallback_toman")) || (await getSetting("plisio_usd_rate_toman")) || "";
        const startMediaKind = ((await getSetting("start_media_kind")) || "none");
        const startMediaValue = (await getSetting("start_media_value")) || "";
        await tg("sendMessage", {
            chat_id: chatId,
            text: `تنظیمات فعلی:\n` +
                `پشتیبانی: ${support ? `@${support}` : "تنظیم نشده"}\n` +
                `کیف پول مقصد: ${wallet}\n` +
                `آدرس سایت (Callback Base): ${publicBaseUrl || "تنظیم نشده"}\n` +
                `کلید Tronado: ${tronadoKeyMasked}\n` +
                `کلید TetraPay: ${tetrapayKeyMasked}\n` +
                `کلید Plisio: ${plisioKeyMasked}\n` +
                `نرخ Plisio: ${plisioAutoRate ? "خودکار (USDT)" : "دستی"}\n` +
                `حاشیه تومان/USDT: ${plisioExtra}\n` +
                `${plisioFallback ? `نرخ دستی (fallback): ${plisioFallback}\n` : ""}` +
                `مدیای شروع: ${startMediaTitle(startMediaKind, startMediaValue)}\n` +
                `بینهایت سراسری: ${infiniteMode ? "روشن" : "خاموش"}\n` +
                `قیمت افزایش هر 1GB: ${formatPriceToman(topupPricePerGb)} تومان\n` +
                `قیمت پیشفرض هر 1GB محصول: ${formatPriceToman(productPricePerGb)} تومان\n` +
                `قیمت هر روز (سفارشی): ${formatPriceToman(customExtraDayPrice)} تومان`,
            reply_markup: {
                inline_keyboard: [
                    [cb("📢 کانال‌های اجباری", "admin_set_mandatory_channels", "primary")],
                    [cb("🆘 یوزرنیم پشتیبانی", "admin_set_support", "primary")],
                    [cb("👛 کیف پول مقصد", "admin_set_wallet", "primary")],
                    [cb("🔑 تنظیمات درگاه‌ها", "admin_gateway_settings", "primary")],
                    [cb("🎬 مدیای شروع", "admin_start_media", "primary")],
                    [cb("📈 قیمت افزایش هر 1GB", "admin_set_topup_price", "primary")],
                    [cb("🏷 قیمت پیشفرض هر 1GB محصول", "admin_set_product_price", "primary")],
                    [cb("🎛 محصول سفارشی", "admin_custom_v2ray_menu", "primary")],
                    [
                        cb(infiniteMode ? "♾️ خاموش‌کردن حالت بینهایت" : "♾️ روشن‌کردن حالت بینهایت", "admin_toggle_global_infinite", infiniteMode ? "danger" : "success")
                    ],
                    [backButton("admin_panel")]
                ]
            }
        });
        return;
    }
    if (data === "admin_start_media") {
        const kindRaw = ((await getSetting("start_media_kind")) || "none");
        const value = (await getSetting("start_media_value")) || "";
        const kind = ["none", "text", "sticker", "animation", "photo"].includes(kindRaw)
            ? kindRaw
            : "none";
        await tg("sendMessage", {
            chat_id: chatId,
            text: `🎬 مدیای شروع\n\n` +
                `وضعیت فعلی: ${startMediaTitle(kind, value)}\n\n` +
                `نکته: این مدیا فقط هنگام /start قبل از منوی اصلی ارسال می‌شود.`,
            reply_markup: {
                inline_keyboard: [
                    [{ text: "🙂 ایموجی/متن", callback_data: "admin_start_media_set_text" }],
                    [{ text: "🧩 استیکر", callback_data: "admin_start_media_set_sticker" }],
                    [{ text: "🎞 گیف", callback_data: "admin_start_media_set_animation" }],
                    [{ text: "🖼 عکس", callback_data: "admin_start_media_set_photo" }],
                    [{ text: "🚫 خاموش", callback_data: "admin_start_media_disable" }],
                    [{ text: "🔙 بازگشت", callback_data: "admin_settings" }]
                ]
            }
        });
        return;
    }
    if (data === "admin_custom_v2ray_menu") {
        const enabled = await getBoolSetting("custom_v2ray_enabled", false);
        const dayPrice = Math.max(0, Math.round((await getNumberSetting("custom_v2ray_extra_day_toman")) || 0));
        const pricePerGb = normalizePricePerGb(await getSetting("product_price_per_gb_toman"), normalizePricePerGb(await getSetting("topup_price_per_gb_toman")));
        const minPrice = Math.max(1, pricePerGb + 30 * dayPrice);
        let productId = Number((await getSetting("custom_v2ray_product_id")) || 0);
        if (enabled && (!Number.isFinite(productId) || productId <= 0)) {
            const ensured = await ensureCustomV2rayProduct();
            if (ensured.ok)
                productId = ensured.productId;
        }
        const productRows = productId ? await sql `SELECT name, sell_mode, is_active FROM products WHERE id = ${productId} LIMIT 1;` : [];
        const productName = productRows.length ? String(productRows[0].name || "-") : "-";
        const sellMode = productRows.length ? parseSellMode(String(productRows[0].sell_mode || "")) : "manual";
        const isActive = productRows.length ? Boolean(productRows[0].is_active) : false;
        const keyboard = [];
        keyboard.push([cb(enabled ? "🚫 خاموش‌کردن سفارشی" : "✅ روشن‌کردن سفارشی", "admin_custom_v2ray_toggle", enabled ? "danger" : "success")]);
        keyboard.push([cb("📅 قیمت هر روز (سفارشی)", "admin_set_custom_v2ray_extra_day", "primary")]);
        if (productId) {
            keyboard.push([cb("✏️ ویرایش محصول سفارشی", `admin_edit_product_${productId}`, "primary")]);
            keyboard.push([cb(sellMode === "panel" ? "⚙️ حالت فروش: پنل" : "⚙️ حالت فروش: دستی", `admin_toggle_product_sell_mode_${productId}`, "primary")]);
            keyboard.push([cb("🧩 تنظیم فروش پنل", `admin_configure_product_panel_${productId}`, "primary")]);
        }
        keyboard.push([backButton("admin_settings")]);
        await tg("sendMessage", {
            chat_id: chatId,
            text: `🎛 محصول سفارشی\n\n` +
                `وضعیت: ${enabled ? "روشن ✅" : "خاموش 🚫"}\n` +
                `محصول: ${productId ? `${productName} (#${productId})${!isActive ? " (مخفی)" : ""}` : "ساخته نشده"}\n` +
                `شروع خرید: 1GB / 30 روز\n` +
                `قیمت هر 1GB: ${formatPriceToman(pricePerGb)} تومان\n` +
                `قیمت هر روز: ${formatPriceToman(dayPrice)} تومان\n` +
                `حداقل مبلغ شروع: ${formatPriceToman(minPrice)} تومان\n\n` +
                `نکته: نوع تحویل (فروش از پنل یا دستی) از طریق ویرایش همین محصول تعیین می‌شود.`,
            reply_markup: { inline_keyboard: keyboard }
        });
        return;
    }
    if (data === "admin_custom_v2ray_toggle") {
        const current = await getBoolSetting("custom_v2ray_enabled", false);
        if (!current) {
            const ensured = await ensureCustomV2rayProduct();
            if (!ensured.ok) {
                await tg("sendMessage", { chat_id: chatId, text: "خطا در ساخت/آماده‌سازی محصول سفارشی." });
                return;
            }
            await sql `UPDATE products SET is_active = TRUE WHERE id = ${ensured.productId};`;
            await setSetting("custom_v2ray_enabled", "true");
            await tg("sendMessage", { chat_id: chatId, text: "سفارشی روشن شد ✅" });
            return;
        }
        const productId = Number((await getSetting("custom_v2ray_product_id")) || 0);
        if (Number.isFinite(productId) && productId > 0) {
            await sql `UPDATE products SET is_active = FALSE WHERE id = ${productId};`;
        }
        await setSetting("custom_v2ray_enabled", "false");
        await tg("sendMessage", { chat_id: chatId, text: "سفارشی خاموش شد ✅" });
        return;
    }
    if (data === "admin_start_media_disable") {
        await setSetting("start_media_kind", "none");
        await setSetting("start_media_value", "");
        await tg("sendMessage", { chat_id: chatId, text: "مدیای شروع خاموش شد ✅" });
        return;
    }
    if (data.startsWith("admin_start_media_set_")) {
        const kind = data.replace("admin_start_media_set_", "").trim();
        if (kind !== "text" && kind !== "sticker" && kind !== "animation" && kind !== "photo") {
            await tg("sendMessage", { chat_id: chatId, text: "گزینه نامعتبر است." });
            return;
        }
        await setState(userId, "admin_set_start_media", { kind });
        const hints = kind === "text"
            ? "متن/ایموجی را ارسال کن.\nبرای پاک‌کردن: -"
            : kind === "sticker"
                ? "استیکر را ارسال کن.\nبرای پاک‌کردن: -"
                : kind === "animation"
                    ? "گیف را ارسال کن.\nبرای پاک‌کردن: -"
                    : "عکس را ارسال کن.\nبرای پاک‌کردن: -";
        await tg("sendMessage", { chat_id: chatId, text: `🎬 تنظیم مدیای شروع\n\n${hints}` });
        return;
    }
    if (data === "admin_set_mandatory_channels") {
        await setState(userId, "admin_set_mandatory_channels");
        const current = await getSetting("mandatory_channels");
        await tg("sendMessage", {
            chat_id: chatId,
            text: `لیست کانال‌های اجباری را ارسال کنید.\n\n` +
                `هر کانال در یک خط یا جدا شده با ویرگول.\n` +
                `مثال:\n<code>@channel1</code>\n<code>@channel2</code>\n\n` +
                `برای غیرفعال کردن: <code>خاموش</code>\n\n` +
                `وضعیت فعلی:\n<code>${escapeHtml(current || "خاموش")}</code>`,
            parse_mode: "HTML"
        });
        return;
    }
    if (data === "admin_set_support") {
        await setState(userId, "admin_set_support");
        await tg("sendMessage", { chat_id: chatId, text: "یوزرنیم پشتیبانی را بدون @ ارسال کنید." });
        return;
    }
    if (data === "admin_set_wallet") {
        await setState(userId, "admin_set_wallet");
        await tg("sendMessage", { chat_id: chatId, text: "آدرس کیف پول مقصد را ارسال کنید." });
        return;
    }
    if (data === "admin_gateway_settings") {
        const publicBaseUrl = await getPublicBaseUrl(env.PUBLIC_BASE_URL);
        const tronadoKeyMasked = maskSecret((await getSetting("tronado_api_key")) || "");
        const tetrapayKeyMasked = maskSecret((await getSetting("tetrapay_api_key")) || "");
        const plisioKeyMasked = maskSecret((await getSetting("plisio_api_key")) || "");
        const plisioAutoRate = await getBoolSetting("plisio_auto_rate", true);
        const plisioExtra = (await getSetting("plisio_usdt_extra_toman")) || "0";
        const plisioFallback = (await getSetting("plisio_usdt_rate_fallback_toman")) || (await getSetting("plisio_usd_rate_toman")) || "";
        await tg("sendMessage", {
            chat_id: chatId,
            text: `تنظیمات درگاه‌ها:\n` +
                `آدرس سایت (Callback Base): ${publicBaseUrl || "تنظیم نشده"}\n` +
                `Tronado: ${tronadoKeyMasked}\n` +
                `TetraPay: ${tetrapayKeyMasked}\n` +
                `Plisio: ${plisioKeyMasked}\n` +
                `نرخ Plisio: ${plisioAutoRate ? "خودکار (IRR→USDT)" : "دستی"}\n` +
                `حاشیه تومان/USDT: ${plisioExtra}\n` +
                `${plisioFallback ? `نرخ دستی (fallback): ${plisioFallback}\n` : ""}\n` +
                `برای پاک‌کردن هر مورد: -`,
            reply_markup: {
                inline_keyboard: [
                    [cb("🌐 آدرس سایت", "admin_set_public_base_url", "primary")],
                    [cb("🔑 کلید Tronado", "admin_set_tronado_api_key", "primary")],
                    [cb("🔑 کلید TetraPay", "admin_set_tetrapay_api_key", "primary")],
                    [cb("🔑 کلید Plisio", "admin_set_plisio_api_key", "primary")],
                    [cb("🪙 کیف پول‌های کریپتو", "admin_crypto_wallets", "primary")],
                    [cb(plisioAutoRate ? "✅ نرخ خودکار Plisio" : "❌ نرخ خودکار Plisio", "admin_toggle_plisio_auto_rate", plisioAutoRate ? "success" : "danger")],
                    [cb("➕ حاشیه تومان/USDT", "admin_set_plisio_extra_toman", "primary")],
                    [cb("🛟 نرخ دستی (fallback)", "admin_set_plisio_fallback_rate", "primary")],
                    [backButton("admin_settings")]
                ]
            }
        });
        return;
    }
    if (data === "admin_crypto_wallets") {
        const wallets = await sql `
      SELECT id, currency, network, address, rate_mode, rate_toman_per_unit, extra_toman_per_unit, active
      FROM crypto_wallets
      ORDER BY currency ASC, network ASC, id ASC;
    `;
        const lines = wallets.map((w) => {
            const row = w;
            const status = cryptoWalletReady(row) ? "✅" : row.active ? "⚠️" : "⛔️";
            const rate = row.rate_mode === "auto"
                ? "خودکار"
                : row.rate_toman_per_unit
                    ? `${formatPriceToman(Number(row.rate_toman_per_unit))} / 1`
                    : "-";
            const extra = Number(row.extra_toman_per_unit || 0);
            const extraText = extra ? ` +${formatPriceToman(extra)}` : "";
            return `${status} ${cryptoWalletTitle(row)} | آدرس: ${shortAddr(row.address)} | نرخ: ${rate}${extraText}`;
        });
        await tg("sendMessage", {
            chat_id: chatId,
            text: `کیف پول‌های کریپتو:\n\n${lines.length ? lines.join("\n") : "هیچ موردی ثبت نشده است."}`,
            reply_markup: {
                inline_keyboard: [
                    [cb("➕ افزودن کیف پول", "admin_crypto_wallet_add", "success")],
                    ...wallets.slice(0, 12).map((w) => {
                        const id = Number(w.id);
                        return [cb(`⚙️ ${String(w.currency)} (${String(w.network)})`, `admin_crypto_wallet_edit_${id}`, "primary")];
                    }),
                    [backButton("admin_gateway_settings")]
                ]
            }
        });
        return;
    }
    if (data === "admin_crypto_wallet_add") {
        await tg("sendMessage", {
            chat_id: chatId,
            text: "کدام کیف پول را می‌خواهید اضافه کنید؟",
            reply_markup: {
                inline_keyboard: [
                    [cb("TRX (TRON)", "admin_crypto_wallet_add_trx_tron", "primary")],
                    [cb("TON (TON)", "admin_crypto_wallet_add_ton_ton", "primary")],
                    [cb("USDT (TRC20)", "admin_crypto_wallet_add_usdt_trc20", "primary")],
                    [cb("USDT (ERC20)", "admin_crypto_wallet_add_usdt_erc20", "primary")],
                    [cb("سایر", "admin_crypto_wallet_add_other", "primary")],
                    [backButton("admin_crypto_wallets")]
                ]
            }
        });
        return;
    }
    if (data.startsWith("admin_crypto_wallet_add_") && data !== "admin_crypto_wallet_add_other") {
        const payload = data.replace("admin_crypto_wallet_add_", "");
        const parts = payload.split("_");
        const currency = (parts[0] || "").toUpperCase();
        const network = (parts[1] || "").toUpperCase();
        const inserted = await sql `
      INSERT INTO crypto_wallets (currency, network, active)
      VALUES (${currency}, ${network}, FALSE)
      ON CONFLICT (currency, network) DO UPDATE SET currency = EXCLUDED.currency
      RETURNING id;
    `;
        const walletId = Number(inserted[0].id);
        await setState(userId, "admin_crypto_wallet_set_address", { walletId });
        await tg("sendMessage", { chat_id: chatId, text: `آدرس کیف پول ${currency} (${network}) را ارسال کنید.\nبرای پاک‌کردن: -` });
        return;
    }
    if (data === "admin_crypto_wallet_add_other") {
        await setState(userId, "admin_crypto_wallet_add_other_currency");
        await tg("sendMessage", { chat_id: chatId, text: "نام ارز را ارسال کنید (مثال: BTC یا LTC):" });
        return;
    }
    if (data.startsWith("admin_crypto_wallet_edit_")) {
        const walletId = Number(data.replace("admin_crypto_wallet_edit_", ""));
        const rows = await sql `
      SELECT id, currency, network, address, rate_mode, rate_toman_per_unit, extra_toman_per_unit, active
      FROM crypto_wallets
      WHERE id = ${walletId}
      LIMIT 1;
    `;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "کیف پول یافت نشد." });
            return;
        }
        const w = rows[0];
        const rate = w.rate_mode === "auto" ? "خودکار" : w.rate_toman_per_unit ? `${formatPriceToman(Number(w.rate_toman_per_unit))} تومان` : "-";
        await tg("sendMessage", {
            chat_id: chatId,
            text: `تنظیم کیف پول:\n` +
                `${cryptoWalletTitle(w)}\n` +
                `وضعیت: ${w.active ? "فعال" : "غیرفعال"}\n` +
                `آدرس: ${w.address || "-"}\n` +
                `نرخ: ${rate}\n` +
                `حاشیه: ${formatPriceToman(Number(w.extra_toman_per_unit || 0))} تومان`,
            reply_markup: {
                inline_keyboard: [
                    [cb("✍️ تنظیم آدرس", `admin_crypto_wallet_set_address_${walletId}`, "primary")],
                    [cb(w.rate_mode === "auto" ? "✅ نرخ خودکار" : "❌ نرخ خودکار", `admin_crypto_wallet_toggle_auto_${walletId}`, w.rate_mode === "auto" ? "success" : "danger")],
                    [cb("💱 تنظیم نرخ دستی", `admin_crypto_wallet_set_rate_${walletId}`, "primary")],
                    [cb("➕ تنظیم حاشیه تومان", `admin_crypto_wallet_set_extra_${walletId}`, "primary")],
                    [cb(w.active ? "⛔️ غیرفعال" : "✅ فعال", `admin_crypto_wallet_toggle_${walletId}`, w.active ? "danger" : "success")],
                    [cb("🗑 حذف", `admin_crypto_wallet_delete_${walletId}`, "danger")],
                    [backButton("admin_crypto_wallets")]
                ]
            }
        });
        return;
    }
    if (data.startsWith("admin_crypto_wallet_set_address_")) {
        const walletId = Number(data.replace("admin_crypto_wallet_set_address_", ""));
        await setState(userId, "admin_crypto_wallet_set_address", { walletId });
        await tg("sendMessage", { chat_id: chatId, text: "آدرس کیف پول را ارسال کنید.\nبرای پاک‌کردن: -" });
        return;
    }
    if (data.startsWith("admin_crypto_wallet_set_rate_")) {
        const walletId = Number(data.replace("admin_crypto_wallet_set_rate_", ""));
        await setState(userId, "admin_crypto_wallet_set_rate", { walletId });
        await tg("sendMessage", { chat_id: chatId, text: "نرخ 1 واحد را به تومان ارسال کنید (فقط عدد).\nبرای پاک‌کردن: -" });
        return;
    }
    if (data.startsWith("admin_crypto_wallet_set_extra_")) {
        const walletId = Number(data.replace("admin_crypto_wallet_set_extra_", ""));
        await setState(userId, "admin_crypto_wallet_set_extra", { walletId });
        await tg("sendMessage", { chat_id: chatId, text: "حاشیه تومان (برای هر 1 واحد) را ارسال کنید (فقط عدد).\nبرای پاک‌کردن: -" });
        return;
    }
    if (data.startsWith("admin_crypto_wallet_toggle_auto_")) {
        const walletId = Number(data.replace("admin_crypto_wallet_toggle_auto_", ""));
        const rows = await sql `SELECT id, currency, rate_mode FROM crypto_wallets WHERE id = ${walletId} LIMIT 1;`;
        if (!rows.length)
            return;
        const current = String(rows[0].rate_mode || "manual");
        const next = current === "auto" ? "manual" : "auto";
        await sql `UPDATE crypto_wallets SET rate_mode = ${next} WHERE id = ${walletId};`;
        await tg("sendMessage", { chat_id: chatId, text: `نرخ ${next === "auto" ? "خودکار" : "دستی"} تنظیم شد ✅` });
        return;
    }
    if (data.startsWith("admin_crypto_wallet_toggle_")) {
        const walletId = Number(data.replace("admin_crypto_wallet_toggle_", ""));
        const rows = await sql `UPDATE crypto_wallets SET active = NOT active WHERE id = ${walletId} RETURNING active;`;
        if (rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: `وضعیت به ${rows[0].active ? "فعال" : "غیرفعال"} تغییر کرد ✅` });
        }
        return;
    }
    if (data.startsWith("admin_crypto_wallet_delete_")) {
        const walletId = Number(data.replace("admin_crypto_wallet_delete_", ""));
        await sql `DELETE FROM crypto_wallets WHERE id = ${walletId};`;
        await tg("sendMessage", { chat_id: chatId, text: "حذف شد ✅" });
        return;
    }
    if (data === "admin_set_public_base_url") {
        await setState(userId, "admin_set_public_base_url");
        await tg("sendMessage", { chat_id: chatId, text: "آدرس کامل سایت را ارسال کنید. مثال: https://example.com\nبرای پاک‌کردن: -" });
        return;
    }
    if (data === "admin_set_tronado_api_key") {
        await setState(userId, "admin_set_tronado_api_key");
        await tg("sendMessage", { chat_id: chatId, text: "کلید Tronado را ارسال کنید.\nبرای پاک‌کردن: -" });
        return;
    }
    if (data === "admin_set_tetrapay_api_key") {
        await setState(userId, "admin_set_tetrapay_api_key");
        await tg("sendMessage", { chat_id: chatId, text: "کلید TetraPay را ارسال کنید.\nبرای پاک‌کردن: -" });
        return;
    }
    if (data === "admin_set_plisio_api_key") {
        await setState(userId, "admin_set_plisio_api_key");
        await tg("sendMessage", { chat_id: chatId, text: "کلید Plisio را ارسال کنید.\nبرای پاک‌کردن: -" });
        return;
    }
    if (data === "admin_toggle_plisio_auto_rate") {
        const current = await getBoolSetting("plisio_auto_rate", true);
        await setSetting("plisio_auto_rate", (!current).toString());
        await tg("sendMessage", { chat_id: chatId, text: `نرخ خودکار Plisio ${!current ? "فعال" : "غیرفعال"} شد ✅` });
        return;
    }
    if (data === "admin_set_plisio_extra_toman") {
        await setState(userId, "admin_set_plisio_extra_toman");
        await tg("sendMessage", { chat_id: chatId, text: "حاشیه را به تومان (برای هر 1 USDT) ارسال کنید. مثال: 2000\nبرای پاک‌کردن: -" });
        return;
    }
    if (data === "admin_set_plisio_fallback_rate") {
        await setState(userId, "admin_set_plisio_fallback_rate");
        await tg("sendMessage", { chat_id: chatId, text: "نرخ دستی USDT را به تومان ارسال کنید (fallback). مثال: 65000\nبرای پاک‌کردن: -" });
        return;
    }
    if (data === "admin_set_topup_price") {
        await setState(userId, "admin_set_topup_price");
        await tg("sendMessage", { chat_id: chatId, text: "قیمت هر 1GB افزایش دیتا را به تومان ارسال کنید. مثال: 500000" });
        return;
    }
    if (data === "admin_set_product_price") {
        await setState(userId, "admin_set_product_price");
        await tg("sendMessage", { chat_id: chatId, text: "قیمت پیشفرض هر 1GB محصول را به تومان ارسال کنید. مثال: 500000" });
        return;
    }
    if (data === "admin_set_custom_v2ray_extra_day") {
        await setState(userId, "admin_set_custom_v2ray_extra_day");
        await tg("sendMessage", { chat_id: chatId, text: "قیمت هر روز برای محصول سفارشی را به تومان ارسال کنید. مثال: 10000\nبرای خاموش: 0" });
        return;
    }
    if (data === "admin_toggle_global_infinite") {
        const current = await getBoolSetting("global_infinite_mode", false);
        await setSetting("global_infinite_mode", (!current).toString());
        await tg("sendMessage", { chat_id: chatId, text: `حالت بینهایت سراسری ${!current ? "روشن" : "خاموش"} شد ✅` });
        return;
    }
    if (data.startsWith("receipt_accept_")) {
        const orderId = Number(data.replace("receipt_accept_", ""));
        if (await isRateLimited(userId, "receipt_accept", 2000)) {
            await tg("sendMessage", { chat_id: chatId, text: "درخواست شما در حال پردازش است. لطفاً چند لحظه صبر کنید." });
            return;
        }
        const result = await finalizeOrder(orderId, userId);
        await tg("sendMessage", { chat_id: chatId, text: result.ok ? "سفارش تایید شد ✅" : `خطا: ${result.reason}` });
        return;
    }
    if (data.startsWith("receipt_deny_")) {
        const orderId = Number(data.replace("receipt_deny_", ""));
        const rows = await sql `
      UPDATE orders
      SET status = 'denied', admin_decision_by = ${userId}
      WHERE id = ${orderId} AND status != 'denied'
      RETURNING telegram_id, purchase_id, wallet_used;
    `;
        if (rows.length) {
            const order = rows[0];
            const walletUsed = Number(order.wallet_used || 0);
            if (walletUsed > 0) {
                await sql `UPDATE users SET wallet_balance = wallet_balance + ${walletUsed} WHERE telegram_id = ${order.telegram_id};`;
                await sql `INSERT INTO wallet_transactions (telegram_id, amount, type, description, created_at) VALUES (${order.telegram_id}, ${walletUsed}, 'refund', ${`برگشت وجه به دلیل رد رسید سفارش ${order.purchase_id}`}, NOW());`;
            }
            await tg("sendMessage", { chat_id: Number(order.telegram_id), text: `رسید سفارش ${order.purchase_id} رد شد ❌` });
        }
        await tg("sendMessage", { chat_id: chatId, text: rows.length ? "رد شد ✅" : "سفارش یافت نشد یا قبلاً بررسی شده." });
        return;
    }
    if (data.startsWith("crypto_accept_")) {
        const orderId = Number(data.replace("crypto_accept_", ""));
        if (await isRateLimited(userId, "crypto_accept", 2000)) {
            await tg("sendMessage", { chat_id: chatId, text: "درخواست شما در حال پردازش است. لطفاً چند لحظه صبر کنید." });
            return;
        }
        const result = await finalizeOrder(orderId, userId);
        await tg("sendMessage", { chat_id: chatId, text: result.ok ? "سفارش تایید شد ✅" : `خطا: ${result.reason}` });
        return;
    }
    if (data.startsWith("crypto_deny_")) {
        const orderId = Number(data.replace("crypto_deny_", ""));
        const rows = await sql `
      UPDATE orders
      SET status = 'denied', admin_decision_by = ${userId}
      WHERE id = ${orderId} AND status != 'denied'
      RETURNING telegram_id, purchase_id, wallet_used;
    `;
        if (rows.length) {
            const order = rows[0];
            const walletUsed = Number(order.wallet_used || 0);
            if (walletUsed > 0) {
                await sql `UPDATE users SET wallet_balance = wallet_balance + ${walletUsed} WHERE telegram_id = ${order.telegram_id};`;
                await sql `INSERT INTO wallet_transactions (telegram_id, amount, type, description, created_at) VALUES (${order.telegram_id}, ${walletUsed}, 'refund', ${`برگشت وجه به دلیل رد پرداخت کریپتو سفارش ${order.purchase_id}`}, NOW());`;
            }
            await tg("sendMessage", { chat_id: Number(order.telegram_id), text: `پرداخت کریپتو سفارش ${order.purchase_id} رد شد ❌` });
        }
        await tg("sendMessage", { chat_id: chatId, text: rows.length ? "رد شد ✅" : "سفارش یافت نشد یا قبلاً بررسی شده." });
        return;
    }
    if (data.startsWith("receipt_ban_")) {
        const payload = data.replace("receipt_ban_", "");
        const [orderIdRaw, targetUserRaw] = payload.split("_");
        const orderId = Number(orderIdRaw);
        const targetUser = Number(targetUserRaw);
        await sql `
      INSERT INTO banned_users (telegram_id, reason, banned_by)
      VALUES (${targetUser}, 'fake_receipt', ${userId})
      ON CONFLICT (telegram_id) DO UPDATE SET reason = EXCLUDED.reason, banned_by = EXCLUDED.banned_by;
    `;
        const rows = await sql `
      UPDATE orders
      SET status = 'denied', admin_decision_by = ${userId}
      WHERE id = ${orderId} AND status != 'denied'
      RETURNING purchase_id, wallet_used;
    `;
        if (rows.length) {
            const order = rows[0];
            const walletUsed = Number(order.wallet_used || 0);
            if (walletUsed > 0) {
                await sql `UPDATE users SET wallet_balance = wallet_balance + ${walletUsed} WHERE telegram_id = ${targetUser};`;
                await sql `INSERT INTO wallet_transactions (telegram_id, amount, type, description, created_at) VALUES (${targetUser}, ${walletUsed}, 'refund', ${`برگشت وجه سفارش ${order.purchase_id}`}, NOW());`;
            }
        }
        try {
            await tg("sendMessage", { chat_id: targetUser, text: "به دلیل ارسال رسید نامعتبر، دسترسی شما مسدود شد." });
        }
        catch (error) {
            logError("ban_user_notify_failed", error, { targetUserId: targetUser, by: userId, mode: "receipt" });
        }
        await tg("sendMessage", { chat_id: chatId, text: rows.length ? "کاربر بن شد ✅" : "بن شد ✅" });
        return;
    }
    if (data.startsWith("crypto_accept_")) {
        const orderId = Number(data.replace("crypto_accept_", ""));
        if (await isRateLimited(userId, "crypto_accept", 2000)) {
            await tg("sendMessage", { chat_id: chatId, text: "درخواست شما در حال پردازش است. لطفاً چند لحظه صبر کنید." });
            return;
        }
        const result = await finalizeOrder(orderId, userId);
        await tg("sendMessage", { chat_id: chatId, text: result.ok ? "سفارش تایید شد ✅" : `خطا: ${result.reason}` });
        return;
    }
    if (data.startsWith("crypto_deny_")) {
        const orderId = Number(data.replace("crypto_deny_", ""));
        const rows = await sql `
      UPDATE orders
      SET status = 'denied', admin_decision_by = ${userId}
      WHERE id = ${orderId}
        AND status = 'receipt_submitted'
        AND payment_method IN ('tronado', 'plisio', 'tetrapay')
      RETURNING telegram_id, purchase_id;
    `;
        if (rows.length) {
            await tg("sendMessage", { chat_id: Number(rows[0].telegram_id), text: `رسید پرداخت سفارش ${rows[0].purchase_id} رد شد ❌` }).catch(() => { });
        }
        await tg("sendMessage", { chat_id: chatId, text: rows.length ? "رد شد ✅" : "سفارش یافت نشد یا قابل رد نیست." });
        return;
    }
    if (data.startsWith("crypto_ban_")) {
        const payload = data.replace("crypto_ban_", "");
        const [orderIdRaw, targetUserRaw] = payload.split("_");
        const orderId = Number(orderIdRaw);
        const targetUser = Number(targetUserRaw);
        await sql `
      INSERT INTO banned_users (telegram_id, reason, banned_by)
      VALUES (${targetUser}, 'fake_crypto_receipt', ${userId})
      ON CONFLICT (telegram_id) DO UPDATE SET reason = EXCLUDED.reason, banned_by = EXCLUDED.banned_by;
    `;
        const rows = await sql `
      UPDATE orders
      SET status = 'denied', admin_decision_by = ${userId}
      WHERE id = ${orderId}
        AND status = 'receipt_submitted'
        AND payment_method IN ('tronado', 'plisio', 'tetrapay')
      RETURNING purchase_id;
    `;
        await tg("sendMessage", { chat_id: targetUser, text: "به دلیل ارسال رسید نامعتبر، دسترسی شما مسدود شد." }).catch(() => { });
        await tg("sendMessage", { chat_id: chatId, text: rows.length ? "کاربر بن شد ✅" : "بن شد ✅" });
        return;
    }
    if (data.startsWith("admin_provide_config_")) {
        const orderId = Number(data.replace("admin_provide_config_", ""));
        await setState(userId, "admin_provide_config", { orderId });
        await tg("sendMessage", { chat_id: chatId, text: "کانفیگ آماده را ارسال کنید تا برای کاربر تحویل شود." });
        return;
    }
    if (data.startsWith("topup_accept_")) {
        const id = Number(data.replace("topup_accept_", ""));
        const rows = await sql `
      UPDATE topup_requests
      SET status = 'paid'
      WHERE id = ${id} AND status = 'receipt_submitted'
      RETURNING telegram_id, purchase_id;
    `;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "این درخواست قابل تایید نیست یا قبلاً بررسی شده است." });
            return;
        }
        await tg("sendMessage", {
            chat_id: Number(rows[0].telegram_id),
            text: `رسید سفارش افزایش دیتا ${rows[0].purchase_id} تایید شد ✅\nادمین به‌زودی افزایش را اعمال می‌کند.`
        });
        const auto = await tryAutoApplyPanelTopup(id, userId);
        if (auto.ok) {
            await tg("sendMessage", { chat_id: chatId, text: `رسید تایید شد و افزایش دیتا خودکار اعمال شد ✅\n${auto.message}` });
            return;
        }
        logInfo("topup_auto_apply_skipped", { topupRequestId: id, reason: auto.message });
        await notifyAdmins(`✅ رسید افزایش دیتا تایید شد: ${rows[0].purchase_id}`, {
            inline_keyboard: [[confirmButton(`done_topup_${id}`, "✅ انجام شد")]]
        });
        await tg("sendMessage", { chat_id: chatId, text: `رسید تایید شد ✅\nاعمال خودکار انجام نشد: ${auto.message}` });
        return;
    }
    if (data.startsWith("topup_deny_")) {
        const id = Number(data.replace("topup_deny_", ""));
        const rows = await sql `
      UPDATE topup_requests
      SET status = 'denied'
      WHERE id = ${id} AND status = 'receipt_submitted'
      RETURNING telegram_id, purchase_id;
    `;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "این درخواست قابل رد نیست یا قبلاً بررسی شده است." });
            return;
        }
        await tg("sendMessage", { chat_id: Number(rows[0].telegram_id), text: `رسید سفارش ${rows[0].purchase_id} رد شد ❌` });
        await tg("sendMessage", { chat_id: chatId, text: "رد شد ✅" });
        return;
    }
    if (data.startsWith("topup_ban_")) {
        const payload = data.replace("topup_ban_", "");
        const [idRaw, targetUserRaw] = payload.split("_");
        const id = Number(idRaw);
        const targetUser = Number(targetUserRaw);
        await sql `
      INSERT INTO banned_users (telegram_id, reason, banned_by)
      VALUES (${targetUser}, 'fake_topup_receipt', ${userId})
      ON CONFLICT (telegram_id) DO UPDATE SET reason = EXCLUDED.reason, banned_by = EXCLUDED.banned_by;
    `;
        await sql `
      UPDATE topup_requests
      SET status = 'denied'
      WHERE id = ${id} AND status = 'receipt_submitted';
    `;
        try {
            await tg("sendMessage", { chat_id: targetUser, text: "به دلیل ارسال رسید نامعتبر، دسترسی شما مسدود شد." });
        }
        catch (error) {
            logError("ban_user_notify_failed", error, { targetUserId: targetUser, by: userId, mode: "topup_receipt" });
        }
        await tg("sendMessage", { chat_id: chatId, text: "کاربر بن شد ✅" });
        return;
    }
    if (data.startsWith("done_topup_")) {
        const id = Number(data.replace("done_topup_", ""));
        const rows = await sql `
      UPDATE topup_requests
      SET status = 'done', done_at = NOW(), done_by = ${userId}
      WHERE id = ${id} AND status = 'paid'
      RETURNING telegram_id, inventory_id, requested_mb, purchase_id;
    `;
        if (!rows.length) {
            await tg("sendMessage", { chat_id: chatId, text: "این درخواست قبلا بسته شده یا یافت نشد." });
            return;
        }
        const cfg = await sql `SELECT config_value FROM inventory WHERE id = ${rows[0].inventory_id} LIMIT 1;`;
        await tg("sendMessage", {
            chat_id: Number(rows[0].telegram_id),
            text: `درخواست افزایش ${rows[0].requested_mb}MB شما انجام شد ✅\n` +
                `شماره سفارش: ${rows[0].purchase_id}\n` +
                `کانفیگ:\n${String(cfg[0]?.config_value || "-")}`
        });
        await tg("sendMessage", { chat_id: chatId, text: "درخواست به حالت Done رفت ✅" });
        return;
    }
}
async function checkMandatoryChannels(userId, chatId, silent = false) {
    if (isAdmin(userId))
        return true;
    const channelsRaw = await getSetting("mandatory_channels");
    if (!channelsRaw)
        return true;
    const channels = channelsRaw.split(",").map(c => c.trim()).filter(Boolean);
    if (channels.length === 0)
        return true;
    const notJoined = [];
    for (const channelItem of channels) {
        let channelId = channelItem;
        let url = "";
        let name = channelItem;
        if (channelItem.includes("|")) {
            const parts = channelItem.split("|");
            channelId = parts[0];
            url = parts[1];
            name = parts[2] || parts[0];
        }
        else if (channelItem.startsWith("@")) {
            url = `https://t.me/${channelItem.replace("@", "")}`;
        }
        else {
            url = "https://t.me/";
        }
        try {
            const result = await tg("getChatMember", { chat_id: channelId, user_id: userId });
            if (!['creator', 'administrator', 'member', 'restricted'].includes(result.status)) {
                notJoined.push({ id: channelId, name: name, url });
            }
        }
        catch (error) {
            notJoined.push({ id: channelId, name: name, url });
            logError("check_channel_membership_failed", error, { channel: channelId, userId });
        }
    }
    if (notJoined.length > 0) {
        if (!silent) {
            const buttons = notJoined.map(c => {
                return [{ text: `عضویت در ${c.name}`, url: c.url }];
            });
            buttons.push([cb("✅ بررسی عضویت", "check_membership", "success")]);
            await tg("sendMessage", {
                chat_id: chatId,
                text: "برای استفاده از ربات، اول باید در کانال‌های زیر عضو بشی.\nبعد از عضویت، روی «بررسی عضویت» بزن.",
                reply_markup: { inline_keyboard: buttons }
            });
        }
        return false;
    }
    return true;
}
async function handleMessage(update) {
    if (!update?.from)
        return;
    const text = (update.text ?? update.caption ?? "").trim();
    const photoFileId = update.photo?.length ? update.photo[update.photo.length - 1].file_id : null;
    const stickerFileId = update.sticker?.file_id || null;
    const animationFileId = update.animation?.file_id || null;
    const chatId = update.chat.id;
    const userId = update.from.id;
    await upsertUser(update.from);
    if (await isBanned(userId)) {
        await tg("sendMessage", { chat_id: chatId, text: "دسترسی شما به دلیل تخلف مسدود شده است." });
        return;
    }
    if (!(await checkMandatoryChannels(userId, chatId))) {
        return;
    }
    if (text === "/start") {
        await clearState(userId);
        await sendStartMedia(chatId);
        await sendMainMenu(chatId, userId);
        return;
    }
    if (text === "/admin" && isAdmin(userId)) {
        await sendAdminPanel(chatId);
        return;
    }
    if (text === "/help") {
        if (isAdmin(userId)) {
            await adminHelp(chatId);
        }
        return;
    }
    const state = await getState(userId);
    if (text === "/cancel") {
        if (state) {
            await clearState(userId);
            await sendMainMenu(chatId, userId, "عملیات جاری لغو شد.");
            return;
        }
        await sendMainMenu(chatId, userId, "هیچ عملیات فعالی برای لغو وجود ندارد.");
        return;
    }
    if (state) {
        const consumed = await parseAndApplyState(chatId, userId, text, photoFileId, stickerFileId, animationFileId, state);
        if (consumed)
            return;
    }
    await sendMainMenu(chatId, userId, "دستور نامعتبر بود. از منوی زیر استفاده کنید:");
}
export async function handleTelegramUpdate(update) {
    await ensureSchema();
    if (update.update_id) {
        const inserted = await sql `
      INSERT INTO processed_updates (update_id)
      VALUES (${update.update_id})
      ON CONFLICT (update_id) DO NOTHING
      RETURNING update_id;
    `;
        if (!inserted.length) {
            logInfo("duplicate_update_ignored", { updateId: update.update_id });
            return;
        }
        // Prune old updates asynchronously without awaiting
        sql `DELETE FROM processed_updates WHERE created_at < NOW() - INTERVAL '1 day'`.catch(() => { });
    }
    if (update.callback_query) {
        await handleCallback(update.callback_query);
        return;
    }
    if (update.message) {
        await handleMessage(update.message);
    }
}
