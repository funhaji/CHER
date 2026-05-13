import type { VercelRequest, VercelResponse } from "@vercel/node";
import { ensureSchema, sql } from "../lib/db.js";
import {
  verifyAdminToken,
  applyAdminSetDataLimitOnMarzban,
  applyAdminSetDataLimitOnSanaei,
  applyAdminSetExpiryOnMarzban,
  applyAdminSetExpiryOnSanaei,
  applyAdminResetUsageOnMarzban,
  applyAdminResetUsageOnSanaei,
  deleteMarzbanUser,
  revokeSanaeiClient,
  parseDeliveryPayload
} from "../lib/bot.js";

export default async function handler(req: VercelRequest, res: VercelResponse) {
  try {
    await ensureSchema();
    const token = typeof req.body?.token === "string" ? req.body.token : "";
    const adminId = await verifyAdminToken(token);
    if (!adminId) {
      return res.status(401).json({ ok: false, error: "Unauthorized" });
    }

    const { action, inventoryId, value } = req.body;
    if (!action || !inventoryId) {
      return res.status(400).json({ ok: false, error: "Missing required fields" });
    }

    const rows = await sql`
      SELECT i.id, i.panel_id, i.delivery_payload, p.panel_config
      FROM inventory i
      JOIN products p ON p.id = i.product_id
      WHERE i.id = ${Number(inventoryId)}
      LIMIT 1;
    `;
    if (!rows.length) {
      return res.status(404).json({ ok: false, error: "Inventory not found" });
    }

    const row = rows[0];
    const delivery = parseDeliveryPayload(row.delivery_payload);
    const panelType = String(delivery.metadata?.panelType || "");
    const panelId = Number(row.panel_id || 0);

    if (!panelId || !panelType) {
      return res.status(400).json({ ok: false, error: "Not a panel config" });
    }

    const panelRows = await sql`
      SELECT id, panel_type, base_url, username, password
      FROM panels
      WHERE id = ${panelId}
      LIMIT 1;
    `;
    if (!panelRows.length) {
      return res.status(404).json({ ok: false, error: "Panel not found" });
    }
    const panel = panelRows[0];

    const username = String(delivery.metadata?.username || "").trim();
    const inboundId = Number(delivery.metadata?.inboundId);
    const email = String(delivery.metadata?.email || "").trim();
    const clientKey = String(
      delivery.metadata?.username ||
      delivery.metadata?.uuid ||
      delivery.metadata?.email ||
      delivery.metadata?.subId ||
      ""
    ).trim();

    let result = { ok: false, message: "Unknown action" };

    if (action === "set_data") {
      const numericValue = Number(value);
      if (isNaN(numericValue) || value === null || value === undefined || String(value).trim() === "") {
        return res.status(400).json({ ok: false, error: "مقدار نامعتبر است (باید عدد باشد)" });
      }

      if (panelType === "marzban") {
        result = await applyAdminSetDataLimitOnMarzban(panel, username, numericValue);
      } else if (panelType === "sanaei") {
        result = await applyAdminSetDataLimitOnSanaei(panel, inboundId, email, numericValue);
      }
    } else if (action === "renew") {
      const expiryTimeMs = Date.now() + 30 * 24 * 60 * 60 * 1000;
      if (panelType === "marzban") {
        const expiryRes = await applyAdminSetExpiryOnMarzban(panel, username, expiryTimeMs);
        if (!expiryRes.ok) {
          result = expiryRes;
        } else {
          const resetRes = await applyAdminResetUsageOnMarzban(panel, username);
          result = resetRes.ok ? { ok: true, message: "Marzban renewed and usage reset." } : resetRes;
        }
      } else if (panelType === "sanaei") {
        const expiryRes = await applyAdminSetExpiryOnSanaei(panel, inboundId, email, expiryTimeMs);
        if (!expiryRes.ok) {
          result = expiryRes;
        } else {
          const resetRes = await applyAdminResetUsageOnSanaei(panel, inboundId, email);
          result = resetRes.ok ? { ok: true, message: "Sanaei renewed and usage reset." } : resetRes;
        }
      }
    } else if (action === "delete") {
      if (panelType === "marzban") {
        result = await deleteMarzbanUser(panel, clientKey);
      } else if (panelType === "sanaei") {
        result = await revokeSanaeiClient(panel, clientKey);
      }

      if (result.ok) {
        await sql`
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
      }
    } else if (action === "get_links") {
      return res.status(200).json({
        ok: true,
        message: "ok",
        subscriptionUrl: delivery.subscriptionUrl || "",
        configLinks: delivery.configLinks || []
      });
    } else if (action === "change_link") {
      if (panelType === "marzban") {
        const { regenerateMarzbanUserLink } = await import("../lib/bot.js");
        const regenRes = await regenerateMarzbanUserLink(panel, username);
        if (!regenRes.ok) {
          result = { ok: false, message: regenRes.message };
        } else {
          const u = regenRes.user as Record<string, unknown>;
          const newConfigLinks = Array.isArray(u.links) ? u.links.map((x) => String(x || "").trim()).filter(Boolean) : [];
          const newSubscriptionUrl = u.subscription_url ? String(u.subscription_url) : undefined;
          
          const newDelivery = { ...delivery };
          if (newSubscriptionUrl) newDelivery.subscriptionUrl = newSubscriptionUrl;
          if (newConfigLinks.length > 0) newDelivery.configLinks = newConfigLinks;
          newDelivery.primaryText = newSubscriptionUrl || newConfigLinks[0] || username;
          
          await sql`
            UPDATE inventory 
            SET delivery_payload = ${JSON.stringify(newDelivery)}::jsonb,
                config_value = ${newDelivery.primaryText}
            WHERE id = ${row.id}
          `;
          result = { ok: true, message: "لینک‌ها با موفقیت تغییر یافتند" };
        }
      } else if (panelType === "sanaei") {
        const { regenerateSanaeiClientLink, buildSanaeiConfigLinks, buildSanaeiSubscriptionUrl, parseJsonObject } = await import("../lib/bot.js");
        const regenRes = await regenerateSanaeiClientLink(panel, clientKey);
        if (!regenRes.ok) {
          result = { ok: false, message: regenRes.message };
        } else {
          const panelConfig = (typeof row.panel_config === "string" ? parseJsonObject(row.panel_config) : (row.panel_config as Record<string, unknown>)) || {};
          const newConfigLinks = buildSanaeiConfigLinks(String(panel.base_url), regenRes.inbound as Record<string, unknown>, regenRes.client as Record<string, unknown>, panelConfig);
          const subId = String((regenRes.client as any).subId || "");
          const newSubscriptionUrl = subId ? buildSanaeiSubscriptionUrl(String(panel.base_url), panelConfig, subId) : undefined;
          
          const newDelivery = { ...delivery };
          newDelivery.subscriptionUrl = newSubscriptionUrl;
          newDelivery.configLinks = newConfigLinks;
          newDelivery.primaryText = newSubscriptionUrl || newConfigLinks[0] || clientKey;
          
          await sql`
            UPDATE inventory 
            SET delivery_payload = ${JSON.stringify(newDelivery)}::jsonb,
                config_value = ${newDelivery.primaryText}
            WHERE id = ${row.id}
          `;
          result = { ok: true, message: "لینک‌ها با موفقیت تغییر یافتند" };
        }
      }
    }

    if (!result.ok) {
      return res.status(500).json({ ok: false, error: result.message });
    }

    return res.status(200).json({ ok: true, message: result.message });
  } catch (error) {
    return res.status(500).json({ ok: false, error: String((error as Error).message || error) });
  }
}
