import { ensureSchema, sql } from "../lib/db.js";
import { verifyAdminToken, loginMarzbanPanel, loginSanaeiPanel, getSanaeiInbounds, normalizeBaseUrl, fetchWithTimeout, parseJsonObject, jsonSuccess } from "../lib/bot.js";
export default async function handler(req, res) {
    // Support both GET (for init) and POST (for scan actions with panel credentials)
    const isPost = req.method === "POST";
    const token = isPost
        ? (typeof req.body?.token === "string" ? req.body.token : "")
        : (typeof req.query.token === "string" ? req.query.token : "");
    // BUG FIX: was missing `await` — verifyAdminToken is async, so without await
    // `adminId` was always a truthy Promise object and the auth check never fired.
    const adminId = await verifyAdminToken(token);
    if (!adminId) {
        return res.status(401).json({ ok: false, error: "توکن نامعتبر یا منقضی شده است." });
    }
    const action = isPost
        ? (typeof req.body?.action === "string" ? req.body.action : "")
        : (typeof req.query.action === "string" ? req.query.action : "");
    try {
        await ensureSchema();
        if (action === "init") {
            const panels = await sql `SELECT id, name, panel_type, base_url, username, password FROM panels WHERE active = TRUE ORDER BY id ASC`;
            return res.status(200).json({
                ok: true,
                panels: panels.map((p) => ({
                    id: Number(p.id),
                    name: String(p.name),
                    type: String(p.panel_type),
                    // Omit credentials from response — client should not receive them.
                    // The server will re-fetch panel credentials per scan request using panelId.
                }))
            });
        }
        if (action === "scan_marzban") {
            // BUG FIX: Panel credentials are no longer passed in the URL (security risk).
            // We now accept panelId via POST body and re-fetch credentials server-side.
            const panelId = Number(isPost ? req.body?.panelId : req.query.panelId);
            const offset = Number(isPost ? req.body?.offset : req.query.offset) || 0;
            const limit = Number(isPost ? req.body?.limit : req.query.limit) || 500;
            const panelRows = await sql `
        SELECT id, name, panel_type, base_url, username, password
        FROM panels WHERE id = ${panelId} AND active = TRUE LIMIT 1
      `;
            if (!panelRows.length) {
                return res.status(404).json({ ok: false, error: "پنل پیدا نشد" });
            }
            const panel = panelRows[0];
            const login = await loginMarzbanPanel({
                base_url: panel.base_url,
                username: panel.username,
                password: panel.password
            });
            if (!login.res.ok || !login.token) {
                return res.status(400).json({ ok: false, error: "اتصال به پنل ناموفق بود" });
            }
            const baseUrl = normalizeBaseUrl(String(panel.base_url || ""));
            const listRes = await fetchWithTimeout(`${baseUrl}/api/users?offset=${offset}&limit=${limit}`, {
                method: "GET",
                headers: { Authorization: `Bearer ${login.token}`, Accept: "application/json" }
            });
            if (!listRes.ok)
                throw new Error("Failed to fetch Marzban users");
            const listData = parseJsonObject(await listRes.text());
            const users = Array.isArray(listData?.users) ? listData.users : [];
            const total = Number(listData?.total || 0);
            const deadUsers = users.filter((u) => {
                return u.status === "limited" || u.status === "expired";
            });
            const deadUsernames = deadUsers.map((u) => String(u.username));
            const dbMap = new Map();
            if (deadUsernames.length > 0) {
                const inventoryMatches = await sql `
          SELECT id, panel_user_key 
          FROM inventory 
          WHERE panel_id = ${panelId} AND panel_user_key = ANY(${deadUsernames})
        `;
                inventoryMatches.forEach((row) => dbMap.set(String(row.panel_user_key), Number(row.id)));
            }
            const mappedDead = deadUsers.map((u) => {
                const username = String(u.username);
                const invId = dbMap.get(username);
                return {
                    panelKey: username,
                    reason: u.status,
                    isBotSold: !!invId,
                    inventoryId: invId || null,
                    details: `Usage: ${Math.round(u.used_traffic / 1048576)}MB / ${u.data_limit ? Math.round(u.data_limit / 1048576) + "MB" : "Unlimited"}`
                };
            });
            return res.status(200).json({
                ok: true,
                dead: mappedDead,
                nextOffset: offset + limit < total ? offset + limit : null,
                hasMore: offset + limit < total,
                // BUG FIX: was returning `identifiers.length` (dead count) — now correctly returns
                // the count of users actually fetched in this page from the panel.
                scannedCount: users.length,
                total
            });
        }
        if (action === "scan_sanaei") {
            // BUG FIX: Same as above — re-fetch panel credentials server-side via panelId.
            const panelId = Number(isPost ? req.body?.panelId : req.query.panelId);
            const panelRows = await sql `
        SELECT id, name, panel_type, base_url, username, password
        FROM panels WHERE id = ${panelId} AND active = TRUE LIMIT 1
      `;
            if (!panelRows.length) {
                return res.status(404).json({ ok: false, error: "پنل پیدا نشد" });
            }
            const panel = panelRows[0];
            const login = await loginSanaeiPanel({
                base_url: panel.base_url,
                username: panel.username,
                password: panel.password
            });
            if (!login.res.ok || !jsonSuccess(login.data) || !login.cookie) {
                return res.status(400).json({ ok: false, error: "اتصال به پنل ناموفق بود" });
            }
            const inbounds = await getSanaeiInbounds(panel.base_url, login.cookie);
            if (!inbounds.res.ok || !jsonSuccess(inbounds.data)) {
                throw new Error("Failed to fetch Sanaei inbounds");
            }
            const deadClients = [];
            const identifiers = [];
            let totalScanned = 0;
            for (const inbound of inbounds.items) {
                const clientStats = Array.isArray(inbound.clientStats) ? inbound.clientStats : [];
                const settings = parseJsonObject(String(inbound.settings || "{}"));
                const clients = Array.isArray(settings?.clients) ? settings.clients : [];
                // BUG FIX: Count all clients, not just dead ones.
                totalScanned += clients.length;
                for (const client of clients) {
                    const stat = clientStats.find((s) => s.email === client.email);
                    if (!stat)
                        continue;
                    const totalGB = Number(client.totalGB || 0); // bytes in DB
                    const usedBytes = Number(stat.up || 0) + Number(stat.down || 0);
                    const expiryTime = Number(client.expiryTime || 0);
                    const now = Date.now();
                    let isDead = false;
                    let reason = "";
                    if (expiryTime > 0 && expiryTime < now) {
                        isDead = true;
                        reason = "expired";
                    }
                    else if (totalGB > 0 && usedBytes >= totalGB) {
                        isDead = true;
                        reason = "limited";
                    }
                    if (isDead) {
                        const key = String(client.email || client.id || client.subId || "");
                        if (key) {
                            identifiers.push(key);
                            deadClients.push({
                                panelKey: key,
                                reason,
                                details: `Usage: ${Math.round(usedBytes / 1048576)}MB / ${totalGB > 0 ? Math.round(totalGB / 1048576) + "MB" : "Unlimited"}`,
                                _email: client.email,
                                _id: client.id,
                                _subId: client.subId
                            });
                        }
                    }
                }
            }
            const dbMap = new Map();
            if (identifiers.length > 0) {
                const inventoryMatches = await sql `
          SELECT id, panel_user_key, config_value
          FROM inventory 
          WHERE panel_id = ${panelId}
        `;
                inventoryMatches.forEach((row) => {
                    const key = String(row.panel_user_key);
                    const val = String(row.config_value);
                    dbMap.set(key, Number(row.id));
                    identifiers.forEach((id) => {
                        if (val.includes(id))
                            dbMap.set(id, Number(row.id));
                    });
                });
            }
            const mappedDead = deadClients.map((c) => {
                const invId = dbMap.get(c.panelKey) || dbMap.get(c._email) || dbMap.get(c._id) || dbMap.get(c._subId);
                return {
                    panelKey: c.panelKey,
                    reason: c.reason,
                    isBotSold: !!invId,
                    inventoryId: invId || null,
                    details: c.details
                };
            });
            return res.status(200).json({
                ok: true,
                dead: mappedDead,
                nextOffset: null,
                hasMore: false,
                scannedCount: totalScanned // BUG FIX: actual total, not dead-only count
            });
        }
        return res.status(400).json({ ok: false, error: "Action not recognized" });
    }
    catch (error) {
        return res.status(500).json({ ok: false, error: String(error.message || error) });
    }
}
