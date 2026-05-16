import { fetchWithTimeout } from "./bot.js";
function assertApiKey(apiKey) {
    const key = apiKey.trim();
    if (!key)
        throw new Error("TETRAPAY api key is not configured");
    return key;
}
export async function createTetrapayOrder(params) {
    const payload = {
        ApiKey: assertApiKey(params.apiKey),
        Hash_id: params.purchaseId,
        Amount: params.amountToman,
        Description: params.description,
        Email: "customer@example.com",
        Mobile: "09120000000",
        CallbackURL: params.callbackUrl
    };
    const res = await fetchWithTimeout("https://tetra98.com/api/create_order", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
    });
    const raw = await res.text();
    let data;
    try {
        data = JSON.parse(raw);
    }
    catch (err) {
        throw new Error(`TetraPay create order failed to parse: ${raw}`);
    }
    if (data.status == 100 || data.status == "100") {
        return {
            ok: true,
            authority: data.Authority,
            paymentUrlBot: data.payment_url_bot,
            paymentUrlWeb: data.payment_url_web,
            trackingId: data.tracking_id
        };
    }
    else {
        return {
            ok: false,
            message: `TetraPay error: ${data.status} - ${raw}`
        };
    }
}
export async function verifyTetrapayOrder(authority, apiKey) {
    const payload = {
        authority,
        ApiKey: assertApiKey(apiKey)
    };
    const res = await fetchWithTimeout("https://tetra98.com/api/verify", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
    });
    const raw = await res.text();
    let data;
    try {
        data = JSON.parse(raw);
    }
    catch (err) {
        throw new Error(`TetraPay verify failed to parse: ${raw}`);
    }
    // Assuming status 100 is success based on typical Iranian gateways
    if (data.status == 100 || data.status == "100") {
        return { ok: true, data };
    }
    else {
        return { ok: false, message: `TetraPay verify error: ${data.status} - ${raw}`, data };
    }
}
