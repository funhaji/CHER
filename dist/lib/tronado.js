import { env } from "./env.js";
function getApiKey() {
    const key = (env.TRONADO_API_KEY || env.TRONADO_X_API_KEY || env.X_API_KEY || "").trim();
    if (!key) {
        throw new Error("TRONADO_API_KEY is not configured");
    }
    return key;
}
export async function getOrderToken(input) {
    const url = `${env.TRONADO_BASE_URL}/api/v3/GetOrderToken`;
    const form = new FormData();
    form.append("PaymentID", input.paymentId);
    form.append("WalletAddress", input.walletAddress);
    form.append("TronAmount", String(input.tronAmount));
    form.append("CallbackUrl", input.callbackUrl);
    form.append("wageFromBusinessPercentage", "0");
    form.append("apiVersion", "1");
    const res = await fetch(url, {
        method: "POST",
        headers: {
            "x-api-key": (input.apiKey || "").trim() || getApiKey()
        },
        body: form
    });
    const data = (await res.json());
    if (!res.ok || !data?.Data?.Token || !data?.Data?.FullPaymentUrl) {
        const msg = data?.Data?.ErrorMessage || data?.Message || `HTTP ${res.status}`;
        throw new Error(msg);
    }
    return {
        token: data.Data.Token,
        paymentUrl: data.Data.FullPaymentUrl
    };
}
export async function getStatusByPaymentId(paymentId, apiKey) {
    const url = `${env.TRONADO_BASE_URL}/Order/GetStatusByPaymentID`;
    const form = new FormData();
    form.append("Id", paymentId);
    const res = await fetch(url, {
        method: "POST",
        headers: {
            "x-api-key": (apiKey || "").trim() || getApiKey()
        },
        body: form
    });
    if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
    }
    return (await res.json());
}
export async function getTronPriceToman(apiKey) {
    const candidates = [
        `${env.TRONADO_BASE_URL}/Price/Tron`,
        `${env.TRONADO_BASE_URL}/api/Price/Tron`
    ];
    for (const url of candidates) {
        try {
            const res = await fetch(url, {
                headers: { "x-api-key": (apiKey || "").trim() || getApiKey() }
            });
            if (!res.ok) {
                continue;
            }
            const data = (await res.json());
            const values = [
                data.price,
                data.Price,
                data.tomanPrice,
                data.TomanPrice,
                data.Data?.price,
                data.Data?.Price
            ];
            const first = values.find((v) => typeof v === "number" || typeof v === "string");
            if (first !== undefined) {
                const n = Number(first);
                if (Number.isFinite(n) && n > 0) {
                    return n;
                }
            }
        }
        catch {
            continue;
        }
    }
    return env.DEFAULT_TRON_PRICE_TOMAN;
}
