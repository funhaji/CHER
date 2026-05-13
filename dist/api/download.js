import { lookup } from "node:dns/promises";
import { Readable } from "stream";
function normalizeTarget(raw) {
    const trimmed = raw.trim();
    if (!trimmed)
        return null;
    let url;
    try {
        url = new URL(trimmed);
    }
    catch {
        return null;
    }
    if (!["http:", "https:"].includes(url.protocol))
        return null;
    return url;
}
function isPrivateIpv4(ip) {
    const parts = ip.split(".").map((x) => Number(x));
    if (parts.length !== 4 || parts.some((n) => !Number.isFinite(n) || n < 0 || n > 255))
        return true;
    const [a, b] = parts;
    if (a === 10)
        return true;
    if (a === 127)
        return true;
    if (a === 0)
        return true;
    if (a === 169 && b === 254)
        return true;
    if (a === 172 && b >= 16 && b <= 31)
        return true;
    if (a === 192 && b === 168)
        return true;
    if (a >= 224)
        return true;
    return false;
}
function isPrivateIpv6(ip) {
    const normalized = ip.toLowerCase();
    if (normalized === "::1")
        return true;
    if (normalized.startsWith("fc") || normalized.startsWith("fd"))
        return true;
    if (normalized.startsWith("fe80:"))
        return true;
    return false;
}
async function isSafeHostname(hostname) {
    if (!hostname)
        return false;
    const lower = hostname.toLowerCase();
    if (lower === "localhost" || lower.endsWith(".local"))
        return false;
    let addrs = [];
    try {
        addrs = await lookup(hostname, { all: true });
    }
    catch {
        return false;
    }
    if (!addrs.length)
        return false;
    for (const a of addrs) {
        if (a.family === 4) {
            if (isPrivateIpv4(a.address))
                return false;
        }
        else if (a.family === 6) {
            if (isPrivateIpv6(a.address))
                return false;
        }
        else {
            return false;
        }
    }
    return true;
}
function sanitizeFilename(raw) {
    const trimmed = raw.trim();
    if (!trimmed)
        return "";
    const withoutCtl = trimmed.replace(/[\r\n\t\0]/g, " ").replace(/\s+/g, " ").trim();
    const withoutSeparators = withoutCtl.replace(/[\\/]/g, "-");
    const withoutQuotes = withoutSeparators.replace(/["']/g, "");
    return withoutQuotes.slice(0, 160);
}
export default async function handler(req, res) {
    const { url } = req.query;
    const filenameRaw = typeof req.query.filename === "string" ? req.query.filename : "";
    if (!url || typeof url !== "string") {
        res.setHeader("Content-Type", "text/plain; charset=utf-8");
        return res.status(400).send("لطفاً آدرس فایل را ارسال کنید.");
    }
    try {
        const targetUrl = normalizeTarget(url);
        if (!targetUrl) {
            res.setHeader("Content-Type", "text/plain; charset=utf-8");
            return res.status(400).send("آدرس وارد شده نامعتبر است.");
        }
        const safe = await isSafeHostname(targetUrl.hostname);
        if (!safe) {
            res.setHeader("Content-Type", "text/plain; charset=utf-8");
            return res.status(400).send("این آدرس برای دانلود مجاز نیست.");
        }
        const fetchOptions = {
            method: req.method === "HEAD" ? "HEAD" : "GET",
            headers: {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                ...(req.headers.range ? { "Range": req.headers.range } : {})
            },
            redirect: "follow"
        };
        const response = await fetch(targetUrl, fetchOptions);
        // Forward status and important headers
        res.status(response.status);
        const headersToForward = [
            "content-type",
            "content-length",
            "content-range",
            "accept-ranges",
            "content-disposition"
        ];
        headersToForward.forEach(h => {
            const val = response.headers.get(h);
            if (val)
                res.setHeader(h, val);
        });
        const forcedName = sanitizeFilename(filenameRaw);
        if (forcedName) {
            res.setHeader("Content-Disposition", `attachment; filename="${forcedName}"`);
        }
        else if (!response.headers.get("content-disposition")) {
            const filename = targetUrl.pathname.split("/").pop() || "downloaded_file";
            res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
        }
        if (req.method === "HEAD") {
            return res.end();
        }
        if (response.body) {
            const nodeStream = Readable.fromWeb(response.body);
            nodeStream.pipe(res);
            await new Promise((resolve, reject) => {
                nodeStream.on("end", resolve);
                nodeStream.on("error", reject);
                res.on("close", resolve);
            });
        }
        else {
            res.status(500).send("Empty response body");
        }
    }
    catch (err) {
        res.setHeader("Content-Type", "text/plain; charset=utf-8");
        res.status(400).send(`آدرس وارد شده نامعتبر است: ${err.message}`);
    }
}
