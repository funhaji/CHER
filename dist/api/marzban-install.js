import crypto from "node:crypto";
import { Client } from "ssh2";
import { verifyAdminToken } from "../lib/bot.js";
function normalizeHost(raw) {
    return String(raw || "").trim();
}
function normalizePort(raw) {
    const n = Number(raw);
    if (!Number.isFinite(n) || n <= 0 || n > 65535)
        return 22;
    return Math.floor(n);
}
function normalizeUsername(raw) {
    return String(raw || "").trim() || "root";
}
function normalizePassword(raw) {
    return String(raw || "");
}
function bashSingleQuote(s) {
    return `'${s.replace(/'/g, `'\\''`)}'`;
}
function pickToken(req) {
    const q = req.query.token;
    if (typeof q === "string" && q)
        return q;
    const b = req.body?.token;
    if (typeof b === "string" && b)
        return b;
    return "";
}
function pickAction(req) {
    const q = req.query.action;
    if (typeof q === "string" && q)
        return q;
    const b = req.body?.action;
    if (typeof b === "string" && b)
        return b;
    return "";
}
async function sshExec(params, command, timeoutMs) {
    return await new Promise((resolve) => {
        const conn = new Client();
        let finished = false;
        let timeout = null;
        const finalize = (res) => {
            if (finished)
                return;
            finished = true;
            if (timeout)
                clearTimeout(timeout);
            try {
                conn.end();
            }
            catch {
                // ignore
            }
            resolve(res);
        };
        timeout = setTimeout(() => {
            finalize({ ok: false, stdout: "", stderr: "timeout", code: null, signal: "timeout" });
        }, timeoutMs);
        conn
            .on("ready", () => {
            conn.exec(command, { pty: false }, (err, stream) => {
                if (err)
                    return finalize({ ok: false, stdout: "", stderr: String(err.message || err), code: null, signal: null });
                let stdout = "";
                let stderr = "";
                stream
                    .on("data", (d) => {
                    stdout += d.toString("utf8");
                })
                    .stderr.on("data", (d) => {
                    stderr += d.toString("utf8");
                });
                stream.on("close", (code, signal) => {
                    finalize({ ok: code === 0, stdout, stderr, code, signal: signal || null });
                });
            });
        })
            .on("error", (err) => {
            finalize({ ok: false, stdout: "", stderr: String(err.message || err), code: null, signal: null });
        })
            .connect({
            host: params.host,
            port: params.port,
            username: params.username,
            password: params.password,
            readyTimeout: Math.max(5_000, Math.min(timeoutMs, 25_000)),
            tryKeyboard: false
        });
    });
}
export default async function handler(req, res) {
    const token = pickToken(req);
    const adminId = await verifyAdminToken(token);
    if (!adminId) {
        return res.status(401).json({ ok: false, error: "Unauthorized" });
    }
    const action = pickAction(req);
    if (!action) {
        return res.status(400).json({ ok: false, error: "Missing action" });
    }
    const body = (req.body || {});
    const host = normalizeHost(body.host ?? req.query.host);
    const port = normalizePort(body.port ?? req.query.port);
    const username = normalizeUsername(body.username ?? req.query.username);
    const password = normalizePassword(body.password ?? req.query.password);
    if (!host)
        return res.status(400).json({ ok: false, error: "Missing host" });
    if (!password)
        return res.status(400).json({ ok: false, error: "Missing password" });
    const ssh = { host, port, username, password };
    try {
        if (action === "start") {
            const jobId = crypto.randomUUID().slice(0, 8);
            const logPath = `/root/marzban_install_${jobId}.log`;
            const script = [
                "set -e",
                "export DEBIAN_FRONTEND=noninteractive",
                `LOG=${bashSingleQuote(logPath)}`,
                'echo "== START $(date -Is) ==" >"$LOG"',
                '( apt-get update && apt-get upgrade -y && apt-get install -y curl git sudo && bash <(curl -Ls https://github.com/Gozargah/Marzban-installer/raw/master/install.sh) ) >>"$LOG" 2>&1',
                'EC=$?',
                'echo "__DONE__:$EC" >>"$LOG"',
                "exit 0"
            ].join("\n");
            const cmd = `nohup bash -lc ${bashSingleQuote(script)} >/dev/null 2>&1 & echo $!`;
            const execRes = await sshExec(ssh, cmd, 9_000);
            if (!execRes.ok) {
                return res.status(500).json({ ok: false, error: execRes.stderr || execRes.stdout || "start_failed" });
            }
            const pid = Number(String(execRes.stdout || "").trim().split(/\s+/).pop() || 0);
            if (!Number.isFinite(pid) || pid <= 0) {
                return res.status(500).json({ ok: false, error: `start_failed: bad pid (${String(execRes.stdout || "").trim()})` });
            }
            return res.status(200).json({ ok: true, jobId, pid, logPath });
        }
        if (action === "tail") {
            const logPath = String(body.logPath || "").trim();
            const pid = Number(body.pid || 0);
            if (!logPath)
                return res.status(400).json({ ok: false, error: "Missing logPath" });
            if (!Number.isFinite(pid) || pid <= 0)
                return res.status(400).json({ ok: false, error: "Missing pid" });
            const safeLog = bashSingleQuote(logPath);
            const cmd = [
                `tail -n 200 ${safeLog} 2>/dev/null || true`,
                `echo "---__META__---"`,
                `ps -p ${Math.floor(pid)} -o pid= >/dev/null 2>&1 && echo "RUNNING:1" || echo "RUNNING:0"`,
                `grep -Eo "__DONE__:[0-9]+" ${safeLog} | tail -n 1 || true`
            ].join(" ; ");
            const execRes = await sshExec(ssh, cmd, 9_000);
            if (!execRes.ok && execRes.stderr === "timeout") {
                return res.status(200).json({ ok: true, running: true, done: false, output: "" });
            }
            if (!execRes.ok) {
                return res.status(500).json({ ok: false, error: execRes.stderr || "tail_failed" });
            }
            const [outputPart, metaPart = ""] = String(execRes.stdout || "").split("---__META__---\n");
            const metaLines = metaPart.split("\n").map((l) => l.trim()).filter(Boolean);
            const running = metaLines.includes("RUNNING:1");
            const doneLine = metaLines.find((l) => l.startsWith("__DONE__:")) || "";
            const done = Boolean(doneLine);
            const exitCode = done ? Number(doneLine.split(":")[1] || 0) : null;
            return res.status(200).json({
                ok: true,
                running,
                done,
                exitCode,
                output: outputPart
            });
        }
        return res.status(400).json({ ok: false, error: "Unknown action" });
    }
    catch (e) {
        return res.status(500).json({ ok: false, error: String(e.message || e) });
    }
}
