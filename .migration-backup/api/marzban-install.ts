import type { VercelRequest, VercelResponse } from "@vercel/node";
import crypto from "node:crypto";
import { Client, type ClientChannel } from "ssh2";
import { verifyAdminToken } from "../lib/bot.js";

type SshParams = {
  host: string;
  port: number;
  username: string;
  password: string;
};

function normalizeHost(raw: unknown) {
  return String(raw || "").trim();
}

function normalizePort(raw: unknown) {
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0 || n > 65535) return 22;
  return Math.floor(n);
}

function normalizeUsername(raw: unknown) {
  return String(raw || "").trim() || "root";
}

function normalizePassword(raw: unknown) {
  return String(raw || "");
}

function bashSingleQuote(s: string) {
  return `'${s.replace(/'/g, `'\\''`)}'`;
}

function pickToken(req: VercelRequest) {
  const q = req.query.token;
  if (typeof q === "string" && q) return q;
  const b = (req.body as any)?.token;
  if (typeof b === "string" && b) return b;
  return "";
}

function pickAction(req: VercelRequest) {
  const q = req.query.action;
  if (typeof q === "string" && q) return q;
  const b = (req.body as any)?.action;
  if (typeof b === "string" && b) return b;
  return "";
}

async function sshExec(params: SshParams, command: string, timeoutMs: number) {
  return await new Promise<{ ok: boolean; stdout: string; stderr: string; code: number | null; signal: string | null }>((resolve) => {
    const conn = new Client();
    let finished = false;
    let timeout: NodeJS.Timeout | null = null;

    const finalize = (res: { ok: boolean; stdout: string; stderr: string; code: number | null; signal: string | null }) => {
      if (finished) return;
      finished = true;
      if (timeout) clearTimeout(timeout);
      try {
        conn.end();
      } catch {
        // ignore
      }
      resolve(res);
    };

    timeout = setTimeout(() => {
      finalize({ ok: false, stdout: "", stderr: "timeout", code: null, signal: "timeout" });
    }, timeoutMs);

    conn
      .on("ready", () => {
        conn.exec(command, { pty: false }, (err: Error | undefined, stream: ClientChannel) => {
          if (err) return finalize({ ok: false, stdout: "", stderr: String(err.message || err), code: null, signal: null });
          let stdout = "";
          let stderr = "";
          stream
            .on("data", (d: Buffer) => {
              stdout += d.toString("utf8");
            })
            .stderr.on("data", (d: Buffer) => {
              stderr += d.toString("utf8");
            });
          stream.on("close", (code: number, signal: string) => {
            finalize({ ok: code === 0, stdout, stderr, code, signal: signal || null });
          });
        });
      })
      .on("error", (err: Error) => {
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

export default async function handler(req: VercelRequest, res: VercelResponse) {
  const token = pickToken(req);
  const adminId = await verifyAdminToken(token);
  if (!adminId) {
    return res.status(401).json({ ok: false, error: "Unauthorized" });
  }

  const action = pickAction(req);
  if (!action) {
    return res.status(400).json({ ok: false, error: "Missing action" });
  }

  const body = (req.body || {}) as any;
  const host = normalizeHost(body.host ?? req.query.host);
  const port = normalizePort(body.port ?? req.query.port);
  const username = normalizeUsername(body.username ?? req.query.username);
  const password = normalizePassword(body.password ?? req.query.password);

  if (!host) return res.status(400).json({ ok: false, error: "Missing host" });
  if (!password) return res.status(400).json({ ok: false, error: "Missing password" });

  const ssh: SshParams = { host, port, username, password };

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
      if (!logPath) return res.status(400).json({ ok: false, error: "Missing logPath" });
      if (!Number.isFinite(pid) || pid <= 0) return res.status(400).json({ ok: false, error: "Missing pid" });

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
  } catch (e) {
    return res.status(500).json({ ok: false, error: String((e as Error).message || e) });
  }
}
