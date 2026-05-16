import type { VercelRequest, VercelResponse } from "@vercel/node";
import { lookup } from "node:dns/promises";
import { logError, logInfo } from "../lib/log.js";

function normalizeTarget(raw: string) {
  const trimmed = raw.trim();
  if (!trimmed) return null;
  const withProtocol = /^https?:\/\//i.test(trimmed) ? trimmed : `https://${trimmed}`;
  let url: URL;
  try {
    url = new URL(withProtocol);
  } catch {
    return null;
  }
  if (!["http:", "https:"].includes(url.protocol)) return null;
  return url;
}

async function fetchWithTimeout(url: string, timeoutMs = 10000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, {
      method: "GET",
      redirect: "follow",
      headers: { "User-Agent": "vercel-reachability-check/1.0" },
      signal: controller.signal
    });
  } finally {
    clearTimeout(timer);
  }
}

export default async function handler(req: VercelRequest, res: VercelResponse) {
  if (req.method !== "GET") {
    res.status(405).json({ ok: false, error: "Method not allowed" });
    return;
  }
  const targetRaw = String(req.query?.target || "");
  if (!targetRaw || targetRaw.length > 500) {
    res.status(400).json({ ok: false, error: "target is required" });
    return;
  }
  const url = normalizeTarget(targetRaw);
  if (!url) {
    res.status(400).json({ ok: false, error: "invalid target" });
    return;
  }
  const startedAt = Date.now();
  try {
    let dns: { address: string; family: number } | null = null;
    try {
      dns = await lookup(url.hostname);
    } catch {
      dns = null;
    }
    const response = await fetchWithTimeout(url.toString());
    const body = await response.text();
    const result = {
      ok: true,
      reachable: response.ok,
      url: url.toString(),
      finalUrl: response.url,
      status: response.status,
      statusText: response.statusText,
      durationMs: Date.now() - startedAt,
      dns,
      bodySnippet: body.slice(0, 400)
    };
    logInfo("reachability_checked", {
      target: url.toString(),
      status: response.status,
      reachable: response.ok,
      durationMs: result.durationMs
    });
    res.status(200).json(result);
  } catch (error) {
    logError("reachability_failed", error, { target: url.toString() });
    res.status(200).json({
      ok: false,
      reachable: false,
      url: url.toString(),
      durationMs: Date.now() - startedAt,
      error: String((error as Error).message || error)
    });
  }
}
