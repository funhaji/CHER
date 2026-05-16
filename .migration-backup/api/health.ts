import type { VercelRequest, VercelResponse } from "@vercel/node";
import { ensureSchema } from "../lib/db.js";
import { logError } from "../lib/log.js";

export default async function handler(_req: VercelRequest, res: VercelResponse) {
  try {
    await ensureSchema();
    res.status(200).json({ ok: true });
  } catch (error) {
    logError("health_check_failed", error);
    res.status(500).json({ ok: false });
  }
}
