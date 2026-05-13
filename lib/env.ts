import { z } from "zod";

const schema = z.object({
  TELEGRAM_BOT_TOKEN: z.string().optional(),
  DATABASE_URL: z.string().optional(),
  POSTGRES_URL: z.string().optional(),
  /** auto: Neon host → @neondatabase/serverless; otherwise TCP postgres (Supabase, local, …). */
  DATABASE_DRIVER: z.enum(["auto", "neon", "postgres"]).default("auto"),
  /**
   * Postgres schema for bot tables. Default on Supabase hosts: `telegram_seller` (avoids clashes with `public.products`, etc.).
   * Set to `public` to force the default schema (not recommended on Supabase if `public.products` already exists).
   */
  DATABASE_SCHEMA: z.string().optional(),
  ADMIN_IDS: z.string().default(""),
  PUBLIC_BASE_URL: z.string().url().optional(),
  TRONADO_API_KEY: z.string().optional(),
  TRONADO_X_API_KEY: z.string().optional(),
  X_API_KEY: z.string().optional(),
  TRONADO_BASE_URL: z.string().url().default("https://bot.tronado.cloud"),
  DEFAULT_TRON_PRICE_TOMAN: z.coerce.number().positive().default(100000),
  BUSINESS_WALLET_ADDRESS: z.string().default(""),
  TEST_PASS: z.string().optional()
});

const parsed = schema.safeParse(process.env);
if (!parsed.success) {
  throw new Error(`Invalid environment variables: ${parsed.error.message}`);
}

export const env = parsed.data;

export const adminIds = env.ADMIN_IDS.split(",")
  .map((x) => x.trim())
  .filter(Boolean)
  .map((x) => Number(x))
  .filter((x) => Number.isFinite(x));

const db = env.DATABASE_URL || env.POSTGRES_URL;
if (!db) {
  throw new Error("DATABASE_URL or POSTGRES_URL is required");
}
export const databaseUrl: string = db;
export const databaseDriver: "auto" | "neon" | "postgres" = env.DATABASE_DRIVER;
