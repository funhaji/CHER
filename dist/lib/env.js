import { z } from "zod";
const schema = z.object({
    TELEGRAM_BOT_TOKEN: z.string().optional(),
    DATABASE_URL: z.string().optional(),
    POSTGRES_URL: z.string().optional(),
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
const db = env.DATABASE_URL || env.POSTGRES_URL;
if (!db) {
    throw new Error("DATABASE_URL or POSTGRES_URL is required");
}
export const databaseUrl = db;
