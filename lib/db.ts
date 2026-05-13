import { neon } from "@neondatabase/serverless";
import postgres from "postgres";
import { databaseDriver, databaseUrl, env } from "./env.js";

function dbHostname(url: string): string {
  try {
    const u = new URL(url.replace(/^postgres(ql)?:/i, "http:"));
    return u.hostname.toLowerCase();
  } catch {
    return "";
  }
}

function isSupabaseHost(url: string): boolean {
  const h = dbHostname(url);
  return h.includes("supabase.co") || h.includes("pooler.supabase.com");
}

/**
 * Where bot tables live. Supabase projects often already have `public.products` (or other names we use),
 * so we default to a dedicated schema there. Neon / local Postgres keep `public` unless overridden.
 */
function resolveDatabaseSchema(url: string, explicit: string | undefined): string | null {
  const t = explicit?.trim();
  if (t === "public") return null;
  if (t) {
    if (!/^[_a-z][_a-z0-9]*$/i.test(t)) {
      throw new Error("DATABASE_SCHEMA must be a simple identifier or the literal 'public'");
    }
    return t.toLowerCase();
  }
  if (isSupabaseHost(url)) return "telegram_seller";
  return null;
}

const resolvedSchema = resolveDatabaseSchema(databaseUrl, env.DATABASE_SCHEMA);

export const databaseAppSchema: string | null = resolvedSchema;

function useNeonServerless(url: string, driver: typeof databaseDriver): boolean {
  if (driver === "neon") return true;
  if (driver === "postgres") return false;
  const host = dbHostname(url);
  return host.endsWith(".neon.tech") || host.includes(".neon.tech");
}

function databaseUrlWithSearchPath(url: string, schema: string | null): string {
  if (!schema) return url;
  if (/[?&]options=/i.test(url)) {
    throw new Error(
      "DATABASE_URL/POSTGRES_URL must not contain options= when using DATABASE_SCHEMA (or Supabase auto-schema). Remove options= from the URL or set DATABASE_SCHEMA=public."
    );
  }
  const opt = encodeURIComponent(`-c search_path=${schema},public`);
  const joiner = url.includes("?") ? "&" : "?";
  return `${url}${joiner}options=${opt}`;
}

function postgresClientOptions(
  url: string,
  applicationSchema: string | null
): NonNullable<Parameters<typeof postgres>[1]> {
  const host = dbHostname(url);
  let port = "5432";
  try {
    const u = new URL(url.replace(/^postgres(ql)?:/i, "http:"));
    if (u.port) port = u.port;
  } catch {
    // keep default
  }
  const transactionPooler =
    host.includes("pooler.supabase.com") || port === "6543";
  const base: NonNullable<Parameters<typeof postgres>[1]> = {
    max: 1,
    idle_timeout: 20,
    connect_timeout: 30,
    prepare: !transactionPooler,
    // ADD COLUMN IF NOT EXISTS still emits NOTICE 42701 when the column exists — expected on every warm start.
    onnotice: (notice) => {
      if (notice.code === "42701") return;
      console.log(notice);
    }
  };
  if (applicationSchema) {
    base.connection = {
      ...base.connection,
      search_path: `${applicationSchema},public`
    };
  }
  return base;
}

/** Tagged template SQL — Neon serverless or TCP Postgres (Supabase, local, …). */
export type Sql = (
  strings: TemplateStringsArray,
  ...values: readonly unknown[]
) => Promise<any[]>;

const useNeon = useNeonServerless(databaseUrl, databaseDriver);

export const sql: Sql = (
  useNeon
    ? neon(databaseUrlWithSearchPath(databaseUrl, resolvedSchema))
    : postgres(databaseUrl, postgresClientOptions(databaseUrl, resolvedSchema))
) as Sql;

async function runDdl(statement: string): Promise<void> {
  if (useNeon) {
    await (sql as unknown as (q: string, p?: readonly unknown[]) => Promise<unknown>)(statement, []);
    return;
  }
  await (sql as unknown as { unsafe: (q: string) => Promise<unknown> }).unsafe(statement);
}

let schemaReady: Promise<void> | null = null;

export async function seedReferenceData() {
  await sql`
    INSERT INTO crypto_wallets (currency, network, active)
    VALUES
      ('TRX', 'TRON', FALSE),
      ('TON', 'TON', FALSE),
      ('USDT', 'TRC20', FALSE)
    ON CONFLICT (currency, network) DO NOTHING;
  `;
  await sql`
    INSERT INTO payment_methods (code, title, active)
    VALUES
      ('card2card', 'کارت‌به‌کارت', TRUE),
      ('tronado', 'ترونادو (TRX)', TRUE),
      ('plisio', 'Plisio', TRUE),
      ('tetrapay', 'تتراپی', TRUE),
      ('crypto', 'کریپتو', TRUE),
      ('swapwallet', 'SwapWallet', TRUE)
    ON CONFLICT (code) DO NOTHING;
  `;
  await sql`
    UPDATE payment_methods
    SET title = CASE code
      WHEN 'card2card' THEN 'کارت‌به‌کارت'
      WHEN 'tronado' THEN 'ترونادو (TRX)'
      WHEN 'plisio' THEN 'Plisio'
      WHEN 'tetrapay' THEN 'تتراپی'
      WHEN 'crypto' THEN 'کریپتو'
      WHEN 'swapwallet' THEN 'SwapWallet'
      ELSE title
    END;
  `;
}

export async function resetBusinessDataPreserveCaches() {
  await ensureSchema();
  await sql`
    TRUNCATE TABLE
      referral_rewards,
      wallet_topups,
      wallet_transactions,
      topup_requests,
      panel_migrations,
      config_forensics,
      banned_users,
      user_states,
      processed_updates,
      orders,
      inventory,
      discounts,
      cards,
      products,
      panels,
      payment_methods,
      crypto_wallets,
      settings,
      users
    RESTART IDENTITY CASCADE;
  `;
  await seedReferenceData();
}

export function ensureSchema() {
  if (!schemaReady) {
    schemaReady = (async () => {
      if (resolvedSchema) {
        await runDdl(`CREATE SCHEMA IF NOT EXISTS ${resolvedSchema}`);
      }
      await sql`
        CREATE TABLE IF NOT EXISTS users (
          telegram_id BIGINT PRIMARY KEY,
          username TEXT,
          first_name TEXT,
          last_name TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
      `;
      await sql`
        CREATE TABLE IF NOT EXISTS panels (
          id BIGSERIAL PRIMARY KEY,
          name TEXT NOT NULL UNIQUE,
          panel_type TEXT NOT NULL,
          base_url TEXT NOT NULL,
          username TEXT,
          password TEXT,
          access_token TEXT,
          active BOOLEAN NOT NULL DEFAULT TRUE,
          allow_customer_migration BOOLEAN NOT NULL DEFAULT FALSE,
          allow_new_sales BOOLEAN NOT NULL DEFAULT FALSE,
          last_check_at TIMESTAMPTZ,
          last_check_ok BOOLEAN,
          last_check_message TEXT,
          cached_meta JSONB NOT NULL DEFAULT '{}'::jsonb,
          priority INT NOT NULL DEFAULT 0,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
      `;
      await sql`
        CREATE TABLE IF NOT EXISTS products (
          id SERIAL PRIMARY KEY,
          name TEXT NOT NULL UNIQUE,
          size_mb INT NOT NULL,
          price_toman INT NOT NULL,
          is_active BOOLEAN NOT NULL DEFAULT TRUE,
          is_infinite BOOLEAN NOT NULL DEFAULT FALSE,
          sell_mode TEXT NOT NULL DEFAULT 'manual',
          panel_id BIGINT REFERENCES panels(id),
          panel_sell_limit INT,
          panel_delivery_mode TEXT NOT NULL DEFAULT 'both',
          panel_config JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
      `;
      await sql`
        CREATE TABLE IF NOT EXISTS inventory (
          id BIGSERIAL PRIMARY KEY,
          product_id INT NOT NULL REFERENCES products(id),
          panel_user_key TEXT,
          config_value TEXT NOT NULL,
          delivery_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
          status TEXT NOT NULL DEFAULT 'available',
          owner_telegram_id BIGINT,
          sold_order_id BIGINT,
          panel_id BIGINT,
          migration_parent_inventory_id BIGINT,
          migrated_to_inventory_id BIGINT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          sold_at TIMESTAMPTZ
        );
      `;
      await sql`
        CREATE TABLE IF NOT EXISTS discounts (
          id SERIAL PRIMARY KEY,
          code TEXT NOT NULL UNIQUE,
          type TEXT NOT NULL,
          amount INT NOT NULL,
          active BOOLEAN NOT NULL DEFAULT TRUE,
          usage_limit INT,
          used_count INT NOT NULL DEFAULT 0,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
      `;
      await sql`
        CREATE TABLE IF NOT EXISTS orders (
          id BIGSERIAL PRIMARY KEY,
          purchase_id TEXT NOT NULL UNIQUE,
          telegram_id BIGINT NOT NULL,
          product_id INT NOT NULL REFERENCES products(id),
          product_name_snapshot TEXT,
          inventory_id BIGINT REFERENCES inventory(id),
          sell_mode TEXT NOT NULL DEFAULT 'manual',
          source_panel_id BIGINT REFERENCES panels(id),
          panel_delivery_mode TEXT NOT NULL DEFAULT 'both',
          panel_config_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
          payment_method TEXT NOT NULL DEFAULT 'tronado',
          receipt_file_id TEXT,
          admin_decision_by BIGINT,
          discount_code TEXT,
          discount_amount INT NOT NULL DEFAULT 0,
          final_price INT NOT NULL,
          tron_amount NUMERIC(18,6) NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending',
          tronado_token TEXT,
          tronado_payment_url TEXT,
          plisio_txn_id TEXT,
          plisio_invoice_url TEXT,
          plisio_status TEXT,
          crypto_wallet_id BIGINT,
          crypto_currency TEXT,
          crypto_network TEXT,
          crypto_address TEXT,
          crypto_amount NUMERIC(18,6),
          crypto_txid TEXT,
          crypto_expires_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          paid_at TIMESTAMPTZ
        );
      `;
      await sql`ALTER TABLE orders ADD COLUMN IF NOT EXISTS card_id BIGINT;`;
      await sql`ALTER TABLE orders ADD COLUMN IF NOT EXISTS product_name_snapshot TEXT;`;
      await sql`ALTER TABLE orders ADD COLUMN IF NOT EXISTS plisio_txn_id TEXT;`;
      await sql`ALTER TABLE orders ADD COLUMN IF NOT EXISTS plisio_invoice_url TEXT;`;
      await sql`ALTER TABLE orders ADD COLUMN IF NOT EXISTS plisio_status TEXT;`;
      await sql`ALTER TABLE orders ADD COLUMN IF NOT EXISTS crypto_wallet_id BIGINT;`;
      await sql`ALTER TABLE orders ADD COLUMN IF NOT EXISTS crypto_currency TEXT;`;
      await sql`ALTER TABLE orders ADD COLUMN IF NOT EXISTS crypto_network TEXT;`;
      await sql`ALTER TABLE orders ADD COLUMN IF NOT EXISTS crypto_address TEXT;`;
      await sql`ALTER TABLE orders ADD COLUMN IF NOT EXISTS crypto_amount NUMERIC(18,6);`;
      await sql`ALTER TABLE orders ADD COLUMN IF NOT EXISTS crypto_txid TEXT;`;
      await sql`ALTER TABLE orders ADD COLUMN IF NOT EXISTS crypto_expires_at TIMESTAMPTZ;`;
      await sql`ALTER TABLE orders ADD COLUMN IF NOT EXISTS swapwallet_invoice_id TEXT;`;
      await sql`ALTER TABLE orders ADD COLUMN IF NOT EXISTS swapwallet_payment_url TEXT;`;
      await sql`ALTER TABLE orders ADD COLUMN IF NOT EXISTS swapwallet_status TEXT;`;

      await sql`
        CREATE TABLE IF NOT EXISTS crypto_wallets (
          id BIGSERIAL PRIMARY KEY,
          currency TEXT NOT NULL,
          network TEXT NOT NULL,
          address TEXT,
          rate_mode TEXT NOT NULL DEFAULT 'manual',
          rate_toman_per_unit INT,
          extra_toman_per_unit INT NOT NULL DEFAULT 0,
          active BOOLEAN NOT NULL DEFAULT FALSE,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          UNIQUE (currency, network)
        );
      `;
      await sql`CREATE INDEX IF NOT EXISTS crypto_wallets_active_idx ON crypto_wallets(active);`;
      await sql`
        INSERT INTO crypto_wallets (currency, network, active)
        VALUES
          ('TRX', 'TRON', FALSE),
          ('TON', 'TON', FALSE),
          ('USDT', 'TRC20', FALSE)
        ON CONFLICT (currency, network) DO NOTHING;
      `;
      await sql`
        CREATE TABLE IF NOT EXISTS crypto_rate_cache (
          symbol TEXT PRIMARY KEY,
          toman_per_unit NUMERIC NOT NULL,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
      `;
      await sql`
        CREATE TABLE IF NOT EXISTS topup_requests (
          id BIGSERIAL PRIMARY KEY,
          purchase_id TEXT UNIQUE,
          telegram_id BIGINT NOT NULL,
          inventory_id BIGINT NOT NULL REFERENCES inventory(id),
          requested_mb INT NOT NULL,
          payment_method TEXT NOT NULL DEFAULT 'card2card',
          card_id BIGINT,
          final_price INT NOT NULL DEFAULT 0,
          receipt_file_id TEXT,
          status TEXT NOT NULL DEFAULT 'pending',
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          done_at TIMESTAMPTZ,
          done_by BIGINT
        );
      `;
      await sql`ALTER TABLE topup_requests ADD COLUMN IF NOT EXISTS purchase_id TEXT;`;
      await sql`ALTER TABLE topup_requests ADD COLUMN IF NOT EXISTS payment_method TEXT NOT NULL DEFAULT 'card2card';`;
      await sql`ALTER TABLE topup_requests ADD COLUMN IF NOT EXISTS card_id BIGINT;`;
      await sql`ALTER TABLE topup_requests ADD COLUMN IF NOT EXISTS final_price INT NOT NULL DEFAULT 0;`;
      await sql`ALTER TABLE topup_requests ADD COLUMN IF NOT EXISTS receipt_file_id TEXT;`;
      await sql`CREATE UNIQUE INDEX IF NOT EXISTS topup_requests_purchase_id_unique_idx ON topup_requests(purchase_id) WHERE purchase_id IS NOT NULL;`;
      await sql`
        CREATE TABLE IF NOT EXISTS settings (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );
      `;
      await sql`
        CREATE TABLE IF NOT EXISTS user_states (
          telegram_id BIGINT PRIMARY KEY,
          state TEXT NOT NULL,
          payload JSONB NOT NULL DEFAULT '{}'::jsonb,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
      `;
      await sql`
        CREATE TABLE IF NOT EXISTS payment_methods (
          code TEXT PRIMARY KEY,
          title TEXT NOT NULL,
          active BOOLEAN NOT NULL DEFAULT TRUE
        );
      `;
      await sql`
        INSERT INTO payment_methods (code, title, active)
        VALUES
          ('card2card', 'کارت‌به‌کارت', TRUE),
          ('tronado', 'ترونادو (TRX)', TRUE),
          ('plisio', 'Plisio', TRUE),
          ('tetrapay', 'تتراپی', TRUE),
          ('crypto', 'کریپتو', TRUE),
          ('swapwallet', 'SwapWallet', TRUE)
        ON CONFLICT (code) DO NOTHING;
      `;
      await sql`
        CREATE TABLE IF NOT EXISTS cards (
          id BIGSERIAL PRIMARY KEY,
          label TEXT NOT NULL,
          card_number TEXT NOT NULL,
          holder_name TEXT,
          bank_name TEXT,
          active BOOLEAN NOT NULL DEFAULT TRUE,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
      `;
      await sql`
        CREATE TABLE IF NOT EXISTS banned_users (
          telegram_id BIGINT PRIMARY KEY,
          reason TEXT NOT NULL DEFAULT 'manual',
          banned_by BIGINT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
      `;
      await sql`
        CREATE TABLE IF NOT EXISTS panel_migrations (
          id BIGSERIAL PRIMARY KEY,
          source_inventory_id BIGINT NOT NULL REFERENCES inventory(id),
          source_panel_id BIGINT REFERENCES panels(id),
          target_panel_id BIGINT NOT NULL REFERENCES panels(id),
          requested_by BIGINT NOT NULL,
          requested_for BIGINT NOT NULL,
          requested_by_role TEXT NOT NULL DEFAULT 'customer',
          status TEXT NOT NULL DEFAULT 'pending',
          source_config_snapshot TEXT,
          target_config_value TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          processed_at TIMESTAMPTZ,
          processed_by BIGINT
        );
      `;
      await sql`
        CREATE TABLE IF NOT EXISTS config_forensics (
          id BIGSERIAL PRIMARY KEY,
          inventory_id BIGINT REFERENCES inventory(id),
          owner_telegram_id BIGINT,
          product_id INT,
          panel_id BIGINT REFERENCES panels(id),
          panel_type TEXT,
          panel_user_key TEXT,
          uuid TEXT,
          source TEXT NOT NULL DEFAULT 'inventory',
          event_type TEXT NOT NULL,
          config_value TEXT,
          metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
      `;
      await sql`ALTER TABLE products ADD COLUMN IF NOT EXISTS is_infinite BOOLEAN NOT NULL DEFAULT FALSE;`;
      await sql`ALTER TABLE products ADD COLUMN IF NOT EXISTS sell_mode TEXT NOT NULL DEFAULT 'manual';`;
      await sql`ALTER TABLE products ADD COLUMN IF NOT EXISTS panel_id BIGINT;`;
      await sql`ALTER TABLE products ADD COLUMN IF NOT EXISTS panel_sell_limit INT;`;
      await sql`ALTER TABLE products ADD COLUMN IF NOT EXISTS panel_delivery_mode TEXT NOT NULL DEFAULT 'both';`;
      await sql`ALTER TABLE products ADD COLUMN IF NOT EXISTS panel_config JSONB NOT NULL DEFAULT '{}'::jsonb;`;
      await sql`ALTER TABLE inventory ADD COLUMN IF NOT EXISTS delivery_payload JSONB NOT NULL DEFAULT '{}'::jsonb;`;
      await sql`ALTER TABLE inventory ADD COLUMN IF NOT EXISTS panel_id BIGINT;`;
      await sql`ALTER TABLE inventory ADD COLUMN IF NOT EXISTS panel_user_key TEXT;`;
      await sql`ALTER TABLE inventory ADD COLUMN IF NOT EXISTS migration_parent_inventory_id BIGINT;`;
      await sql`ALTER TABLE inventory ADD COLUMN IF NOT EXISTS migrated_to_inventory_id BIGINT;`;
      await sql`ALTER TABLE orders ADD COLUMN IF NOT EXISTS sell_mode TEXT NOT NULL DEFAULT 'manual';`;
      await sql`ALTER TABLE orders ADD COLUMN IF NOT EXISTS source_panel_id BIGINT;`;
      await sql`ALTER TABLE orders ADD COLUMN IF NOT EXISTS panel_delivery_mode TEXT NOT NULL DEFAULT 'both';`;
      await sql`ALTER TABLE orders ADD COLUMN IF NOT EXISTS panel_config_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb;`;
      await sql`ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_method TEXT NOT NULL DEFAULT 'tronado';`;
      await sql`ALTER TABLE orders ADD COLUMN IF NOT EXISTS receipt_file_id TEXT;`;
      await sql`ALTER TABLE orders ADD COLUMN IF NOT EXISTS admin_decision_by BIGINT;`;
      await sql`ALTER TABLE panels ADD COLUMN IF NOT EXISTS allow_new_sales BOOLEAN NOT NULL DEFAULT FALSE;`;
      await sql`ALTER TABLE panels ADD COLUMN IF NOT EXISTS last_check_at TIMESTAMPTZ;`;
      await sql`ALTER TABLE panels ADD COLUMN IF NOT EXISTS last_check_ok BOOLEAN;`;
      await sql`ALTER TABLE panels ADD COLUMN IF NOT EXISTS last_check_message TEXT;`;
      await sql`ALTER TABLE panels ADD COLUMN IF NOT EXISTS cached_meta JSONB NOT NULL DEFAULT '{}'::jsonb;`;
      await sql`CREATE INDEX IF NOT EXISTS inventory_owner_status_idx ON inventory(owner_telegram_id, status);`;
      await sql`CREATE INDEX IF NOT EXISTS inventory_product_status_idx ON inventory(product_id, status);`;
      await sql`CREATE INDEX IF NOT EXISTS inventory_panel_user_key_idx ON inventory(panel_id, panel_user_key);`;
      await sql`CREATE INDEX IF NOT EXISTS panel_migrations_status_idx ON panel_migrations(status, created_at DESC);`;
      await sql`CREATE INDEX IF NOT EXISTS config_forensics_owner_idx ON config_forensics(owner_telegram_id, created_at DESC);`;
      await sql`CREATE INDEX IF NOT EXISTS config_forensics_uuid_idx ON config_forensics(uuid);`;
      await sql`CREATE INDEX IF NOT EXISTS config_forensics_panel_user_idx ON config_forensics(panel_user_key);`;
      await sql`ALTER TABLE users ADD COLUMN IF NOT EXISTS wallet_balance INT NOT NULL DEFAULT 0;`;
      await sql`ALTER TABLE orders ADD COLUMN IF NOT EXISTS wallet_used INT NOT NULL DEFAULT 0;`;
      await sql`ALTER TABLE users ADD COLUMN IF NOT EXISTS referred_by_telegram_id BIGINT;`;
      await sql`ALTER TABLE users ADD COLUMN IF NOT EXISTS referral_joined_at TIMESTAMPTZ;`;
      await sql`ALTER TABLE users ADD COLUMN IF NOT EXISTS referral_qualified_at TIMESTAMPTZ;`;
      await sql`CREATE INDEX IF NOT EXISTS users_referred_by_idx ON users(referred_by_telegram_id, referral_qualified_at DESC);`;

      await sql`
        UPDATE inventory
        SET panel_user_key = COALESCE(
          delivery_payload->'metadata'->>'username',
          delivery_payload->'metadata'->>'email',
          delivery_payload->'metadata'->>'subId',
          delivery_payload->'metadata'->>'uuid'
        )
        WHERE panel_user_key IS NULL
          AND delivery_payload ? 'metadata'
          AND COALESCE(
            delivery_payload->'metadata'->>'username',
            delivery_payload->'metadata'->>'email',
            delivery_payload->'metadata'->>'subId',
            delivery_payload->'metadata'->>'uuid'
          ) IS NOT NULL;
      `;
      
      await sql`
        CREATE TABLE IF NOT EXISTS wallet_transactions (
          id BIGSERIAL PRIMARY KEY,
          telegram_id BIGINT NOT NULL REFERENCES users(telegram_id),
          amount INT NOT NULL,
          type TEXT NOT NULL,
          description TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
      `;
      await sql`CREATE INDEX IF NOT EXISTS wallet_transactions_telegram_id_idx ON wallet_transactions(telegram_id, created_at DESC);`;
      
      await sql`
        CREATE TABLE IF NOT EXISTS wallet_topups (
          id BIGSERIAL PRIMARY KEY,
          telegram_id BIGINT NOT NULL REFERENCES users(telegram_id),
          amount INT NOT NULL,
          payment_method TEXT NOT NULL,
          receipt_file_id TEXT,
          status TEXT NOT NULL DEFAULT 'pending',
          admin_decision_by BIGINT,
          crypto_network TEXT,
          crypto_address TEXT,
          crypto_amount NUMERIC(18,6),
          crypto_txid TEXT,
          crypto_expires_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          done_at TIMESTAMPTZ
        );
      `;
      await sql`ALTER TABLE wallet_topups ADD COLUMN IF NOT EXISTS crypto_network TEXT;`;
      await sql`ALTER TABLE wallet_topups ADD COLUMN IF NOT EXISTS crypto_address TEXT;`;
      await sql`ALTER TABLE wallet_topups ADD COLUMN IF NOT EXISTS crypto_amount NUMERIC(18,6);`;
      await sql`ALTER TABLE wallet_topups ADD COLUMN IF NOT EXISTS crypto_txid TEXT;`;
      await sql`ALTER TABLE wallet_topups ADD COLUMN IF NOT EXISTS crypto_expires_at TIMESTAMPTZ;`;

      await sql`
        CREATE TABLE IF NOT EXISTS referral_rewards (
          id BIGSERIAL PRIMARY KEY,
          inviter_telegram_id BIGINT NOT NULL REFERENCES users(telegram_id),
          reward_batch INT NOT NULL,
          referred_count_snapshot INT NOT NULL DEFAULT 0,
          threshold_snapshot INT NOT NULL DEFAULT 0,
          reward_type TEXT NOT NULL,
          reward_delivery_mode TEXT,
          status TEXT NOT NULL DEFAULT 'granted',
          wallet_amount INT NOT NULL DEFAULT 0,
          product_id INT REFERENCES products(id),
          order_id BIGINT REFERENCES orders(id),
          description TEXT,
          failure_reason TEXT,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
      `;
      await sql`ALTER TABLE referral_rewards ADD COLUMN IF NOT EXISTS reward_delivery_mode TEXT;`;
      await sql`ALTER TABLE referral_rewards ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'granted';`;
      await sql`ALTER TABLE referral_rewards ADD COLUMN IF NOT EXISTS failure_reason TEXT;`;
      await sql`ALTER TABLE referral_rewards ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();`;
      await sql`CREATE UNIQUE INDEX IF NOT EXISTS referral_rewards_inviter_batch_idx ON referral_rewards(inviter_telegram_id, reward_batch);`;
      await sql`CREATE INDEX IF NOT EXISTS referral_rewards_inviter_created_idx ON referral_rewards(inviter_telegram_id, created_at DESC);`;

      await sql`
        INSERT INTO payment_methods (code, title, active)
        VALUES ('tronado', 'TRON', TRUE), ('card2card', 'کارت‌به‌کارت', TRUE), ('tetrapay', 'تتراپی', TRUE), ('plisio', 'پلیسیو (کریپتو)', TRUE)
        ON CONFLICT (code) DO NOTHING;
      `;
      await sql`UPDATE payment_methods SET title = 'پلیسیو (کریپتو)' WHERE code = 'plisio';`;
      await sql`
        INSERT INTO payment_methods (code, title, active)
        VALUES ('crypto', 'کریپتو', TRUE)
        ON CONFLICT (code) DO NOTHING;
      `;

      await sql`
        UPDATE products
        SET panel_config = jsonb_set(panel_config, '{data_limit_mb}', to_jsonb(size_mb), true)
        WHERE sell_mode = 'panel'
          AND (
            (panel_config->>'data_limit_mb') IS NULL
            OR (panel_config->>'data_limit_mb') !~ '^[0-9]+$'
            OR (panel_config->>'data_limit_mb')::int <= 0
            OR (panel_config->>'data_limit_mb')::int <> size_mb
          );
      `;

      await sql`
        CREATE TABLE IF NOT EXISTS processed_updates (
          update_id BIGINT PRIMARY KEY,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
      `;
      await sql`
        CREATE TABLE IF NOT EXISTS runtime_logs (
          id BIGSERIAL PRIMARY KEY,
          level TEXT NOT NULL,
          event TEXT NOT NULL,
          payload JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
      `;
      await sql`CREATE INDEX IF NOT EXISTS runtime_logs_created_idx ON runtime_logs(created_at DESC);`;
      
      await sql`
        DELETE FROM products p
        WHERE p.name IN ('1GB کانفیگ', '500MB کانفیگ')
          AND NOT EXISTS (SELECT 1 FROM orders o WHERE o.product_id = p.id)
          AND NOT EXISTS (SELECT 1 FROM inventory i WHERE i.product_id = p.id);
      `;
    })();
  }
  return schemaReady;
}
