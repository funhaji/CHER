import { sql } from "./db.js";

type LogLevel = "INFO" | "ERROR";

function toErrorDetails(error: unknown) {
  if (error instanceof Error) {
    const withCause = error as Error & { cause?: unknown };
    return {
      name: error.name,
      message: error.message,
      stack: error.stack,
      cause: withCause.cause ? String(withCause.cause) : undefined
    };
  }
  return { message: String(error) };
}

function write(level: LogLevel, event: string, data?: Record<string, unknown>) {
  const payload = {
    level,
    event,
    timestamp: new Date().toISOString(),
    ...data
  };
  const line = JSON.stringify(payload);
  if (level === "ERROR") {
    console.error(line);
  } else {
    console.log(line);
  }
  sql`
    INSERT INTO runtime_logs (level, event, payload)
    VALUES (${level}, ${event}, ${JSON.stringify(payload)}::jsonb);
  `.catch(() => {});
}

export function logInfo(event: string, data?: Record<string, unknown>) {
  write("INFO", event, data);
}

export function logError(event: string, error: unknown, data?: Record<string, unknown>) {
  write("ERROR", event, { ...data, error: toErrorDetails(error) });
}
