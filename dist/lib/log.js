import { sql } from "./db.js";
function toErrorDetails(error) {
    if (error instanceof Error) {
        const withCause = error;
        return {
            name: error.name,
            message: error.message,
            stack: error.stack,
            cause: withCause.cause ? String(withCause.cause) : undefined
        };
    }
    return { message: String(error) };
}
function write(level, event, data) {
    const payload = {
        level,
        event,
        timestamp: new Date().toISOString(),
        ...data
    };
    const line = JSON.stringify(payload);
    if (level === "ERROR") {
        console.error(line);
    }
    else {
        console.log(line);
    }
    sql `
    INSERT INTO runtime_logs (level, event, payload)
    VALUES (${level}, ${event}, ${JSON.stringify(payload)}::jsonb);
  `.catch(() => { });
}
export function logInfo(event, data) {
    write("INFO", event, data);
}
export function logError(event, error, data) {
    write("ERROR", event, { ...data, error: toErrorDetails(error) });
}
