import type { SupervisorEvent } from "./supervisor-contracts.js";

const sensitiveKey = /(?:api[-_]?key|authorization|cookie|credential|password|secret|token)/i;
const bearerValue = /\bBearer\s+[A-Za-z0-9._~+\/-]+=*/gi;
const likelyKeyValue = /\b(?:sk|key|token)-[A-Za-z0-9_-]{12,}\b/gi;

function redactValue(value: unknown, key?: string): unknown {
  if (key && sensitiveKey.test(key)) return "[REDACTED]";
  if (typeof value === "string") {
    return value
      .replace(bearerValue, "Bearer [REDACTED]")
      .replace(likelyKeyValue, "[REDACTED]");
  }
  if (Array.isArray(value)) return value.map((item) => redactValue(item));
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([childKey, childValue]) => [
        childKey,
        redactValue(childValue, childKey),
      ]),
    );
  }
  return value;
}

export function redactSupervisorEvent(event: SupervisorEvent): SupervisorEvent {
  return {
    ...event,
    summary: redactValue(event.summary) as string,
    payload: redactValue(event.payload) as Record<string, unknown>,
  };
}
