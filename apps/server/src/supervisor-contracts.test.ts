import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import {
  createSupervisorCommand,
  createSupervisorEvent,
  supervisorCommandSchema,
  supervisorEventSchema,
} from "./supervisor-contracts.js";
import { redactSupervisorEvent } from "./supervisor-redaction.js";

describe("supervisor contracts", () => {
  it("creates versioned events and commands", () => {
    const runId = randomUUID();
    const agentId = randomUUID();
    const event = createSupervisorEvent({
      type: "run.queued",
      runId,
      agentId,
      source: "control-plane",
      severity: "info",
      summary: "Run queued",
      payload: {},
    });
    const command = createSupervisorCommand({
      type: "run.cancel",
      runId,
      agentId,
      runtimeInstanceId: "runtime-1",
      source: "operator",
      reason: "Operator requested cancellation",
    });

    expect(supervisorEventSchema.parse(event)).toEqual(event);
    expect(supervisorCommandSchema.parse(command)).toEqual(command);
    expect(event.schemaVersion).toBe(1);
    expect(command.schemaVersion).toBe(1);
  });

  it("redacts sensitive values recursively", () => {
    const event = createSupervisorEvent({
      type: "run.tool_activity",
      runId: randomUUID(),
      agentId: randomUUID(),
      source: "runtime",
      severity: "info",
      summary: "Called with Bearer abcdefghijklmnop",
      payload: {
        authorization: "Bearer visible-secret",
        nested: { apiKey: "key-abcdefghijklmnop", safe: "keep-me" },
      },
    });

    const redacted = redactSupervisorEvent(event);
    expect(redacted.summary).toBe("Called with Bearer [REDACTED]");
    expect(redacted.payload).toEqual({
      authorization: "[REDACTED]",
      nested: { apiKey: "[REDACTED]", safe: "keep-me" },
    });
  });
});
