import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  createSupervisorCommand,
  createSupervisorEvent,
} from "./supervisor-contracts.js";
import { SupervisorLedger } from "./supervisor-ledger.js";

const temporaryDirectories: string[] = [];
const openLedgers: SupervisorLedger[] = [];

afterEach(async () => {
  for (const ledger of openLedgers.splice(0)) ledger.close();
  await Promise.all(
    temporaryDirectories.splice(0).map((directory) =>
      rm(directory, { recursive: true, force: true }),
    ),
  );
});

async function makeLedger(): Promise<SupervisorLedger> {
  const root = await mkdtemp(path.join(tmpdir(), "supervisor-ledger-test-"));
  temporaryDirectories.push(root);
  const ledger = new SupervisorLedger(path.join(root, "ledger.sqlite"));
  openLedgers.push(ledger);
  await ledger.initialize();
  return ledger;
}

describe("supervisor alerts", () => {
  it("counts repeats of one alert instead of storing duplicates", async () => {
    const ledger = await makeLedger();
    const runId = randomUUID();
    const alert = {
      alertId: "grouped-alert",
      runId,
      eventId: randomUUID(),
      ruleId: "secret-file-access",
      severity: "critical" as const,
      evidence: "cat .env",
      createdAt: "2026-08-30T10:00:00.000Z",
    };

    expect(ledger.recordAlert(alert)).toBe(true);
    expect(
      ledger.recordAlert({
        ...alert,
        eventId: randomUUID(),
        createdAt: "2026-08-30T10:00:04.000Z",
      }),
    ).toBe(false);

    const stored = ledger.listAlerts({ runId });
    expect(stored).toHaveLength(1);
    expect(stored[0]?.occurrences).toBe(2);
    expect(stored[0]?.eventId).toBe(alert.eventId);
    expect(stored[0]?.lastSeenAt).toBe("2026-08-30T10:00:04.000Z");
  });
});

describe("supervisor ledger", () => {
  it("stores each event once and materializes run health", async () => {
    const ledger = await makeLedger();
    const runId = randomUUID();
    const agentId = randomUUID();
    const queued = createSupervisorEvent({
      eventId: randomUUID(),
      occurredAt: "2026-08-30T10:00:00.000Z",
      type: "run.queued",
      runId,
      agentId,
      source: "control-plane",
      severity: "info",
      summary: "Run queued",
      payload: { token: "must-not-persist" },
    });
    const started = createSupervisorEvent({
      occurredAt: "2026-08-30T10:00:01.000Z",
      type: "run.started",
      runId,
      agentId,
      runtimeInstanceId: "runtime-1",
      source: "runtime",
      severity: "info",
      summary: "Runtime started",
      payload: {},
    });
    const heartbeat = createSupervisorEvent({
      occurredAt: "2026-08-30T10:00:03.000Z",
      type: "runtime.heartbeat",
      runId,
      agentId,
      runtimeInstanceId: "runtime-1",
      source: "runtime",
      severity: "info",
      summary: "Runtime heartbeat",
      payload: {},
    });

    expect(
      ledger.recordEvent(queued, { topic: "events", partition: 0, offset: "0" }),
    ).toBe(true);
    expect(
      ledger.recordEvent(queued, { topic: "events", partition: 0, offset: "0" }),
    ).toBe(false);
    ledger.recordEvent(started, { topic: "events", partition: 0, offset: "1" });
    ledger.recordEvent(heartbeat, { topic: "events", partition: 0, offset: "2" });

    expect(ledger.listEvents(runId)).toHaveLength(3);
    expect(ledger.listEvents(runId)[0]?.payload).toEqual({ token: "[REDACTED]" });
    expect(ledger.getRun(runId)).toMatchObject({
      state: "running",
      health: "healthy",
      runtimeInstanceId: "runtime-1",
      lastHeartbeatAt: "2026-08-30T10:00:03.000Z",
    });
  });

  it("does not let an older replay regress a terminal run", async () => {
    const ledger = await makeLedger();
    const runId = randomUUID();
    const agentId = randomUUID();
    ledger.recordEvent(
      createSupervisorEvent({
        occurredAt: "2026-08-30T10:00:10.000Z",
        type: "run.completed",
        runId,
        agentId,
        source: "control-plane",
        severity: "info",
        summary: "Run completed",
        payload: {},
      }),
    );
    ledger.recordEvent(
      createSupervisorEvent({
        occurredAt: "2026-08-30T10:00:01.000Z",
        type: "run.started",
        runId,
        agentId,
        source: "runtime",
        severity: "info",
        summary: "Delayed start replay",
        payload: {},
      }),
    );

    expect(ledger.getRun(runId)).toMatchObject({
      state: "completed",
      health: "terminal",
      endedAt: "2026-08-30T10:00:10.000Z",
    });
  });

  it("marks commands idempotently", async () => {
    const ledger = await makeLedger();
    const command = createSupervisorCommand({
      type: "run.cancel",
      runId: randomUUID(),
      agentId: randomUUID(),
      runtimeInstanceId: "runtime-1",
      source: "operator",
      reason: "test",
    });

    expect(ledger.markCommandProcessed(command)).toBe(true);
    expect(ledger.markCommandProcessed(command)).toBe(false);
    expect(ledger.isCommandProcessed(command.commandId)).toBe(true);
  });
});
