import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import {
  createSupervisorEvent,
  type SupervisorCommand,
  type SupervisorEvent,
} from "./supervisor-contracts.js";
import { SupervisorLedger } from "./supervisor-ledger.js";
import { SupervisorWatchdog } from "./supervisor-watchdog.js";

const runId = "10000000-0000-4000-8000-000000000001";
const agentId = "20000000-0000-4000-8000-000000000002";
const runtimeInstanceId = "watchdog-test";

describe("Supervisor watchdog", () => {
  it("claims one stalled run and publishes exactly one cancellation", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "supervisor-watchdog-"));
    const ledger = new SupervisorLedger(path.join(directory, "ledger.sqlite"));
    await ledger.initialize();
    try {
      const record = (
        type: "run.started" | "runtime.heartbeat",
        occurredAt: string,
      ) =>
        ledger.recordEvent(
          createSupervisorEvent({
            type,
            occurredAt,
            runId,
            agentId,
            runtimeInstanceId,
            source: type === "run.started" ? "control-plane" : "runtime",
            severity: "info",
            summary: type,
            payload: {},
          }),
        );
      record("run.started", "2026-08-30T00:00:00.000Z");
      record("runtime.heartbeat", "2026-08-30T00:00:00.000Z");
      record("runtime.heartbeat", "2026-08-30T00:00:02.000Z");
      record("runtime.heartbeat", "2026-08-30T00:00:04.000Z");

      let current = new Date("2026-08-30T00:00:12.001Z");
      const publishedEvents: SupervisorEvent[] = [];
      const publishedCommands: SupervisorCommand[] = [];
      const watchdog = new SupervisorWatchdog(
        { supervisorStallAfterMs: 8_000, supervisorWatchdogIntervalMs: 1_000 },
        ledger,
        {
          publishEvent: async (event) => void publishedEvents.push(event),
          publishCommand: async (command) => void publishedCommands.push(command),
        },
        () => current,
      );

      await watchdog.tick();
      expect(publishedEvents.map((event) => event.type)).toEqual([
        "supervisor.stalled",
      ]);
      expect(publishedCommands).toHaveLength(1);
      expect(publishedCommands[0]).toMatchObject({
        type: "run.cancel",
        runId,
        agentId,
        runtimeInstanceId,
      });
      expect(ledger.getRun(runId)?.health).toBe("stalled");

      current = new Date("2026-08-30T00:00:20.000Z");
      await watchdog.tick();
      expect(publishedEvents).toHaveLength(1);
      expect(publishedCommands).toHaveLength(1);

      const command = publishedCommands[0];
      expect(command).toBeDefined();
      if (command) ledger.markCommandProcessed(command);
      ledger.recordEvent(
        createSupervisorEvent({
          type: "run.cancelled",
          occurredAt: current.toISOString(),
          runId,
          agentId,
          runtimeInstanceId,
          source: "control-plane",
          severity: "warning",
          summary: "Run cancelled",
          payload: {},
        }),
      );
      expect(ledger.getRun(runId)).toMatchObject({
        state: "cancelled",
        health: "terminal",
      });
    } finally {
      ledger.close();
      await rm(directory, { recursive: true, force: true });
    }
  });

  it("retries an outbox command with the same id after publication fails", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "supervisor-outbox-"));
    const ledger = new SupervisorLedger(path.join(directory, "ledger.sqlite"));
    await ledger.initialize();
    try {
      ledger.recordEvent(
        createSupervisorEvent({
          type: "run.started",
          occurredAt: "2026-08-30T00:00:00.000Z",
          runId,
          agentId,
          runtimeInstanceId,
          source: "control-plane",
          severity: "info",
          summary: "Run started",
          payload: {},
        }),
      );
      let current = new Date("2026-08-30T00:00:09.000Z");
      let attempts = 0;
      const commandIds: string[] = [];
      const watchdog = new SupervisorWatchdog(
        { supervisorStallAfterMs: 8_000, supervisorWatchdogIntervalMs: 1_000 },
        ledger,
        {
          publishEvent: async () => undefined,
          publishCommand: async (command) => {
            attempts += 1;
            commandIds.push(command.commandId);
            if (attempts === 1) throw new Error("Kafka unavailable");
          },
        },
        () => current,
      );

      await watchdog.tick();
      current = new Date("2026-08-30T00:00:10.001Z");
      await watchdog.tick();

      expect(attempts).toBe(2);
      expect(new Set(commandIds).size).toBe(1);
      expect(ledger.listPendingCommands(current.toISOString())).toHaveLength(0);
    } finally {
      ledger.close();
      await rm(directory, { recursive: true, force: true });
    }
  });
});
