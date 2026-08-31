import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { loadConfig } from "./config.js";
import {
  createSupervisorCommand,
  createSupervisorEvent,
} from "./supervisor-contracts.js";
import { SupervisorCoordinator } from "./supervisor-coordinator.js";

const kafkaDescribe = process.env.RUN_KAFKA_INTEGRATION === "1" ? describe : describe.skip;

kafkaDescribe("Kafka supervisor integration", () => {
  let root: string;
  let coordinator: SupervisorCoordinator;

  beforeAll(async () => {
    root = await mkdtemp(path.join(tmpdir(), "supervisor-kafka-test-"));
    const suffix = randomUUID();
    const config = loadConfig({
      NODE_ENV: "test",
      KAFKA_BROKERS: process.env.KAFKA_BROKERS ?? "127.0.0.1:29092",
      KAFKA_CLIENT_ID: "supervisor-integration-" + suffix,
      // Topics are per-run as well as the consumer groups. Sharing the real
      // topics let this test's synthetic run reach a running control plane,
      // which materialised it in the operator's ledger and left it stalled
      // forever, because no live Runtime owns "integration-runtime".
      KAFKA_EVENTS_TOPIC: "agent-run-events-test-" + suffix,
      KAFKA_COMMANDS_TOPIC: "agent-run-commands-test-" + suffix,
      KAFKA_EVENT_CONSUMER_GROUP: "supervisor-ledger-test-" + suffix,
      KAFKA_COMMAND_CONSUMER_GROUP: "supervisor-commands-test-" + suffix,
      SUPERVISOR_LEDGER_PATH: path.join(root, "ledger.sqlite"),
    });
    coordinator = new SupervisorCoordinator(config);
    await coordinator.start();
  }, 30_000);

  afterAll(async () => {
    await coordinator?.stop();
    if (root) await rm(root, { recursive: true, force: true });
  });

  it("round-trips ordered events and an idempotent command", async () => {
    const runId = randomUUID();
    const agentId = randomUUID();
    const queued = createSupervisorEvent({
      type: "run.queued",
      runId,
      agentId,
      source: "control-plane",
      severity: "info",
      summary: "Integration run queued",
      payload: {},
    });
    const started = createSupervisorEvent({
      type: "run.started",
      runId,
      agentId,
      runtimeInstanceId: "integration-runtime",
      source: "runtime",
      severity: "info",
      summary: "Integration runtime started",
      payload: {},
    });

    await coordinator.publishEvent(queued);
    await coordinator.publishEvent(started);
    await coordinator.publishEvent(started);

    await expect
      .poll(() => coordinator.ledger.listEvents(runId).length, { timeout: 10_000 })
      .toBe(2);
    expect(coordinator.ledger.getRun(runId)).toMatchObject({
      state: "running",
      health: "healthy",
    });

    const command = createSupervisorCommand({
      type: "run.cancel",
      runId,
      agentId,
      runtimeInstanceId: "integration-runtime",
      source: "operator",
      reason: "Integration cancellation",
    });
    await coordinator.publishCommand(command);
    await coordinator.publishCommand(command);
    await expect
      .poll(() => coordinator.ledger.isCommandProcessed(command.commandId), {
        timeout: 10_000,
      })
      .toBe(true);
  });
});
