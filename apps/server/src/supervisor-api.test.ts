import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import type { AgentService } from "./agent-service.js";
import { createApp } from "./app.js";
import { loadConfig } from "./config.js";
import {
  createSupervisorEvent,
  type SupervisorCommand,
  type SupervisorEvent,
} from "./supervisor-contracts.js";
import { SupervisorLedger } from "./supervisor-ledger.js";
import type { SupervisorApiGateway } from "./supervisor-api.js";

const temporaryDirectories: string[] = [];
const openLedgers: SupervisorLedger[] = [];

afterEach(async () => {
  for (const ledger of openLedgers.splice(0)) ledger.close();
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { recursive: true, force: true })),
  );
});

const service = {
  listAgents: () => [],
  systemInfo: async () => ({}),
  simulateMissingHeartbeat: async (runId: string) => ({
    runId,
    agentId: randomUUID(),
    runtimeInstanceId: "default",
    pausedAt: "2026-08-30T10:00:09.000Z",
  }),
} as unknown as AgentService;

class RecordingGateway implements SupervisorApiGateway {
  readonly published: SupervisorCommand[] = [];
  failNextPublish = false;

  constructor(readonly ledger: SupervisorLedger) {}

  async publishCommand(command: SupervisorCommand): Promise<void> {
    if (this.failNextPublish) {
      this.failNextPublish = false;
      throw new Error("Kafka is unreachable");
    }
    this.published.push(command);
  }
}

async function makeLedger(): Promise<SupervisorLedger> {
  const root = await mkdtemp(path.join(tmpdir(), "supervisor-api-test-"));
  temporaryDirectories.push(root);
  const ledger = new SupervisorLedger(path.join(root, "ledger.sqlite"));
  openLedgers.push(ledger);
  await ledger.initialize();
  return ledger;
}

interface Fixture {
  runId: string;
  agentId: string;
  toolEvent: SupervisorEvent;
}

function seed(ledger: SupervisorLedger): Fixture {
  const runId = randomUUID();
  const agentId = randomUUID();
  const base = {
    runId,
    agentId,
    runtimeInstanceId: "default",
  } as const;
  ledger.recordEvent(
    createSupervisorEvent({
      ...base,
      type: "run.queued",
      occurredAt: "2026-08-30T10:00:00.000Z",
      source: "control-plane",
      severity: "info",
      summary: "Agent run queued",
      payload: {},
    }),
  );
  ledger.recordEvent(
    createSupervisorEvent({
      ...base,
      type: "run.started",
      occurredAt: "2026-08-30T10:00:01.000Z",
      source: "runtime",
      severity: "info",
      summary: "Agent Runtime started",
      payload: {},
    }),
  );
  const toolEvent = createSupervisorEvent({
    ...base,
    type: "run.tool_activity",
    occurredAt: "2026-08-30T10:00:05.000Z",
    source: "runtime",
    severity: "info",
    summary: "command_execution: cat .env",
    payload: { itemType: "command_execution", command: "cat .env" },
  });
  ledger.recordEvent(toolEvent);
  ledger.recordAlert({
    alertId: "alert-1",
    runId,
    eventId: toolEvent.eventId,
    ruleId: "secret-file-access",
    severity: "critical",
    evidence: "cat .env",
    createdAt: "2026-08-30T10:00:05.000Z",
  });
  return { runId, agentId, toolEvent };
}

async function makeApp(
  extraEnvironment: NodeJS.ProcessEnv = {},
): Promise<{
  app: Awaited<ReturnType<typeof createApp>>;
  gateway: RecordingGateway;
  fixture: Fixture;
}> {
  const ledger = await makeLedger();
  const fixture = seed(ledger);
  const gateway = new RecordingGateway(ledger);
  const app = await createApp(
    loadConfig({ NODE_ENV: "test", ...extraEnvironment }),
    service,
    gateway,
  );
  return { app, gateway, fixture };
}

describe("supervisor API", () => {
  it("reports counters, runs, timelines, and alert evidence", async () => {
    const { app, fixture } = await makeApp();

    const overview = await app.inject("/api/supervisor/overview");
    expect(overview.statusCode).toBe(200);
    expect(overview.json().overview.runs.running).toBe(1);
    expect(overview.json().overview.alerts.open).toBe(1);
    expect(overview.json().settings.stallAfterMs).toBe(8_000);

    const runs = await app.inject("/api/supervisor/runs");
    expect(runs.json().runs).toHaveLength(1);
    expect(runs.json().runs[0].runId).toBe(fixture.runId);
    expect(runs.json().runs[0].heartbeatOverdue).toBe(true);

    const filtered = await app.inject("/api/supervisor/runs?state=completed");
    expect(filtered.json().runs).toHaveLength(0);

    const timeline = await app.inject(
      "/api/supervisor/runs/" + fixture.runId + "/events",
    );
    expect(timeline.json().events.map((event: SupervisorEvent) => event.type)).toEqual([
      "run.queued",
      "run.started",
      "run.tool_activity",
    ]);

    const alerts = await app.inject("/api/supervisor/alerts");
    expect(alerts.json().alerts).toHaveLength(1);
    expect(alerts.json().alerts[0].ruleId).toBe("secret-file-access");
    expect(alerts.json().alerts[0].event.eventId).toBe(fixture.toolEvent.eventId);

    const search = await app.inject("/api/supervisor/events?text=.env");
    expect(search.json().events).toHaveLength(1);

    await app.close();
  });

  it("answers 404 for a run the ledger has never seen", async () => {
    const { app } = await makeApp();
    const missing = await app.inject("/api/supervisor/runs/" + randomUUID());
    expect(missing.statusCode).toBe(404);
    await app.close();
  });

  it("publishes an operator cancellation and refuses terminal runs", async () => {
    const { app, gateway, fixture } = await makeApp();

    const accepted = await app.inject({
      method: "POST",
      url: "/api/supervisor/runs/" + fixture.runId + "/cancel",
      payload: { reason: "Operator stopped the demo run" },
    });
    expect(accepted.statusCode).toBe(202);
    expect(accepted.json().published).toBe(true);
    expect(gateway.published).toHaveLength(1);
    expect(gateway.published[0]?.source).toBe("operator");
    expect(gateway.published[0]?.runId).toBe(fixture.runId);

    gateway.ledger.recordEvent(
      createSupervisorEvent({
        type: "run.cancelled",
        occurredAt: "2026-08-30T10:00:20.000Z",
        runId: fixture.runId,
        agentId: fixture.agentId,
        runtimeInstanceId: "default",
        source: "control-plane",
        severity: "warning",
        summary: "Agent run cancelled",
        payload: {},
      }),
    );
    const conflict = await app.inject({
      method: "POST",
      url: "/api/supervisor/runs/" + fixture.runId + "/cancel",
    });
    expect(conflict.statusCode).toBe(409);
    await app.close();
  });

  it("queues the cancellation in the outbox when Kafka is unreachable", async () => {
    const { app, gateway, fixture } = await makeApp();
    gateway.failNextPublish = true;

    const accepted = await app.inject({
      method: "POST",
      url: "/api/supervisor/runs/" + fixture.runId + "/cancel",
    });
    expect(accepted.statusCode).toBe(202);
    expect(accepted.json().published).toBe(false);
    expect(gateway.published).toHaveLength(0);

    const pending = gateway.ledger.listPendingCommands(
      new Date(Date.now() + 1_000).toISOString(),
    );
    expect(pending).toHaveLength(1);
    expect(pending[0]?.command.commandId).toBe(accepted.json().commandId);
    await app.close();
  });

  it("exposes the demo control only when ENABLE_DEMO_CONTROLS is true", async () => {
    const disabled = await makeApp();
    const hidden = await disabled.app.inject({
      method: "POST",
      url: "/api/supervisor/runs/" + disabled.fixture.runId + "/simulate-stall",
    });
    expect(hidden.statusCode).toBe(404);
    await disabled.app.close();

    const enabled = await makeApp({ ENABLE_DEMO_CONTROLS: "true" });
    const accepted = await enabled.app.inject({
      method: "POST",
      url: "/api/supervisor/runs/" + enabled.fixture.runId + "/simulate-stall",
    });
    expect(accepted.statusCode).toBe(202);
    expect(accepted.json().stallAfterMs).toBe(8_000);
    await enabled.app.close();
  });

  it("reports 503 while the supervisor is disabled", async () => {
    const app = await createApp(loadConfig({ NODE_ENV: "test" }), service, null);
    const response = await app.inject("/api/supervisor/overview");
    expect(response.statusCode).toBe(503);
    await app.close();
  });
});
