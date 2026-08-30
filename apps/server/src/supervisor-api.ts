import type { FastifyInstance } from "fastify";
import { z } from "zod";
import type { AgentService } from "./agent-service.js";
import type { AppConfig } from "./config.js";
import { HttpError } from "./errors.js";
import {
  createSupervisorCommand,
  supervisorEventTypeSchema,
  type SupervisorCommand,
} from "./supervisor-contracts.js";
import type {
  SupervisorAlertRecord,
  SupervisorEventRecord,
  SupervisorLedger,
  SupervisorRunRecord,
} from "./supervisor-ledger.js";

/**
 * The read side of the supervisor plus a command publisher. SupervisorCoordinator
 * satisfies this structurally; tests can supply an in-memory double.
 */
export interface SupervisorApiGateway {
  readonly ledger: SupervisorLedger;
  publishCommand(command: SupervisorCommand): Promise<void>;
}

export interface SupervisorRunView extends SupervisorRunRecord {
  lastHeartbeatAgeMs: number | null;
  heartbeatOverdue: boolean;
}

export interface SupervisorAlertView extends SupervisorAlertRecord {
  event: {
    eventId: string;
    type: SupervisorEventRecord["type"];
    occurredAt: string;
    severity: SupervisorEventRecord["severity"];
    summary: string;
  } | null;
}

const runIdParams = z.object({ runId: z.string().uuid() });
const listRunsQuery = z.object({
  state: z
    .enum(["queued", "running", "completed", "failed", "cancelled"])
    .optional(),
  health: z.enum(["pending", "healthy", "stalled", "terminal"]).optional(),
  limit: z.coerce.number().int().min(1).max(500).default(100),
});
const eventsQuery = z.object({
  limit: z.coerce.number().int().min(1).max(500).default(200),
});
const alertsQuery = z.object({
  runId: z.string().uuid().optional(),
  limit: z.coerce.number().int().min(1).max(500).default(50),
});
const searchEventsQuery = z.object({
  runId: z.string().uuid().optional(),
  agentId: z.string().uuid().optional(),
  type: z
    .union([supervisorEventTypeSchema, z.array(supervisorEventTypeSchema)])
    .optional(),
  severity: z
    .union([
      z.enum(["info", "warning", "critical"]),
      z.array(z.enum(["info", "warning", "critical"])),
    ])
    .optional(),
  text: z.string().trim().min(1).max(200).optional(),
  since: z.string().datetime({ offset: true }).optional(),
  limit: z.coerce.number().int().min(1).max(200).default(50),
});
const cancelBody = z
  .object({ reason: z.string().trim().min(1).max(500).optional() })
  .optional();

function toArray<T>(value: T | T[] | undefined): T[] | undefined {
  if (value === undefined) return undefined;
  return Array.isArray(value) ? value : [value];
}

export function toRunView(
  run: SupervisorRunRecord,
  config: Pick<AppConfig, "supervisorStallAfterMs">,
  nowMs = Date.now(),
): SupervisorRunView {
  const reference = run.lastHeartbeatAt ?? run.startedAt;
  const lastHeartbeatAgeMs =
    run.health === "terminal" || !reference
      ? null
      : Math.max(0, nowMs - Date.parse(reference));
  return {
    ...run,
    lastHeartbeatAgeMs,
    heartbeatOverdue:
      lastHeartbeatAgeMs !== null &&
      lastHeartbeatAgeMs > config.supervisorStallAfterMs,
  };
}

export async function registerSupervisorRoutes(
  app: FastifyInstance,
  config: AppConfig,
  service: AgentService,
  supervisor: SupervisorApiGateway | null,
): Promise<void> {
  const requireSupervisor = (): SupervisorApiGateway => {
    if (!supervisor) {
      throw new HttpError(
        503,
        "The run supervisor is disabled. Set KAFKA_ENABLED=true and restart.",
      );
    }
    return supervisor;
  };

  const attachEvents = (
    alerts: SupervisorAlertRecord[],
    ledger: SupervisorLedger,
  ): SupervisorAlertView[] => {
    const events = new Map(
      ledger
        .getEventsByIds(alerts.map((alert) => alert.eventId))
        .map((event) => [event.eventId, event] as const),
    );
    return alerts.map((alert) => {
      const event = events.get(alert.eventId);
      return {
        ...alert,
        event: event
          ? {
              eventId: event.eventId,
              type: event.type,
              occurredAt: event.occurredAt,
              severity: event.severity,
              summary: event.summary,
            }
          : null,
      };
    });
  };

  app.get("/api/supervisor/overview", async () => {
    const { ledger } = requireSupervisor();
    return {
      overview: ledger.getOverview(),
      settings: {
        stallAfterMs: config.supervisorStallAfterMs,
        heartbeatIntervalMs: config.supervisorHeartbeatIntervalMs,
        watchdogIntervalMs: config.supervisorWatchdogIntervalMs,
        demoControlsEnabled: config.demoControlsEnabled,
        runtimeInstanceId: config.runtimeInstanceId,
      },
    };
  });

  app.get("/api/supervisor/runs", async (request) => {
    const { ledger } = requireSupervisor();
    const query = listRunsQuery.parse(request.query);
    const nowMs = Date.now();
    const runs = ledger
      .listRuns(query.limit)
      .filter((run) => !query.state || run.state === query.state)
      .filter((run) => !query.health || run.health === query.health)
      .map((run) => toRunView(run, config, nowMs));
    return { runs };
  });

  app.get("/api/supervisor/runs/:runId", async (request) => {
    const { ledger } = requireSupervisor();
    const { runId } = runIdParams.parse(request.params);
    const run = ledger.getRun(runId);
    if (!run) throw new HttpError(404, "Run not found in the supervisor ledger");
    return {
      run: toRunView(run, config),
      alerts: attachEvents(ledger.listAlerts({ runId }), ledger),
    };
  });

  app.get("/api/supervisor/runs/:runId/events", async (request) => {
    const { ledger } = requireSupervisor();
    const { runId } = runIdParams.parse(request.params);
    const { limit } = eventsQuery.parse(request.query);
    if (!ledger.getRun(runId)) {
      throw new HttpError(404, "Run not found in the supervisor ledger");
    }
    return { events: ledger.listEvents(runId, limit) };
  });

  app.get("/api/supervisor/events", async (request) => {
    const { ledger } = requireSupervisor();
    const query = searchEventsQuery.parse(request.query);
    return {
      events: ledger.searchEvents({
        runId: query.runId,
        agentId: query.agentId,
        types: toArray(query.type),
        severities: toArray(query.severity),
        text: query.text,
        since: query.since,
        limit: query.limit,
      }),
    };
  });

  app.get("/api/supervisor/alerts", async (request) => {
    const { ledger } = requireSupervisor();
    const query = alertsQuery.parse(request.query);
    return {
      alerts: attachEvents(
        ledger.listAlerts({ runId: query.runId, limit: query.limit }),
        ledger,
      ),
    };
  });

  app.post("/api/supervisor/runs/:runId/cancel", async (request, reply) => {
    const gateway = requireSupervisor();
    const { runId } = runIdParams.parse(request.params);
    const body = cancelBody.parse(request.body ?? {});
    const run = gateway.ledger.getRun(runId);
    if (!run) throw new HttpError(404, "Run not found in the supervisor ledger");
    if (run.health === "terminal") {
      throw new HttpError(409, "This run has already reached a terminal state");
    }
    if (!run.runtimeInstanceId) {
      throw new HttpError(
        409,
        "This run has no recorded Runtime instance; cancellation cannot verify the target",
      );
    }
    const command = createSupervisorCommand({
      type: "run.cancel",
      runId: run.runId,
      agentId: run.agentId,
      runtimeInstanceId: run.runtimeInstanceId,
      source: "operator",
      reason: body?.reason ?? "Operator requested cancellation from the dashboard",
    });
    let published = true;
    try {
      await gateway.publishCommand(command);
    } catch (error) {
      // Kafka is unreachable: keep the command durable so the watchdog retries.
      published = false;
      gateway.ledger.enqueueCommand(command);
      request.log.warn(
        { err: error, commandId: command.commandId },
        "Cancellation command queued for retry",
      );
    }
    return reply.code(202).send({
      commandId: command.commandId,
      runId: command.runId,
      published,
    });
  });

  if (config.demoControlsEnabled) {
    app.post(
      "/api/supervisor/runs/:runId/simulate-stall",
      async (request, reply) => {
        requireSupervisor();
        const { runId } = runIdParams.parse(request.params);
        const result = await service.simulateMissingHeartbeat(runId);
        return reply.code(202).send({
          ...result,
          stallAfterMs: config.supervisorStallAfterMs,
        });
      },
    );
  }
}
