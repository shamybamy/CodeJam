import { z } from "zod";
import type { AppConfig } from "./config.js";
import type {
  SupervisorEventRecord,
  SupervisorLedger,
} from "./supervisor-ledger.js";
import { supervisorEventTypeSchema } from "./supervisor-contracts.js";
import { toRunView } from "./supervisor-api.js";

/**
 * The complete tool surface the operator chatbot may reach. Every tool is
 * read-only and answers from the ledger; the chatbot has no SQL, shell, Kafka,
 * or Docker access, and no tool outside this registry can be invoked.
 */
export interface SupervisorChatCitation {
  runId: string;
  eventId?: string;
  alertId?: string;
  occurredAt: string;
  label: string;
}

export interface SupervisorToolResult {
  data: unknown;
  citations: SupervisorChatCitation[];
}

export interface SupervisorToolContext {
  ledger: SupervisorLedger;
  config: Pick<AppConfig, "supervisorStallAfterMs">;
}

export interface SupervisorChatTool<Schema extends z.ZodTypeAny = z.ZodTypeAny> {
  name: string;
  description: string;
  schema: Schema;
  run(
    context: SupervisorToolContext,
    args: z.infer<Schema>,
  ): SupervisorToolResult;
}

const runStateSchema = z.enum([
  "queued",
  "running",
  "completed",
  "failed",
  "cancelled",
]);
const runHealthSchema = z.enum(["pending", "healthy", "stalled", "terminal"]);
const severitySchema = z.enum(["info", "warning", "critical"]);

function compactEvent(event: SupervisorEventRecord): Record<string, unknown> {
  const command =
    typeof event.payload.command === "string" ? event.payload.command : null;
  return {
    eventId: event.eventId,
    runId: event.runId,
    type: event.type,
    occurredAt: event.occurredAt,
    severity: event.severity,
    source: event.source,
    summary: event.summary.slice(0, 300),
    ...(command ? { command: command.slice(0, 300) } : {}),
  };
}

function eventCitations(
  events: SupervisorEventRecord[],
): SupervisorChatCitation[] {
  return events.map((event) => ({
    runId: event.runId,
    eventId: event.eventId,
    occurredAt: event.occurredAt,
    label: event.type + " · run " + event.runId.slice(0, 8),
  }));
}

const getSystemOverview: SupervisorChatTool<z.ZodObject<Record<string, never>>> = {
  name: "getSystemOverview",
  description:
    "Counts of runs by state, run health, stored events, and open alerts.",
  schema: z.object({}),
  run: ({ ledger }) => ({ data: ledger.getOverview(), citations: [] }),
};

const listRunsSchema = z.object({
  state: runStateSchema.optional(),
  health: runHealthSchema.optional(),
  limit: z.number().int().min(1).max(50).optional(),
});

const listRuns: SupervisorChatTool<typeof listRunsSchema> = {
  name: "listRuns",
  description:
    "Recent runs with their state, health, heartbeat age, and latest summary. Filter by state or health.",
  schema: listRunsSchema,
  run: ({ ledger, config }, args) => {
    const nowMs = Date.now();
    const runs = ledger
      .listRuns(50)
      .filter((run) => !args.state || run.state === args.state)
      .filter((run) => !args.health || run.health === args.health)
      .slice(0, args.limit ?? 20)
      .map((run) => toRunView(run, config, nowMs));
    return {
      data: runs.map((run) => ({
        runId: run.runId,
        agentId: run.agentId,
        state: run.state,
        health: run.health,
        startedAt: run.startedAt,
        endedAt: run.endedAt,
        lastHeartbeatAgeMs: run.lastHeartbeatAgeMs,
        lastSummary: run.lastSummary.slice(0, 200),
      })),
      citations: runs.map((run) => ({
        runId: run.runId,
        occurredAt: run.lastEventAt,
        label: "run " + run.runId.slice(0, 8) + " · " + run.state,
      })),
    };
  },
};

const getRunTimelineSchema = z.object({
  runId: z.string().uuid(),
  limit: z.number().int().min(1).max(100).optional(),
});

const getRunTimeline: SupervisorChatTool<typeof getRunTimelineSchema> = {
  name: "getRunTimeline",
  description: "Every stored event for one run, oldest first.",
  schema: getRunTimelineSchema,
  run: ({ ledger }, args) => {
    const events = ledger.listEvents(args.runId, args.limit ?? 60);
    return {
      data: events.map(compactEvent),
      citations: eventCitations(events.slice(-8)),
    };
  },
};

const searchEventsSchema = z.object({
  text: z.string().min(1).max(120).optional(),
  runId: z.string().uuid().optional(),
  type: supervisorEventTypeSchema.optional(),
  severity: severitySchema.optional(),
  limit: z.number().int().min(1).max(50).optional(),
});

const searchEvents: SupervisorChatTool<typeof searchEventsSchema> = {
  name: "searchEvents",
  description:
    "Search stored events by free text, run, event type, or severity. Newest first.",
  schema: searchEventsSchema,
  run: ({ ledger }, args) => {
    const events = ledger.searchEvents({
      text: args.text,
      runId: args.runId,
      types: args.type ? [args.type] : undefined,
      severities: args.severity ? [args.severity] : undefined,
      limit: args.limit ?? 20,
    });
    return { data: events.map(compactEvent), citations: eventCitations(events) };
  },
};

const listAlertsSchema = z.object({
  runId: z.string().uuid().optional(),
  limit: z.number().int().min(1).max(50).optional(),
});

const listAlerts: SupervisorChatTool<typeof listAlertsSchema> = {
  name: "listAlerts",
  description:
    "Suspicious-activity alerts raised by the deterministic rules, with the rule, the evidence, and the event that triggered them.",
  schema: listAlertsSchema,
  run: ({ ledger }, args) => {
    const alerts = ledger.listAlerts({
      runId: args.runId,
      limit: args.limit ?? 20,
    });
    const events = new Map(
      ledger
        .getEventsByIds(alerts.map((alert) => alert.eventId))
        .map((event) => [event.eventId, event] as const),
    );
    return {
      data: alerts.map((alert) => ({
        alertId: alert.alertId,
        runId: alert.runId,
        ruleId: alert.ruleId,
        severity: alert.severity,
        status: alert.status,
        createdAt: alert.createdAt,
        evidence: alert.evidence,
        triggeringEvent: events.get(alert.eventId)
          ? compactEvent(events.get(alert.eventId) as SupervisorEventRecord)
          : { eventId: alert.eventId },
      })),
      citations: alerts.map((alert) => ({
        runId: alert.runId,
        eventId: alert.eventId,
        alertId: alert.alertId,
        occurredAt: alert.createdAt,
        label: alert.ruleId + " · run " + alert.runId.slice(0, 8),
      })),
    };
  },
};

const getRunHealthSchema = z.object({ runId: z.string().uuid() });

const getRunHealth: SupervisorChatTool<typeof getRunHealthSchema> = {
  name: "getRunHealth",
  description:
    "Current state, health, heartbeat age, and open alert count for one run.",
  schema: getRunHealthSchema,
  run: ({ ledger, config }, args) => {
    const run = ledger.getRun(args.runId);
    if (!run) {
      return { data: { runId: args.runId, found: false }, citations: [] };
    }
    const view = toRunView(run, config);
    const alerts = ledger.listAlerts({ runId: args.runId, limit: 20 });
    return {
      data: {
        found: true,
        ...view,
        openAlerts: alerts.filter((alert) => alert.status === "open").length,
        alertRules: [...new Set(alerts.map((alert) => alert.ruleId))],
      },
      citations: [
        {
          runId: run.runId,
          occurredAt: run.lastEventAt,
          label: "run " + run.runId.slice(0, 8) + " · " + run.health,
        },
      ],
    };
  },
};

export const SUPERVISOR_CHAT_TOOLS: SupervisorChatTool[] = [
  getSystemOverview,
  listRuns,
  getRunTimeline,
  searchEvents,
  listAlerts,
  getRunHealth,
];

export function findChatTool(name: string): SupervisorChatTool | null {
  return SUPERVISOR_CHAT_TOOLS.find((tool) => tool.name === name) ?? null;
}

/**
 * Keyword plan used when the model does not choose a valid tool. It keeps the
 * chatbot answering from evidence even with a small local model.
 */
export function planToolCalls(
  question: string,
  runId?: string,
): { tool: string; arguments: Record<string, unknown> }[] {
  const text = question.toLowerCase();
  const uuid = question.match(
    /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i,
  )?.[0];
  const target = uuid ?? runId;
  const suspicious =
    /suspicious|malicious|attack|exfiltrat|secret|credential|escape|privilege|alert|dangerous|intent/.test(
      text,
    );
  const plan: { tool: string; arguments: Record<string, unknown> }[] = [];

  if (suspicious) {
    plan.push({ tool: "listAlerts", arguments: {} });
    plan.push({
      tool: "searchEvents",
      arguments: { type: "alert.raised", limit: 20 },
    });
    return plan;
  }
  if (target) {
    plan.push({ tool: "getRunHealth", arguments: { runId: target } });
    plan.push({ tool: "getRunTimeline", arguments: { runId: target } });
    return plan;
  }
  plan.push({ tool: "getSystemOverview", arguments: {} });
  plan.push({ tool: "listRuns", arguments: {} });
  if (/stall|stuck|hung|unhealthy|heartbeat|fail/.test(text)) {
    plan.push({
      tool: "searchEvents",
      arguments: { type: "supervisor.stalled", limit: 20 },
    });
  }
  return plan;
}
