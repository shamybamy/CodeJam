import { randomUUID } from "node:crypto";
import { z } from "zod";

export const SUPERVISOR_SCHEMA_VERSION = 1 as const;

export const supervisorEventTypeSchema = z.enum([
  "run.queued",
  "run.started",
  "runtime.heartbeat",
  "runtime.exited",
  "run.tool_activity",
  "run.completed",
  "run.failed",
  "run.cancelled",
  "supervisor.stalled",
  "supervisor.demo_paused",
  "alert.raised",
  "supervisor.recovered",
]);

export const supervisorEventSchema = z.object({
  schemaVersion: z.literal(SUPERVISOR_SCHEMA_VERSION),
  eventId: z.string().uuid(),
  type: supervisorEventTypeSchema,
  occurredAt: z.string().datetime({ offset: true }),
  runId: z.string().uuid(),
  agentId: z.string().uuid(),
  runtimeInstanceId: z.string().min(1).max(128).optional(),
  source: z.enum(["control-plane", "runtime", "supervisor", "operator"]),
  severity: z.enum(["info", "warning", "critical"]),
  summary: z.string().trim().min(1).max(2_000),
  payload: z.record(z.string(), z.unknown()),
});

export type SupervisorEvent = z.infer<typeof supervisorEventSchema>;
export type SupervisorEventType = z.infer<typeof supervisorEventTypeSchema>;

export function createSupervisorEvent(
  input: Omit<SupervisorEvent, "schemaVersion" | "eventId" | "occurredAt"> & {
    eventId?: string;
    occurredAt?: string;
  },
): SupervisorEvent {
  return supervisorEventSchema.parse({
    ...input,
    schemaVersion: SUPERVISOR_SCHEMA_VERSION,
    eventId: input.eventId ?? randomUUID(),
    occurredAt: input.occurredAt ?? new Date().toISOString(),
  });
}

export const supervisorCommandSchema = z.object({
  schemaVersion: z.literal(SUPERVISOR_SCHEMA_VERSION),
  commandId: z.string().uuid(),
  type: z.literal("run.cancel"),
  requestedAt: z.string().datetime({ offset: true }),
  runId: z.string().uuid(),
  agentId: z.string().uuid(),
  runtimeInstanceId: z.string().min(1).max(128),
  source: z.enum(["supervisor", "operator"]),
  reason: z.string().trim().min(1).max(2_000),
});

export type SupervisorCommand = z.infer<typeof supervisorCommandSchema>;

export function createSupervisorCommand(
  input: Omit<SupervisorCommand, "schemaVersion" | "commandId" | "requestedAt"> & {
    commandId?: string;
    requestedAt?: string;
  },
): SupervisorCommand {
  return supervisorCommandSchema.parse({
    ...input,
    schemaVersion: SUPERVISOR_SCHEMA_VERSION,
    commandId: input.commandId ?? randomUUID(),
    requestedAt: input.requestedAt ?? new Date().toISOString(),
  });
}

export interface KafkaRecordMetadata {
  topic: string;
  partition: number;
  offset: string;
}
