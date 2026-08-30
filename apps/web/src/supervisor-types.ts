export type SupervisorRunState =
  | "queued"
  | "running"
  | "completed"
  | "failed"
  | "cancelled";
export type SupervisorRunHealth = "pending" | "healthy" | "stalled" | "terminal";
export type SupervisorSeverity = "info" | "warning" | "critical";
export type SupervisorSource =
  | "control-plane"
  | "runtime"
  | "supervisor"
  | "operator";

export type SupervisorEventType =
  | "run.queued"
  | "run.started"
  | "runtime.heartbeat"
  | "runtime.exited"
  | "run.tool_activity"
  | "run.completed"
  | "run.failed"
  | "run.cancelled"
  | "supervisor.stalled"
  | "supervisor.demo_paused"
  | "alert.raised"
  | "supervisor.recovered";

export interface SupervisorRun {
  runId: string;
  agentId: string;
  runtimeInstanceId: string | null;
  state: SupervisorRunState;
  health: SupervisorRunHealth;
  startedAt: string | null;
  lastEventAt: string;
  lastHeartbeatAt: string | null;
  endedAt: string | null;
  lastSummary: string;
  lastHeartbeatAgeMs: number | null;
  heartbeatOverdue: boolean;
}

export interface SupervisorEventRecord {
  schemaVersion: 1;
  eventId: string;
  type: SupervisorEventType;
  occurredAt: string;
  runId: string;
  agentId: string;
  runtimeInstanceId?: string;
  source: SupervisorSource;
  severity: SupervisorSeverity;
  summary: string;
  payload: Record<string, unknown>;
  topic: string | null;
  partition: number | null;
  offset: string | null;
  receivedAt: string;
}

export interface SupervisorAlert {
  alertId: string;
  runId: string;
  eventId: string;
  ruleId: string;
  severity: "warning" | "critical";
  status: "open" | "acknowledged";
  evidence: string;
  createdAt: string;
  occurrences: number;
  lastSeenAt: string;
  event: {
    eventId: string;
    type: SupervisorEventType;
    occurredAt: string;
    severity: SupervisorSeverity;
    summary: string;
  } | null;
}

export interface SupervisorOverview {
  generatedAt: string;
  runs: {
    total: number;
    queued: number;
    running: number;
    completed: number;
    failed: number;
    cancelled: number;
  };
  health: {
    pending: number;
    healthy: number;
    stalled: number;
    terminal: number;
  };
  alerts: {
    total: number;
    open: number;
    critical: number;
    warning: number;
    flaggedRuns: number;
  };
  events: { total: number };
}

export interface SupervisorSettings {
  stallAfterMs: number;
  heartbeatIntervalMs: number;
  watchdogIntervalMs: number;
  demoControlsEnabled: boolean;
  runtimeInstanceId: string;
}

export interface SupervisorOverviewResponse {
  overview: SupervisorOverview;
  settings: SupervisorSettings;
}

export interface SupervisorCitation {
  runId: string;
  eventId?: string;
  alertId?: string;
  occurredAt: string;
  label: string;
}

export interface SupervisorChatReply {
  answer: string;
  citations: SupervisorCitation[];
  toolCalls: { tool: string; arguments: Record<string, unknown> }[];
}
