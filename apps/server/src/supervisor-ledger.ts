import { mkdir } from "node:fs/promises";
import path from "node:path";
import { DatabaseSync, type StatementResultingChanges } from "node:sqlite";
import type {
  KafkaRecordMetadata,
  SupervisorCommand,
  SupervisorEvent,
} from "./supervisor-contracts.js";
import {
  createSupervisorCommand,
  createSupervisorEvent,
  supervisorCommandSchema,
} from "./supervisor-contracts.js";
import { redactSupervisorEvent } from "./supervisor-redaction.js";

export type SupervisorRunState =
  | "queued"
  | "running"
  | "completed"
  | "failed"
  | "cancelled";
export type SupervisorRunHealth = "pending" | "healthy" | "stalled" | "terminal";

export interface SupervisorRunRecord {
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
}

export interface SupervisorEventRecord extends SupervisorEvent {
  topic: string | null;
  partition: number | null;
  offset: string | null;
  receivedAt: string;
}

export interface SupervisorStallClaim {
  event: SupervisorEvent;
  command: SupervisorCommand;
}

export interface SupervisorCommandOutboxRecord {
  command: SupervisorCommand;
  attempts: number;
}

export type SupervisorAlertSeverity = "warning" | "critical";
export type SupervisorAlertStatus = "open" | "acknowledged";

export interface SupervisorAlertRecord {
  alertId: string;
  runId: string;
  /** The event that first triggered this alert. */
  eventId: string;
  ruleId: string;
  severity: SupervisorAlertSeverity;
  status: SupervisorAlertStatus;
  evidence: string;
  createdAt: string;
  /** How many stored events matched this rule with this evidence. */
  occurrences: number;
  lastSeenAt: string;
}

export interface SupervisorAlertInput {
  alertId: string;
  runId: string;
  eventId: string;
  ruleId: string;
  severity: SupervisorAlertSeverity;
  evidence: string;
  createdAt?: string;
}

export interface SupervisorEventQuery {
  runId?: string | undefined;
  agentId?: string | undefined;
  types?: SupervisorEvent["type"][] | undefined;
  severities?: SupervisorEvent["severity"][] | undefined;
  text?: string | undefined;
  since?: string | undefined;
  limit?: number | undefined;
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
  events: {
    total: number;
  };
}

interface CommandOutboxRow {
  payload_json: string;
  attempts: number;
}

interface AlertRow {
  alert_id: string;
  run_id: string;
  event_id: string;
  rule_id: string;
  severity: SupervisorAlertSeverity;
  status: SupervisorAlertStatus;
  evidence: string;
  created_at: string;
  occurrences: number;
  last_seen_at: string;
}

interface RunRow {
  run_id: string;
  agent_id: string;
  runtime_instance_id: string | null;
  state: SupervisorRunState;
  health: SupervisorRunHealth;
  started_at: string | null;
  last_event_at: string;
  last_heartbeat_at: string | null;
  ended_at: string | null;
  last_summary: string;
}

interface EventRow {
  event_id: string;
  type: SupervisorEvent["type"];
  occurred_at: string;
  run_id: string;
  agent_id: string;
  runtime_instance_id: string | null;
  source: SupervisorEvent["source"];
  severity: SupervisorEvent["severity"];
  summary: string;
  payload_json: string;
  kafka_topic: string | null;
  kafka_partition: number | null;
  kafka_offset: string | null;
  received_at: string;
}

function changes(result: StatementResultingChanges): number {
  return Number(result.changes);
}

export class SupervisorLedger {
  private database: DatabaseSync | null = null;

  constructor(private readonly filePath: string) {}

  async initialize(): Promise<void> {
    await mkdir(path.dirname(this.filePath), { recursive: true });
    this.database = new DatabaseSync(this.filePath);
    this.database.exec("PRAGMA journal_mode = WAL");
    this.database.exec("PRAGMA foreign_keys = ON");
    this.database.exec("PRAGMA busy_timeout = 5000");
    this.database.exec(`
      CREATE TABLE IF NOT EXISTS runs (
        run_id TEXT PRIMARY KEY,
        agent_id TEXT NOT NULL,
        runtime_instance_id TEXT,
        state TEXT NOT NULL,
        health TEXT NOT NULL,
        started_at TEXT,
        last_event_at TEXT NOT NULL,
        last_heartbeat_at TEXT,
        ended_at TEXT,
        last_summary TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS events (
        event_id TEXT PRIMARY KEY,
        schema_version INTEGER NOT NULL,
        type TEXT NOT NULL,
        occurred_at TEXT NOT NULL,
        run_id TEXT NOT NULL,
        agent_id TEXT NOT NULL,
        runtime_instance_id TEXT,
        source TEXT NOT NULL,
        severity TEXT NOT NULL,
        summary TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        kafka_topic TEXT,
        kafka_partition INTEGER,
        kafka_offset TEXT,
        received_at TEXT NOT NULL,
        UNIQUE(kafka_topic, kafka_partition, kafka_offset)
      );
      CREATE INDEX IF NOT EXISTS events_run_time_idx
        ON events(run_id, occurred_at, event_id);
      CREATE TABLE IF NOT EXISTS alerts (
        alert_id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL,
        event_id TEXT NOT NULL,
        rule_id TEXT NOT NULL,
        severity TEXT NOT NULL,
        status TEXT NOT NULL,
        evidence TEXT NOT NULL,
        created_at TEXT NOT NULL,
        occurrences INTEGER NOT NULL DEFAULT 1,
        last_seen_at TEXT
      );
      CREATE INDEX IF NOT EXISTS alerts_created_idx
        ON alerts(created_at DESC, alert_id);
      CREATE INDEX IF NOT EXISTS alerts_run_idx ON alerts(run_id);
      CREATE TABLE IF NOT EXISTS processed_commands (
        command_id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL,
        type TEXT NOT NULL,
        processed_at TEXT NOT NULL,
        kafka_topic TEXT,
        kafka_partition INTEGER,
        kafka_offset TEXT
      );
      CREATE TABLE IF NOT EXISTS command_outbox (
        command_id TEXT PRIMARY KEY,
        payload_json TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        attempts INTEGER NOT NULL DEFAULT 0,
        available_at TEXT NOT NULL,
        last_error TEXT
      );
    `);
    // Ledgers created before alert grouping existed keep their rows.
    this.addMissingColumn("alerts", "occurrences", "INTEGER NOT NULL DEFAULT 1");
    this.addMissingColumn("alerts", "last_seen_at", "TEXT");
  }

  private addMissingColumn(
    table: string,
    column: string,
    definition: string,
  ): void {
    const database = this.getDatabase();
    const columns = database
      .prepare("SELECT name FROM pragma_table_info(?)")
      .all(table) as unknown as { name: string }[];
    if (columns.some((entry) => entry.name === column)) return;
    database.exec(
      "ALTER TABLE " + table + " ADD COLUMN " + column + " " + definition,
    );
  }

  close(): void {
    this.database?.close();
    this.database = null;
  }

  recordEvent(event: SupervisorEvent, metadata?: KafkaRecordMetadata): boolean {
    const database = this.getDatabase();
    const sanitized = redactSupervisorEvent(event);
    const receivedAt = new Date().toISOString();
    database.exec("BEGIN IMMEDIATE");
    try {
      const result = database
        .prepare(`
          INSERT OR IGNORE INTO events (
            event_id, schema_version, type, occurred_at, run_id, agent_id,
            runtime_instance_id, source, severity, summary, payload_json,
            kafka_topic, kafka_partition, kafka_offset, received_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `)
        .run(
          sanitized.eventId,
          sanitized.schemaVersion,
          sanitized.type,
          sanitized.occurredAt,
          sanitized.runId,
          sanitized.agentId,
          sanitized.runtimeInstanceId ?? null,
          sanitized.source,
          sanitized.severity,
          sanitized.summary,
          JSON.stringify(sanitized.payload),
          metadata?.topic ?? null,
          metadata?.partition ?? null,
          metadata?.offset ?? null,
          receivedAt,
        );
      if (changes(result) === 0) {
        if (metadata) {
          database
            .prepare(`
              UPDATE events SET
                kafka_topic = COALESCE(kafka_topic, ?),
                kafka_partition = COALESCE(kafka_partition, ?),
                kafka_offset = COALESCE(kafka_offset, ?)
              WHERE event_id = ?
            `)
            .run(
              metadata.topic,
              metadata.partition,
              metadata.offset,
              sanitized.eventId,
            );
        }
        database.exec("COMMIT");
        return false;
      }
      this.materializeRun(sanitized);
      database.exec("COMMIT");
      return true;
    } catch (error) {
      database.exec("ROLLBACK");
      throw error;
    }
  }

  getRun(runId: string): SupervisorRunRecord | null {
    const row = this.getDatabase()
      .prepare("SELECT * FROM runs WHERE run_id = ?")
      .get(runId) as RunRow | undefined;
    return row ? this.mapRun(row) : null;
  }

  listRuns(limit = 100): SupervisorRunRecord[] {
    const rows = this.getDatabase()
      .prepare("SELECT * FROM runs ORDER BY last_event_at DESC LIMIT ?")
      .all(limit) as unknown as RunRow[];
    return rows.map((row) => this.mapRun(row));
  }

  listEvents(runId: string, limit = 200): SupervisorEventRecord[] {
    const rows = this.getDatabase()
      .prepare(`
        SELECT * FROM events
        WHERE run_id = ?
        ORDER BY occurred_at ASC, rowid ASC
        LIMIT ?
      `)
      .all(runId, limit) as unknown as EventRow[];
    return rows.map((row) => this.mapEvent(row));
  }

  isCommandProcessed(commandId: string): boolean {
    return Boolean(
      this.getDatabase()
        .prepare("SELECT 1 FROM processed_commands WHERE command_id = ?")
        .get(commandId),
    );
  }

  markCommandProcessed(
    command: SupervisorCommand,
    metadata?: KafkaRecordMetadata,
  ): boolean {
    const result = this.getDatabase()
      .prepare(`
        INSERT OR IGNORE INTO processed_commands (
          command_id, run_id, type, processed_at,
          kafka_topic, kafka_partition, kafka_offset
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
      `)
      .run(
        command.commandId,
        command.runId,
        command.type,
        new Date().toISOString(),
        metadata?.topic ?? null,
        metadata?.partition ?? null,
        metadata?.offset ?? null,
      );
    return changes(result) > 0;
  }

  claimStalledRuns(
    cutoffAt: string,
    occurredAt = new Date().toISOString(),
  ): SupervisorStallClaim[] {
    const database = this.getDatabase();
    database.exec("BEGIN IMMEDIATE");
    try {
      const candidates = database
        .prepare(`
          SELECT * FROM runs
          WHERE state = 'running'
            AND health IN ('pending', 'healthy')
            AND COALESCE(last_heartbeat_at, started_at, last_event_at) < ?
          ORDER BY COALESCE(last_heartbeat_at, started_at, last_event_at) ASC
          LIMIT 100
        `)
        .all(cutoffAt) as unknown as RunRow[];
      const claims = candidates.map((run) => {
        if (!run.runtime_instance_id) {
          throw new Error("Cannot cancel a stalled run without a Runtime instance ID");
        }
        const event = createSupervisorEvent({
          type: "supervisor.stalled",
          occurredAt,
          runId: run.run_id,
          agentId: run.agent_id,
          runtimeInstanceId: run.runtime_instance_id,
          source: "supervisor",
          severity: "critical",
          summary: "Agent Runtime heartbeat is missing",
          payload: {
            lastHeartbeatAt: run.last_heartbeat_at,
            stallCutoffAt: cutoffAt,
          },
        });
        const command = createSupervisorCommand({
          type: "run.cancel",
          runId: run.run_id,
          agentId: run.agent_id,
          runtimeInstanceId: run.runtime_instance_id,
          source: "supervisor",
          reason: "Runtime heartbeat exceeded the stall threshold",
        });
        database
          .prepare(`
            INSERT INTO events (
              event_id, schema_version, type, occurred_at, run_id, agent_id,
              runtime_instance_id, source, severity, summary, payload_json,
              kafka_topic, kafka_partition, kafka_offset, received_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?)
          `)
          .run(
            event.eventId,
            event.schemaVersion,
            event.type,
            event.occurredAt,
            event.runId,
            event.agentId,
            event.runtimeInstanceId ?? run.runtime_instance_id,
            event.source,
            event.severity,
            event.summary,
            JSON.stringify(event.payload),
            occurredAt,
          );
        database
          .prepare(`
            UPDATE runs SET
              health = 'stalled',
              last_event_at = ?,
              last_summary = ?
            WHERE run_id = ?
          `)
          .run(occurredAt, event.summary, run.run_id);
        database
          .prepare(`
            INSERT INTO command_outbox (
              command_id, payload_json, status, attempts, available_at, last_error
            ) VALUES (?, ?, 'pending', 0, ?, NULL)
          `)
          .run(command.commandId, JSON.stringify(command), occurredAt);
        return { event, command };
      });
      database.exec("COMMIT");
      return claims;
    } catch (error) {
      database.exec("ROLLBACK");
      throw error;
    }
  }

  listPendingCommands(
    availableAt: string,
    limit = 100,
  ): SupervisorCommandOutboxRecord[] {
    const rows = this.getDatabase()
      .prepare(`
        SELECT payload_json, attempts FROM command_outbox
        WHERE status = 'pending' AND available_at <= ?
        ORDER BY available_at ASC
        LIMIT ?
      `)
      .all(availableAt, limit) as unknown as CommandOutboxRow[];
    return rows.map((row) => ({
      command: supervisorCommandSchema.parse(JSON.parse(row.payload_json)),
      attempts: row.attempts,
    }));
  }

  markCommandOutboxSent(commandId: string): void {
    this.getDatabase()
      .prepare(`
        UPDATE command_outbox
        SET status = 'sent', attempts = attempts + 1, last_error = NULL
        WHERE command_id = ?
      `)
      .run(commandId);
  }

  rescheduleCommandOutbox(
    commandId: string,
    availableAt: string,
    error: string,
  ): void {
    this.getDatabase()
      .prepare(`
        UPDATE command_outbox
        SET attempts = attempts + 1, available_at = ?, last_error = ?
        WHERE command_id = ? AND status = 'pending'
      `)
      .run(availableAt, error.slice(0, 2_000), commandId);
  }

  getEvent(eventId: string): SupervisorEventRecord | null {
    const row = this.getDatabase()
      .prepare("SELECT * FROM events WHERE event_id = ?")
      .get(eventId) as EventRow | undefined;
    return row ? this.mapEvent(row) : null;
  }

  getEventsByIds(eventIds: string[]): SupervisorEventRecord[] {
    if (eventIds.length === 0) return [];
    const placeholders = eventIds.map(() => "?").join(", ");
    const rows = this.getDatabase()
      .prepare(`
        SELECT * FROM events
        WHERE event_id IN (${placeholders})
        ORDER BY occurred_at ASC, rowid ASC
      `)
      .all(...eventIds) as unknown as EventRow[];
    return rows.map((row) => this.mapEvent(row));
  }

  /**
   * Filtered event search for the operator APIs. Every caller-supplied value is
   * bound as a parameter; only the number of placeholders varies.
   */
  searchEvents(query: SupervisorEventQuery = {}): SupervisorEventRecord[] {
    const conditions: string[] = [];
    const parameters: (string | number)[] = [];
    if (query.runId) {
      conditions.push("run_id = ?");
      parameters.push(query.runId);
    }
    if (query.agentId) {
      conditions.push("agent_id = ?");
      parameters.push(query.agentId);
    }
    if (query.types?.length) {
      conditions.push(`type IN (${query.types.map(() => "?").join(", ")})`);
      parameters.push(...query.types);
    }
    if (query.severities?.length) {
      conditions.push(
        `severity IN (${query.severities.map(() => "?").join(", ")})`,
      );
      parameters.push(...query.severities);
    }
    if (query.since) {
      conditions.push("occurred_at >= ?");
      parameters.push(query.since);
    }
    const text = query.text?.trim();
    if (text) {
      conditions.push("(summary LIKE ? ESCAPE '\\' OR payload_json LIKE ? ESCAPE '\\')");
      const pattern = "%" + text.replace(/[\\%_]/g, "\\$&") + "%";
      parameters.push(pattern, pattern);
    }
    const where = conditions.length ? "WHERE " + conditions.join(" AND ") : "";
    const limit = Math.min(Math.max(query.limit ?? 50, 1), 500);
    const rows = this.getDatabase()
      .prepare(`
        SELECT * FROM events
        ${where}
        ORDER BY occurred_at DESC, rowid DESC
        LIMIT ?
      `)
      .all(...parameters, limit) as unknown as EventRow[];
    return rows.map((row) => this.mapEvent(row));
  }

  /**
   * Alert IDs are derived from the rule and the event that triggered it, so
   * replaying the same Kafka event never raises a duplicate alert.
   */
  recordAlert(input: SupervisorAlertInput): boolean {
    const database = this.getDatabase();
    const seenAt = input.createdAt ?? new Date().toISOString();
    const result = database
      .prepare(`
        INSERT OR IGNORE INTO alerts (
          alert_id, run_id, event_id, rule_id, severity, status, evidence,
          created_at, occurrences, last_seen_at
        ) VALUES (?, ?, ?, ?, ?, 'open', ?, ?, 1, ?)
      `)
      .run(
        input.alertId,
        input.runId,
        input.eventId,
        input.ruleId,
        input.severity,
        input.evidence.slice(0, 4_000),
        seenAt,
        seenAt,
      );
    if (changes(result) > 0) return true;
    // Same rule, same run, same evidence: count the repeat instead of raising a
    // second alert, and keep the first triggering event as the citation.
    database
      .prepare(`
        UPDATE alerts SET
          occurrences = occurrences + 1,
          last_seen_at = MAX(COALESCE(last_seen_at, created_at), ?)
        WHERE alert_id = ?
      `)
      .run(seenAt, input.alertId);
    return false;
  }

  listAlerts(
    options: { runId?: string | undefined; limit?: number | undefined } = {},
  ): SupervisorAlertRecord[] {
    const limit = Math.min(Math.max(options.limit ?? 50, 1), 500);
    const rows = options.runId
      ? (this.getDatabase()
          .prepare(`
            SELECT * FROM alerts WHERE run_id = ?
            ORDER BY created_at DESC, alert_id DESC LIMIT ?
          `)
          .all(options.runId, limit) as unknown as AlertRow[])
      : (this.getDatabase()
          .prepare(`
            SELECT * FROM alerts
            ORDER BY created_at DESC, alert_id DESC LIMIT ?
          `)
          .all(limit) as unknown as AlertRow[]);
    return rows.map((row) => ({
      alertId: row.alert_id,
      runId: row.run_id,
      eventId: row.event_id,
      ruleId: row.rule_id,
      severity: row.severity,
      status: row.status,
      evidence: row.evidence,
      createdAt: row.created_at,
      occurrences: Number(row.occurrences ?? 1),
      lastSeenAt: row.last_seen_at ?? row.created_at,
    }));
  }

  /** Queues a command for the watchdog to publish when Kafka is unreachable. */
  enqueueCommand(
    command: SupervisorCommand,
    availableAt = new Date().toISOString(),
  ): boolean {
    const result = this.getDatabase()
      .prepare(`
        INSERT OR IGNORE INTO command_outbox (
          command_id, payload_json, status, attempts, available_at, last_error
        ) VALUES (?, ?, 'pending', 0, ?, NULL)
      `)
      .run(command.commandId, JSON.stringify(command), availableAt);
    return changes(result) > 0;
  }

  getOverview(): SupervisorOverview {
    const database = this.getDatabase();
    const countBy = (column: "state" | "health"): Record<string, number> => {
      const rows = database
        .prepare(`SELECT ${column} AS bucket, COUNT(*) AS total FROM runs GROUP BY ${column}`)
        .all() as unknown as { bucket: string; total: number }[];
      return Object.fromEntries(rows.map((row) => [row.bucket, Number(row.total)]));
    };
    const states = countBy("state");
    const healths = countBy("health");
    const alertRow = database
      .prepare(`
        SELECT
          COUNT(*) AS total,
          COALESCE(SUM(status = 'open'), 0) AS open,
          COALESCE(SUM(severity = 'critical'), 0) AS critical,
          COALESCE(SUM(severity = 'warning'), 0) AS warning,
          COUNT(DISTINCT run_id) AS flagged_runs
        FROM alerts
      `)
      .get() as unknown as {
      total: number;
      open: number;
      critical: number;
      warning: number;
      flagged_runs: number;
    };
    const eventRow = database
      .prepare("SELECT COUNT(*) AS total FROM events")
      .get() as unknown as { total: number };
    const runTotal = Object.values(states).reduce((sum, value) => sum + value, 0);
    return {
      generatedAt: new Date().toISOString(),
      runs: {
        total: runTotal,
        queued: states.queued ?? 0,
        running: states.running ?? 0,
        completed: states.completed ?? 0,
        failed: states.failed ?? 0,
        cancelled: states.cancelled ?? 0,
      },
      health: {
        pending: healths.pending ?? 0,
        healthy: healths.healthy ?? 0,
        stalled: healths.stalled ?? 0,
        terminal: healths.terminal ?? 0,
      },
      alerts: {
        total: Number(alertRow.total),
        open: Number(alertRow.open),
        critical: Number(alertRow.critical),
        warning: Number(alertRow.warning),
        flaggedRuns: Number(alertRow.flagged_runs),
      },
      events: { total: Number(eventRow.total) },
    };
  }

  private mapEvent(row: EventRow): SupervisorEventRecord {
    return {
      schemaVersion: 1,
      eventId: row.event_id,
      type: row.type,
      occurredAt: row.occurred_at,
      runId: row.run_id,
      agentId: row.agent_id,
      ...(row.runtime_instance_id
        ? { runtimeInstanceId: row.runtime_instance_id }
        : {}),
      source: row.source,
      severity: row.severity,
      summary: row.summary,
      payload: JSON.parse(row.payload_json) as Record<string, unknown>,
      topic: row.kafka_topic,
      partition: row.kafka_partition,
      offset: row.kafka_offset,
      receivedAt: row.received_at,
    };
  }

  private materializeRun(event: SupervisorEvent): void {
    const current = this.getRun(event.runId);
    if (current && current.lastEventAt > event.occurredAt) return;

    let state: SupervisorRunState = current?.state ?? "queued";
    let health: SupervisorRunHealth = current?.health ?? "pending";
    let startedAt = current?.startedAt ?? null;
    let lastHeartbeatAt = current?.lastHeartbeatAt ?? null;
    let endedAt = current?.endedAt ?? null;

    switch (event.type) {
      case "run.queued":
        state = "queued";
        health = "pending";
        break;
      case "run.started":
        state = "running";
        health = "healthy";
        startedAt = event.occurredAt;
        break;
      case "runtime.heartbeat":
        state = "running";
        health = "healthy";
        lastHeartbeatAt = event.occurredAt;
        break;
      case "runtime.exited":
        break;
      case "supervisor.stalled":
        state = "running";
        health = "stalled";
        break;
      case "supervisor.recovered":
        if (!endedAt) health = "healthy";
        break;
      case "run.completed":
        state = "completed";
        health = "terminal";
        endedAt = event.occurredAt;
        break;
      case "run.failed":
        state = "failed";
        health = "terminal";
        endedAt = event.occurredAt;
        break;
      case "run.cancelled":
        state = "cancelled";
        health = "terminal";
        endedAt = event.occurredAt;
        break;
      case "run.tool_activity":
      case "alert.raised":
      case "supervisor.demo_paused":
        break;
    }

    this.getDatabase()
      .prepare(`
        INSERT INTO runs (
          run_id, agent_id, runtime_instance_id, state, health, started_at,
          last_event_at, last_heartbeat_at, ended_at, last_summary
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id) DO UPDATE SET
          agent_id = excluded.agent_id,
          runtime_instance_id = COALESCE(excluded.runtime_instance_id, runs.runtime_instance_id),
          state = excluded.state,
          health = excluded.health,
          started_at = COALESCE(excluded.started_at, runs.started_at),
          last_event_at = excluded.last_event_at,
          last_heartbeat_at = COALESCE(excluded.last_heartbeat_at, runs.last_heartbeat_at),
          ended_at = COALESCE(excluded.ended_at, runs.ended_at),
          last_summary = excluded.last_summary
        WHERE excluded.last_event_at >= runs.last_event_at
      `)
      .run(
        event.runId,
        event.agentId,
        event.runtimeInstanceId ?? current?.runtimeInstanceId ?? null,
        state,
        health,
        startedAt,
        event.occurredAt,
        lastHeartbeatAt,
        endedAt,
        event.summary,
      );
  }

  private mapRun(row: RunRow): SupervisorRunRecord {
    return {
      runId: row.run_id,
      agentId: row.agent_id,
      runtimeInstanceId: row.runtime_instance_id,
      state: row.state,
      health: row.health,
      startedAt: row.started_at,
      lastEventAt: row.last_event_at,
      lastHeartbeatAt: row.last_heartbeat_at,
      endedAt: row.ended_at,
      lastSummary: row.last_summary,
    };
  }

  private getDatabase(): DatabaseSync {
    if (!this.database) throw new Error("Supervisor ledger is not initialized");
    return this.database;
  }
}
