import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { api, ApiError } from "./api";
import type {
  SupervisorAlert,
  SupervisorChatReply,
  SupervisorEventRecord,
  SupervisorOverviewResponse,
  SupervisorRun,
  SupervisorRunHealth,
  SupervisorRunState,
} from "./supervisor-types";
import { presentTimelineEvent } from "./timeline-presentation";

const POLL_INTERVAL_MS = 2_000;

const suggestedQuestions = [
  "Check all logs for suspicious intentions.",
  "Which runs are unhealthy right now?",
  "What happened to the most recent cancelled run?",
];

function shortId(value: string): string {
  return value.slice(0, 8);
}

function formatClock(value: string): string {
  return new Intl.DateTimeFormat(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(new Date(value));
}

function formatAge(ms: number | null): string {
  if (ms === null) return "—";
  if (ms < 1_000) return "just now";
  const seconds = Math.floor(ms / 1_000);
  if (seconds < 60) return seconds + "s ago";
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return minutes + "m ago";
  return Math.floor(minutes / 60) + "h ago";
}

function Counter({
  label,
  value,
  tone,
}: {
  label: string;
  value: number;
  tone: "neutral" | "good" | "warn" | "bad";
}) {
  return (
    <div className={"sup-counter sup-counter-" + tone}>
      <strong>{value}</strong>
      <span>{label}</span>
    </div>
  );
}

function EventRow({ event }: { event: SupervisorEventRecord }) {
  const presentation = presentTimelineEvent(event);
  const kafkaPosition =
    event.offset === null
      ? "Not recorded"
      : `${event.topic ?? "unknown topic"} · partition ${event.partition ?? "?"} · offset ${event.offset}`;
  return (
    <li className={"sup-event sup-event-" + event.severity}>
      <span className="sup-event-time">{formatClock(event.occurredAt)}</span>
      <div className="sup-event-body">
        <div className="sup-event-head">
          <strong>{presentation.title}</strong>
          {presentation.status && (
            <span className="sup-chip">{presentation.status}</span>
          )}
          {event.severity !== "info" && (
            <span className={"sup-chip sup-chip-" + event.severity}>
              {event.severity}
            </span>
          )}
        </div>
        {presentation.description && <p>{presentation.description}</p>}
        <details className="sup-event-details">
          <summary>Technical details</summary>
          <dl>
            <div>
              <dt>Event</dt>
              <dd><code>{event.type}</code></dd>
            </div>
            <div>
              <dt>Source</dt>
              <dd>{event.source}</dd>
            </div>
            <div>
              <dt>Kafka position</dt>
              <dd>{kafkaPosition}</dd>
            </div>
            <div>
              <dt>Event ID</dt>
              <dd><code>{event.eventId}</code></dd>
            </div>
          </dl>
          {presentation.command && (
            <div className="sup-event-technical-block">
              <span>Original command</span>
              <pre className="sup-evidence">{presentation.command}</pre>
            </div>
          )}
          {presentation.output && (
            <div className="sup-event-technical-block">
              <span>Command output</span>
              <pre className="sup-evidence">{presentation.output}</pre>
            </div>
          )}
        </details>
      </div>
    </li>
  );
}

export default function SupervisorDashboard() {
  const [overview, setOverview] = useState<SupervisorOverviewResponse | null>(
    null,
  );
  const [runs, setRuns] = useState<SupervisorRun[]>([]);
  const [alerts, setAlerts] = useState<SupervisorAlert[]>([]);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [events, setEvents] = useState<SupervisorEventRecord[]>([]);
  const [runAlerts, setRunAlerts] = useState<SupervisorAlert[]>([]);
  const [stateFilter, setStateFilter] = useState<SupervisorRunState | "all">(
    "all",
  );
  const [healthFilter, setHealthFilter] = useState<SupervisorRunHealth | "all">(
    "all",
  );
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [pendingAction, setPendingAction] = useState<string | null>(null);
  const [question, setQuestion] = useState("");
  const [chatBusy, setChatBusy] = useState(false);
  const [chatLog, setChatLog] = useState<
    { question: string; reply: SupervisorChatReply | null; error?: string }[]
  >([]);
  const selectedRunRef = useRef<string | null>(null);
  selectedRunRef.current = selectedRunId;

  const refresh = useCallback(async () => {
    const [overviewResult, runsResult, alertsResult] = await Promise.all([
      api.supervisorOverview(),
      api.supervisorRuns(),
      api.supervisorAlerts(),
    ]);
    setOverview(overviewResult);
    setRuns(runsResult.runs);
    setAlerts(alertsResult.alerts);
    setError(null);
  }, []);

  const refreshSelected = useCallback(async (runId: string) => {
    const [detail, timeline] = await Promise.all([
      api.supervisorRun(runId),
      api.supervisorRunEvents(runId),
    ]);
    if (selectedRunRef.current !== runId) return;
    setRunAlerts(detail.alerts);
    setEvents(timeline.events);
  }, []);

  useEffect(() => {
    let cancelled = false;
    const tick = () => {
      void refresh().catch((reason) => {
        if (cancelled) return;
        setError(reason instanceof Error ? reason.message : String(reason));
      });
      const runId = selectedRunRef.current;
      if (runId) void refreshSelected(runId).catch(() => undefined);
    };
    tick();
    const timer = window.setInterval(tick, POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [refresh, refreshSelected]);

  useEffect(() => {
    if (!selectedRunId) {
      setEvents([]);
      setRunAlerts([]);
      return;
    }
    void refreshSelected(selectedRunId).catch((reason) =>
      setError(reason instanceof Error ? reason.message : String(reason)),
    );
  }, [refreshSelected, selectedRunId]);

  const visibleRuns = useMemo(
    () =>
      runs
        .filter((run) => stateFilter === "all" || run.state === stateFilter)
        .filter((run) => healthFilter === "all" || run.health === healthFilter),
    [runs, stateFilter, healthFilter],
  );

  const selectedRun = useMemo(
    () => runs.find((run) => run.runId === selectedRunId) ?? null,
    [runs, selectedRunId],
  );

  const alertedRunIds = useMemo(
    () => new Set(alerts.map((alert) => alert.runId)),
    [alerts],
  );

  const runAction = async (
    label: string,
    action: () => Promise<string>,
  ): Promise<void> => {
    setPendingAction(label);
    setNotice(null);
    try {
      setNotice(await action());
      await refresh();
    } catch (reason) {
      setError(
        reason instanceof ApiError
          ? reason.message
          : reason instanceof Error
            ? reason.message
            : String(reason),
      );
    } finally {
      setPendingAction(null);
    }
  };

  const cancelRun = (runId: string) =>
    runAction("cancel", async () => {
      const result = await api.supervisorCancel(runId);
      return result.published
        ? "Cancellation command " +
            shortId(result.commandId) +
            " published to Kafka."
        : "Kafka is unreachable; cancellation " +
            shortId(result.commandId) +
            " is queued in the outbox for retry.";
    });

  const simulateStall = (runId: string) =>
    runAction("simulate", async () => {
      const result = await api.supervisorSimulateStall(runId);
      return (
        "Runtime container paused. Heartbeats stop now; the watchdog reacts after " +
        Math.round(result.stallAfterMs / 1_000) +
        "s."
      );
    });

  const askChat = async (value: string) => {
    const trimmed = value.trim();
    if (!trimmed || chatBusy) return;
    setChatBusy(true);
    setQuestion("");
    setChatLog((log) => [...log, { question: trimmed, reply: null }]);
    try {
      const reply = await api.supervisorChat(
        trimmed,
        selectedRunId ?? undefined,
      );
      setChatLog((log) =>
        log.map((entry, index) =>
          index === log.length - 1 ? { ...entry, reply } : entry,
        ),
      );
    } catch (reason) {
      const message =
        reason instanceof Error ? reason.message : String(reason);
      setChatLog((log) =>
        log.map((entry, index) =>
          index === log.length - 1 ? { ...entry, error: message } : entry,
        ),
      );
    } finally {
      setChatBusy(false);
    }
  };

  if (error && !overview) {
    return (
      <section className="supervisor">
        <div className="error-banner" role="alert">
          <span>{error}</span>
        </div>
      </section>
    );
  }

  const settings = overview?.settings;
  const counts = overview?.overview;

  return (
    <section className="supervisor">
      <header className="sup-header">
        <div>
          <span className="eyebrow">Run supervisor</span>
          <h1>Agent run health</h1>
          <p>
            Kafka run events and commands, reconciled into a SQLite ledger.
            {settings
              ? " Stall threshold " +
                Math.round(settings.stallAfterMs / 1_000) +
                "s · heartbeat every " +
                Math.round(settings.heartbeatIntervalMs / 1_000) +
                "s."
              : ""}
          </p>
        </div>
        <div className="sup-counters">
          <Counter label="Running" value={counts?.runs.running ?? 0} tone="neutral" />
          <Counter label="Healthy" value={counts?.health.healthy ?? 0} tone="good" />
          <Counter label="Stalled" value={counts?.health.stalled ?? 0} tone="warn" />
          <Counter label="Failed" value={counts?.runs.failed ?? 0} tone="bad" />
          <Counter
            label="Flagged"
            value={counts?.alerts.flaggedRuns ?? 0}
            tone={counts?.alerts.open ? "bad" : "neutral"}
          />
        </div>
      </header>

      {error && (
        <div className="error-banner" role="alert">
          <span>{error}</span>
          <button onClick={() => setError(null)}>×</button>
        </div>
      )}
      {notice && (
        <div className="sup-notice" role="status">
          <span>{notice}</span>
          <button onClick={() => setNotice(null)}>×</button>
        </div>
      )}

      <div className="sup-grid">
        <div className="sup-column">
          <section className="sup-card">
            <div className="sup-card-head">
              <h2>Runs</h2>
              <div className="sup-filters">
                <select
                  aria-label="Filter by state"
                  value={stateFilter}
                  onChange={(event) =>
                    setStateFilter(event.target.value as SupervisorRunState | "all")
                  }
                >
                  <option value="all">All states</option>
                  <option value="queued">Queued</option>
                  <option value="running">Running</option>
                  <option value="completed">Completed</option>
                  <option value="failed">Failed</option>
                  <option value="cancelled">Cancelled</option>
                </select>
                <select
                  aria-label="Filter by health"
                  value={healthFilter}
                  onChange={(event) =>
                    setHealthFilter(
                      event.target.value as SupervisorRunHealth | "all",
                    )
                  }
                >
                  <option value="all">All health</option>
                  <option value="pending">Pending</option>
                  <option value="healthy">Healthy</option>
                  <option value="stalled">Stalled</option>
                  <option value="terminal">Terminal</option>
                </select>
              </div>
            </div>
            <div className="sup-table-wrap">
              <table className="sup-table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>State</th>
                    <th>Health</th>
                    <th>Last heartbeat</th>
                    <th>Latest event</th>
                  </tr>
                </thead>
                <tbody>
                  {visibleRuns.map((run) => (
                    <tr
                      key={run.runId}
                      className={
                        (run.runId === selectedRunId ? "selected " : "") +
                        (run.health === "stalled" ? "row-stalled" : "")
                      }
                      onClick={() => setSelectedRunId(run.runId)}
                    >
                      <td>
                        <code>{shortId(run.runId)}</code>
                        {alertedRunIds.has(run.runId) && (
                          <span className="sup-flag" title="Suspicious activity">
                            ▲
                          </span>
                        )}
                      </td>
                      <td>
                        <span className={"sup-state sup-state-" + run.state}>
                          {run.state}
                        </span>
                      </td>
                      <td>
                        <span className={"sup-health sup-health-" + run.health}>
                          {run.health}
                        </span>
                      </td>
                      <td className={run.heartbeatOverdue ? "overdue" : ""}>
                        {formatAge(run.lastHeartbeatAgeMs)}
                      </td>
                      <td className="sup-summary">{run.lastSummary}</td>
                    </tr>
                  ))}
                  {visibleRuns.length === 0 && (
                    <tr>
                      <td colSpan={5} className="sup-empty">
                        No runs match this filter yet.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </section>

          <section className="sup-card">
            <div className="sup-card-head">
              <h2>Suspicious activity</h2>
              <span className="sup-chip">{alerts.length} alerts</span>
            </div>
            <ul className="sup-alerts">
              {alerts.map((alert) => (
                <li
                  key={alert.alertId}
                  className={"sup-alert sup-alert-" + alert.severity}
                >
                  <div className="sup-alert-head">
                    <strong>{alert.ruleId}</strong>
                    <button
                      className="button button-ghost"
                      onClick={() => setSelectedRunId(alert.runId)}
                    >
                      run {shortId(alert.runId)}
                    </button>
                    {alert.occurrences > 1 && (
                      <span className="sup-chip" title="Matching events">
                        ×{alert.occurrences}
                      </span>
                    )}
                    <span className="sup-chip">{formatClock(alert.createdAt)}</span>
                  </div>
                  <pre className="sup-evidence">{alert.evidence}</pre>
                  <span className="sup-alert-ref">
                    event {shortId(alert.eventId)}
                    {alert.event ? " · " + alert.event.type : ""}
                  </span>
                </li>
              ))}
              {alerts.length === 0 && (
                <li className="sup-empty">
                  No rule has matched. Alerts appear here with the matching
                  evidence and the event that produced it.
                </li>
              )}
            </ul>
          </section>
        </div>

        <div className="sup-column">
          <section className="sup-card">
            <div className="sup-card-head">
              <h2>{selectedRun ? "Run " + shortId(selectedRun.runId) : "Run timeline"}</h2>
              {selectedRun && (
                <div className="sup-actions">
                  <button
                    className="button button-ghost"
                    disabled={
                      pendingAction !== null || selectedRun.health === "terminal"
                    }
                    onClick={() => void cancelRun(selectedRun.runId)}
                  >
                    Cancel run
                  </button>
                  {settings?.demoControlsEnabled && (
                    <button
                      className="button button-ghost sup-demo"
                      disabled={
                        pendingAction !== null ||
                        !["queued", "running"].includes(selectedRun.state)
                      }
                      onClick={() => void simulateStall(selectedRun.runId)}
                    >
                      Simulate missing heartbeat
                    </button>
                  )}
                </div>
              )}
            </div>
            {selectedRun ? (
              <>
                <dl className="sup-meta">
                  <div>
                    <dt>Agent</dt>
                    <dd>
                      <code>{shortId(selectedRun.agentId)}</code>
                    </dd>
                  </div>
                  <div>
                    <dt>Runtime</dt>
                    <dd>{selectedRun.runtimeInstanceId ?? "—"}</dd>
                  </div>
                  <div>
                    <dt>Last heartbeat</dt>
                    <dd className={selectedRun.heartbeatOverdue ? "overdue" : ""}>
                      {formatAge(selectedRun.lastHeartbeatAgeMs)}
                    </dd>
                  </div>
                  <div>
                    <dt>Alerts</dt>
                    <dd>{runAlerts.length}</dd>
                  </div>
                </dl>
                <ul className="sup-timeline">
                  {events.map((event) => (
                    <EventRow key={event.eventId} event={event} />
                  ))}
                  {events.length === 0 && (
                    <li className="sup-empty">No events recorded for this run.</li>
                  )}
                </ul>
              </>
            ) : (
              <p className="sup-empty">
                Select a run to see its Kafka timeline, heartbeat age, and
                cancellation controls.
              </p>
            )}
          </section>

          <section className="sup-card">
            <div className="sup-card-head">
              <h2>Operator chat</h2>
              <span className="sup-chip sup-chip-quiet">read-only</span>
            </div>
            <div className="sup-chat-log">
              {chatLog.length === 0 && (
                <p className="sup-empty">
                  Ask about run health or suspicious activity. Answers cite the
                  stored events and alerts they are based on.
                </p>
              )}
              {chatLog.map((entry, index) => (
                <div key={index} className="sup-chat-entry">
                  <p className="sup-chat-question">{entry.question}</p>
                  {entry.error && (
                    <p className="sup-chat-error">{entry.error}</p>
                  )}
                  {!entry.reply && !entry.error && (
                    <p className="sup-chat-pending">Reading the ledger…</p>
                  )}
                  {entry.reply && (
                    <div className="sup-chat-answer">
                      <p>{entry.reply.answer}</p>
                      {entry.reply.citations.length > 0 && (
                        <ul className="sup-citations">
                          {entry.reply.citations.map((citation, position) => (
                            <li key={position}>
                              <button
                                className="button button-ghost"
                                onClick={() => setSelectedRunId(citation.runId)}
                              >
                                {citation.label}
                              </button>
                            </li>
                          ))}
                        </ul>
                      )}
                      {entry.reply.toolCalls.length > 0 && (
                        <span className="sup-chip sup-chip-quiet">
                          {entry.reply.toolCalls
                            .map((call) => call.tool)
                            .join(" → ")}
                        </span>
                      )}
                    </div>
                  )}
                </div>
              ))}
            </div>
            <div className="sup-suggestions">
              {suggestedQuestions.map((suggestion) => (
                <button
                  key={suggestion}
                  className="button button-ghost"
                  disabled={chatBusy}
                  onClick={() => void askChat(suggestion)}
                >
                  {suggestion}
                </button>
              ))}
            </div>
            <form
              className="sup-chat-form"
              onSubmit={(event) => {
                event.preventDefault();
                void askChat(question);
              }}
            >
              <input
                value={question}
                placeholder="Ask about runs, alerts, or evidence"
                onChange={(event) => setQuestion(event.target.value)}
              />
              <button
                className="button button-primary"
                disabled={chatBusy || !question.trim()}
              >
                {chatBusy ? "Asking…" : "Ask"}
              </button>
            </form>
          </section>
        </div>
      </div>
    </section>
  );
}
