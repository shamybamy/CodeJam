import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "./api";
import SupervisorDashboard from "./SupervisorDashboard";
import type {
  SupervisorAlert,
  SupervisorEventRecord,
  SupervisorOverviewResponse,
  SupervisorRun,
} from "./supervisor-types";

vi.mock("./api", () => {
  class MockApiError extends Error {
    constructor(
      message: string,
      public readonly status: number,
    ) {
      super(message);
    }
  }
  return {
    ApiError: MockApiError,
    api: {
      supervisorOverview: vi.fn(),
      supervisorRuns: vi.fn(),
      supervisorRun: vi.fn(),
      supervisorRunEvents: vi.fn(),
      supervisorAlerts: vi.fn(),
      supervisorCancel: vi.fn(),
      supervisorSimulateStall: vi.fn(),
      supervisorChat: vi.fn(),
    },
  };
});

const runningRun: SupervisorRun = {
  runId: "11111111-1111-4111-8111-111111111111",
  agentId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  runtimeInstanceId: "local-test",
  state: "running",
  health: "healthy",
  startedAt: "2026-08-30T10:00:00.000Z",
  lastEventAt: "2026-08-30T10:00:04.000Z",
  lastHeartbeatAt: "2026-08-30T10:00:04.000Z",
  endedAt: null,
  lastSummary: "Runtime heartbeat received",
  lastHeartbeatAgeMs: 1_500,
  heartbeatOverdue: false,
};

const completedRun: SupervisorRun = {
  runId: "22222222-2222-4222-8222-222222222222",
  agentId: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
  runtimeInstanceId: "local-test",
  state: "completed",
  health: "terminal",
  startedAt: "2026-08-30T09:00:00.000Z",
  lastEventAt: "2026-08-30T09:00:03.000Z",
  lastHeartbeatAt: "2026-08-30T09:00:02.000Z",
  endedAt: "2026-08-30T09:00:03.000Z",
  lastSummary: "Agent run completed",
  lastHeartbeatAgeMs: null,
  heartbeatOverdue: false,
};

const alert: SupervisorAlert = {
  alertId: "alert-1",
  runId: runningRun.runId,
  eventId: "33333333-3333-4333-8333-333333333333",
  ruleId: "secret-file-access",
  severity: "critical",
  status: "open",
  evidence: "cat .env",
  createdAt: "2026-08-30T10:00:03.000Z",
  occurrences: 2,
  lastSeenAt: "2026-08-30T10:00:03.000Z",
  event: {
    eventId: "33333333-3333-4333-8333-333333333333",
    type: "run.tool_activity",
    occurredAt: "2026-08-30T10:00:03.000Z",
    severity: "info",
    summary: "Command execution",
  },
};

const heartbeatEvent: SupervisorEventRecord = {
  schemaVersion: 1,
  eventId: "44444444-4444-4444-8444-444444444444",
  type: "runtime.heartbeat",
  occurredAt: "2026-08-30T10:00:04.000Z",
  runId: runningRun.runId,
  agentId: runningRun.agentId,
  runtimeInstanceId: "local-test",
  source: "runtime",
  severity: "info",
  summary: "Agent Runtime heartbeat",
  payload: { sequence: 2 },
  topic: "agent-run-events-v1",
  partition: 1,
  offset: "42",
  receivedAt: "2026-08-30T10:00:04.100Z",
};

const fileWriteEvent: SupervisorEventRecord = {
  ...heartbeatEvent,
  eventId: "66666666-6666-4666-8666-666666666666",
  type: "run.tool_activity",
  occurredAt: "2026-08-30T10:00:04.500Z",
  summary:
    "command_execution: /bin/bash -lc \"echo 'OK' > health-check.txt\"",
  payload: {
    itemType: "command_execution",
    itemId: "item-1",
    status: "completed",
    command: "/bin/bash -lc \"echo 'OK' > health-check.txt\"",
    exitCode: 0,
    detail: null,
  },
  offset: "43",
};

const overview: SupervisorOverviewResponse = {
  overview: {
    generatedAt: "2026-08-30T10:00:05.000Z",
    runs: {
      total: 2,
      queued: 0,
      running: 1,
      completed: 1,
      failed: 0,
      cancelled: 0,
    },
    health: {
      pending: 0,
      healthy: 1,
      stalled: 0,
      terminal: 1,
    },
    alerts: {
      total: 1,
      open: 1,
      critical: 1,
      warning: 0,
      flaggedRuns: 1,
    },
    events: { total: 12 },
  },
  settings: {
    stallAfterMs: 8_000,
    heartbeatIntervalMs: 2_000,
    watchdogIntervalMs: 1_000,
    demoControlsEnabled: true,
    runtimeInstanceId: "local-test",
  },
};

const mockApi = vi.mocked(api);

beforeEach(() => {
  mockApi.supervisorOverview.mockResolvedValue(overview);
  mockApi.supervisorRuns.mockResolvedValue({
    runs: [runningRun, completedRun],
  });
  mockApi.supervisorAlerts.mockResolvedValue({ alerts: [alert] });
  mockApi.supervisorRun.mockResolvedValue({ run: runningRun, alerts: [alert] });
  mockApi.supervisorRunEvents.mockResolvedValue({
    events: [heartbeatEvent, fileWriteEvent],
  });
  mockApi.supervisorCancel.mockResolvedValue({
    commandId: "55555555-5555-4555-8555-555555555555",
    runId: runningRun.runId,
    published: true,
  });
  mockApi.supervisorSimulateStall.mockResolvedValue({
    runId: runningRun.runId,
    pausedAt: "2026-08-30T10:00:05.000Z",
    stallAfterMs: 8_000,
  });
  mockApi.supervisorChat.mockResolvedValue({
    answer: "Run 11111111 matched secret-file-access.",
    citations: [
      {
        runId: runningRun.runId,
        eventId: alert.eventId,
        alertId: alert.alertId,
        occurredAt: alert.createdAt,
        label: "secret-file-access · run 11111111",
      },
    ],
    toolCalls: [{ tool: "listAlerts", arguments: {} }],
  });
});

describe("Supervisor dashboard", () => {
  it("renders counters, runs, alerts, and filters the run table", async () => {
    render(<SupervisorDashboard />);

    expect(await screen.findByText("Runtime heartbeat received")).toBeInTheDocument();
    expect(screen.getByText("11111111")).toBeInTheDocument();
    expect(screen.getByText("22222222")).toBeInTheDocument();
    expect(screen.getByText("secret-file-access")).toBeInTheDocument();
    expect(screen.getByTitle("Matching events")).toHaveTextContent("×2");
    expect(screen.getByText(/Stall threshold 8s/)).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("Filter by state"), {
      target: { value: "completed" },
    });
    expect(screen.queryByText("11111111")).not.toBeInTheDocument();
    expect(screen.getByText("22222222")).toBeInTheDocument();
  });

  it("loads a selected timeline with its Kafka offset", async () => {
    const user = userEvent.setup();
    render(<SupervisorDashboard />);
    await user.click(await screen.findByText("11111111"));

    expect(await screen.findByText("Heartbeat #2 received")).toBeInTheDocument();
    expect(screen.getByText('File "health-check.txt" was written')).toBeInTheDocument();

    const details = screen.getAllByText("Technical details")[0];
    await user.click(details);
    expect(screen.getByText("runtime.heartbeat")).toBeInTheDocument();
    expect(
      screen.getByText("agent-run-events-v1 · partition 1 · offset 42"),
    ).toBeInTheDocument();
    expect(mockApi.supervisorRun).toHaveBeenCalledWith(runningRun.runId);
    expect(mockApi.supervisorRunEvents).toHaveBeenCalledWith(runningRun.runId);
    expect(screen.getByText("Alerts").nextElementSibling).toHaveTextContent("1");
  });

  it("keeps the original command behind readable timeline details", async () => {
    const user = userEvent.setup();
    render(<SupervisorDashboard />);
    await user.click(await screen.findByText("11111111"));

    expect(
      await screen.findByText('File "health-check.txt" was written'),
    ).toBeInTheDocument();
    expect(screen.queryByText(/command_execution:/)).not.toBeInTheDocument();

    const details = screen.getAllByText("Technical details")[1];
    await user.click(details);
    expect(
      screen.getByText('/bin/bash -lc "echo \'OK\' > health-check.txt"'),
    ).toBeInTheDocument();
    expect(screen.getByText("run.tool_activity")).toBeInTheDocument();
    expect(screen.getByText("agent-run-events-v1 · partition 1 · offset 43"))
      .toBeInTheDocument();
  });

  it("publishes cancel and demo-stall actions for a selected run", async () => {
    const user = userEvent.setup();
    render(<SupervisorDashboard />);
    await user.click(await screen.findByText("11111111"));

    await user.click(await screen.findByRole("button", { name: "Cancel run" }));
    expect(mockApi.supervisorCancel).toHaveBeenCalledWith(runningRun.runId);
    expect(await screen.findByRole("status")).toHaveTextContent(
      "Cancellation command 55555555 published to Kafka.",
    );

    await user.click(
      screen.getByRole("button", { name: "Simulate missing heartbeat" }),
    );
    expect(mockApi.supervisorSimulateStall).toHaveBeenCalledWith(runningRun.runId);
    expect(await screen.findByRole("status")).toHaveTextContent(
      "Runtime container paused",
    );
  });

  it("renders a grounded chat answer, citations, and tool names", async () => {
    const user = userEvent.setup();
    render(<SupervisorDashboard />);
    const question = "Check all logs for suspicious intentions.";

    await user.click(await screen.findByRole("button", { name: question }));

    expect(mockApi.supervisorChat).toHaveBeenCalledWith(question, undefined);
    expect(
      await screen.findByText("Run 11111111 matched secret-file-access."),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", {
        name: "secret-file-access · run 11111111",
      }),
    ).toBeInTheDocument();
    expect(screen.getByText("listAlerts")).toBeInTheDocument();
  });

  it("shows an initial API failure as an accessible error", async () => {
    mockApi.supervisorOverview.mockRejectedValueOnce(
      new Error("Supervisor unavailable"),
    );
    render(<SupervisorDashboard />);

    expect(await screen.findByRole("alert")).toHaveTextContent(
      "Supervisor unavailable",
    );
    expect(screen.queryByRole("heading", { name: "Runs" })).not.toBeInTheDocument();
  });

  it("keeps each counter label paired with its value", async () => {
    render(<SupervisorDashboard />);
    await screen.findByText("Runtime heartbeat received");

    const runningCounter = screen.getByText("Running", {
      selector: ".sup-counter span",
    }).parentElement;
    expect(runningCounter).not.toBeNull();
    expect(within(runningCounter as HTMLElement).getByText("1")).toBeInTheDocument();
  });
});
