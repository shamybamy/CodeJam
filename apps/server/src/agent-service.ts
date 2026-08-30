import { randomUUID } from "node:crypto";
import type { AppConfig } from "./config.js";
import { isModelConfigured } from "./config.js";
import { HttpError, RunCancelledError } from "./errors.js";
import { JsonStore } from "./store.js";
import {
  createSupervisorEvent,
  type SupervisorCommand,
  type SupervisorEvent,
} from "./supervisor-contracts.js";
import type {
  Agent,
  AgentRun,
  AgentRunner,
  CreateAgentInput,
  Message,
  RunnerLifecycleEvent,
  UpdateAgentInput,
} from "./types.js";
import { WorkspaceManager } from "./workspace.js";

const now = () => new Date().toISOString();

/** Human-readable one-liner for a Codex tool call, used as the event summary. */
function describeToolActivity(payload: Record<string, unknown>): string {
  const itemType =
    typeof payload.itemType === "string" ? payload.itemType : "tool call";
  const command = typeof payload.command === "string" ? payload.command : "";
  const label = command ? itemType + ": " + command : "Agent " + itemType;
  return label.replace(/\s+/g, " ").trim().slice(0, 500) || "Agent tool activity";
}

export interface SupervisorEventPublisher {
  publishEvent(event: SupervisorEvent): Promise<void>;
}

export class AgentService {
  private readonly activeExecutions = new Map<string, Promise<void>>();
  private readonly cancellationRequests = new Set<string>();
  private readonly cancellationReasons = new Map<string, string>();

  constructor(
    private readonly config: AppConfig,
    private readonly store: JsonStore,
    private readonly workspaces: WorkspaceManager,
    private readonly runner: AgentRunner,
    private readonly supervisor?: SupervisorEventPublisher,
  ) {}

  async initialize(): Promise<void> {
    await this.store.initialize();
    await this.workspaces.initialize();
    await this.store.mutate((database) => {
      for (const run of database.runs) {
        if (run.status === "queued" || run.status === "running") {
          run.status = "cancelled";
          run.error = "Server restarted while this run was active";
          run.completedAt = now();
        }
      }
      for (const agent of database.agents) {
        if (agent.status === "busy") {
          agent.status = "ready";
          agent.updatedAt = now();
        }
      }
    });
  }

  listAgents(): Agent[] {
    return this.store
      .snapshot()
      .agents.sort((left, right) => right.updatedAt.localeCompare(left.updatedAt));
  }

  getAgent(id: string): Agent {
    const agent = this.store.snapshot().agents.find((item) => item.id === id);
    if (!agent) {
      throw new HttpError(404, "Agent not found");
    }
    return agent;
  }

  async createAgent(input: CreateAgentInput): Promise<Agent> {
    const timestamp = now();
    const id = randomUUID();
    const agent: Agent = {
      id,
      name: input.name.trim(),
      description: input.description?.trim() ?? "",
      instructions: input.instructions?.trim() ?? "",
      status: "ready",
      workspacePath: this.workspaces.workspacePath(id),
      codexThreadId: null,
      lastError: null,
      createdAt: timestamp,
      updatedAt: timestamp,
    };
    await this.workspaces.create(agent);
    await this.store.mutate((database) => database.agents.push(agent));
    return agent;
  }

  async updateAgent(id: string, input: UpdateAgentInput): Promise<Agent> {
    const current = this.getAgent(id);
    if (current.status === "busy") {
      throw new HttpError(409, "Stop the active run before editing this Agent");
    }
    const updated = await this.store.mutate((database) => {
      const agent = database.agents.find((item) => item.id === id);
      if (!agent) {
        throw new HttpError(404, "Agent not found");
      }
      if (agent.status === "busy") {
        throw new HttpError(409, "Stop the active run before editing this Agent");
      }
      if (input.name !== undefined) agent.name = input.name.trim();
      if (input.description !== undefined) agent.description = input.description.trim();
      if (input.instructions !== undefined) agent.instructions = input.instructions.trim();
      agent.lastError = null;
      agent.updatedAt = now();
      return structuredClone(agent);
    });
    await this.workspaces.writeInstructions(updated);
    return updated;
  }

  async deleteAgent(id: string): Promise<{ archivedWorkspace: string }> {
    const agent = this.getAgent(id);
    await this.cancelExecution(id);
    const archivedWorkspace = await this.workspaces.archive(agent);
    await this.store.mutate((database) => {
      database.agents = database.agents.filter((item) => item.id !== id);
      database.messages = database.messages.filter((item) => item.agentId !== id);
      database.runs = database.runs.filter((item) => item.agentId !== id);
    });
    return { archivedWorkspace };
  }

  async startAgent(id: string): Promise<Agent> {
    return this.setStatus(id, "ready");
  }

  async stopAgent(id: string): Promise<Agent> {
    this.getAgent(id);
    await this.cancelExecution(id);
    return this.setStatus(id, "stopped");
  }

  getMessages(agentId: string): Message[] {
    this.getAgent(agentId);
    return this.store
      .snapshot()
      .messages.filter((message) => message.agentId === agentId)
      .sort((left, right) => left.createdAt.localeCompare(right.createdAt));
  }

  getRun(runId: string): AgentRun {
    const run = this.store.snapshot().runs.find((item) => item.id === runId);
    if (!run) {
      throw new HttpError(404, "Run not found");
    }
    return run;
  }

  getRuns(agentId: string): AgentRun[] {
    this.getAgent(agentId);
    return this.store
      .snapshot()
      .runs.filter((run) => run.agentId === agentId)
      .sort((left, right) => right.createdAt.localeCompare(left.createdAt));
  }

  async sendMessage(
    agentId: string,
    prompt: string,
  ): Promise<{ run: AgentRun; message: Message }> {
    if (!isModelConfigured(this.config)) {
      throw new HttpError(
        503,
        "The model provider is not configured. Set MODEL_ID and MODEL_API_KEY, then restart.",
      );
    }
    const timestamp = now();
    const runId = randomUUID();
    const run: AgentRun = {
      id: runId,
      agentId,
      status: "queued",
      prompt,
      output: null,
      error: null,
      usage: null,
      startedAt: null,
      completedAt: null,
      createdAt: timestamp,
    };
    const message: Message = {
      id: randomUUID(),
      agentId,
      runId,
      role: "user",
      content: prompt,
      createdAt: timestamp,
    };
    const agentAtStart = await this.store.mutate((database) => {
      const storedAgent = database.agents.find((item) => item.id === agentId);
      if (!storedAgent) {
        throw new HttpError(404, "Agent not found");
      }
      if (storedAgent.status === "stopped") {
        throw new HttpError(409, "Start the Agent before sending a message");
      }
      if (storedAgent.status === "busy") {
        throw new HttpError(409, "This Agent is already running");
      }
      database.runs.push(run);
      database.messages.push(message);
      const snapshot = structuredClone(storedAgent);
      storedAgent.status = "busy";
      storedAgent.lastError = null;
      storedAgent.updatedAt = timestamp;
      return snapshot;
    });
    try {
      await this.publishEvent(
        createSupervisorEvent({
          type: "run.queued",
          runId,
          agentId,
          runtimeInstanceId: this.config.runtimeInstanceId,
          source: "control-plane",
          severity: "info",
          summary: "Agent run queued",
          payload: { promptLength: prompt.length },
        }),
      );
    } catch (error) {
      await this.store.mutate((database) => {
        database.runs = database.runs.filter((item) => item.id !== runId);
        database.messages = database.messages.filter((item) => item.runId !== runId);
        const storedAgent = database.agents.find((item) => item.id === agentId);
        if (storedAgent) {
          storedAgent.status = "ready";
          storedAgent.updatedAt = now();
        }
      });
      throw new HttpError(
        503,
        "The run supervisor is unavailable; the Agent was not started",
      );
    }
    const execution = this.executeRun(agentAtStart, run);
    this.activeExecutions.set(agentId, execution);
    void execution
      .finally(() => {
        if (this.activeExecutions.get(agentId) === execution) {
          this.activeExecutions.delete(agentId);
        }
      })
      .catch(() => undefined);
    return { run, message };
  }

  async systemInfo(): Promise<Record<string, unknown>> {
    return {
      modelConfigured: isModelConfigured(this.config),
      modelProvider: this.config.modelProvider,
      modelBaseUrl: this.config.modelBaseUrl,
      modelId: this.config.modelId || null,
      codexAvailable: await this.runner.isAvailable(),
      codexSandboxMode: this.config.codexSandboxMode,
      runtimeProvider: this.config.runtimeProvider,
      containerEngine:
        this.config.runtimeProvider === "container"
          ? this.config.containerEngine
          : null,
      runtime:
        this.config.runtimeProvider === "container"
          ? "Codex CLI in " + this.config.containerEngine + " Runtime"
          : "Codex CLI in application container",
    };
  }

  async handleSupervisorCommand(command: SupervisorCommand): Promise<void> {
    if (command.runtimeInstanceId !== this.config.runtimeInstanceId) {
      // Another Runtime instance owns this command. Ignoring it keeps the shared
      // command topic moving; throwing would retry a message this process can
      // never execute and block every later cancellation on the partition.
      return;
    }
    const run = this.store
      .snapshot()
      .runs.find((item) => item.id === command.runId);
    if (!run) return;
    if (run.agentId !== command.agentId) {
      throw new Error("Cancellation command Agent does not match the stored run");
    }
    if (["completed", "failed", "cancelled"].includes(run.status)) return;

    const removed = await this.cancelExecution(
      command.agentId,
      command.runId,
      command.reason,
    );
    await this.publishEventSafely(
      createSupervisorEvent({
        type: "supervisor.recovered",
        runId: command.runId,
        agentId: command.agentId,
        runtimeInstanceId: command.runtimeInstanceId,
        source: "supervisor",
        severity: "info",
        summary: removed
          ? "Verified Runtime container removed"
          : "Run recovered without an active Runtime container",
        payload: {
          commandId: command.commandId,
          labelsVerified: removed,
          containerRemoved: removed,
        },
      }),
    );
  }

  /**
   * Demo control. Freezes the verified Runtime container so its heartbeat stops
   * for real; the watchdog then observes a genuine missed heartbeat. Nothing
   * here writes to the ledger's heartbeat timestamps.
   */
  async simulateMissingHeartbeat(runId: string): Promise<{
    runId: string;
    agentId: string;
    runtimeInstanceId: string;
    pausedAt: string;
  }> {
    const run = this.getRun(runId);
    if (run.status !== "running" && run.status !== "queued") {
      throw new HttpError(409, "This run is no longer active");
    }
    if (!this.runner.pause) {
      throw new HttpError(
        409,
        "The active Runtime provider cannot be paused. Start the POC with RUNTIME_PROVIDER=container.",
      );
    }
    const target = {
      runId: run.id,
      agentId: run.agentId,
      runtimeInstanceId: this.config.runtimeInstanceId,
    };
    const paused = await this.runner.pause(target);
    if (!paused) {
      throw new HttpError(
        409,
        "No Runtime container with matching labels is active for this run",
      );
    }
    const pausedAt = now();
    await this.publishEventSafely(
      createSupervisorEvent({
        type: "supervisor.demo_paused",
        occurredAt: pausedAt,
        runId: run.id,
        agentId: run.agentId,
        runtimeInstanceId: this.config.runtimeInstanceId,
        source: "operator",
        severity: "warning",
        summary: "Demo control paused the Agent Runtime container",
        payload: {
          control: "simulate-stall",
          labelsVerified: true,
          expectStallAfterMs: this.config.supervisorStallAfterMs,
        },
      }),
    );
    return { ...target, pausedAt };
  }

  private async executeRun(agentAtStart: Agent, run: AgentRun): Promise<void> {
    await this.store.mutate((database) => {
      const storedRun = database.runs.find((item) => item.id === run.id);
      if (storedRun) {
        storedRun.status = "running";
        storedRun.startedAt = now();
      }
    });
    let lifecycleQueue = Promise.resolve();
    const enqueueLifecycleEvent = (event: RunnerLifecycleEvent) => {
      lifecycleQueue = lifecycleQueue
        .then(() => this.publishLifecycleEvent(run, event))
        .catch((error) => {
          console.warn(
            "[supervisor] Failed to publish Runtime lifecycle event",
            error instanceof Error ? error.message : String(error),
          );
        });
      return lifecycleQueue;
    };
    try {
      if (this.cancellationRequests.has(run.id)) {
        throw new RunCancelledError();
      }
      const result = await this.runner.run({
        runId: run.id,
        agentId: agentAtStart.id,
        runtimeInstanceId: this.config.runtimeInstanceId,
        workspacePath: agentAtStart.workspacePath,
        prompt: run.prompt,
        threadId: agentAtStart.codexThreadId,
        onLifecycleEvent: enqueueLifecycleEvent,
      });
      await lifecycleQueue;
      const completedAt = now();
      await this.store.mutate((database) => {
        const storedRun = database.runs.find((item) => item.id === run.id);
        const agent = database.agents.find((item) => item.id === agentAtStart.id);
        if (!storedRun || !agent) return;
        storedRun.status = "completed";
        storedRun.output = result.output;
        storedRun.usage = result.usage;
        storedRun.completedAt = completedAt;
        database.messages.push({
          id: randomUUID(),
          agentId: agent.id,
          runId: run.id,
          role: "assistant",
          content: result.output,
          createdAt: completedAt,
        });
        agent.status = "ready";
        agent.codexThreadId = result.threadId;
        agent.lastError = null;
        agent.updatedAt = completedAt;
      });
      await this.publishEventSafely(
        createSupervisorEvent({
          type: "run.completed",
          occurredAt: completedAt,
          runId: run.id,
          agentId: agentAtStart.id,
          runtimeInstanceId: this.config.runtimeInstanceId,
          source: "control-plane",
          severity: "info",
          summary: "Agent run completed",
          payload: { usage: result.usage ?? {} },
        }),
      );
    } catch (error) {
      await lifecycleQueue;
      const completedAt = now();
      const cancelled = error instanceof RunCancelledError;
      const message = error instanceof Error ? error.message : String(error);
      await this.store.mutate((database) => {
        const storedRun = database.runs.find((item) => item.id === run.id);
        const agent = database.agents.find((item) => item.id === agentAtStart.id);
        if (storedRun) {
          storedRun.status = cancelled ? "cancelled" : "failed";
          storedRun.error = message;
          storedRun.completedAt = completedAt;
        }
        if (agent) {
          if (agent.status !== "stopped") {
            agent.status = cancelled ? "ready" : "error";
          }
          agent.lastError = cancelled ? null : message;
          agent.updatedAt = completedAt;
        }
      });
      await this.publishEventSafely(
        createSupervisorEvent({
          type: cancelled ? "run.cancelled" : "run.failed",
          occurredAt: completedAt,
          runId: run.id,
          agentId: agentAtStart.id,
          runtimeInstanceId: this.config.runtimeInstanceId,
          source: "control-plane",
          severity: cancelled ? "warning" : "critical",
          summary: cancelled ? "Agent run cancelled" : "Agent run failed",
          payload: {
            reason: this.cancellationReasons.get(run.id) ?? message,
          },
        }),
      );
    }
  }

  private publishLifecycleEvent(
    run: AgentRun,
    event: RunnerLifecycleEvent,
  ): Promise<void> {
    const type = event.type === "runtime.started" ? "run.started" : event.type;
    const exitCode =
      event.type === "runtime.exited" ? event.payload.exitCode : null;
    return this.publishEvent(
      createSupervisorEvent({
        type,
        occurredAt: event.occurredAt,
        runId: run.id,
        agentId: run.agentId,
        runtimeInstanceId: this.config.runtimeInstanceId,
        source: event.origin === "runtime" ? "runtime" : "control-plane",
        severity:
          event.type === "runtime.exited" && exitCode !== 0 ? "warning" : "info",
        summary:
          event.type === "runtime.started"
            ? "Agent Runtime started"
            : event.type === "runtime.heartbeat"
              ? "Agent Runtime heartbeat"
              : event.type === "run.tool_activity"
                ? describeToolActivity(event.payload)
                : "Agent Runtime exited",
        payload: event.payload,
      }),
    );
  }

  private publishEvent(event: SupervisorEvent): Promise<void> {
    return this.supervisor?.publishEvent(event) ?? Promise.resolve();
  }

  private async publishEventSafely(event: SupervisorEvent): Promise<void> {
    try {
      await this.publishEvent(event);
    } catch (error) {
      console.warn(
        "[supervisor] Failed to publish event",
        event.type,
        error instanceof Error ? error.message : String(error),
      );
    }
  }

  private async setStatus(id: string, status: Agent["status"]): Promise<Agent> {
    return this.store.mutate((database) => {
      const agent = database.agents.find((item) => item.id === id);
      if (!agent) {
        throw new HttpError(404, "Agent not found");
      }
      if (status === "ready" && agent.status === "busy") {
        throw new HttpError(409, "Stop the active run before starting this Agent");
      }
      agent.status = status;
      if (status === "ready") agent.lastError = null;
      agent.updatedAt = now();
      return structuredClone(agent);
    });
  }

  private async cancelExecution(
    agentId: string,
    expectedRunId?: string,
    reason = "Operator requested cancellation",
  ): Promise<boolean> {
    const run = this.store
      .snapshot()
      .runs.find(
        (item) =>
          item.agentId === agentId &&
          (!expectedRunId || item.id === expectedRunId) &&
          (item.status === "queued" || item.status === "running"),
      );
    if (!run) return false;
    this.cancellationRequests.add(run.id);
    this.cancellationReasons.set(run.id, reason);
    try {
      const removed = await this.runner.cancel({
        runId: run.id,
        agentId,
        runtimeInstanceId: this.config.runtimeInstanceId,
      });
      const execution = this.activeExecutions.get(agentId);
      if (execution) {
        await execution;
      }
      return removed;
    } finally {
      this.cancellationRequests.delete(run.id);
      this.cancellationReasons.delete(run.id);
    }
  }
}
