import { execFile, spawn, type ChildProcess } from "node:child_process";
import { promisify } from "node:util";
import type { AppConfig } from "./config.js";
import {
  buildCodexArgs,
  parseCodexEventLine,
  type CodexToolActivity,
} from "./codex-runner.js";
import { RunCancelledError } from "./errors.js";
import type {
  AgentRunner,
  RunnerCancellation,
  RunnerLifecycleEvent,
  RunUsage,
  RunnerRequest,
  RunnerResult,
} from "./types.js";

const execFileAsync = promisify(execFile);

interface ActiveContainer {
  child: ChildProcess;
  containerName: string;
  runId: string;
  agentId: string;
  runtimeInstanceId: string;
  cancelled: boolean;
  timedOut: boolean;
  outputExceeded: boolean;
  settled: Promise<void>;
  termination: Promise<void> | null;
}

interface ParsedEvents {
  messages: string[];
  threadId: string | null;
  usage: RunUsage | null;
  errors: string[];
}

export const RUNTIME_CONTROL_PREFIX = "__CODEJAM_RUNTIME_EVENT__";

interface RuntimeControlEvent {
  type: RunnerLifecycleEvent["type"];
  occurredAt: string;
  runId: string;
  agentId: string;
  runtimeInstanceId: string;
  payload: Record<string, unknown>;
}

export function parseRuntimeControlLine(line: string): RuntimeControlEvent | null {
  if (!line.startsWith(RUNTIME_CONTROL_PREFIX)) return null;
  try {
    const value = JSON.parse(line.slice(RUNTIME_CONTROL_PREFIX.length)) as Record<
      string,
      unknown
    >;
    if (
      !["runtime.started", "runtime.heartbeat", "runtime.exited"].includes(
        String(value.type),
      ) ||
      typeof value.occurredAt !== "string" ||
      Number.isNaN(Date.parse(value.occurredAt)) ||
      typeof value.runId !== "string" ||
      typeof value.agentId !== "string" ||
      typeof value.runtimeInstanceId !== "string" ||
      !value.payload ||
      typeof value.payload !== "object" ||
      Array.isArray(value.payload)
    ) {
      return null;
    }
    return value as unknown as RuntimeControlEvent;
  } catch {
    return null;
  }
}

export function containerName(agentId: string, instanceId = "default"): string {
  const safeInstance = instanceId.replace(/[^a-zA-Z0-9_.-]/g, "-").slice(0, 32);
  const safeAgent = agentId.replace(/[^a-zA-Z0-9_.-]/g, "-").slice(0, 48);
  return "launchpad-" + safeInstance + "-" + safeAgent;
}

export function buildContainerRunArgs(
  request: RunnerRequest,
  config: AppConfig,
): string[] {
  const name = containerName(request.agentId, config.runtimeInstanceId);
  const engineName = config.containerEngine.split(/[\\/]/).at(-1)?.toLowerCase();
  return [
    "run",
    "--rm",
    "--init",
    "--name",
    name,
    "--label",
    "io.codejam.launchpad=agent-runtime",
    "--label",
    "io.codejam.agent-id=" + request.agentId,
    "--label",
    "io.codejam.instance-id=" + config.runtimeInstanceId,
    "--label",
    "io.codejam.runtime-instance-id=" + request.runtimeInstanceId,
    "--label",
    "io.codejam.run-id=" + request.runId,
    ...(engineName === "podman" ? ["--userns", "keep-id"] : []),
    "--network",
    "bridge",
    "--security-opt",
    "no-new-privileges",
    "--cap-drop",
    "ALL",
    "--cpus",
    String(config.containerCpuLimit),
    "--memory",
    config.containerMemoryLimit,
    "--pids-limit",
    String(config.containerPidsLimit),
    "--user",
    config.containerUser,
    "--env",
    "MODEL_API_KEY",
    "--env",
    "CODEX_HOME=/codex-home",
    "--env",
    "HOME=/tmp",
    "--env",
    "NO_COLOR=1",
    "--env",
    "CODEJAM_RUN_ID=" + request.runId,
    "--env",
    "CODEJAM_AGENT_ID=" + request.agentId,
    "--env",
    "CODEJAM_RUNTIME_INSTANCE_ID=" + request.runtimeInstanceId,
    "--env",
    "CODEJAM_HEARTBEAT_INTERVAL_MS=" + config.supervisorHeartbeatIntervalMs,
    "--mount",
    "type=bind,src=" + request.workspacePath + ",dst=/workspace",
    "--mount",
    "type=bind,src=" + config.codexHome + ",dst=/codex-home",
    "--workdir",
    "/workspace",
    config.containerRuntimeImage,
    "node",
    "/opt/codejam/agent-runtime-wrapper.mjs",
    "codex",
    ...buildCodexArgs(request, config.codexSandboxMode, "/workspace"),
  ];
}

export class ContainerCodexRunner implements AgentRunner {
  private readonly active = new Map<string, ActiveContainer>();

  constructor(private readonly config: AppConfig) {}

  async isAvailable(): Promise<boolean> {
    try {
      await execFileAsync(this.config.containerEngine, ["version"], {
        timeout: 5_000,
        env: this.childEnvironment(),
      });
      await execFileAsync(
        this.config.containerEngine,
        ["image", "inspect", this.config.containerRuntimeImage],
        { timeout: 5_000, env: this.childEnvironment() },
      );
      return true;
    } catch {
      return false;
    }
  }

  async cancel(target: RunnerCancellation): Promise<boolean> {
    if (target.runtimeInstanceId !== this.config.runtimeInstanceId) return false;
    const active = this.active.get(target.agentId);
    if (
      active &&
      (active.runId !== target.runId ||
        active.runtimeInstanceId !== target.runtimeInstanceId)
    ) {
      return false;
    }

    const name = active?.containerName ?? containerName(target.agentId, target.runtimeInstanceId);
    const inspection = await this.inspectContainerIdentity(name, target);
    if (inspection === "missing") return false;
    if (inspection === "mismatch") {
      throw new Error(
        "Runtime labels did not match the cancellation command; refusing removal",
      );
    }
    if (active) active.cancelled = true;
    await execFileAsync(this.config.containerEngine, ["rm", "--force", name], {
      timeout: 8_000,
      env: this.childEnvironment(),
    });
    if (active) await active.settled;
    return true;
  }

  /**
   * Freezes the verified Runtime container. The heartbeat process lives inside
   * that container, so its heartbeats genuinely stop and the watchdog observes
   * a real missed heartbeat rather than an edited timestamp.
   */
  async pause(target: RunnerCancellation): Promise<boolean> {
    if (target.runtimeInstanceId !== this.config.runtimeInstanceId) return false;
    const active = this.active.get(target.agentId);
    if (
      active &&
      (active.runId !== target.runId ||
        active.runtimeInstanceId !== target.runtimeInstanceId)
    ) {
      return false;
    }
    const name =
      active?.containerName ??
      containerName(target.agentId, target.runtimeInstanceId);
    const inspection = await this.inspectContainerIdentity(name, target);
    if (inspection === "missing") return false;
    if (inspection === "mismatch") {
      throw new Error(
        "Runtime labels did not match the demo control; refusing to pause",
      );
    }
    await execFileAsync(this.config.containerEngine, ["pause", name], {
      timeout: 8_000,
      env: this.childEnvironment(),
    });
    return true;
  }

  private removeContainer(active: ActiveContainer): Promise<void> {
    if (!active.termination) {
      const target: RunnerCancellation = {
        runId: active.runId,
        agentId: active.agentId,
        runtimeInstanceId: active.runtimeInstanceId,
      };
      active.termination = this.inspectContainerIdentity(active.containerName, target)
        .then(async (inspection) => {
          if (inspection === "mismatch") {
            throw new Error(
              "Runtime labels changed unexpectedly; refusing automatic removal",
            );
          }
          if (inspection === "match") {
            await execFileAsync(
              this.config.containerEngine,
              ["rm", "--force", active.containerName],
              { timeout: 8_000, env: this.childEnvironment() },
            );
            return;
          }
          active.child.kill("SIGTERM");
          const forceKill = setTimeout(() => active.child.kill("SIGKILL"), 3_000);
          forceKill.unref();
        });
    }
    return active.termination;
  }

  private async inspectContainerIdentity(
    name: string,
    target: RunnerCancellation,
  ): Promise<"match" | "mismatch" | "missing"> {
    let output: string;
    try {
      const result = await execFileAsync(
        this.config.containerEngine,
        ["inspect", "--format", "{{json .Config.Labels}}", name],
        { timeout: 5_000, env: this.childEnvironment() },
      );
      output = String(result.stdout).trim();
    } catch {
      return "missing";
    }
    try {
      const labels = JSON.parse(output) as Record<string, unknown>;
      return labels["io.codejam.launchpad"] === "agent-runtime" &&
        labels["io.codejam.agent-id"] === target.agentId &&
        labels["io.codejam.run-id"] === target.runId &&
        labels["io.codejam.runtime-instance-id"] === target.runtimeInstanceId
        ? "match"
        : "mismatch";
    } catch {
      return "mismatch";
    }
  }

  async run(request: RunnerRequest): Promise<RunnerResult> {
    if (this.active.has(request.agentId)) {
      throw new Error("Agent already has an active Runtime container");
    }

    const child = spawn(
      this.config.containerEngine,
      buildContainerRunArgs(request, this.config),
      {
        cwd: request.workspacePath,
        env: this.childEnvironment(),
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    const settled = new Promise<void>((resolve) => {
      child.once("close", () => resolve());
      child.once("error", () => resolve());
    });
    const active: ActiveContainer = {
      child,
      containerName: containerName(request.agentId, this.config.runtimeInstanceId),
      runId: request.runId,
      agentId: request.agentId,
      runtimeInstanceId: request.runtimeInstanceId,
      cancelled: false,
      timedOut: false,
      outputExceeded: false,
      settled,
      termination: null,
    };
    this.active.set(request.agentId, active);

    const parsed: ParsedEvents = {
      messages: [],
      threadId: request.threadId,
      usage: null,
      errors: [],
    };
    let stdout = "";
    let stderr = "";
    let stderrBuffer = "";
    let totalBytes = 0;
    let sawRuntimeExit = false;

    const notifyLifecycle = (event: RunnerLifecycleEvent) => {
      try {
        void Promise.resolve(request.onLifecycleEvent?.(event)).catch(() => undefined);
      } catch {
        // A telemetry callback must not break the Agent process stream.
      }
    };
    const notifyToolActivity = (activity: CodexToolActivity) => {
      notifyLifecycle({
        type: "run.tool_activity",
        occurredAt: new Date().toISOString(),
        origin: "runtime",
        payload: { ...activity },
      });
    };
    const consumeStderrLine = (line: string, newline = true) => {
      const control = parseRuntimeControlLine(line);
      if (
        control &&
        control.runId === request.runId &&
        control.agentId === request.agentId &&
        control.runtimeInstanceId === request.runtimeInstanceId
      ) {
        if (control.type === "runtime.exited") {
          sawRuntimeExit = true;
          const exitCode =
            typeof control.payload.exitCode === "number"
              ? control.payload.exitCode
              : null;
          const signal =
            typeof control.payload.signal === "string"
              ? control.payload.signal
              : null;
          notifyLifecycle({
            type: "runtime.exited",
            occurredAt: control.occurredAt,
            origin: "runtime",
            payload: { ...control.payload, exitCode, signal },
          });
        } else {
          notifyLifecycle({
            type: control.type,
            occurredAt: control.occurredAt,
            origin: "runtime",
            payload: control.payload,
          });
        }
        return;
      }
      const visible = line + (newline ? "\n" : "");
      totalBytes += Buffer.byteLength(visible);
      stderr += visible;
      if (stderr.length > 16_384) stderr = stderr.slice(-16_384);
    };
    const enforceOutputLimit = () => {
      if (totalBytes > this.config.codexMaxOutputBytes) {
        active.outputExceeded = true;
        void this.removeContainer(active).catch(() => undefined);
      }
    };
    const consume = (chunk: Buffer, target: "stdout" | "stderr") => {
      if (target === "stdout") {
        totalBytes += chunk.byteLength;
        stdout += chunk.toString("utf8");
        const lines = stdout.split(/\r?\n/);
        stdout = lines.pop() ?? "";
        for (const line of lines) {
          parseCodexEventLine(line, parsed, notifyToolActivity);
        }
      } else {
        stderrBuffer += chunk.toString("utf8");
        const lines = stderrBuffer.split(/\r?\n/);
        stderrBuffer = lines.pop() ?? "";
        for (const line of lines) consumeStderrLine(line);
        if (stderrBuffer.length > 65_536) {
          consumeStderrLine(stderrBuffer, false);
          stderrBuffer = "";
        }
      }
      enforceOutputLimit();
    };

    child.stdout.on("data", (chunk: Buffer) => consume(chunk, "stdout"));
    child.stderr.on("data", (chunk: Buffer) => consume(chunk, "stderr"));

    const timeout = setTimeout(() => {
      active.timedOut = true;
      void this.removeContainer(active).catch(() => undefined);
    }, this.config.codexTimeoutMs);
    timeout.unref();

    try {
      const exit = await new Promise<{
        exitCode: number;
        signal: NodeJS.Signals | null;
      }>((resolve, reject) => {
        child.once("error", reject);
        child.once("close", (code, signal) =>
          resolve({ exitCode: code ?? 1, signal }),
        );
      });
      if (stdout.trim()) {
        parseCodexEventLine(stdout.trim(), parsed, notifyToolActivity);
      }
      if (stderrBuffer) consumeStderrLine(stderrBuffer, false);
      if (!sawRuntimeExit) {
        notifyLifecycle({
          type: "runtime.exited",
          occurredAt: new Date().toISOString(),
          origin: "control-plane",
          payload: { exitCode: exit.exitCode, signal: exit.signal },
        });
      }
      if (active.cancelled) throw new RunCancelledError();
      if (active.timedOut) {
        throw new Error("Runtime timed out after " + this.config.codexTimeoutMs + " ms");
      }
      if (active.outputExceeded) {
        throw new Error("Codex output exceeded CODEX_MAX_OUTPUT_BYTES");
      }
      if (exit.exitCode !== 0) {
        const detail = parsed.errors.at(-1) ?? stderr.trim() ?? "No error detail";
        throw new Error(
          this.config.containerEngine +
            " Runtime exited with code " +
            exit.exitCode +
            ": " +
            detail,
        );
      }
      const output = parsed.messages.at(-1)?.trim();
      if (!output) throw new Error("Codex completed without an agent message");
      return { output, threadId: parsed.threadId, usage: parsed.usage };
    } finally {
      clearTimeout(timeout);
      this.active.delete(request.agentId);
    }
  }

  private childEnvironment(): NodeJS.ProcessEnv {
    const environment: NodeJS.ProcessEnv = {
      MODEL_API_KEY: this.config.modelApiKey,
      NO_COLOR: "1",
    };
    for (const name of [
      "PATH",
      "HOME",
      "TMPDIR",
      "LANG",
      "LC_ALL",
      "XDG_RUNTIME_DIR",
    ] as const) {
      if (process.env[name] !== undefined) environment[name] = process.env[name];
    }
    return environment;
  }
}
