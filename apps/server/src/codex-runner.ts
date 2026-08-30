import { execFile } from "node:child_process";
import { spawn, type ChildProcess } from "node:child_process";
import { promisify } from "node:util";
import type { AppConfig } from "./config.js";
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

export interface ParsedEvents {
  messages: string[];
  threadId: string | null;
  usage: RunUsage | null;
  errors: string[];
}

export function buildCodexArgs(
  request: RunnerRequest,
  sandboxMode: AppConfig["codexSandboxMode"],
  workspacePath = request.workspacePath,
): string[] {
  const args = [
    "exec",
    "--json",
    "--sandbox",
    sandboxMode,
    "--skip-git-repo-check",
    "-C",
    workspacePath,
  ];
  if (request.threadId) {
    args.push("resume", request.threadId, request.prompt);
  } else {
    args.push(request.prompt);
  }
  return args;
}

/** Tool use reported by `codex exec --json`, normalised for the supervisor. */
export interface CodexToolActivity {
  itemType: string;
  itemId: string | null;
  status: string | null;
  command: string | null;
  exitCode: number | null;
  detail: string | null;
}

const TOOL_ACTIVITY_ITEM_TYPES = new Set([
  "command_execution",
  "file_change",
  "mcp_tool_call",
  "web_search",
  "patch_apply",
]);

function text(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value : null;
}

function truncate(value: string, max = 800): string {
  return value.length > max ? value.slice(0, max) + "…" : value;
}

function toToolActivity(
  item: Record<string, unknown>,
  eventType: string,
): CodexToolActivity | null {
  const itemType = typeof item.type === "string" ? item.type : "";
  if (!TOOL_ACTIVITY_ITEM_TYPES.has(itemType)) return null;
  // Commands are reported twice so a run frozen mid-command still leaves
  // evidence of what it was doing; everything else is reported on completion.
  if (eventType === "item.started" && itemType !== "command_execution") {
    return null;
  }

  let command = text(item.command);
  let detail: string | null = null;
  if (itemType === "file_change" || itemType === "patch_apply") {
    const changes = Array.isArray(item.changes) ? item.changes : [];
    const paths = changes
      .map((change) =>
        change && typeof change === "object"
          ? text((change as Record<string, unknown>).path)
          : null,
      )
      .filter((value): value is string => Boolean(value));
    command = command ?? (paths.length ? paths.join(", ") : null);
    detail = paths.length ? paths.join("\n") : null;
  } else if (itemType === "mcp_tool_call") {
    const server = text(item.server) ?? "unknown";
    const tool = text(item.tool) ?? "unknown";
    command = command ?? server + "." + tool;
  } else if (itemType === "web_search") {
    command = command ?? text(item.query);
  } else {
    detail = text(item.aggregated_output) ?? text(item.output);
  }

  return {
    itemType,
    itemId: text(item.id),
    status: text(item.status) ?? (eventType === "item.started" ? "started" : null),
    command: command ? truncate(command, 2_000) : null,
    exitCode: typeof item.exit_code === "number" ? item.exit_code : null,
    detail: detail ? truncate(detail) : null,
  };
}

export function parseCodexEventLine(
  line: string,
  parsed: ParsedEvents,
  onToolActivity?: (activity: CodexToolActivity) => void,
): void {
  let event: Record<string, unknown>;
  try {
    event = JSON.parse(line) as Record<string, unknown>;
  } catch {
    return;
  }

  if (event.type === "thread.started" && typeof event.thread_id === "string") {
    parsed.threadId = event.thread_id;
  }

  if (
    (event.type === "item.completed" || event.type === "item.started") &&
    event.item &&
    typeof event.item === "object"
  ) {
    const item = event.item as Record<string, unknown>;
    if (
      event.type === "item.completed" &&
      item.type === "agent_message" &&
      typeof item.text === "string"
    ) {
      parsed.messages.push(item.text);
    }
    if (onToolActivity) {
      const activity = toToolActivity(item, event.type);
      if (activity) onToolActivity(activity);
    }
  }

  if (event.type === "turn.completed" && event.usage && typeof event.usage === "object") {
    const usage = event.usage as Record<string, unknown>;
    parsed.usage = {
      ...(typeof usage.input_tokens === "number"
        ? { inputTokens: usage.input_tokens }
        : {}),
      ...(typeof usage.cached_input_tokens === "number"
        ? { cachedInputTokens: usage.cached_input_tokens }
        : {}),
      ...(typeof usage.output_tokens === "number"
        ? { outputTokens: usage.output_tokens }
        : {}),
    };
  }

  if (event.type === "error") {
    const message =
      typeof event.message === "string"
        ? event.message
        : typeof event.error === "string"
          ? event.error
          : "Codex reported an unknown error";
    parsed.errors.push(message);
  }
}

export class CodexRunner implements AgentRunner {
  private readonly active = new Map<
    string,
    {
      child: ChildProcess;
      runId: string;
      runtimeInstanceId: string;
      cancelled: boolean;
      timedOut: boolean;
      outputExceeded: boolean;
      settled: Promise<void>;
      forceKillTimer: NodeJS.Timeout | null;
    }
  >();

  constructor(private readonly config: AppConfig) {}

  async isAvailable(): Promise<boolean> {
    try {
      await execFileAsync(this.config.codexBin, ["--version"], {
        timeout: 5_000,
        env: this.childEnvironment(),
      });
      return true;
    } catch {
      return false;
    }
  }

  async cancel(target: RunnerCancellation): Promise<boolean> {
    const active = this.active.get(target.agentId);
    if (!active) {
      return false;
    }
    if (
      active.runId !== target.runId ||
      active.runtimeInstanceId !== target.runtimeInstanceId
    ) {
      return false;
    }
    active.cancelled = true;
    this.terminate(active);
    await active.settled;
    return true;
  }

  async run(request: RunnerRequest): Promise<RunnerResult> {
    if (this.active.has(request.agentId)) {
      throw new Error("Agent already has an active Codex process");
    }

    const args = buildCodexArgs(request, this.config.codexSandboxMode);
    const child = spawn(this.config.codexBin, args, {
      cwd: request.workspacePath,
      env: this.childEnvironment(),
      stdio: ["ignore", "pipe", "pipe"],
    });
    const settled = new Promise<void>((resolve) => {
      child.once("close", () => resolve());
      child.once("error", () => resolve());
    });
    const active = {
      child,
      runId: request.runId,
      runtimeInstanceId: request.runtimeInstanceId,
      cancelled: false,
      timedOut: false,
      outputExceeded: false,
      settled,
      forceKillTimer: null as NodeJS.Timeout | null,
    };
    this.active.set(request.agentId, active);

    const notifyLifecycle = (event: RunnerLifecycleEvent) => {
      try {
        void Promise.resolve(request.onLifecycleEvent?.(event)).catch(() => undefined);
      } catch {
        // A telemetry callback must not break the Agent process stream.
      }
    };
    notifyLifecycle({
      type: "runtime.started",
      occurredAt: new Date().toISOString(),
      origin: "control-plane",
      payload: { pid: child.pid ?? null },
    });
    let heartbeatSequence = 0;
    const heartbeat = setInterval(() => {
      heartbeatSequence += 1;
      notifyLifecycle({
        type: "runtime.heartbeat",
        occurredAt: new Date().toISOString(),
        origin: "control-plane",
        payload: { sequence: heartbeatSequence },
      });
    }, this.config.supervisorHeartbeatIntervalMs);
    heartbeat.unref();

    const notifyToolActivity = (activity: CodexToolActivity) => {
      notifyLifecycle({
        type: "run.tool_activity",
        occurredAt: new Date().toISOString(),
        origin: "control-plane",
        payload: { ...activity },
      });
    };

    const parsed: ParsedEvents = {
      messages: [],
      threadId: request.threadId,
      usage: null,
      errors: [],
    };
    let stdout = "";
    let stderr = "";
    let totalBytes = 0;

    const consume = (chunk: Buffer, target: "stdout" | "stderr") => {
      totalBytes += chunk.byteLength;
      if (totalBytes > this.config.codexMaxOutputBytes) {
        active.outputExceeded = true;
        this.terminate(active);
        return;
      }
      if (target === "stdout") {
        stdout += chunk.toString("utf8");
        const lines = stdout.split(/\r?\n/);
        stdout = lines.pop() ?? "";
        for (const line of lines) {
          parseCodexEventLine(line, parsed, notifyToolActivity);
        }
      } else {
        stderr += chunk.toString("utf8");
        if (stderr.length > 16_384) {
          stderr = stderr.slice(-16_384);
        }
      }
    };

    child.stdout.on("data", (chunk: Buffer) => consume(chunk, "stdout"));
    child.stderr.on("data", (chunk: Buffer) => consume(chunk, "stderr"));

    const timeout = setTimeout(() => {
      active.timedOut = true;
      this.terminate(active);
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
      notifyLifecycle({
        type: "runtime.exited",
        occurredAt: new Date().toISOString(),
        origin: "control-plane",
        payload: { exitCode: exit.exitCode, signal: exit.signal },
      });
      if (stdout.trim()) {
        parseCodexEventLine(stdout.trim(), parsed, notifyToolActivity);
      }
      if (active.cancelled) {
        throw new RunCancelledError();
      }
      if (active.timedOut) {
        throw new Error("Codex timed out after " + this.config.codexTimeoutMs + " ms");
      }
      if (active.outputExceeded) {
        throw new Error("Codex output exceeded CODEX_MAX_OUTPUT_BYTES");
      }
      if (exit.exitCode !== 0) {
        const detail = parsed.errors.at(-1) ?? stderr.trim() ?? "No error detail";
        throw new Error("Codex exited with code " + exit.exitCode + ": " + detail);
      }
      const output = parsed.messages.at(-1)?.trim();
      if (!output) {
        throw new Error("Codex completed without an agent message");
      }
      return {
        output,
        threadId: parsed.threadId,
        usage: parsed.usage,
      };
    } finally {
      clearTimeout(timeout);
      clearInterval(heartbeat);
      if (active.forceKillTimer) clearTimeout(active.forceKillTimer);
      this.active.delete(request.agentId);
    }
  }

  private terminate(active: {
    child: ChildProcess;
    forceKillTimer: NodeJS.Timeout | null;
  }): void {
    if (active.child.exitCode !== null || active.child.signalCode !== null) return;
    active.child.kill("SIGTERM");
    if (!active.forceKillTimer) {
      active.forceKillTimer = setTimeout(() => active.child.kill("SIGKILL"), 3_000);
      active.forceKillTimer.unref();
    }
  }

  private childEnvironment(): NodeJS.ProcessEnv {
    const inheritedNames = [
      "PATH",
      "HOME",
      "TMPDIR",
      "LANG",
      "LC_ALL",
      "SSL_CERT_FILE",
      "SSL_CERT_DIR",
      "HTTP_PROXY",
      "HTTPS_PROXY",
      "NO_PROXY",
      "NODE_EXTRA_CA_CERTS",
      "TERM",
    ] as const;
    const environment: NodeJS.ProcessEnv = {
      CODEX_HOME: this.config.codexHome,
      MODEL_API_KEY: this.config.modelApiKey,
      NO_COLOR: "1",
    };
    for (const name of inheritedNames) {
      if (process.env[name] !== undefined) environment[name] = process.env[name];
    }
    return environment;
  }
}
