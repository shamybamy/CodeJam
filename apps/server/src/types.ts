export type AgentStatus = "ready" | "busy" | "stopped" | "error";
export type RunStatus = "queued" | "running" | "completed" | "failed" | "cancelled";
export type MessageRole = "user" | "assistant";

export interface Agent {
  id: string;
  name: string;
  description: string;
  instructions: string;
  status: AgentStatus;
  workspacePath: string;
  codexThreadId: string | null;
  lastError: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface Message {
  id: string;
  agentId: string;
  runId: string;
  role: MessageRole;
  content: string;
  createdAt: string;
}

export interface RunUsage {
  inputTokens?: number;
  cachedInputTokens?: number;
  outputTokens?: number;
}

export interface AgentRun {
  id: string;
  agentId: string;
  status: RunStatus;
  prompt: string;
  output: string | null;
  error: string | null;
  usage: RunUsage | null;
  startedAt: string | null;
  completedAt: string | null;
  createdAt: string;
}

export interface Database {
  version: 1;
  agents: Agent[];
  messages: Message[];
  runs: AgentRun[];
}

export interface CreateAgentInput {
  name: string;
  description?: string | undefined;
  instructions?: string | undefined;
}

export interface UpdateAgentInput {
  name?: string | undefined;
  description?: string | undefined;
  instructions?: string | undefined;
}

export interface RunnerResult {
  output: string;
  threadId: string | null;
  usage: RunUsage | null;
}

export type RunnerLifecycleEvent =
  | {
      type: "runtime.started" | "runtime.heartbeat" | "run.tool_activity";
      occurredAt: string;
      origin: "runtime" | "control-plane";
      payload: Record<string, unknown>;
    }
  | {
      type: "runtime.exited";
      occurredAt: string;
      origin: "runtime" | "control-plane";
      payload: Record<string, unknown> & {
        exitCode: number | null;
        signal: string | null;
      };
    };

export interface RunnerRequest {
  runId: string;
  agentId: string;
  runtimeInstanceId: string;
  workspacePath: string;
  prompt: string;
  threadId: string | null;
  onLifecycleEvent?:
    | ((event: RunnerLifecycleEvent) => Promise<void> | void)
    | undefined;
}

export interface RunnerCancellation {
  runId: string;
  agentId: string;
  runtimeInstanceId: string;
}

export interface AgentRunner {
  run(request: RunnerRequest): Promise<RunnerResult>;
  cancel(target: RunnerCancellation): Promise<boolean>;
  isAvailable(): Promise<boolean>;
  /**
   * Freezes the Runtime so its heartbeat genuinely stops. Only Runtimes that
   * own a container can honour this; the demo control reports "unsupported"
   * for every other provider instead of faking a missed heartbeat.
   */
  pause?(target: RunnerCancellation): Promise<boolean>;
}
