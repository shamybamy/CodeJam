import type { SupervisorEventRecord } from "./supervisor-types";

export interface TimelinePresentation {
  title: string;
  description?: string;
  status?: string;
  command?: string;
  output?: string;
}

function text(payload: Record<string, unknown>, key: string): string | null {
  const value = payload[key];
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function number(payload: Record<string, unknown>, key: string): number | null {
  const value = payload[key];
  return typeof value === "number" ? value : null;
}

function writtenFile(command: string): { path: string; append: boolean } | null {
  const match = command.match(
    /(?:^|[\s;])(>>|>)\s*(?:"([^"]+)"|'([^']+)'|([^\s"'`;|&]+))/,
  );
  const path = match?.[2] ?? match?.[3] ?? match?.[4];
  if (!path || path.startsWith("/dev/")) return null;
  return { path, append: match?.[1] === ">>" };
}

function toolActivity(event: SupervisorEventRecord): TimelinePresentation {
  const payload = event.payload;
  const itemType = text(payload, "itemType") ?? "tool call";
  const command = text(payload, "command") ?? undefined;
  const output = text(payload, "detail") ?? undefined;
  const rawStatus = text(payload, "status");
  const exitCode = number(payload, "exitCode");
  const failed = exitCode !== null && exitCode !== 0;
  const completed = rawStatus === "completed";
  const status = failed
    ? "exit " + exitCode
    : completed
      ? "completed"
      : rawStatus === "in_progress" || rawStatus === "started"
        ? "in progress"
        : rawStatus ?? undefined;

  if (itemType === "command_execution" && command) {
    const write = writtenFile(command);
    if (write) {
      const action = write.append ? "Content" : "File";
      const title = failed
        ? action + ' write to "' + write.path + '" failed'
        : completed
          ? write.append
            ? 'Content was appended to "' + write.path + '"'
            : 'File "' + write.path + '" was written'
          : write.append
            ? 'Appending content to "' + write.path + '"'
            : 'Writing file "' + write.path + '"';
      return {
        title,
        description: failed
          ? "The command exited with code " + exitCode + "."
          : completed
            ? "The command completed successfully."
            : "The command has started.",
        status,
        command,
        output,
      };
    }
    return {
      title: failed
        ? "Command failed"
        : completed
          ? "Command completed"
          : "Command started",
      description: failed
        ? "The command exited with code " + exitCode + "."
        : completed
          ? "The command completed successfully."
          : "The Agent Runtime is executing a shell command.",
      status,
      command,
      output,
    };
  }

  if (itemType === "file_change" || itemType === "patch_apply") {
    return {
      title: command ? 'Updated "' + command + '"' : "Workspace files updated",
      description: "The Agent applied a file change to its workspace.",
      status,
      command,
      output,
    };
  }
  if (itemType === "web_search") {
    return {
      title: command ? 'Searched for "' + command + '"' : "Web search performed",
      status,
      command,
      output,
    };
  }
  if (itemType === "mcp_tool_call") {
    return {
      title: command ? 'Called tool "' + command + '"' : "External tool called",
      status,
      command,
      output,
    };
  }
  return {
    title: "Agent tool activity",
    description: event.summary,
    status,
    command,
    output,
  };
}

export function presentTimelineEvent(
  event: SupervisorEventRecord,
): TimelinePresentation {
  const reason = text(event.payload, "reason");
  const exitCode = number(event.payload, "exitCode");
  switch (event.type) {
    case "run.queued":
      return { title: "Run queued", description: "Waiting for an Agent Runtime." };
    case "run.started":
      return {
        title: "Agent Runtime started",
        description: "The isolated runtime is ready and processing the request.",
      };
    case "runtime.heartbeat": {
      const sequence = number(event.payload, "sequence");
      return {
        title:
          sequence === null
            ? "Runtime heartbeat received"
            : "Heartbeat #" + sequence + " received",
        description: "The Agent Runtime is responsive.",
      };
    }
    case "runtime.exited":
      return exitCode === 0
        ? { title: "Runtime exited normally", status: "exit 0" }
        : {
            title: "Runtime exited unexpectedly",
            description:
              exitCode === null
                ? "No exit code was reported."
                : "The process exited with code " + exitCode + ".",
            status: exitCode === null ? undefined : "exit " + exitCode,
          };
    case "run.tool_activity":
      return toolActivity(event);
    case "run.completed":
      return { title: "Run completed successfully" };
    case "run.failed":
      return { title: "Run failed", description: reason ?? event.summary };
    case "run.cancelled":
      return { title: "Run was cancelled", description: reason ?? event.summary };
    case "supervisor.stalled":
      return {
        title: "Heartbeat missed — run marked as stalled",
        description: reason ?? "The supervisor stopped receiving runtime heartbeats.",
      };
    case "supervisor.demo_paused":
      return {
        title: "Runtime paused for heartbeat test",
        description: "Demo controls paused the container so heartbeats would stop.",
      };
    case "alert.raised": {
      const ruleId = text(event.payload, "ruleId");
      return {
        title: ruleId ? "Security alert: " + ruleId : "Security alert raised",
        description: event.summary,
      };
    }
    case "supervisor.recovered": {
      const removed = event.payload.containerRemoved === true;
      return {
        title: removed ? "Runtime cleaned up and Agent recovered" : "Agent recovered",
        description: event.summary,
      };
    }
    default:
      return { title: event.summary };
  }
}
