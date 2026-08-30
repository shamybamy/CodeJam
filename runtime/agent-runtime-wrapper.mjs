import { spawn } from "node:child_process";

const CONTROL_PREFIX = "__CODEJAM_RUNTIME_EVENT__";
const [command, ...args] = process.argv.slice(2);

if (!command) {
  process.stderr.write("Agent Runtime wrapper requires a child command.\n");
  process.exit(64);
}

const identity = {
  runId: process.env.CODEJAM_RUN_ID ?? "",
  agentId: process.env.CODEJAM_AGENT_ID ?? "",
  runtimeInstanceId: process.env.CODEJAM_RUNTIME_INSTANCE_ID ?? "",
};
const heartbeatIntervalMs = Number.parseInt(
  process.env.CODEJAM_HEARTBEAT_INTERVAL_MS ?? "2000",
  10,
);

function emit(type, payload = {}) {
  process.stderr.write(
    CONTROL_PREFIX +
      JSON.stringify({
        type,
        occurredAt: new Date().toISOString(),
        ...identity,
        payload,
      }) +
      "\n",
  );
}

const child = spawn(command, args, {
  env: process.env,
  stdio: ["inherit", "inherit", "pipe"],
});

// The control plane only trusts wrapper-authored lines. Escape any child stderr
// line that tries to imitate the private control prefix before forwarding it.
let childStderr = "";
function forwardChildStderr(line, newline = true) {
  const safeLine = line.startsWith(CONTROL_PREFIX)
    ? "[child-output] " + line
    : line;
  process.stderr.write(safeLine + (newline ? "\n" : ""));
}
child.stderr.setEncoding("utf8");
child.stderr.on("data", (chunk) => {
  childStderr += chunk;
  const lines = childStderr.split(/\r?\n/);
  childStderr = lines.pop() ?? "";
  for (const line of lines) forwardChildStderr(line);
  if (childStderr.length > 65_536) {
    forwardChildStderr(childStderr, false);
    childStderr = "";
  }
});
child.stderr.once("end", () => {
  if (childStderr) forwardChildStderr(childStderr, false);
  childStderr = "";
});

let heartbeatSequence = 0;
let finished = false;
emit("runtime.started", { pid: child.pid ?? null });
const heartbeat = setInterval(() => {
  heartbeatSequence += 1;
  emit("runtime.heartbeat", { sequence: heartbeatSequence });
}, Number.isFinite(heartbeatIntervalMs) && heartbeatIntervalMs >= 250
  ? heartbeatIntervalMs
  : 2000);

for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"]) {
  process.on(signal, () => {
    if (!finished) child.kill(signal);
  });
}

function finish(exitCode, signal, error) {
  if (finished) return;
  finished = true;
  clearInterval(heartbeat);
  emit("runtime.exited", {
    exitCode,
    signal,
    ...(error ? { error } : {}),
  });
  process.exitCode = exitCode ?? 1;
}

child.once("error", (error) => finish(1, null, error.message));
child.once("exit", (code, signal) => finish(code, signal, null));
