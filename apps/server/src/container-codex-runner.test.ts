import { describe, expect, it } from "vitest";
import { loadConfig } from "./config.js";
import {
  buildContainerRunArgs,
  containerName,
  parseRuntimeControlLine,
  RUNTIME_CONTROL_PREFIX,
} from "./container-codex-runner.js";

describe("Container Codex runner", () => {
  it("builds an isolated Docker/Podman-compatible invocation", () => {
    const config = loadConfig({
      NODE_ENV: "test",
      ARK_API_KEY: "secret-that-must-not-appear-in-argv",
      ARK_MODEL: "ep-test",
      CODEX_HOME: "/tmp/codex-home",
      RUNTIME_PROVIDER: "container",
      CONTAINER_ENGINE: "podman",
      CONTAINER_RUNTIME_IMAGE: "runtime:test",
      CONTAINER_USER: "501:20",
      RUNTIME_INSTANCE_ID: "test-instance",
    });
    const args = buildContainerRunArgs(
      {
        runId: "00000000-0000-4000-8000-000000000001",
        agentId: "agent/unsafe",
        runtimeInstanceId: "test-instance",
        workspacePath: "/tmp/agent-workspace",
        prompt: "write a small program",
        threadId: null,
      },
      config,
    );

    expect(containerName("agent/unsafe", "test-instance")).toBe(
      "launchpad-test-instance-agent-unsafe",
    );
    expect(args).toContain("runtime:test");
    expect(args).toContain("type=bind,src=/tmp/agent-workspace,dst=/workspace");
    expect(args).toContain("type=bind,src=/tmp/codex-home,dst=/codex-home");
    expect(args).toContain("501:20");
    expect(args).toContain("workspace-write");
    expect(args).toContain("/workspace");
    expect(args).toContain("io.codejam.instance-id=test-instance");
    expect(args).toContain("io.codejam.runtime-instance-id=test-instance");
    expect(args).toContain("io.codejam.run-id=00000000-0000-4000-8000-000000000001");
    expect(args).toContain("/opt/codejam/agent-runtime-wrapper.mjs");
    expect(args).toContain("keep-id");
    expect(args).toContain("MODEL_API_KEY");
    expect(args).not.toContain("ARK_API_KEY");
    expect(args).not.toContain("secret-that-must-not-appear-in-argv");
  });

  it("resumes a thread inside the mounted Runtime workspace", () => {
    const config = loadConfig({
      NODE_ENV: "test",
      CODEX_HOME: "/tmp/codex-home",
      RUNTIME_PROVIDER: "container",
    });
    const args = buildContainerRunArgs(
      {
        runId: "00000000-0000-4000-8000-000000000002",
        agentId: "agent",
        runtimeInstanceId: config.runtimeInstanceId,
        workspacePath: "/tmp/workspace",
        prompt: "continue",
        threadId: "thread-123",
      },
      config,
    );
    expect(args.slice(-3)).toEqual(["resume", "thread-123", "continue"]);
    expect(args).not.toContain("keep-id");
  });

  it("recognizes only structured Runtime control lines", () => {
    const event = parseRuntimeControlLine(
      RUNTIME_CONTROL_PREFIX +
        JSON.stringify({
          type: "runtime.heartbeat",
          occurredAt: "2026-08-30T10:00:00.000Z",
          runId: "run-1",
          agentId: "agent-1",
          runtimeInstanceId: "runtime-1",
          payload: { sequence: 2 },
        }),
    );

    expect(event).toMatchObject({
      type: "runtime.heartbeat",
      runId: "run-1",
      payload: { sequence: 2 },
    });
    expect(parseRuntimeControlLine("normal stderr")).toBeNull();
    expect(
      parseRuntimeControlLine("[child-output] " + RUNTIME_CONTROL_PREFIX + "{}"),
    ).toBeNull();
  });
});
