import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { isModelConfigured, loadConfig, writeCodexConfig } from "./config.js";

const temporaryDirectories: string[] = [];

afterEach(async () => {
  await Promise.all(
    temporaryDirectories.splice(0).map((directory) =>
      rm(directory, { recursive: true, force: true }),
    ),
  );
});

describe("model provider configuration", () => {
  it("defaults the container POC to local Ollama", () => {
    const config = loadConfig({
      NODE_ENV: "test",
      RUNTIME_PROVIDER: "container",
    });

    expect(config.modelProvider).toBe("ollama");
    expect(config.modelApiKey).toBe("ollama");
    expect(config.modelId).toBe("qwen3:8b");
    expect(config.modelBaseUrl).toBe("http://host.docker.internal:11434/v1");
    expect(isModelConfigured(config)).toBe(true);
  });

  it("infers Ark from legacy credentials", () => {
    const config = loadConfig({
      NODE_ENV: "test",
      ARK_API_KEY: "legacy-key",
      ARK_MODEL: "ep-legacy",
      ARK_BASE_URL: "https://ark.example.test/api/v3/",
    });

    expect(config.modelProvider).toBe("ark");
    expect(config.modelApiKey).toBe("legacy-key");
    expect(config.modelId).toBe("ep-legacy");
    expect(config.modelBaseUrl).toBe("https://ark.example.test/api/v3");
    expect(isModelConfigured(config)).toBe(true);
  });

  it("prefers generic model settings over legacy Ark values", () => {
    const config = loadConfig({
      NODE_ENV: "test",
      MODEL_PROVIDER: "ollama",
      MODEL_API_KEY: "generic-key",
      MODEL_ID: "generic-model",
      MODEL_BASE_URL: "http://models.example.test/v1/",
      ARK_API_KEY: "legacy-key",
      ARK_MODEL: "ep-legacy",
    });

    expect(config.modelProvider).toBe("ollama");
    expect(config.modelApiKey).toBe("generic-key");
    expect(config.modelId).toBe("generic-model");
    expect(config.modelBaseUrl).toBe("http://models.example.test/v1");
  });

  it("writes a provider-neutral Codex config without embedding the key", async () => {
    const codexHome = await mkdtemp(path.join(tmpdir(), "launchpad-codex-config-"));
    temporaryDirectories.push(codexHome);
    const config = loadConfig({
      NODE_ENV: "test",
      CODEX_HOME: codexHome,
      MODEL_PROVIDER: "ollama",
      MODEL_API_KEY: "secret-not-in-config",
      MODEL_ID: "qwen3:8b",
      MODEL_BASE_URL: "http://host.docker.internal:11434/v1",
    });

    await writeCodexConfig(config);
    const contents = await readFile(path.join(codexHome, "config.toml"), "utf8");

    expect(contents).toContain('model = "qwen3:8b"');
    expect(contents).toContain('model_provider = "launchpad_model"');
    expect(contents).toContain('name = "Local Ollama"');
    expect(contents).toContain('env_key = "MODEL_API_KEY"');
    expect(contents).not.toContain("secret-not-in-config");
  });

  it("requires the stall threshold to exceed the heartbeat interval", () => {
    expect(() =>
      loadConfig({
        NODE_ENV: "test",
        SUPERVISOR_HEARTBEAT_INTERVAL_MS: "2000",
        SUPERVISOR_STALL_AFTER_MS: "2000",
      }),
    ).toThrow(/STALL_AFTER_MS/);
  });
});
