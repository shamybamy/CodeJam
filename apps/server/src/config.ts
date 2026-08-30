import { readFileSync } from "node:fs";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { z } from "zod";

const envSchema = z.object({
  HOST: z.string().default("0.0.0.0"),
  PORT: z.coerce.number().int().min(1).max(65535).default(3000),
  LOG_LEVEL: z.string().default("info"),
  APP_DATA_DIR: z.string().default(path.resolve(".data")),
  AGENT_WORKSPACE_ROOT: z.string().default(path.resolve("workspaces")),
  CODEX_HOME: z.string().default(path.resolve("codex-home")),
  CODEX_BIN: z.string().default("codex"),
  CODEX_SANDBOX_MODE: z
    .enum(["read-only", "workspace-write", "danger-full-access"])
    .default("workspace-write"),
  CODEX_TIMEOUT_MS: z.coerce.number().int().min(1_000).default(600_000),
  CODEX_MAX_OUTPUT_BYTES: z.coerce.number().int().min(65_536).default(2_097_152),
  RUNTIME_PROVIDER: z.enum(["local-process", "container"]).default("local-process"),
  CONTAINER_ENGINE: z.string().min(1).default("docker"),
  CONTAINER_RUNTIME_IMAGE: z.string().min(1).default("volc-agent-runtime:local"),
  CONTAINER_CPU_LIMIT: z.coerce.number().positive().default(2),
  CONTAINER_MEMORY_LIMIT: z
    .string()
    .regex(/^\d+(?:\.\d+)?[bkmg]$/i)
    .default("2g"),
  CONTAINER_PIDS_LIMIT: z.coerce.number().int().positive().default(256),
  CONTAINER_USER: z.string().optional(),
  RUNTIME_INSTANCE_ID: z
    .string()
    .trim()
    .min(1)
    .max(48)
    .regex(/^[a-zA-Z0-9_.-]+$/)
    .default("default"),
  APP_AUTH_TOKEN: z
    .string()
    .trim()
    .max(128)
    .regex(/^[A-Za-z0-9._~-]*$/, "APP_AUTH_TOKEN must use URL-safe characters")
    .optional(),
  MODEL_PROVIDER: z.enum(["ollama", "ark"]).optional(),
  MODEL_API_KEY: z.string().optional(),
  MODEL_ID: z.string().optional(),
  MODEL_BASE_URL: z.string().url().optional(),
  ARK_API_KEY: z.string().optional(),
  ARK_MODEL: z.string().optional(),
  ARK_BASE_URL: z.string().url().optional(),
  KAFKA_ENABLED: z.enum(["true", "false"]).default("true"),
  KAFKA_BROKERS: z.string().default("127.0.0.1:29092"),
  KAFKA_CLIENT_ID: z.string().trim().min(1).default("codejam-supervisor"),
  KAFKA_EVENTS_TOPIC: z.string().trim().min(1).default("agent-run-events-v1"),
  KAFKA_COMMANDS_TOPIC: z
    .string()
    .trim()
    .min(1)
    .default("agent-run-commands-v1"),
  KAFKA_EVENT_CONSUMER_GROUP: z
    .string()
    .trim()
    .min(1)
    .default("supervisor-ledger-v1"),
  KAFKA_COMMAND_CONSUMER_GROUP: z
    .string()
    .trim()
    .min(1)
    .default("supervisor-commands-v1"),
  SUPERVISOR_LEDGER_PATH: z.string().optional(),
  SUPERVISOR_HEARTBEAT_INTERVAL_MS: z.coerce
    .number()
    .int()
    .min(250)
    .default(2_000),
  SUPERVISOR_STALL_AFTER_MS: z.coerce.number().int().min(1_000).default(8_000),
  SUPERVISOR_WATCHDOG_INTERVAL_MS: z.coerce
    .number()
    .int()
    .min(250)
    .default(1_000),
  SUPERVISOR_CHAT_BASE_URL: z.string().url().optional(),
  SUPERVISOR_CHAT_MODEL: z.string().optional(),
  SUPERVISOR_CHAT_TIMEOUT_MS: z.coerce
    .number()
    .int()
    .min(1_000)
    .default(120_000),
  SUPERVISOR_CHAT_MAX_TOOL_CALLS: z.coerce
    .number()
    .int()
    .min(1)
    .max(5)
    .default(3),
  ENABLE_DEMO_CONTROLS: z.enum(["true", "false"]).default("false"),
  NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
});

export type AppConfig = ReturnType<typeof loadConfig>;

/**
 * IPv4 default gateway from /proc/net/route. Inside a WSL distro this is the
 * Windows host, which is where a desktop Ollama usually listens.
 */
function defaultGatewayAddress(): string | null {
  try {
    const table = readFileSync("/proc/net/route", "utf8").split("\n").slice(1);
    for (const line of table) {
      const [, destination, gateway, flags] = line.trim().split(/\s+/);
      if (destination !== "00000000" || !gateway) continue;
      if (flags && (Number.parseInt(flags, 16) & 0x2) === 0) continue;
      const octets = gateway.match(/../g);
      if (!octets || octets.length !== 4) continue;
      return octets
        .reverse()
        .map((octet) => Number.parseInt(octet, 16))
        .join(".");
    }
  } catch {
    // Not Linux, or the table is unreadable: the other candidates still apply.
  }
  return null;
}

export function loadConfig(environment: NodeJS.ProcessEnv = process.env) {
  const env = envSchema.parse(environment);
  const authToken = env.APP_AUTH_TOKEN?.trim() ?? "";
  const loopbackHosts = new Set(["127.0.0.1", "::1", "localhost"]);
  if (env.NODE_ENV === "production" && !loopbackHosts.has(env.HOST)) {
    if (authToken.length < 24 || authToken.startsWith("replace-")) {
      throw new Error(
        "APP_AUTH_TOKEN must contain at least 24 characters for a non-loopback production server",
      );
    }
  }
  const defaultContainerUser =
    typeof process.getuid === "function" && typeof process.getgid === "function"
      ? process.getuid() + ":" + process.getgid()
      : "1000:1000";
  const hasLegacyArkCredentials = Boolean(
    env.ARK_API_KEY?.trim() || env.ARK_MODEL?.trim(),
  );
  const modelProvider =
    env.MODEL_PROVIDER ?? (hasLegacyArkCredentials ? "ark" : "ollama");
  const modelApiKey =
    env.MODEL_API_KEY?.trim() ||
    env.ARK_API_KEY?.trim() ||
    (modelProvider === "ollama" ? "ollama" : "");
  const modelId =
    env.MODEL_ID?.trim() ||
    env.ARK_MODEL?.trim() ||
    (modelProvider === "ollama" ? "qwen3:8b" : "");
  const defaultModelBaseUrl =
    modelProvider === "ollama"
      ? env.RUNTIME_PROVIDER === "container"
        ? "http://host.docker.internal:11434/v1"
        : "http://127.0.0.1:11434/v1"
      : "https://ark.cn-beijing.volces.com/api/v3";
  const modelBaseUrl = (
    env.MODEL_BASE_URL ??
    env.ARK_BASE_URL ??
    defaultModelBaseUrl
  ).replace(/\/+$/, "");
  // The Runtime container reaches Ollama through host.docker.internal, but the
  // control plane may sit somewhere else entirely: on the host, in a container,
  // or in a WSL distro whose host is only reachable through the default gateway.
  // An explicit SUPERVISOR_CHAT_BASE_URL wins; otherwise the chat client probes
  // these candidates in order and keeps the first that answers.
  const chatBaseUrlCandidates = env.SUPERVISOR_CHAT_BASE_URL
    ? [env.SUPERVISOR_CHAT_BASE_URL.replace(/\/+$/, "")]
    : [
        ...new Set(
          [
            modelBaseUrl.replace("host.docker.internal", "127.0.0.1"),
            modelBaseUrl,
            ...(defaultGatewayAddress()
              ? [
                  modelBaseUrl.replace(
                    /\/\/[^/:]+/,
                    "//" + defaultGatewayAddress(),
                  ),
                ]
              : []),
          ].map((candidate) => candidate.replace(/\/+$/, "")),
        ),
      ];
  const chatBaseUrl = chatBaseUrlCandidates[0] ?? modelBaseUrl;
  const dataDirectory = path.resolve(env.APP_DATA_DIR);
  const kafkaBrokers = env.KAFKA_BROKERS.split(",")
    .map((broker) => broker.trim())
    .filter(Boolean);
  if (kafkaBrokers.length === 0) {
    throw new Error("KAFKA_BROKERS must contain at least one host:port entry");
  }
  if (env.SUPERVISOR_STALL_AFTER_MS <= env.SUPERVISOR_HEARTBEAT_INTERVAL_MS) {
    throw new Error(
      "SUPERVISOR_STALL_AFTER_MS must be greater than SUPERVISOR_HEARTBEAT_INTERVAL_MS",
    );
  }
  return {
    host: env.HOST,
    port: env.PORT,
    logLevel: env.LOG_LEVEL,
    dataDirectory,
    workspaceRoot: path.resolve(env.AGENT_WORKSPACE_ROOT),
    codexHome: path.resolve(env.CODEX_HOME),
    codexBin: env.CODEX_BIN,
    codexSandboxMode: env.CODEX_SANDBOX_MODE,
    codexTimeoutMs: env.CODEX_TIMEOUT_MS,
    codexMaxOutputBytes: env.CODEX_MAX_OUTPUT_BYTES,
    runtimeProvider: env.RUNTIME_PROVIDER,
    containerEngine: env.CONTAINER_ENGINE,
    containerRuntimeImage: env.CONTAINER_RUNTIME_IMAGE,
    containerCpuLimit: env.CONTAINER_CPU_LIMIT,
    containerMemoryLimit: env.CONTAINER_MEMORY_LIMIT,
    containerPidsLimit: env.CONTAINER_PIDS_LIMIT,
    containerUser: env.CONTAINER_USER?.trim() || defaultContainerUser,
    runtimeInstanceId: env.RUNTIME_INSTANCE_ID,
    authToken,
    modelProvider,
    modelApiKey,
    modelId,
    modelBaseUrl,
    kafkaEnabled: env.KAFKA_ENABLED === "true",
    kafkaBrokers,
    kafkaClientId: env.KAFKA_CLIENT_ID,
    kafkaEventsTopic: env.KAFKA_EVENTS_TOPIC,
    kafkaCommandsTopic: env.KAFKA_COMMANDS_TOPIC,
    kafkaEventConsumerGroup: env.KAFKA_EVENT_CONSUMER_GROUP,
    kafkaCommandConsumerGroup: env.KAFKA_COMMAND_CONSUMER_GROUP,
    supervisorLedgerPath: path.resolve(
      env.SUPERVISOR_LEDGER_PATH ?? path.join(dataDirectory, "supervisor.sqlite"),
    ),
    supervisorHeartbeatIntervalMs: env.SUPERVISOR_HEARTBEAT_INTERVAL_MS,
    supervisorStallAfterMs: env.SUPERVISOR_STALL_AFTER_MS,
    supervisorWatchdogIntervalMs: env.SUPERVISOR_WATCHDOG_INTERVAL_MS,
    chatBaseUrl,
    chatBaseUrlCandidates,
    chatModelId: env.SUPERVISOR_CHAT_MODEL?.trim() || modelId,
    chatTimeoutMs: env.SUPERVISOR_CHAT_TIMEOUT_MS,
    chatMaxToolCalls: env.SUPERVISOR_CHAT_MAX_TOOL_CALLS,
    demoControlsEnabled: env.ENABLE_DEMO_CONTROLS === "true",
    nodeEnv: env.NODE_ENV,
  };
}

export function isModelConfigured(config: AppConfig): boolean {
  return (
    config.modelApiKey.length > 0 &&
    !config.modelApiKey.startsWith("replace-") &&
    config.modelId.length > 0 &&
    !config.modelId.includes("replace-")
  );
}

export async function writeCodexConfig(config: AppConfig): Promise<void> {
  await mkdir(config.codexHome, { recursive: true });
  const toml = [
    "# Generated by Volc Agent Launchpad. Edit environment variables, not this file.",
    "model = " + JSON.stringify(config.modelId || "model-not-configured"),
    'model_provider = "launchpad_model"',
    "",
    "[model_providers.launchpad_model]",
    "name = " +
      JSON.stringify(config.modelProvider === "ollama" ? "Local Ollama" : "BytePlus ModelArk"),
    "base_url = " + JSON.stringify(config.modelBaseUrl),
    'env_key = "MODEL_API_KEY"',
    'wire_api = "responses"',
    "requires_openai_auth = false",
    "",
  ].join("\n");
  await writeFile(path.join(config.codexHome, "config.toml"), toml, {
    encoding: "utf8",
    mode: 0o600,
  });
}
