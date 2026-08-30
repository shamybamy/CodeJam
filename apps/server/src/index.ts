import path from "node:path";
import { AgentService } from "./agent-service.js";
import { createApp } from "./app.js";
import { loadConfig, writeCodexConfig } from "./config.js";
import { createRunner } from "./runner-factory.js";
import { JsonStore } from "./store.js";
import { SupervisorCoordinator } from "./supervisor-coordinator.js";
import { WorkspaceManager } from "./workspace.js";

const config = loadConfig();
await writeCodexConfig(config);

const store = new JsonStore(path.join(config.dataDirectory, "launchpad.json"));
const workspaces = new WorkspaceManager(config.workspaceRoot);
const runner = createRunner(config);
const supervisor = config.kafkaEnabled ? new SupervisorCoordinator(config) : null;
const service = new AgentService(
  config,
  store,
  workspaces,
  runner,
  supervisor ?? undefined,
);
await service.initialize();

supervisor?.setCommandHandler((command) =>
  service.handleSupervisorCommand(command),
);
await supervisor?.start();

const app = await createApp(config, service, supervisor);
if (supervisor) {
  app.addHook("onClose", async () => supervisor.stop());
}

const shutdown = async (signal: string) => {
  app.log.info({ signal }, "Shutting down");
  await app.close();
  process.exit(0);
};

process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT", () => void shutdown("SIGINT"));

await app.listen({ host: config.host, port: config.port });
