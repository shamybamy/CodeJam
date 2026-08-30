import type { AppConfig } from "./config.js";
import type {
  SupervisorCommand,
  SupervisorEvent,
} from "./supervisor-contracts.js";
import { SupervisorLedger } from "./supervisor-ledger.js";

export interface SupervisorWatchdogPublisher {
  publishEvent(event: SupervisorEvent): Promise<void>;
  publishCommand(command: SupervisorCommand): Promise<void>;
}

export class SupervisorWatchdog {
  private timer: NodeJS.Timeout | null = null;
  private inFlight: Promise<void> | null = null;

  constructor(
    private readonly config: Pick<
      AppConfig,
      "supervisorStallAfterMs" | "supervisorWatchdogIntervalMs"
    >,
    private readonly ledger: SupervisorLedger,
    private readonly publisher: SupervisorWatchdogPublisher,
    private readonly clock: () => Date = () => new Date(),
  ) {}

  start(): void {
    if (this.timer) return;
    void this.tick();
    this.timer = setInterval(
      () => void this.tick(),
      this.config.supervisorWatchdogIntervalMs,
    );
    this.timer.unref();
  }

  async stop(): Promise<void> {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    await this.inFlight;
  }

  tick(): Promise<void> {
    if (this.inFlight) return this.inFlight;
    this.inFlight = this.performTick().finally(() => {
      this.inFlight = null;
    });
    return this.inFlight;
  }

  private async performTick(): Promise<void> {
    const current = this.clock();
    const occurredAt = current.toISOString();
    const cutoffAt = new Date(
      current.getTime() - this.config.supervisorStallAfterMs,
    ).toISOString();
    const claims = this.ledger.claimStalledRuns(cutoffAt, occurredAt);

    for (const claim of claims) {
      try {
        await this.publisher.publishEvent(claim.event);
      } catch (error) {
        console.warn(
          "[supervisor] Stalled event is recorded locally but Kafka publication failed",
          error instanceof Error ? error.message : String(error),
        );
      }
    }

    for (const record of this.ledger.listPendingCommands(occurredAt)) {
      try {
        await this.publisher.publishCommand(record.command);
        this.ledger.markCommandOutboxSent(record.command.commandId);
      } catch (error) {
        const retryDelayMs = Math.min(30_000, 1_000 * 2 ** record.attempts);
        this.ledger.rescheduleCommandOutbox(
          record.command.commandId,
          new Date(current.getTime() + retryDelayMs).toISOString(),
          error instanceof Error ? error.message : String(error),
        );
      }
    }
  }
}
