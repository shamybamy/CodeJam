import type { AppConfig } from "./config.js";
import type {
  KafkaRecordMetadata,
  SupervisorCommand,
  SupervisorEvent,
} from "./supervisor-contracts.js";
import { createSupervisorEvent } from "./supervisor-contracts.js";
import { SupervisorKafkaBus } from "./supervisor-kafka.js";
import { SupervisorLedger } from "./supervisor-ledger.js";
import {
  evaluateSuspiciousActivity,
  toAlertInput,
} from "./supervisor-rules.js";
import { SupervisorWatchdog } from "./supervisor-watchdog.js";

export type SupervisorCommandHandler = (
  command: SupervisorCommand,
  metadata: KafkaRecordMetadata,
) => Promise<void> | void;

const MAX_COMMAND_ATTEMPTS = 5;

export class SupervisorCoordinator {
  readonly ledger: SupervisorLedger;
  readonly bus: SupervisorKafkaBus;
  readonly watchdog: SupervisorWatchdog;
  private commandHandler: SupervisorCommandHandler;
  private readonly commandAttempts = new Map<string, number>();

  constructor(
    config: AppConfig,
    commandHandler: SupervisorCommandHandler = () => undefined,
  ) {
    this.commandHandler = commandHandler;
    this.ledger = new SupervisorLedger(config.supervisorLedgerPath);
    this.bus = new SupervisorKafkaBus(config);
    this.watchdog = new SupervisorWatchdog(config, this.ledger, this.bus);
  }

  setCommandHandler(commandHandler: SupervisorCommandHandler): void {
    this.commandHandler = commandHandler;
  }

  async start(): Promise<void> {
    await this.ledger.initialize();
    try {
      await this.bus.start({
        onEvent: (event, metadata) => {
          const stored = this.ledger.recordEvent(event, metadata);
          // Only newly stored events are scored, so replaying a partition
          // cannot raise the same alert twice.
          if (stored) void this.raiseAlerts(event);
        },
        onCommand: async (command, metadata) => {
          if (this.ledger.isCommandProcessed(command.commandId)) return;
          try {
            await this.commandHandler(command, metadata);
          } catch (error) {
            const attempts =
              (this.commandAttempts.get(command.commandId) ?? 0) + 1;
            this.commandAttempts.set(command.commandId, attempts);
            // Transient failures (a busy container engine) deserve a retry, but
            // one permanently failing command must not block the partition.
            if (attempts < MAX_COMMAND_ATTEMPTS) throw error;
            console.error(
              "[supervisor] Abandoning command after " +
                attempts +
                " attempts",
              command.commandId,
              error instanceof Error ? error.message : String(error),
            );
            this.commandAttempts.delete(command.commandId);
            this.ledger.markCommandProcessed(command, metadata);
            return;
          }
          this.commandAttempts.delete(command.commandId);
          this.ledger.markCommandProcessed(command, metadata);
        },
        onInvalidMessage: (error, metadata) => {
          console.warn(
            "[supervisor] Ignoring invalid Kafka message",
            metadata,
            error.message,
          );
        },
      });
      this.watchdog.start();
    } catch (error) {
      this.ledger.close();
      throw error;
    }
  }

  /**
   * Scores one stored event against the deterministic rules and records any
   * alert. `alert.raised` events are never scored, otherwise an alert's own
   * evidence would trigger the rule that produced it.
   */
  private async raiseAlerts(event: SupervisorEvent): Promise<void> {
    if (event.type === "alert.raised") return;
    for (const match of evaluateSuspiciousActivity(event)) {
      const alert = toAlertInput(event, match);
      if (!this.ledger.recordAlert(alert)) continue;
      try {
        await this.publishEvent(
          createSupervisorEvent({
            type: "alert.raised",
            runId: event.runId,
            agentId: event.agentId,
            ...(event.runtimeInstanceId
              ? { runtimeInstanceId: event.runtimeInstanceId }
              : {}),
            source: "supervisor",
            severity: match.rule.severity,
            summary: match.rule.title,
            payload: {
              alertId: alert.alertId,
              ruleId: match.rule.id,
              rationale: match.rule.rationale,
              evidence: match.evidence,
              triggeringEventId: event.eventId,
              triggeringEventType: event.type,
            },
          }),
        );
      } catch (error) {
        console.warn(
          "[supervisor] Alert is recorded locally but Kafka publication failed",
          match.rule.id,
          error instanceof Error ? error.message : String(error),
        );
      }
    }
  }

  publishEvent(event: SupervisorEvent): Promise<void> {
    return this.bus.publishEvent(event);
  }

  publishCommand(command: SupervisorCommand): Promise<void> {
    return this.bus.publishCommand(command);
  }

  async stop(): Promise<void> {
    await this.watchdog.stop();
    await this.bus.stop();
    this.ledger.close();
  }
}
