import {
  Kafka,
  logLevel,
  Partitioners,
  type Admin,
  type Consumer,
  type Producer,
} from "kafkajs";
import type { AppConfig } from "./config.js";
import {
  supervisorCommandSchema,
  supervisorEventSchema,
  type KafkaRecordMetadata,
  type SupervisorCommand,
  type SupervisorEvent,
} from "./supervisor-contracts.js";
import { redactSupervisorEvent } from "./supervisor-redaction.js";

export interface SupervisorKafkaHandlers {
  onEvent: (
    event: SupervisorEvent,
    metadata: KafkaRecordMetadata,
  ) => Promise<void> | void;
  onCommand: (
    command: SupervisorCommand,
    metadata: KafkaRecordMetadata,
  ) => Promise<void> | void;
  onInvalidMessage?: (error: Error, metadata: KafkaRecordMetadata) => void;
}

export class SupervisorKafkaBus {
  private readonly admin: Admin;
  private readonly producer: Producer;
  private readonly eventConsumer: Consumer;
  private readonly commandConsumer: Consumer;
  private started = false;

  constructor(private readonly config: AppConfig) {
    const kafka = new Kafka({
      clientId: config.kafkaClientId,
      brokers: config.kafkaBrokers,
      logLevel: logLevel.WARN,
      retry: { retries: 8 },
    });
    this.admin = kafka.admin();
    this.producer = kafka.producer({
      allowAutoTopicCreation: false,
      createPartitioner: Partitioners.DefaultPartitioner,
    });
    this.eventConsumer = kafka.consumer({
      groupId: config.kafkaEventConsumerGroup,
      allowAutoTopicCreation: false,
    });
    this.commandConsumer = kafka.consumer({
      groupId: config.kafkaCommandConsumerGroup,
      allowAutoTopicCreation: false,
    });
  }

  async start(handlers: SupervisorKafkaHandlers): Promise<void> {
    if (this.started) return;
    await this.admin.connect();
    try {
      const existingTopics = new Set(await this.admin.listTopics());
      const topics = [
        this.config.kafkaEventsTopic,
        this.config.kafkaCommandsTopic,
      ]
        .filter((topic) => !existingTopics.has(topic))
        .map((topic) => ({
          topic,
          numPartitions: 3,
          replicationFactor: 1,
        }));
      if (topics.length > 0) {
        await this.admin.createTopics({ waitForLeaders: true, topics });
      }
    } finally {
      await this.admin.disconnect();
    }

    await this.producer.connect();
    await this.eventConsumer.connect();
    await this.commandConsumer.connect();
    await this.eventConsumer.subscribe({
      topic: this.config.kafkaEventsTopic,
      fromBeginning: true,
    });
    await this.commandConsumer.subscribe({
      topic: this.config.kafkaCommandsTopic,
      fromBeginning: true,
    });

    await this.eventConsumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const metadata = { topic, partition, offset: message.offset };
        let event: SupervisorEvent;
        try {
          event = supervisorEventSchema.parse(
            JSON.parse(message.value?.toString("utf8") ?? "null"),
          );
        } catch (error) {
          handlers.onInvalidMessage?.(
            error instanceof Error ? error : new Error(String(error)),
            metadata,
          );
          return;
        }
        await handlers.onEvent(event, metadata);
      },
    });
    await this.commandConsumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const metadata = { topic, partition, offset: message.offset };
        let command: SupervisorCommand;
        try {
          command = supervisorCommandSchema.parse(
            JSON.parse(message.value?.toString("utf8") ?? "null"),
          );
        } catch (error) {
          handlers.onInvalidMessage?.(
            error instanceof Error ? error : new Error(String(error)),
            metadata,
          );
          return;
        }
        await handlers.onCommand(command, metadata);
      },
    });
    this.started = true;
  }

  async publishEvent(event: SupervisorEvent): Promise<void> {
    this.assertStarted();
    const sanitized = redactSupervisorEvent(event);
    await this.producer.send({
      topic: this.config.kafkaEventsTopic,
      messages: [{ key: sanitized.runId, value: JSON.stringify(sanitized) }],
    });
  }

  async publishCommand(command: SupervisorCommand): Promise<void> {
    this.assertStarted();
    await this.producer.send({
      topic: this.config.kafkaCommandsTopic,
      messages: [{ key: command.runId, value: JSON.stringify(command) }],
    });
  }

  async stop(): Promise<void> {
    if (!this.started) return;
    this.started = false;
    await Promise.allSettled([
      this.eventConsumer.disconnect(),
      this.commandConsumer.disconnect(),
      this.producer.disconnect(),
    ]);
  }

  private assertStarted(): void {
    if (!this.started) throw new Error("Supervisor Kafka bus is not started");
  }
}
