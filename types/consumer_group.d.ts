import * as Kafka from "./kafka";
import { BaseConsumer } from "./base_consumer";

export interface ConsumerGroupPartition {
    topic: string;
    partition: number;
}

export interface ConsumerGroupInitOptions {
    /**
      * topics - list of topic names to subscribe to.
      */
    topics: string[];
    /**
      * handler - called with each fetched message set.
      */
    handler: (messageSet: Kafka.Message[], topic: string, partition: number, highwaterMarkOffset: number) => Promise<any> | any;
    /**
      * onPartitionsRevoked - called with partitions being revoked before they are released
      * (a good place to commit final offsets).
      */
    onPartitionsRevoked?: (partitions: ConsumerGroupPartition[]) => void;
    /**
      * onPartitionsAssigned - called with newly assigned partitions after they are subscribed.
      */
    onPartitionsAssigned?: (partitions: ConsumerGroupPartition[]) => void;
}

/**
  * ConsumerGroup implements the next-gen consumer rebalance protocol (KIP-848).
  * Membership is driven by a single ConsumerGroupHeartbeat RPC with broker-side
  * ("server") assignment; the client only reconciles the assignment it receives.
  */
export class ConsumerGroup extends BaseConsumer {
    constructor(options?: ConsumerGroupOptions);

    init(options: ConsumerGroupInitOptions): Promise<any>;
    commitOffset(commits: Kafka.Commit | Kafka.Commit[]): Promise<any>;
    fetchOffset(commits: Kafka.Commit | Kafka.Commit[]): Promise<Kafka.Commit[]>;
    end(): Promise<void>;
}

export interface ConsumerGroupOptions {
    /**
      * groupId - group ID for committing and fetching offsets.
      *
      * Defaults to "no-kafka-group-v0.9"
      */
    groupId?: string;
    /**
      * sessionTimeout - rebalance timeout in ms sent to the coordinator.
      *
      * defaults to 45000
      */
    sessionTimeout?: number;
    /**
      * assignor - server-side assignor name: "uniform" or "range".
      *
      * defaults to "uniform"
      */
    assignor?: "uniform" | "range";
    /**
      * groupInstanceId - static membership instance id (KIP-345). When set, end()
      * leaves with a static-leave epoch so the broker preserves the assignment.
      */
    groupInstanceId?: string;
    /**
      * rackId - client rack id for rack-aware fetching.
      */
    rackId?: string;
    /**
      * startingOffset - starting position (time) when there is no committed offset.
      *
      * defaults to Kafka.LATEST_OFFSET
      */
    startingOffset?: Kafka.OFFSET;
    /**
      * recoveryOffset - recovery position used on OffsetOutOfRange error.
      *
      * defaults to Kafka.LATEST_OFFSET
      */
    recoveryOffset?: Kafka.OFFSET;
    maxWaitTime?: number;
    idleTimeout?: number;
    minBytes?: number;
    maxBytes?: number;
    handlerConcurrency?: number;
    isolationLevel?: 0 | 1;
    clientId?: string;
    connectionString?: string;
    connectionTimeout?: number;
    socketTimeout?: number;
    asyncCompression?: boolean;
    reconnectionDelay?: {
        min?: number;
        max?: number;
    };
    brokerRedirection?: Kafka.BrokerRedirectionFunction | Kafka.BrokerRedirectionMap;
    logger?: Kafka.Logger;
}
