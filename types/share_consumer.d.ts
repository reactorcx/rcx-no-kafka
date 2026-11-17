import { Message } from "./kafka";

export declare const ACKNOWLEDGE_TYPE: {
    GAP: 0;
    ACCEPT: 1;
    RELEASE: 2;
    REJECT: 3;
};
export type ACKNOWLEDGE_TYPE = typeof ACKNOWLEDGE_TYPE[keyof typeof ACKNOWLEDGE_TYPE];

export interface AcquiredRecord {
    firstOffset: number;
    lastOffset: number;
}

export interface AcknowledgeRecord {
    topic: string;
    partition: number;
    firstOffset?: number;
    lastOffset?: number;
    offset?: number;
}

export interface ShareConsumerOptions {
    connectionString?: string;
    groupId?: string;
    maxWaitTime?: number;
    idleTimeout?: number;
    minBytes?: number;
    maxBytes?: number;
    handlerConcurrency?: number;
    clientId?: string;
    ssl?: {
        cert?: string | Buffer;
        key?: string | Buffer;
        ca?: string | Buffer;
        rejectUnauthorized?: boolean;
    };
    logger?: {
        logLevel?: 0 | 1 | 2 | 3 | 4 | 5;
        logFunction?: any;
    };
}

export interface ShareConsumerInitOptions {
    topics: string[];
    handler: (messageSet: Message[], topic: string, partition: number, acquiredRecords: AcquiredRecord[]) => Promise<void> | void;
}

export class ShareConsumer {
    constructor(options?: ShareConsumerOptions);

    init(options: ShareConsumerInitOptions): Promise<void>;
    /**
      * Queues acknowledgements, sent with the next fetch. Only records from their partition's latest
      * delivery can be acknowledged (as in Java): others are logged and not sent, and are redelivered.
      */
    /**
      * Queues acknowledgements, sent with the next fetch. Only records from their partition's latest
      * delivery can be acknowledged (as in Java): others are logged and not sent, and are redelivered.
      */
    acknowledge(records: AcknowledgeRecord | AcknowledgeRecord[], type?: ACKNOWLEDGE_TYPE): Promise<void>;
    /**
      * Sends the acknowledgements made so far, leaves the group and closes (30 s at most). Handlers
      * still running are not waited for: what they acknowledge afterwards is not sent, and their
      * records are redelivered.
      */
    end(): Promise<void>;
}
