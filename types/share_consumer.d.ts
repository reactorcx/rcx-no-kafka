import { Message } from "./kafka";

export const enum ACKNOWLEDGE_TYPE {
    GAP = 0,
    ACCEPT = 1,
    RELEASE = 2,
    REJECT = 3
}

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
    acknowledge(records: AcknowledgeRecord | AcknowledgeRecord[], type?: ACKNOWLEDGE_TYPE): Promise<void>;
    end(): Promise<void>;
}
