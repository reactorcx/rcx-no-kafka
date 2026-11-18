import { SendMessage } from "../../producer";

export class DefaultPartitioner {
    /**
      * seed - starting point of the round-robin used for messages without a key.
      * default: random
      */
    constructor(seed?: number);
    /**
      * The partition method receives 3 arguments:
      * * the topic name,
      * * the topic's partitions (metadata objects with a `partitionId`), and
      * * the message (useful to partition by key, etc.).
      * The call to partition can be sync or async (return a Promise).
      */
    partition(topic: string, partitions: any[], message: SendMessage["message"]): number | Promise<number>;
    /**
      * getKey - the key to hash. Default: message.key
      */
    getKey(message: SendMessage["message"]): any;
    /**
      * hashKey - murmur2 of the key's bytes: a Buffer as is, anything else as String(key)
      */
    hashKey(key: any): number;
}
