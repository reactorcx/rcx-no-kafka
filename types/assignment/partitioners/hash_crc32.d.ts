import { DefaultPartitioner } from "./default";

/**
  * Partitions by CRC32 of the key's bytes (a Buffer as is, anything else as String(key));
  * messages without a key go to partition 0.
  */
export class HashCRC32Partitioner extends DefaultPartitioner {
}
