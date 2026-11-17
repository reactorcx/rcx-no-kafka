import { DefaultPartitioner } from "./default";

/**
  * Partitions by CRC32 of the key: a Buffer by its bytes, a string by the low byte of each
  * char code (its bytes only if ASCII), anything else as String(key). Messages without a key
  * go to partition 0.
  */
export class HashCRC32Partitioner extends DefaultPartitioner {
}
