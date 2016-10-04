package com.microsoft.azure.documentdb;

/**
 * Specifies the partition scheme for an multiple-partitioned collection.
 */
public enum PartitionKind {
    /**
     * The Partition of a document is calculated based on the hash value of the PartitionKey.
     */
    Hash
}
