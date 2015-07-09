package com.microsoft.azure.documentdb;

/**
 * Specifies the supported indexing modes.
 */
public enum IndexingMode {
    /**
     * Index is updated synchronously with a create or update operation.
     * <p>
     * With consistent indexing, query behavior is the same as the default consistency level for the collection. The
     * index is always kept up to date with the data.
     */
    Consistent,

    /**
     * Index is updated asynchronously with respect to a create or update operation.
     * <p>
     * With lazy indexing, queries are eventually consistent. The index is updated when the collection is idle.
     */
    Lazy,

    /**
     * No index is provided.
     * <p>
     * Setting IndexingMode to "None" drops the index. Use this if you don't want to maintain the index for a document
     * collection, to save the storage cost or improve the write throughput. Your queries will degenerate to scans of
     * the entire collection.
     */
    None
}
