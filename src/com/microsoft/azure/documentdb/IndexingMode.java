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
    Lazy
}
