package com.microsoft.azure.documentdb;

/**
 * Indexing modes.
 */
public enum IndexType {
    /**
     * This is supplied for a path which has no sorting requirement. This kind
     * of an index has better precision than corresponding range index.
     */
    Hash,

    /**
     * This is supplied for a path which requires sorting.
     */
    Range
}
