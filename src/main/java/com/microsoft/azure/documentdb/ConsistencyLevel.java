/* 
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 */

package com.microsoft.azure.documentdb;

/**
 * Represents the consistency levels supported for DocumentDB client operations.
 * <p>
 * The requested ConsistencyLevel must match or be weaker than that provisioned for the database account. Consistency
 * levels by order of strength are Strong, BoundedStaleness, Session and Eventual.
 */
public enum ConsistencyLevel {

    /**
     * Strong Consistency guarantees that read operations always return the value that was last written.
     */
    Strong,

    /**
     * Bounded Staleness guarantees that reads are not too out-of-date. This can be configured based on number of
     * operations (MaxStalenessPrefix) or time (MaxStalenessIntervalInSeconds)
     */
    BoundedStaleness,

    /**
     * Session Consistency guarantees monotonic reads (you never read old data, then new, then old again), monotonic
     * writes (writes are ordered) and read your writes (your writes are immediately visible to your reads) within
     * any single session.
     */
    Session,

    /**
     * Eventual Consistency guarantees that reads will return a subset of writes. All writes will be eventually be
     * available for reads.
     */
    Eventual

}
