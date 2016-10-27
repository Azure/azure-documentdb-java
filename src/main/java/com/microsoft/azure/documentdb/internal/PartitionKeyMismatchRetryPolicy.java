package com.microsoft.azure.documentdb.internal;

import java.util.logging.Logger;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.internal.routing.ClientCollectionCache;

/**
 * A RetryPolicy implementation that ensures the PartitionKeyDefinitionMap is up-to-date.
 * Entries in the PartitionKeyDefinitionMap can become stale if a collection is deleted
 * and then recreated with the same name but a different partition key definition, if
 * the request is made using name-based links.
 */
final class PartitionKeyMismatchRetryPolicy implements RetryPolicy {
    private static int maxRetryCount = 1;
    private final int retryIntervalInMS = 0;
    private final ClientCollectionCache clientCollectionCache;
    private String resourcePath;
    private int currentAttempt = 0;
    private Logger logger;

    /**
     * Creates a new instance of the PartitionKeyMismatchRetryPolicy class.
     */
    public PartitionKeyMismatchRetryPolicy(
            String resourcePath,
            ClientCollectionCache clientCollectionCache) {
        this.resourcePath = resourcePath;
        this.clientCollectionCache = clientCollectionCache;
        this.logger = Logger.getLogger(this.getClass().getPackage().getName());
    }

    /**
     * Gets the number of milliseconds to wait before retry the operation.
     * <p>
     * For partition key mismatch failures, we retry immediately.
     *
     * @return the number of milliseconds to wait before retry the operation.
     */
    public long getRetryAfterInMilliseconds() {
        return this.retryIntervalInMS;
    }

    /**
     * Should the caller retry the operation.
     * <p>
     * This retry policy should only be invoked if HttpStatusCode is 400 (BadRequest)
     * and SubStatusCode is 1001 (PartitionKeyMismatch).
     *
     * @param exception the exception to check.
     * @return true if should retry.
     */
    public boolean shouldRetry(DocumentClientException exception) {
        if (this.currentAttempt >= maxRetryCount) {
            return false;
        }

        this.logger.info("Partition key definition changed. Refresh partition key definition map and retry.");
        String collectionLink = Utils.getCollectionName(this.resourcePath);
        this.clientCollectionCache.refresh(collectionLink);
        this.currentAttempt++;

        return true;
    }
}