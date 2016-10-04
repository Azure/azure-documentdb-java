package com.microsoft.azure.documentdb.internal;

import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;

import com.microsoft.azure.documentdb.DocumentClientException;

/**
 * A RetryPolicy implementation that ensures session reads succeed across
 * geo-replicated databases.
 */
final class SessionReadRetryPolicy implements RetryPolicy {
    private static int maxRetryCount = 1;
    private final int retryIntervalInMS = 0;
    private final DocumentServiceRequest currentRequest;
    private final EndpointManager globalEndpointManager;
    private int currentAttempt = 0;
    private Logger logger;

    /**
     * Creates a new instance of the SessionReadRetryPolicy class.
     *
     * @param globalEndpointManager the GlobalEndpointManager instance.
     * @param request               the current request to be retried.
     */
    public SessionReadRetryPolicy(EndpointManager globalEndpointManager, DocumentServiceRequest request) {
        this.globalEndpointManager = globalEndpointManager;
        this.currentRequest = request;
        this.logger = Logger.getLogger(this.getClass().getPackage().getName());
    }

    /**
     * Gets the number of milliseconds to wait before retry the operation.
     * <p>
     * For session read failures, we retry immediately.
     *
     * @return the number of milliseconds to wait before retry the operation.
     */
    public long getRetryAfterInMilliseconds() {
        return this.retryIntervalInMS;
    }

    /**
     * Should the caller retry the operation.
     * <p>
     * This retry policy should only be invoked if HttpStatusCode is 404 (NotFound)
     * and 1002 (ReadSessionNotAvailable).
     *
     * @param exception the exception to check.
     * @return true if should retry.
     */
    public boolean shouldRetry(DocumentClientException exception) {
        if (this.currentAttempt >= maxRetryCount) {
            return false;
        }

        boolean shouldRetry = false;
        if (!Utils.isWriteOperation(this.currentRequest.getOperationType()) && this.currentRequest.getEndpointOverride() == null &&
                !StringUtils.equalsIgnoreCase(this.globalEndpointManager.getReadEndpoint().toString(), this.globalEndpointManager.getWriteEndpoint().toString())) {
            this.logger.info("Read with session token not available in read region. Try read from write region.");

            this.currentRequest.setEndpointOverride(this.globalEndpointManager.getWriteEndpoint());
            shouldRetry = true;
            this.currentAttempt++;
        }

        return shouldRetry;
    }
}