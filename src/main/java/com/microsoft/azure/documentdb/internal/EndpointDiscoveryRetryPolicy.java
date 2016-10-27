package com.microsoft.azure.documentdb.internal;

import java.util.logging.Logger;

import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.DocumentClientException;

final class EndpointDiscoveryRetryPolicy implements RetryPolicy {
    private static int maxRetryCount = 120;
    private final int retryIntervalInMS = 1000;
    private final EndpointManager globalEndpointManager;
    private final ConnectionPolicy connectionPolicy;
    private int currentAttempt = 0;
    private Logger logger;

    /**
     * A RetryPolicy implementation that handles endpoint change exceptions.
     *
     * @param client the DocumentClient instance.
     */
    public EndpointDiscoveryRetryPolicy(ConnectionPolicy connectionPolicy, EndpointManager globalEndpointManager) {
        this.connectionPolicy = connectionPolicy;
        this.globalEndpointManager = globalEndpointManager;
        this.logger = Logger.getLogger(this.getClass().getPackage().getName());
    }

    /**
     * Gets the number of milliseconds to wait before retry the operation.
     *
     * @return the number of milliseconds to wait before retry the operation.
     */
    public long getRetryAfterInMilliseconds() {
        return this.retryIntervalInMS;
    }

    /**
     * Should the caller retry the operation.
     * <p>
     * This retry policy should only be invoked if HttpStatusCode is 403 (Forbidden)
     * and SubStatusCode is 3 (WriteForbidden).
     *
     * @param exception the exception to check.
     * @return true if should retry.
     */
    public boolean shouldRetry(DocumentClientException exception) {
        if (!this.connectionPolicy.getEnableEndpointDiscovery()) {
            return false;
        }

        if (this.currentAttempt >= maxRetryCount) {
            return false;
        }

        this.logger.info(String.format("Write region changed, refresh region list and retry after %d milliseconds",
                this.retryIntervalInMS));
        globalEndpointManager.refreshEndpointList();
        this.currentAttempt++;

        return true;
    }
}
