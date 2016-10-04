package com.microsoft.azure.documentdb.internal;

import java.util.logging.Logger;

import com.microsoft.azure.documentdb.DocumentClientException;

final class ResourceThrottleRetryPolicy implements RetryPolicy {
    private final long defaultRetryInSeconds = 5;
    private final int maxAttemptCount;
    private final Logger logger;
    private int maxWaitTimeInMilliseconds = 60 * 1000;
    private int currentAttemptCount = 0;
    private long retryAfterInMilliseconds = 0;
    private int cumulativeWaitTime = 0;

    public ResourceThrottleRetryPolicy(int maxAttemptCount, int maxWaitTimeInSeconds) {
        if (maxWaitTimeInSeconds > Integer.MAX_VALUE / 1000) {
            throw new IllegalArgumentException("maxWaitTimeInSeconds must be less than " + Integer.MAX_VALUE / 1000);
        }

        this.maxAttemptCount = maxAttemptCount;
        this.maxWaitTimeInMilliseconds = 1000 * maxWaitTimeInSeconds;
        this.logger = Logger.getLogger(this.getClass().getPackage().getName());
    }

    public long getRetryAfterInMilliseconds() {
        return this.retryAfterInMilliseconds;
    }

    /**
     * Should the caller retry the operation.
     * <p>
     * This retry policy should only be invoked if HttpStatusCode is 429 (Too Many Requests).
     *
     * @param exception the exception to check.
     * @return true if should retry.
     */
    public boolean shouldRetry(DocumentClientException exception) {
        this.retryAfterInMilliseconds = 0;

        if (this.currentAttemptCount >= this.maxAttemptCount) {
            return false;
        }

        this.retryAfterInMilliseconds =
                exception.getRetryAfterInMilliseconds();
        if (this.retryAfterInMilliseconds == 0) {
            // we should never reach here as BE should turn non-zero of
            // retry delay.
            this.retryAfterInMilliseconds = this.defaultRetryInSeconds * 1000;
        }

        this.cumulativeWaitTime += this.retryAfterInMilliseconds;
        if (this.cumulativeWaitTime >= this.maxWaitTimeInMilliseconds) {
            return false;
        }

        this.logger.info(String.format("Operation will be retried after %d milliseconds. Exception: %s",
                this.retryAfterInMilliseconds,
                exception.getMessage()));
        this.currentAttemptCount++;
        return true;
    }
}
