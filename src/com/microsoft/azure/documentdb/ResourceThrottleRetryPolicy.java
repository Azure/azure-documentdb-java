package com.microsoft.azure.documentdb;

import java.util.logging.Logger;

final class ResourceThrottleRetryPolicy {
    private final long defaultRetryInSeconds = 5;
    private final int maxAttemptCount;
    private final Logger logger;
    private int currentAttemptCount = 0;
    private long retryAfterInMilliseconds = 0;

    public ResourceThrottleRetryPolicy(int maxRetryCount) {
        this.maxAttemptCount = maxRetryCount;
        this.logger = Logger.getLogger(this.getClass().getPackage().getName());
    }

    public long getRetryAfterInMilliseconds() {
        return this.retryAfterInMilliseconds;
    }

    /**
     * Should the caller retry the operation.
     * 
     * @param exception the exception to check.
     * @return true if should retry.
     */
    public boolean shouldRetry(DocumentClientException exception) {
        this.retryAfterInMilliseconds = 0;

        if (this.currentAttemptCount < this.maxAttemptCount &&
                this.CheckIfRetryNeeded(exception)) {
            this.currentAttemptCount++;
            this.logger.info(String.format("Operation will be retried after %d milliseconds. Exception: %s",
                                           this.retryAfterInMilliseconds,
                                           exception.getMessage()));
            return true;
        }

        return false;
    }

    /**
     * Returns True if the given exception is retriable.
     * 
     * @param exception the exception to check.
     * @return true if return is needed.
     */
    private boolean CheckIfRetryNeeded(DocumentClientException exception) {
        this.retryAfterInMilliseconds = 0;

        if (exception.getStatusCode() == 429) {
            this.retryAfterInMilliseconds =
                    exception.getRetryAfterInMilliseconds();

            if (this.retryAfterInMilliseconds == 0) {
                // we should never reach here as BE should turn non-zero of
                // retry delay.
                this.retryAfterInMilliseconds = this.defaultRetryInSeconds;
            }

            return true;
        }

        return false;
    }
}
