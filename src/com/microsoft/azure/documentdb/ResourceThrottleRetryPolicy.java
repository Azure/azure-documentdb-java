package com.microsoft.azure.documentdb;

import java.util.logging.Logger;

final class ResourceThrottleRetryPolicy {
    private final long defaultRetryInSeconds = 5;

    private int maxAttemptCount;
    private int currentAttemptCount = 0;

    private long retryAfterInMilliseconds = 0;
    
    private final Logger logger = Logger.getLogger(
        this.getClass().getPackage().getName());

    public ResourceThrottleRetryPolicy(int maxRetryCount) {
        this.maxAttemptCount = maxRetryCount;
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
    public boolean shouldRetry(Exception exception) {
        this.retryAfterInMilliseconds = 0;

        if (this.currentAttemptCount < this.maxAttemptCount &&
                this.CheckIfRetryNeeded(exception)) {
            this.currentAttemptCount++;
            this.logger.info(String.format("Operation will be retried after %d milliseconds. Exception: %s",
                                           this.retryAfterInMilliseconds,
                                           exception.getMessage()));
            return true;
        } else {
            this.logger.warning(String.format("Operation will NOT be retried. Exception: %s",
                                              exception.getMessage()));
            return false;
        }
    }

    /**
     * Returns True if the given exception is retriable.
     * 
     * @param exception the exception to check.
     * @return true if return is needed.
     */
    private boolean CheckIfRetryNeeded(Exception exception) {
        this.retryAfterInMilliseconds = 0;

        if (exception instanceof DocumentClientException) {
            DocumentClientException dce = (DocumentClientException) exception;

            if (dce.getStatusCode() == 429) {
                this.retryAfterInMilliseconds =
                    dce.getRetryAfterInMilliseconds();

                if (this.retryAfterInMilliseconds == 0) {
                    // we should never reach here as BE should turn non-zero of
                    // retry delay.
                    this.retryAfterInMilliseconds = this.defaultRetryInSeconds;
                }

                return true;
            }
        }

        return false;
    }
}
