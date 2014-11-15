package com.microsoft.azure.documentdb;

final class RetryPolicy
{

    private static RetryPolicy defaultPolicy;
    private int maxRetryAttemptsOnRequest;
    private int maxRetryAttemptsOnQuery;

    /**
     * Initialize the instance of the ConnectionPolicy class
     * 
     */
    public RetryPolicy() {
        this.maxRetryAttemptsOnRequest = 0;
        this.maxRetryAttemptsOnQuery = 3;
    }

    /**
     * Sets the maximum number of retry in case of resource throttled, for request.
     * 
     * @param maxRetryAttemptsOnRequest the max retry attempts on request.
     */
    public void setMaxRetryAttemptsOnRequest(int maxRetryAttemptsOnRequest) {
        this.maxRetryAttemptsOnRequest = maxRetryAttemptsOnRequest;
    }

    /**
     * Gets the maximum number of retry in case of resource throttled, for request.
     * 
     * @return the max retry attempts on request.
     */
    public int getMaxRetryAttemptsOnRequest() {
        return this.maxRetryAttemptsOnRequest;
    }

    /**
     * Sets the maximum number of retry in case of resource throttled, for query.
     * 
     * @param maxRetryAttemptsOnQuery the max retry attempts on query.
     */
    public void setMaxRetryAttemptsOnQuery(int maxRetryAttemptsOnQuery) {
        this.maxRetryAttemptsOnQuery = maxRetryAttemptsOnQuery;
    }

    /**
     * Gets the maximum number of retry in case of resource throttled, for query.
     * 
     * @return the max retry attempts on query.
     */
    public int getMaxRetryAttemptsOnQuery() {
        return this.maxRetryAttemptsOnQuery;
    }

    /**
     * Gets the default retry policy.
     * 
     * @return the default retry policy.
     */
    public static RetryPolicy getDefault() {
        if (RetryPolicy.defaultPolicy == null) {
            RetryPolicy.defaultPolicy = new RetryPolicy();
        }

        return RetryPolicy.defaultPolicy;
    }
}
