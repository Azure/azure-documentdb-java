package com.microsoft.azure.documentdb;

interface RetryPolicy {
    public boolean shouldRetry(DocumentClientException exception);
    public long getRetryAfterInMilliseconds();
}
