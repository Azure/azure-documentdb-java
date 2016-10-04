package com.microsoft.azure.documentdb.internal;

import com.microsoft.azure.documentdb.DocumentClientException;

public interface RetryPolicy {
    public boolean shouldRetry(DocumentClientException exception) throws DocumentClientException;

    public long getRetryAfterInMilliseconds();
}
