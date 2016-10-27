package com.microsoft.azure.documentdb.directconnectivity;

import org.apache.http.HttpStatus;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.internal.RequestChargeTracker;

abstract class ReadResult {
    private final StoreReadResult response;
    private final RequestChargeTracker requestChargeTracker;

    protected ReadResult(StoreReadResult response, RequestChargeTracker requestChargeTracker) {
        this.response = response;
        this.requestChargeTracker = requestChargeTracker;
    }

    public StoreResponse getResponse() throws DocumentClientException {
        if (!this.isValidResult()) {
            throw new DocumentClientException(HttpStatus.SC_INTERNAL_SERVER_ERROR, "Result received is not valid.");
        }

        return this.response.toStoreResponse(this.requestChargeTracker);
    }

    public StoreReadResult getStoreReadResult() {
        return this.response;
    }

    protected abstract boolean isValidResult();
}