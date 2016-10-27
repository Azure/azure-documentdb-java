package com.microsoft.azure.documentdb.directconnectivity;

import com.microsoft.azure.documentdb.internal.RequestChargeTracker;

class ReadPrimaryResult extends ReadResult {
    private boolean isSuccessful;
    private boolean shouldRetryOnSecondary;

    protected ReadPrimaryResult(boolean isSuccessful, boolean shouldRetryOnSecondary, StoreReadResult response, RequestChargeTracker requestChargeTracker) {
        super(response, requestChargeTracker);
        this.shouldRetryOnSecondary = shouldRetryOnSecondary;
        this.isSuccessful = isSuccessful;
    }

    public boolean isSuccessful() {
        return isSuccessful;
    }

    public boolean isShouldRetryOnSecondary() {
        return shouldRetryOnSecondary;
    }

    @Override
    protected boolean isValidResult() {
        return this.isSuccessful;
    }
}