package com.microsoft.azure.documentdb.directconnectivity;

import java.util.List;

class ReadReplicaResult {
    private boolean retryWithForceRefresh;
    private List<StoreReadResult> responses;

    public ReadReplicaResult(boolean retryWithForceRefresh, List<StoreReadResult> responses) {
        this.retryWithForceRefresh = retryWithForceRefresh;
        this.responses = responses;
    }

    public boolean isRetryWithForceRefresh() {
        return retryWithForceRefresh;
    }

    public void setRetryWithForceRefresh(boolean retryWithForceRefresh) {
        this.retryWithForceRefresh = retryWithForceRefresh;
    }

    public List<StoreReadResult> getResponses() {
        return responses;
    }

    public void setResponses(List<StoreReadResult> responses) {
        this.responses = responses;
    }
}