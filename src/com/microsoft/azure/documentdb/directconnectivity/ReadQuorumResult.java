package com.microsoft.azure.documentdb.directconnectivity;

import com.microsoft.azure.documentdb.internal.RequestChargeTracker;

class ReadQuorumResult extends ReadResult {
    private ReadQuorumResultKind quorumResult;
    private long selectedLsn;

    protected ReadQuorumResult(ReadQuorumResultKind quorumResult, long selectedLsn, StoreReadResult response, RequestChargeTracker requestChargeTracker) {
        super(response, requestChargeTracker);
        this.quorumResult = quorumResult;
        this.selectedLsn = selectedLsn;
    }

    public ReadQuorumResultKind getQuorumResult() {
        return quorumResult;
    }

    public long getSelectedLsn() {
        return selectedLsn;
    }

    @Override
    protected boolean isValidResult() {
        return this.quorumResult == ReadQuorumResultKind.QuorumMet || this.quorumResult == ReadQuorumResultKind.QuorumSelected;
    }
}