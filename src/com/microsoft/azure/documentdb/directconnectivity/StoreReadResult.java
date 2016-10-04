package com.microsoft.azure.documentdb.directconnectivity;

import org.apache.http.HttpStatus;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.internal.HttpConstants;
import com.microsoft.azure.documentdb.internal.RequestChargeTracker;

public class StoreReadResult {
    private StoreResponse storeResponse;
    private DocumentClientException excetpion;
    private long LSN;

    private String partitionKeyRangeId;
    private long quorumAckedLSN;
    private double requestCharge;
    private int currentReplicaSetSize;
    private int currentWriteQuorum;
    private boolean isValid;
    private boolean isGoneException;
    private boolean isNotFoundException;

    public StoreReadResult(StoreResponse storeResponse, DocumentClientException excetpion, long lSN,
                           String partitionKeyRangeId, long quorumAckedLSN, double requestCharge, int currentReplicaSetSize,
                           int currentWriteQuorum, boolean isValid) {
        this.storeResponse = storeResponse;
        this.excetpion = excetpion;
        this.LSN = lSN;
        this.partitionKeyRangeId = partitionKeyRangeId;
        this.quorumAckedLSN = quorumAckedLSN;
        this.requestCharge = requestCharge;
        this.currentReplicaSetSize = currentReplicaSetSize;
        this.currentWriteQuorum = currentWriteQuorum;
        this.isValid = isValid;
        this.isGoneException = this.excetpion != null && this.excetpion.getStatusCode() == HttpStatus.SC_GONE;
        this.isNotFoundException = this.excetpion != null && this.excetpion.getStatusCode() == HttpStatus.SC_NOT_FOUND;
    }

    public DocumentClientException getExcetpion() {
        return excetpion;
    }

    public long getLSN() {
        return LSN;
    }

    public void setLSN(long lSN) {
        LSN = lSN;
    }

    public String getPartitionKeyRangeId() {
        return partitionKeyRangeId;
    }

    public void setPartitionKeyRangeId(String partitionKeyRangeId) {
        this.partitionKeyRangeId = partitionKeyRangeId;
    }

    public long getQuorumAckedLSN() {
        return quorumAckedLSN;
    }

    public void setQuorumAckedLSN(long quorumAckedLSN) {
        this.quorumAckedLSN = quorumAckedLSN;
    }

    public double getRequestCharge() {
        return requestCharge;
    }

    private void setRequestCharge(RequestChargeTracker chargeTracker) {
        if (this.excetpion != null) {
            this.excetpion.getResponseHeaders().put(HttpConstants.HttpHeaders.REQUEST_CHARGE, Double.toString(chargeTracker.getTotalRequestCharge()));
        } else if (this.storeResponse.getResponseHeaderNames() != null) {
            for (int i = 0; i < this.storeResponse.getResponseHeaderNames().length; i++) {
                if (this.storeResponse.getResponseHeaderNames()[i].equals(HttpConstants.HttpHeaders.REQUEST_CHARGE)) {
                    this.storeResponse.getResponseHeaderValues()[i] = Double.toString(chargeTracker.getTotalRequestCharge());
                }
            }
        }
    }

    public int getCurrentReplicaSetSize() {
        return currentReplicaSetSize;
    }

    public void setCurrentReplicaSetSize(int currentReplicaSetSize) {
        this.currentReplicaSetSize = currentReplicaSetSize;
    }

    public int getCurrentWriteQuorum() {
        return currentWriteQuorum;
    }

    public void setCurrentWriteQuorum(int currentWriteQuorum) {
        this.currentWriteQuorum = currentWriteQuorum;
    }

    public boolean isValid() {
        return isValid;
    }

    public void setValid(boolean isValid) {
        this.isValid = isValid;
    }

    public boolean isGoneException() {
        return isGoneException;
    }

    public void setGoneException(boolean isGoneException) {
        this.isGoneException = isGoneException;
    }

    public boolean isNotFoundException() {
        return isNotFoundException;
    }

    public void setNotFoundException(boolean isNotFoundException) {
        this.isNotFoundException = isNotFoundException;
    }

    public StoreResponse toStoreResponse(RequestChargeTracker chargeTracker) throws DocumentClientException {
        if (!this.isValid) {
            if (this.excetpion == null) {
                throw new DocumentClientException((int) HttpStatus.SC_INTERNAL_SERVER_ERROR, "Unknown server error occurred when processing this request.");
            }

            throw this.excetpion;
        }

        if (chargeTracker != null) {
            this.setRequestCharge(chargeTracker);
        }

        if (this.excetpion != null) throw this.excetpion;
        return this.storeResponse;
    }
}
