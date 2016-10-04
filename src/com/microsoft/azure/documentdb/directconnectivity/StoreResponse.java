package com.microsoft.azure.documentdb.directconnectivity;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;

import com.microsoft.azure.documentdb.internal.HttpConstants;

public class StoreResponse {
    private int status;
    private String[] responseHeaderNames;
    private String[] responseHeaderValues;
    private HttpEntity responseBody;

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String[] getResponseHeaderNames() {
        return responseHeaderNames;
    }

    public void setResponseHeaderNames(String[] responseHeaderNames) {
        this.responseHeaderNames = responseHeaderNames;
    }

    public String[] getResponseHeaderValues() {
        return responseHeaderValues;
    }

    public void setResponseHeaderValues(String[] responseHeaderValues) {
        this.responseHeaderValues = responseHeaderValues;
    }

    public HttpEntity getResponseBody() {
        return responseBody;
    }

    public void setResponseBody(HttpEntity responseBody) {
        this.responseBody = responseBody;
    }

    public long getLSN() {
        String lsnString = this.getHeaderValue(WFConstants.BackendHeaders.LSN);
        if (StringUtils.isNotEmpty(lsnString)) {
            return Long.parseLong(lsnString);
        }

        return -1;
    }

    public String getPartitionKeyRangeId() {
        return this.getHeaderValue(WFConstants.BackendHeaders.PARTITION_KEY_RANGE_ID);
    }

    public long getCollectionPartitionIndex() {
        String partitionIndexString = this.getHeaderValue(WFConstants.BackendHeaders.COLLECTION_PARTITION_INDEX);
        if (StringUtils.isNotEmpty(partitionIndexString)) {
            return Long.parseLong(partitionIndexString);
        }

        return -1;
    }

    public int getCollectionServiceIndex() {
        String serviceIndexString = this.getHeaderValue(WFConstants.BackendHeaders.COLLECTION_PARTITION_INDEX);
        if (StringUtils.isNotEmpty(serviceIndexString)) {
            return Integer.parseInt(serviceIndexString);
        }

        return -1;
    }

    public String getContinuation() {
        return this.getHeaderValue(HttpConstants.HttpHeaders.CONTINUATION);
    }

    public String getHeaderValue(String attribute) {
        if (this.responseHeaderValues == null || this.responseHeaderNames.length != this.responseHeaderValues.length) {
            return null;
        }

        for (int i = 0; i < responseHeaderNames.length; i++) {
            if (responseHeaderNames[i].equals(attribute)) {
                return responseHeaderValues[i];
            }
        }

        return null;
    }

}
