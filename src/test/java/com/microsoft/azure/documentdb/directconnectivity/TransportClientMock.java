package com.microsoft.azure.documentdb.directconnectivity;

import java.net.URI;
import java.util.ArrayList;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.directconnectivity.StoreResponse;
import com.microsoft.azure.documentdb.directconnectivity.TransportClient;
import com.microsoft.azure.documentdb.internal.DocumentServiceRequest;

class TransportClientMock extends TransportClient {
    private TransportClient originalTransportClient;
    private DocumentClientException exceptionToThrow;
    private ArrayList<StoreResponse> valuesToReturn;
    private int numberOfInvocationWithAction = Integer.MAX_VALUE;
    private int currentNumberOfReturns = 0;

    public TransportClientMock(TransportClient transportClient) {
        this.originalTransportClient = transportClient;
    }

    public TransportClientMock setExceptionToThrow(DocumentClientException e) {
        this.reset();
        this.exceptionToThrow = e;
        return this;
    }

    public TransportClientMock setValuesToReturn(ArrayList<StoreResponse> response) {
        this.reset();
        this.valuesToReturn = response;
        return this;
    }

    public void times(int numberOfInvocationWithAction) {
        this.numberOfInvocationWithAction = numberOfInvocationWithAction;
        this.currentNumberOfReturns = 0;
    }

    public void reset() {
        this.numberOfInvocationWithAction = Integer.MAX_VALUE;
        this.exceptionToThrow = null;
        this.valuesToReturn = null;
        this.currentNumberOfReturns = 0;
    }

    @Override
    public synchronized StoreResponse invokeStore(URI physicalAddress, DocumentServiceRequest request)
            throws DocumentClientException {
        if (exceptionToThrow != null && currentNumberOfReturns < numberOfInvocationWithAction) {
            currentNumberOfReturns++;
            throw exceptionToThrow;
        } else if (valuesToReturn != null && currentNumberOfReturns < numberOfInvocationWithAction) {
            StoreResponse response = null;
            if (valuesToReturn.size() == 1) {
                response = valuesToReturn.get(0);
            } else if (valuesToReturn.size() == numberOfInvocationWithAction) {
                response = valuesToReturn.get(currentNumberOfReturns);
            } else {
                throw new IllegalStateException("Scenario not supported by mock.");
            }

            currentNumberOfReturns++;
            return response;
        }

        return this.originalTransportClient.invokeStore(physicalAddress, request);
    }
}