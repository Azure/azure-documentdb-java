package com.microsoft.azure.documentdb.directconnectivity;

import java.net.URI;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.internal.DocumentServiceRequest;

public abstract class TransportClient {
    public abstract StoreResponse invokeStore(URI physicalAddress, DocumentServiceRequest request) throws DocumentClientException;

    public StoreResponse invokeResourceOperation(URI physicalAddress, DocumentServiceRequest request) throws DocumentClientException {
        StoreResponse response = this.invokeStore(physicalAddress, request);
        return response;
    }
}
