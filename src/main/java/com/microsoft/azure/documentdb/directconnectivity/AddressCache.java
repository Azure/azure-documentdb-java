package com.microsoft.azure.documentdb.directconnectivity;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.internal.DocumentServiceRequest;

public abstract class AddressCache {
    public abstract AddressInformation[] resolve(DocumentServiceRequest request) throws DocumentClientException;
}
