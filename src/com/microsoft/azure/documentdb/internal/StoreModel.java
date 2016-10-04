package com.microsoft.azure.documentdb.internal;

import com.microsoft.azure.documentdb.DocumentClientException;

public interface StoreModel {
    DocumentServiceResponse processMessage(DocumentServiceRequest request) throws DocumentClientException;
}
