package com.microsoft.azure.documentdb.internal;

import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.ResourceResponse;

/**
 * Define a delegate used by the RetryUtility to retry an document create operation.
 */
public abstract interface RetryCreateDocumentDelegate {
    abstract ResourceResponse<Document> apply() throws DocumentClientException;
}

