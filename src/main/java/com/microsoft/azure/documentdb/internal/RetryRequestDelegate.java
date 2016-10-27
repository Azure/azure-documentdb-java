package com.microsoft.azure.documentdb.internal;

import com.microsoft.azure.documentdb.DocumentClientException;

/**
 * Define a delegate used by the RetryUtility to retry a DocumentServiceRequest.
 *
 */
public abstract interface RetryRequestDelegate {
    /**
     * @param request the request to be issued
     * @return the DocumentServiceResponse instance
     * @throws DocumentClientException the original exception
     */
    abstract DocumentServiceResponse apply(DocumentServiceRequest request) throws DocumentClientException;
}