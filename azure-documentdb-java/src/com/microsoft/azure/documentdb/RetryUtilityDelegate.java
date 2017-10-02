package com.microsoft.azure.documentdb;

/**
 * Define a delegate used by the RetryUtility to retry a DocumentServiceRequest.
 *
 * @param request
 *            the request to be issued.
 *            
 * @throws DocumentClientException
 */
abstract interface RetryRequestDelegate {
    abstract DocumentServiceResponse apply(DocumentServiceRequest request) throws DocumentClientException;
}

/**
* Define a delegate used by the RetryUtility to retry an document create operation.
* 
* @throws DocumentClientException
*/
abstract interface RetryCreateDocumentDelegate {
    abstract ResourceResponse<Document> apply() throws DocumentClientException;
}
