package com.microsoft.azure.documentdb;

/**
 * Define a delegate used by the BackoffRetryUtility to retry throttled requests
 * other than query.
 *
 * @param request
 *            the request to be issued.
 * @throws DocumentClientException
 */
abstract interface BackoffRetryUtilityDelegate {
    abstract DocumentServiceResponse apply(DocumentServiceRequest request) throws DocumentClientException;
}

/**
* Define a delegate used by the BackoffRetryUtility to retry throttled requests
* for QueryIterable.
*
* @param request
*            the request to be issued.
* @throws DocumentClientException
*/
abstract interface QueryBackoffRetryUtilityDelegate {
    abstract void apply() throws Exception;
}
