package com.microsoft.azure.documentdb.internal;

import com.microsoft.azure.documentdb.internal.routing.ClientCollectionCache;
import org.apache.http.HttpStatus;

import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.ResourceResponse;
import com.microsoft.azure.documentdb.directconnectivity.GoneAndRetryWithRetryPolicy;

/**
 * A utility class to manage retries for various retryable service errors. It
 * invokes a delegate function to execute the target operation. Upon error, it
 * waits a period of time as defined by the RetryPolicy instance before
 * re-issuing the same request. Different RetryPolicy implementations decides
 * the wait period based on its own logic.
 */
public class RetryUtility {

    /**
     * Executes the code block in the delegate and retry if needed.
     * <p>
     * This method is used to retry an existing DocumentServiceRequest.
     *
     * @param delegate              the delegate to execute.
     * @param documentClient        the DocumentClient instance.
     * @param globalEndpointManager the EndpointManager instance.
     * @param request               the request parameter for the execution.
     * @return                      the DocumentServiceResponse instance
     * @throws DocumentClientException the original exception if retry is not applicable.
     */
    public static DocumentServiceResponse executeDocumentClientRequest(
            RetryRequestDelegate delegate,
            DocumentClient documentClient,
            EndpointManager globalEndpointManager,
            DocumentServiceRequest request) throws DocumentClientException {

        DocumentServiceResponse response = null;

        EndpointDiscoveryRetryPolicy discoveryRetryPolicy = new EndpointDiscoveryRetryPolicy(
                documentClient.getConnectionPolicy(),
                globalEndpointManager);

        ResourceThrottleRetryPolicy throttleRetryPolicy = new ResourceThrottleRetryPolicy(
                documentClient.getConnectionPolicy().getRetryOptions().getMaxRetryAttemptsOnThrottledRequests(),
                documentClient.getConnectionPolicy().getRetryOptions().getMaxRetryWaitTimeInSeconds());

        SessionReadRetryPolicy sessionReadRetryPolicy = new SessionReadRetryPolicy(
                globalEndpointManager, request);

        while (true) {
            try {
                response = delegate.apply(request);
                break;
            } catch (DocumentClientException e) {
                RetryPolicy retryPolicy = null;
                if (e.getStatusCode() == HttpConstants.StatusCodes.FORBIDDEN && e.getSubStatusCode() != null
                        && e.getSubStatusCode() == HttpConstants.SubStatusCodes.FORBIDDEN_WRITEFORBIDDEN) {
                    // If HttpStatusCode is 403 (Forbidden) and SubStatusCode is
                    // 3 (WriteForbidden),
                    // invoke the endpoint discovery retry policy
                    retryPolicy = discoveryRetryPolicy;
                } else if (e.getStatusCode() == HttpConstants.StatusCodes.TOO_MANY_REQUESTS) {
                    // If HttpStatusCode is 429 (Too Many Requests), invoke the
                    // throttle retry policy
                    retryPolicy = throttleRetryPolicy;
                } else if (e.getStatusCode() == HttpConstants.StatusCodes.NOTFOUND && e.getSubStatusCode() != null
                        && e.getSubStatusCode() == HttpConstants.SubStatusCodes.READ_SESSION_NOT_AVAILABLE) {
                    // If HttpStatusCode is 404 (NotFound) and SubStatusCode is
                    // 1002 (ReadSessionNotAvailable), invoke the session read retry policy
                    retryPolicy = sessionReadRetryPolicy;
                }

                boolean retry = (retryPolicy != null && retryPolicy.shouldRetry(e));
                if (!retry) {
                    throw e;
                }

                RetryUtility.delayForRetry(retryPolicy);
            }
        }

        return response;
    }

    /**
     * Executes the code block in the delegate and retry if needed.
     * <p>
     * This method is used to retry an existing DocumentServiceRequest.
     *
     * @param delegate the delegate to execute.
     * @param request  the request parameter for the execution.
     * @throws DocumentClientException the original exception if retry is not applicable.
     * @return         the DocumentServiceResponse instance
     */
    public static DocumentServiceResponse executeStoreClientRequest(RetryRequestDelegate delegate,
                                                                    DocumentServiceRequest request) throws DocumentClientException {
        DocumentServiceResponse response = null;

        GoneAndRetryWithRetryPolicy goneAndRetryWithRetryPolicy = new GoneAndRetryWithRetryPolicy(request);

        while (true) {
            try {
                response = delegate.apply(request);
                break;
            } catch (DocumentClientException e) {
                RetryPolicy retryPolicy = null;
                if (e.getStatusCode() == HttpConstants.StatusCodes.RETRY_WITH || e.getStatusCode() == HttpStatus.SC_GONE) {
                    // if HttpStatusCode is 449 ( retryWith ) or 410 ( gone exception ). invoke the GoneAndRetry policy 
                    retryPolicy = goneAndRetryWithRetryPolicy;
                }

                boolean retry = (retryPolicy != null && retryPolicy.shouldRetry(e));
                if (!retry) {
                    throw e;
                }

                RetryUtility.delayForRetry(retryPolicy);
            }
        }

        return response;
    }

    /**
     * Executes the code block in the delegate and retry if needed.
     * <p>
     * This method is used to retry a document create operation for partitioned collection
     * in the case where the request failed because the collection has changed.
     *
     * @param delegate                  the delegate to execute.
     * @param clientCollectionCache     the cache the maps collection ResourceId or resource name to collection
     * @param resourcePath              the path to the collection resource.
     * @throws DocumentClientException  the original exception if retry is not applicable.
     * @return                          the response resource.
     */
    public static ResourceResponse<Document> executeCreateDocument(
            RetryCreateDocumentDelegate delegate,
            ClientCollectionCache clientCollectionCache,
            String resourcePath)
            throws DocumentClientException {
        ResourceResponse<Document> result = null;

        PartitionKeyMismatchRetryPolicy keyMismatchRetryPolicy = new PartitionKeyMismatchRetryPolicy(
                resourcePath,
                clientCollectionCache);

        while (true) {
            try {
                result = delegate.apply();
                break;
            } catch (DocumentClientException e) {
                RetryPolicy retryPolicy = null;
                if (e.getStatusCode() == HttpConstants.StatusCodes.BADREQUEST && e.getSubStatusCode() != null
                        && e.getSubStatusCode() == HttpConstants.SubStatusCodes.PARTITION_KEY_MISMATCH) {
                    // If HttpStatusCode is 404 (NotFound) and SubStatusCode is
                    // 1001 (PartitionKeyMismatch), invoke the partition key mismatch retry policy
                    retryPolicy = keyMismatchRetryPolicy;
                }

                boolean retry = (retryPolicy != null && retryPolicy.shouldRetry(e));
                if (!retry) {
                    throw e;
                }

                RetryUtility.delayForRetry(retryPolicy);
            }
        }

        return result;
    }

    private static void delayForRetry(RetryPolicy retryPolicy) {
        final long delay = retryPolicy.getRetryAfterInMilliseconds();
        if (delay > 0) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                // Ignore the interruption.
            }
        }
    }
}
