package com.microsoft.azure.documentdb;

import java.util.Map;

/**
 * The base class for Azure DocumentDB client exceptions.
 * 
 */
public class DocumentClientException extends Exception {
    private static final long serialVersionUID = 1L;

    private Error error;
    private int statusCode;
    private Map<String, String> responseHeaders;

    /**
     * Constructor.
     * 
     * @param statusCode the status code.
     * @param errorResource the error resource.
     * @param responseHeaders the response headers.
     */
    public DocumentClientException(int statusCode, Error errorResource, Map<String, String> responseHeaders) {
        super(errorResource.getMessage());

        this.statusCode = statusCode;
        this.error = errorResource;
        this.responseHeaders = responseHeaders;
    }

    /**
     * Gets the activity ID associated with the request.
     * 
     * @return the activity ID.
     */
    public String getActivityId() {
        if (this.responseHeaders != null) {
            return this.responseHeaders.get(HttpConstants.HttpHeaders.ACTIVITY_ID);
        }
        return null;
    }

    /**
     * Gets the request status code.
     * 
     * @return the status code.
     */
    public int getStatusCode() {
        return this.statusCode;
    }

    /**
     * Gets the error code associated with the exception.
     * 
     * @return the error.
     */
    public Error getError() {
        return this.error;
    }

    /**
     * Gets the recommended time interval after which the client can retry failed requests
     * 
     * @return the recommended time interval after which the client can retry failed requests.
     */
    public long getRetryAfterInMilliseconds() {
        if (this.responseHeaders != null) {
            String header = this.responseHeaders.get(
            HttpConstants.HttpHeaders.RETRY_AFTER_IN_MILLISECONDS);

            if (header != null && !header.isEmpty()) {
                long retryIntervalInMilliseconds = Long.valueOf(header);
                return retryIntervalInMilliseconds;
            }
        }

        //
        // In the absence of explicit guidance from the backend, don't introduce
        // any unilateral retry delays here.
        //
        return 0;
    }
}
