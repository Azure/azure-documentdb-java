package com.microsoft.azure.documentdb;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.microsoft.azure.documentdb.internal.Constants;
import com.microsoft.azure.documentdb.internal.HttpConstants;

/**
 * This class defines a custom exception type for all operations on
 * DocumentClient. Applications are expected to catch DocumentClientException
 * and handle errors as appropriate when calling methods on DocumentClient.
 * <p>
 * Errors coming from the service during normal execution are converted to
 * DocumentClientException before returning to the application with the following exception:
 * <p>
 * When a BE error is encountered during a QueryIterable&lt;T&gt; iteration, an IllegalStateException
 * is thrown instead of DocumentClientException.
 * <p>
 * When a transport level error happens that request is not able to reach the service,
 * an IllegalStateException is thrown instead of DocumentClientException.
 */
public class DocumentClientException extends Exception {
    private static final long serialVersionUID = 1L;

    private Error error;
    private String resourceAddress;
    private int statusCode;
    private Map<String, String> responseHeaders;

    /**
     * Creates a new instance of the DocumentClientException class.
     *
     * @param statusCode the http status code of the response.
     */
    public DocumentClientException(int statusCode) {
        this.statusCode = statusCode;
    }

    /**
     * Creates a new instance of the DocumentClientException class.
     *
     * @param statusCode   the http status code of the response.
     * @param errorMessage the error message.
     */
    public DocumentClientException(int statusCode, String errorMessage) {
        Error error = new Error();
        error.set(Constants.Properties.MESSAGE, errorMessage);
        this.statusCode = statusCode;
        this.error = error;
    }

    /**
     * Creates a new instance of the DocumentClientException class.
     *
     * @param statusCode     the http status code of the response.
     * @param innerException the original exception.
     */
    public DocumentClientException(int statusCode, Exception innerException) {
        super(innerException);
        this.statusCode = statusCode;
    }


    /**
     * Creates a new instance of the DocumentClientException class.
     *
     * @param statusCode      the http status code of the response.
     * @param errorResource   the error resource object.
     * @param responseHeaders the response headers.
     */
    public DocumentClientException(int statusCode, Error errorResource, Map<String, String> responseHeaders) {
        this(null, statusCode, errorResource, responseHeaders);
    }

    /**
     * Creates a new instance of the DocumentClientException class.
     *
     * @param resourceAddress the address of the resource the request is associated with.
     * @param statusCode      the http status code of the response.
     * @param errorResource   the error resource object.
     * @param responseHeaders the response headers.
     */

    public DocumentClientException(String resourceAddress, int statusCode, Error errorResource, Map<String, String> responseHeaders) {
        super(errorResource.getMessage());

        this.resourceAddress = resourceAddress;
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
     * Gets the http status code.
     *
     * @return the status code.
     */
    public int getStatusCode() {
        return this.statusCode;
    }

    /**
     * Gets the sub status code.
     *
     * @return the status code.
     */
    public Integer getSubStatusCode() {
        Integer code = null;
        if (this.responseHeaders != null) {
            String subStatusString = this.responseHeaders.get(HttpConstants.HttpHeaders.SUB_STATUS);
            if (StringUtils.isNotEmpty(subStatusString)) {
                try {
                    code = Integer.valueOf(subStatusString);
                } catch (NumberFormatException e) {
                    // If value cannot be parsed as Integer, return null.
                }
            }
        }

        return code;
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
     * Gets the recommended time interval after which the client can retry
     * failed requests
     *
     * @return the recommended time interval after which the client can retry
     * failed requests.
     */
    public long getRetryAfterInMilliseconds() {
        long retryIntervalInMilliseconds = 0;

        if (this.responseHeaders != null) {
            String header = this.responseHeaders.get(HttpConstants.HttpHeaders.RETRY_AFTER_IN_MILLISECONDS);

            if (StringUtils.isNotEmpty(header)) {
                try {
                    retryIntervalInMilliseconds = Long.valueOf(header);
                } catch (NumberFormatException e) {
                    // If the value cannot be parsed as long, return 0.
                }
            }
        }

        //
        // In the absence of explicit guidance from the backend, don't introduce
        // any unilateral retry delays here.
        return retryIntervalInMilliseconds;
    }

    /**
     * Gets the response headers as key-value pairs
     *
     * @return the response headers
     */
    public Map<String, String> getResponseHeaders() {
        return this.responseHeaders;
    }

    /**
     * Gets the resource address associated with this exception.
     *
     * @return the resource address associated with this exception.
     */
    String getResourceAddress() {
        return this.resourceAddress;
    }
}
