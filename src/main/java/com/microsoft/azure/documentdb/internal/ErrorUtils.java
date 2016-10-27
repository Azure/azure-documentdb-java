package com.microsoft.azure.documentdb.internal;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.ParseException;
import org.apache.http.util.EntityUtils;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.Error;

public class ErrorUtils {

    public static void maybeThrowException(String requestUri, HttpResponse response, boolean isGatewayResponse, Logger logger) throws DocumentClientException {
        if (requestUri == null) {
            throw new IllegalArgumentException("requestUri");
        }

        int statusCode = response.getStatusLine().getStatusCode();

        if (statusCode >= HttpConstants.StatusCodes.MINIMUM_STATUSCODE_AS_ERROR_GATEWAY) {
            HttpEntity httpEntity = response.getEntity();
            String body = "";
            if (httpEntity != null) {
                try {
                    body = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                    EntityUtils.consume(response.getEntity());
                } catch (ParseException | IOException e) {
                    if (logger != null) {
                        logger.log(Level.SEVERE, e.toString(), e);
                    }

                    throw new IllegalStateException("Failed to get content from the http response", e);
                }
            }

            Map<String, String> responseHeaders = new HashMap<String, String>();
            for (Header header : response.getAllHeaders()) {
                responseHeaders.put(header.getName(), header.getValue());
            }

            String statusCodeString = response.getStatusLine().getReasonPhrase() != null
                    ? response.getStatusLine().getReasonPhrase().replace(" ", "")
                    : "";
            Error error = null;
            if (isGatewayResponse) {
                error = new Error(body);
                error = new Error(statusCodeString,
                        String.format("%s, StatusCode: %s", error.getMessage(), statusCodeString),
                        error.getPartitionedQueryExecutionInfo());
            } else {
                error = new Error(statusCodeString,
                        String.format("%s\r\nActivityId: %s, Request URI: %s, StatusCode: %s",
                                body,
                                responseHeaders.get(HttpConstants.HttpHeaders.ACTIVITY_ID),
                                requestUri,
                                statusCodeString));
            }

            throw new DocumentClientException(statusCode, error, responseHeaders);
        }
    }
}
