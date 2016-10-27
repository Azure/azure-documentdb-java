package com.microsoft.azure.documentdb.directconnectivity;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.message.BasicNameValuePair;

import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.Error;
import com.microsoft.azure.documentdb.internal.DocumentServiceRequest;
import com.microsoft.azure.documentdb.internal.ErrorUtils;
import com.microsoft.azure.documentdb.internal.HttpConstants;
import com.microsoft.azure.documentdb.internal.OperationType;
import com.microsoft.azure.documentdb.internal.ResourceType;
import com.microsoft.azure.documentdb.internal.RuntimeConstants;
import com.microsoft.azure.documentdb.internal.UserAgentContainer;
import com.microsoft.azure.documentdb.internal.Utils;

public class HttpTransportClient extends TransportClient {

    private final Logger logger;
    private final HttpClient httpClient;
    private final LinkedList<NameValuePair> defaultHeaders;

    public HttpTransportClient(ConnectionPolicy connectionPolicy, UserAgentContainer userAgentContainer) {
        this.httpClient = HttpClientFactory.createHttpClient(
                connectionPolicy.getMaxPoolSize(),
                connectionPolicy.getIdleConnectionTimeout(),
                connectionPolicy.getRequestTimeout());

        if (userAgentContainer == null) {
            userAgentContainer = new UserAgentContainer();
        }

        this.defaultHeaders = new LinkedList<NameValuePair>();
        this.defaultHeaders.add(new BasicNameValuePair(HttpConstants.HttpHeaders.VERSION, HttpConstants.Versions.CURRENT_VERSION));
        this.defaultHeaders.add(new BasicNameValuePair(HttpConstants.HttpHeaders.USER_AGENT, userAgentContainer.getUserAgent()));
        this.defaultHeaders.add(new BasicNameValuePair(HttpConstants.HttpHeaders.ACCEPT, RuntimeConstants.MediaTypes.JSON));
        this.defaultHeaders.add(new BasicNameValuePair(HttpConstants.HttpHeaders.CACHE_CONTROL, "no-cache"));
        this.logger = Logger.getLogger(this.getClass().getPackage().getName());
    }

    private static void addHeader(LinkedList<NameValuePair> requestHeaders, String headerName, DocumentServiceRequest request) {
        String headerValue = request.getHeaders().get(headerName);
        if (!StringUtils.isEmpty(headerValue)) {
            requestHeaders.add(new BasicNameValuePair(headerName, headerValue));
        }
    }

    private static void addHeader(LinkedList<NameValuePair> requestHeaders, String headerName, String headerValue) {
        if (!StringUtils.isEmpty(headerValue)) {
            requestHeaders.add(new BasicNameValuePair(headerName, headerValue));
        }
    }

    private static String GetDateHeader(Map<String, String> headers) {
        if (headers == null) {
            return StringUtils.EMPTY;
        }

        // Since Date header is overridden by some proxies/http client libraries, we support
        // an additional date header 'x-ms-date' and prefer that to the regular 'date' header.
        String date = headers.get(HttpConstants.HttpHeaders.X_DATE);
        if (StringUtils.isEmpty(date)) {
            date = headers.get(HttpConstants.HttpHeaders.HTTP_DATE);
        }

        return date != null ? date : StringUtils.EMPTY;
    }

    @Override
    public StoreResponse invokeStore(URI physicalAddress, DocumentServiceRequest request) throws DocumentClientException {
        HttpRequestBase httpRequest = this.prepareHttpMessage(request.getActivityId(), physicalAddress, request.getOperationType(), request.getResourceType(), request);
        HttpResponse response;
        try {
            response = this.httpClient.execute(httpRequest);
        } catch (ClientProtocolException e) {
            throw new DocumentClientException(HttpStatus.SC_SERVICE_UNAVAILABLE, e);
        } catch (IOException e) {
            httpRequest.releaseConnection();
            if (e instanceof NoHttpResponseException) {
                throw new DocumentClientException(HttpStatus.SC_GONE, e);
            } else {
                throw new DocumentClientException(HttpStatus.SC_SERVICE_UNAVAILABLE, e);
            }
        }

        return this.processResponse(physicalAddress.getPath(), response, request.getActivityId());
    }

    private StoreResponse processResponse(String requestUri, HttpResponse response, String activityId) throws DocumentClientException {
        if (requestUri == null) {
            throw new IllegalArgumentException("requestUri");
        }

        if (response == null) {
            Map<String, String> headers = new HashMap<String, String>();
            headers.put(HttpConstants.HttpHeaders.ACTIVITY_ID, activityId);
            headers.put(HttpConstants.HttpHeaders.REQUEST_VALIDATION_FAILURE, "1");

            String errorBodyTemplate = "{'code':'%d', 'message':'Message: {\"Errors\":[\"%s\"]}'}";
            String errorBody = String.format(errorBodyTemplate, HttpStatus.SC_INTERNAL_SERVER_ERROR, "The backend response was not in the correct format.");
            throw new DocumentClientException(HttpStatus.SC_INTERNAL_SERVER_ERROR, new Error(errorBody), headers);
        }

        // Successful request is when status code is < 300 or status code is 304 ( Not modified )
        if (response.getStatusLine().getStatusCode() < 300 || response.getStatusLine().getStatusCode() == 304) {
            return createStoreResponseFromHttpResponse(response);
        } else {
            ErrorUtils.maybeThrowException(requestUri, response, false, this.logger);
            return null;
        }
    }

    public HttpRequestBase prepareHttpMessage(String activityId, URI physicalAddress, OperationType operationType,
                                              ResourceType resourceType, DocumentServiceRequest request) {
        LinkedList<NameValuePair> requestHeaders = new LinkedList<NameValuePair>();

        HttpTransportClient.addHeader(requestHeaders, HttpConstants.HttpHeaders.VERSION, request);
        HttpTransportClient.addHeader(requestHeaders, HttpConstants.HttpHeaders.USER_AGENT, request);
        HttpTransportClient.addHeader(requestHeaders, HttpConstants.HttpHeaders.PAGE_SIZE, request);
        HttpTransportClient.addHeader(requestHeaders, HttpConstants.HttpHeaders.PRE_TRIGGER_INCLUDE, request);
        HttpTransportClient.addHeader(requestHeaders, HttpConstants.HttpHeaders.PRE_TRIGGER_EXCLUDE, request);
        HttpTransportClient.addHeader(requestHeaders, HttpConstants.HttpHeaders.POST_TRIGGER_INCLUDE, request);
        HttpTransportClient.addHeader(requestHeaders, HttpConstants.HttpHeaders.POST_TRIGGER_EXCLUDE, request);
        HttpTransportClient.addHeader(requestHeaders, HttpConstants.HttpHeaders.AUTHORIZATION, request);
        HttpTransportClient.addHeader(requestHeaders, HttpConstants.HttpHeaders.INDEXING_DIRECTIVE, request);
        HttpTransportClient.addHeader(requestHeaders, HttpConstants.HttpHeaders.CONSISTENCY_LEVEL, request);
        HttpTransportClient.addHeader(requestHeaders, HttpConstants.HttpHeaders.SESSION_TOKEN, request);
        HttpTransportClient.addHeader(requestHeaders, HttpConstants.HttpHeaders.PREFER, request);
        HttpTransportClient.addHeader(requestHeaders, HttpConstants.HttpHeaders.RESOURCE_TOKEN_EXPIRY, request);
        HttpTransportClient.addHeader(requestHeaders, HttpConstants.HttpHeaders.ENABLE_SCAN_IN_QUERY, request);
        HttpTransportClient.addHeader(requestHeaders, HttpConstants.HttpHeaders.EMIT_VERBOSE_TRACES_IN_QUERY, request);
        HttpTransportClient.addHeader(requestHeaders, HttpConstants.HttpHeaders.CONTINUATION, request.getContinuation());
        HttpTransportClient.addHeader(requestHeaders, HttpConstants.HttpHeaders.ACTIVITY_ID, activityId);
        HttpTransportClient.addHeader(requestHeaders, HttpConstants.HttpHeaders.PARTITION_KEY, request);
        HttpTransportClient.addHeader(requestHeaders, HttpConstants.HttpHeaders.PARTITION_KEY_RANGE_ID, request);

        String dateHeader = HttpTransportClient.GetDateHeader(request.getHeaders());
        HttpTransportClient.addHeader(requestHeaders, HttpConstants.HttpHeaders.X_DATE, dateHeader);
        HttpTransportClient.addHeader(requestHeaders, "Match", this.GetMatch(request, request.getOperationType()));

        String fanoutRequestHeader = request.getHeaders().get(WFConstants.BackendHeaders.IS_FANOUT_REQUEST);
        HttpTransportClient.addHeader(requestHeaders, WFConstants.BackendHeaders.IS_FANOUT_REQUEST, fanoutRequestHeader);

        if (!request.getIsNameBased()) {
            HttpTransportClient.addHeader(requestHeaders, WFConstants.BackendHeaders.ResourceId, request.getResourceId());
        }

        if (request.getResourceType() == ResourceType.DocumentCollection) {
            HttpTransportClient.addHeader(requestHeaders, WFConstants.BackendHeaders.COLLECTION_PARTITION_INDEX, request.getHeaders().get(WFConstants.BackendHeaders.COLLECTION_PARTITION_INDEX));
            HttpTransportClient.addHeader(requestHeaders, WFConstants.BackendHeaders.COLLECTION_SERVICE_INDEX, request.getHeaders().get(WFConstants.BackendHeaders.COLLECTION_SERVICE_INDEX));
        }

        if (request.getHeaders().get(WFConstants.BackendHeaders.BIND_REPLICA_DIRECTIVE) != null) {
            HttpTransportClient.addHeader(requestHeaders, WFConstants.BackendHeaders.BIND_REPLICA_DIRECTIVE, request.getHeaders().get(WFConstants.BackendHeaders.BIND_REPLICA_DIRECTIVE));
            HttpTransportClient.addHeader(requestHeaders, WFConstants.BackendHeaders.PRIMARY_MASTER_KEY, request.getHeaders().get(WFConstants.BackendHeaders.PRIMARY_MASTER_KEY));
            HttpTransportClient.addHeader(requestHeaders, WFConstants.BackendHeaders.SECONDARY_MASTER_KEY, request.getHeaders().get(WFConstants.BackendHeaders.SECONDARY_MASTER_KEY));
            HttpTransportClient.addHeader(requestHeaders, WFConstants.BackendHeaders.PRIMARY_READONLY_KEY, request.getHeaders().get(WFConstants.BackendHeaders.PRIMARY_READONLY_KEY));
            HttpTransportClient.addHeader(requestHeaders, WFConstants.BackendHeaders.SECONDARY_READONLY_KEY, request.getHeaders().get(WFConstants.BackendHeaders.SECONDARY_READONLY_KEY));
            HttpTransportClient.addHeader(requestHeaders, WFConstants.BackendHeaders.BIND_MIN_EFFECTIVE_PARTITION_KEY, request.getHeaders().get(WFConstants.BackendHeaders.BIND_MIN_EFFECTIVE_PARTITION_KEY));
            HttpTransportClient.addHeader(requestHeaders, WFConstants.BackendHeaders.BIND_MAX_EFFECTIVE_PARTITION_KEY, request.getHeaders().get(WFConstants.BackendHeaders.BIND_MAX_EFFECTIVE_PARTITION_KEY));
            HttpTransportClient.addHeader(requestHeaders, WFConstants.BackendHeaders.BIND_PARTITION_KEY_RANGE_ID, request.getHeaders().get(WFConstants.BackendHeaders.BIND_PARTITION_KEY_RANGE_ID));
            HttpTransportClient.addHeader(requestHeaders, WFConstants.BackendHeaders.BIND_PARTITION_KEY_RANGE_RID_PREFIX, request.getHeaders().get(WFConstants.BackendHeaders.BIND_PARTITION_KEY_RANGE_RID_PREFIX));
            HttpTransportClient.addHeader(requestHeaders, WFConstants.BackendHeaders.MINIMUM_ALLOWED_CLIENT_VERSION, request.getHeaders().get(WFConstants.BackendHeaders.MINIMUM_ALLOWED_CLIENT_VERSION));
        }

        // Upsert
        HttpTransportClient.addHeader(requestHeaders, HttpConstants.HttpHeaders.IS_UPSERT, request);

        // SupportSpatialLegacyCoordinates
        HttpTransportClient.addHeader(requestHeaders, WFConstants.BackendHeaders.PARTITION_COUNT, request);

        HttpTransportClient.addHeader(requestHeaders, WFConstants.BackendHeaders.COLLECTION_RID, request);

        URI resourceUri = null;
        try {
            resourceUri = new URI(
                    physicalAddress.getScheme(),
                    physicalAddress.getUserInfo(),
                    physicalAddress.getHost(),
                    physicalAddress.getPort(),
                    physicalAddress.getPath() + Utils.trimBeginingAndEndingSlashes(request.getPath()),
                    physicalAddress.getQuery(),
                    physicalAddress.getFragment()
            );
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
        switch (operationType) {
            case Create:
            case Upsert:
            case ExecuteJavaScript:
                HttpPost postRequest = new HttpPost(resourceUri);
                postRequest.setEntity(request.getBody());
                this.addHeadersToRequest(postRequest, requestHeaders);

                return postRequest;

            case Replace:
                HttpPut putRequest = new HttpPut(resourceUri);
                putRequest.setEntity(request.getBody());
                this.addHeadersToRequest(putRequest, requestHeaders);

                return putRequest;

            case Delete:
                HttpDelete deleteRequest = new HttpDelete(resourceUri);
                this.addHeadersToRequest(deleteRequest, requestHeaders);

                return deleteRequest;
            case Read:
            case ReadFeed:
                HttpGet getRequest = new HttpGet(resourceUri);
                this.addHeadersToRequest(getRequest, requestHeaders);
                return getRequest;
            case Query:
            case SqlQuery:
                HttpPost queryPostRequest = new HttpPost(resourceUri);
                queryPostRequest.setEntity(request.getBody());
                HttpTransportClient.addHeader(requestHeaders, HttpConstants.HttpHeaders.CONTENT_TYPE, RuntimeConstants.MediaTypes.QUERY_JSON);
                HttpTransportClient.addHeader(requestHeaders, HttpConstants.HttpHeaders.IS_QUERY, Boolean.TRUE.toString());
                this.addHeadersToRequest(queryPostRequest, requestHeaders);
                return queryPostRequest;
            case Head:
                HttpHead headRequest = new HttpHead(resourceUri);
                this.addHeadersToRequest(headRequest, requestHeaders);
                return headRequest;
            default:
                throw new IllegalStateException("Invalid operation type");

        }
    }

    private StoreResponse createStoreResponseFromHttpResponse(HttpResponse responseMessage) {
        StoreResponse response = new StoreResponse();
        Header[] allHeaders = responseMessage.getAllHeaders();
        String[] headers = new String[allHeaders.length];
        String[] values = new String[allHeaders.length];

        for (int i = 0; i < allHeaders.length; i++) {
            headers[i] = allHeaders[i].getName();
            values[i] = allHeaders[i].getValue();
        }

        response.setResponseHeaderNames(headers);
        response.setResponseHeaderValues(values);
        response.setStatus(responseMessage.getStatusLine().getStatusCode());

        if (responseMessage.getEntity() != null) {
            response.setResponseBody(responseMessage.getEntity());
        }

        return response;
    }

    private void addHeadersToRequest(HttpRequestBase request, LinkedList<NameValuePair> requestHeaders) {
        for (NameValuePair nameValuePair : requestHeaders) {
            if (!nameValuePair.getName().equals("x-ms-session-token")) {
                request.addHeader(nameValuePair.getName(), nameValuePair.getValue());
            }
        }

        for (NameValuePair nameValuePair : this.defaultHeaders) {
            request.addHeader(nameValuePair.getName(), nameValuePair.getValue());
        }
    }

    private String GetMatch(DocumentServiceRequest request, OperationType resourceOperation) {
        switch (resourceOperation) {
            case Delete:
            case ExecuteJavaScript:
            case Replace:
            case Update:
                return request.getHeaders().get(HttpConstants.HttpHeaders.IF_MATCH);

            case Read:
                return request.getHeaders().get(HttpConstants.HttpHeaders.IF_NONE_MATCH);

            default:
                return null;
        }
    }
}
