/*
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 */

package com.microsoft.azure.documentdb.internal;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.directconnectivity.HttpClientFactory;

public class GatewayProxy implements StoreModel {

    private final Logger logger;
    private Map<String, String> defaultHeaders;
    private ConnectionPolicy connectionPolicy;
    private HttpClient httpClient;
    private HttpClient mediaHttpClient;
    private PoolingHttpClientConnectionManager connectionManager;
    private QueryCompatibilityMode queryCompatibilityMode;
    private EndpointManager globalEndpointManager;

    public GatewayProxy(ConnectionPolicy connectionPolicy,
                        ConsistencyLevel consistencyLevel,
                        QueryCompatibilityMode queryCompatibilityMode,
                        String masterKey,
                        Map<String, String> resourceTokens,
                        UserAgentContainer userAgentContainer,
                        EndpointManager globalEndpointManager) {
        this.logger = Logger.getLogger(this.getClass().getPackage().getName());
        this.defaultHeaders = new HashMap<String, String>();
        this.defaultHeaders.put(HttpConstants.HttpHeaders.CACHE_CONTROL,
                                "no-cache");
        this.defaultHeaders.put(HttpConstants.HttpHeaders.VERSION,
                                HttpConstants.Versions.CURRENT_VERSION);

        if (userAgentContainer == null) {
            userAgentContainer = new UserAgentContainer();
        }

        this.defaultHeaders.put(HttpConstants.HttpHeaders.USER_AGENT, userAgentContainer.getUserAgent());

        if (consistencyLevel != null) {
            this.defaultHeaders.put(HttpConstants.HttpHeaders.CONSISTENCY_LEVEL,
                                    consistencyLevel.toString());
        }

        this.connectionPolicy = connectionPolicy;        
        this.globalEndpointManager = globalEndpointManager;
        this.queryCompatibilityMode = queryCompatibilityMode;

        // Initialize connection manager.
        this.connectionManager = HttpClientFactory.createConnectionManager(this.connectionPolicy.getMaxPoolSize(), this.connectionPolicy.getIdleConnectionTimeout());
    }

    public DocumentServiceResponse doCreate(DocumentServiceRequest request)
        throws DocumentClientException {
        return this.performPostRequest(request);
    }
    
    public DocumentServiceResponse doUpsert(DocumentServiceRequest request)
        throws DocumentClientException {
        return this.performPostRequest(request);
    }

    public DocumentServiceResponse doRead(DocumentServiceRequest request)
        throws DocumentClientException {
        return this.performGetRequest(request);
    }
    
    public DocumentServiceResponse doReplace(DocumentServiceRequest request)
        throws DocumentClientException {
        return this.performPutRequest(request);
    }

    public DocumentServiceResponse doDelete(DocumentServiceRequest request)
        throws DocumentClientException {
        return this.performDeleteRequest(request);
    }

    public DocumentServiceResponse doExecute(DocumentServiceRequest request)
        throws DocumentClientException {
        return this.performPostRequest(request);
    }

    public DocumentServiceResponse doReadFeed(DocumentServiceRequest request)
        throws DocumentClientException {
        return this.performGetRequest(request);
    }

    public DocumentServiceResponse doQuery(DocumentServiceRequest request)
        throws DocumentClientException {
        request.getHeaders().put(HttpConstants.HttpHeaders.IS_QUERY, "true");

        switch (this.queryCompatibilityMode) {
            case SqlQuery:
                request.getHeaders().put(HttpConstants.HttpHeaders.CONTENT_TYPE,
                                         RuntimeConstants.MediaTypes.SQL);
                break;
            case Default:
            case Query:
            default:
                request.getHeaders().put(HttpConstants.HttpHeaders.CONTENT_TYPE,
                                         RuntimeConstants.MediaTypes.QUERY_JSON);
                break;
        }

        return this.performPostRequest(request);
    }

    private HttpClient getHttpClient(boolean isForMedia) {
        if (isForMedia) {
            if (this.mediaHttpClient == null)
                this.mediaHttpClient = HttpClientFactory.createHttpClient(this.connectionManager, this.connectionPolicy.getMediaRequestTimeout());
            return this.mediaHttpClient;
        } else {
            if (this.httpClient == null)
                this.httpClient = HttpClientFactory.createHttpClient(this.connectionManager, this.connectionPolicy.getRequestTimeout());
            return this.httpClient;
        }
    }

    private void fillHttpRequestBaseWithHeaders(Map<String, String> headers, HttpRequestBase httpBase) {
        // Add default headers.
        for (Map.Entry<String, String> entry : this.defaultHeaders.entrySet()) {
            httpBase.setHeader(entry.getKey(), entry.getValue());
        }
        // Add override headers.
        if (headers != null) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                httpBase.setHeader(entry.getKey(), entry.getValue());
            }
        }
    }

    private DocumentServiceResponse performDeleteRequest(DocumentServiceRequest request) 
            throws DocumentClientException {
        URI rootUri = request.getEndpointOverride();
        if (rootUri == null) {
            rootUri = this.globalEndpointManager.resolveServiceEndpoint(request.getOperationType());
        }
        
        URI uri;
        try {
            uri = new URI("https",
                          null,
                          rootUri.getHost(),
                          rootUri.getPort(),
                          request.getPath(),
                          null,  // Query string not used.
                          null);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Incorrect uri from request.", e);
        }
        
        HttpDelete httpDelete = new HttpDelete(uri);
        this.fillHttpRequestBaseWithHeaders(request.getHeaders(), httpDelete);
        HttpResponse response = null;
        try {
            response = this.getHttpClient(request.getIsMedia()).execute(httpDelete);
        } catch (IOException e) {
            httpDelete.releaseConnection();
            throw new IllegalStateException("Http client execution failed.", e);
        }

        ErrorUtils.maybeThrowException(uri.getPath(), response, true, this.logger);

        // No content in delete request, we can release the connection directly;
        httpDelete.releaseConnection();
        return new DocumentServiceResponse(response);
    }

    DocumentServiceResponse performGetRequest(DocumentServiceRequest request)
            throws DocumentClientException {
        URI rootUri = request.getEndpointOverride();
        if (rootUri == null) {
            if (request.getIsMedia()) {
                // For media read request, always use the write endpoint.
                rootUri = this.globalEndpointManager.getWriteEndpoint();
            } else {
                rootUri = this.globalEndpointManager.resolveServiceEndpoint(request.getOperationType());
            }
        }
        
        URI uri;
        try {
            uri = new URI("https",
                          null,
                          rootUri.getHost(),
                          rootUri.getPort(),
                          request.getPath(),
                          null,  // Query string not used.
                          null);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Incorrect uri from request.", e);
        }
        
        HttpGet httpGet = new HttpGet(uri);
        this.fillHttpRequestBaseWithHeaders(request.getHeaders(), httpGet);
        HttpResponse response = null;
        try {
            response = this.getHttpClient(request.getIsMedia()).execute(httpGet);
        } catch (IOException e) {
            httpGet.releaseConnection();
            throw new IllegalStateException("Http client execution failed.", e);
        }

        ErrorUtils.maybeThrowException(uri.getPath(), response, true, this.logger);
        return new DocumentServiceResponse(response);
    }

    DocumentServiceResponse performPostRequest(DocumentServiceRequest request)
            throws DocumentClientException {
        URI rootUri = request.getEndpointOverride();
        if (rootUri == null) {
            rootUri = this.globalEndpointManager.resolveServiceEndpoint(request.getOperationType());
        }
        
        URI uri;
        try {
            uri = new URI("https",
                          null,
                          rootUri.getHost(),
                          rootUri.getPort(),
                          request.getPath(),
                          null,  // Query string not used.
                          null);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Incorrect uri from request.", e);
        }

        HttpPost httpPost = new HttpPost(uri);
        this.fillHttpRequestBaseWithHeaders(request.getHeaders(), httpPost);
        httpPost.setEntity(request.getBody());
        HttpResponse response = null;
        try {
            response = this.getHttpClient(request.getIsMedia()).execute(httpPost);
        } catch (IOException e) {
            httpPost.releaseConnection();
            throw new IllegalStateException("Http client execution failed.", e);
        }

        ErrorUtils.maybeThrowException(uri.getPath(), response, true, this.logger);
        return new DocumentServiceResponse(response);
    }

    DocumentServiceResponse performPutRequest(DocumentServiceRequest request) 
            throws DocumentClientException {
        
        URI rootUri = request.getEndpointOverride();
        if (rootUri == null) {
            rootUri = this.globalEndpointManager.resolveServiceEndpoint(request.getOperationType());
        }
        
        URI uri;
        try {
            uri = new URI("https",
                          null,
                          rootUri.getHost(),
                          rootUri.getPort(),
                          request.getPath(),
                          null,  // Query string not used.
                          null);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Incorrect uri from request.", e);
        }
        
        HttpPut httpPut = new HttpPut(uri);
        this.fillHttpRequestBaseWithHeaders(request.getHeaders(), httpPut);
        httpPut.setEntity(request.getBody());
        HttpResponse response = null;
        try {
            httpPut.releaseConnection();
            response = this.getHttpClient(request.getIsMedia()).execute(httpPut);
        } catch (IOException e) {
            throw new IllegalStateException("Http client execution failed.", e);
        }

        ErrorUtils.maybeThrowException(uri.getPath(), response, true, this.logger);
        return new DocumentServiceResponse(response);
    }

	@Override
	public DocumentServiceResponse processMessage(DocumentServiceRequest request) throws DocumentClientException {
        switch (request.getOperationType()) {
			case Create:
				return this.doCreate(request);
			case Upsert:
				return this.doUpsert(request);
			case Delete:
				return this.doDelete(request);
			case ExecuteJavaScript:
				return this.doExecute(request);
			case Read:
				return this.doRead(request);
			case ReadFeed:
				return this.doReadFeed(request);
			case Replace:
				return this.doReplace(request);
			case SqlQuery:
			case Query:
				return this.doQuery(request);
		default:
			throw new IllegalStateException("Unknown operation type " + request.getOperationType());
        }
    }
}
