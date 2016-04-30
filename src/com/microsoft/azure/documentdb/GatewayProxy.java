/*
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 */

package com.microsoft.azure.documentdb;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.client.params.CookiePolicy;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.impl.conn.ProxySelectorRoutePlanner;
import org.apache.http.impl.conn.SchemeRegistryFactory;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.util.EntityUtils;


class GatewayProxy {

    private URI serviceEndpoint;
    private Map<String, String> defaultHeaders;
    private String masterKey;
    private Map<String, String> resourceTokens;
    private ConnectionPolicy connectionPolicy;
    private HttpClient httpClient;
    private HttpClient mediaHttpClient;
    private PoolingClientConnectionManager connectionManager;
    private DocumentClient.QueryCompatibilityMode queryCompatibilityMode;

    public GatewayProxy(URI serviceEndpoint,
                        ConnectionPolicy connectionPolicy,
                        ConsistencyLevel consistencyLevel,
                        DocumentClient.QueryCompatibilityMode queryCompatibilityMode,
                        String masterKey,
                        Map<String, String> resourceTokens,
                        UserAgentContainer userAgentContainer) {
        this.serviceEndpoint = serviceEndpoint;
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
        this.queryCompatibilityMode = queryCompatibilityMode;
        this.masterKey = masterKey;
        this.resourceTokens = resourceTokens;

        // Initialize connection manager.
        this.connectionManager = new PoolingClientConnectionManager(SchemeRegistryFactory.createDefault());
        this.connectionManager.setMaxTotal(this.connectionPolicy.getMaxPoolSize());
        this.connectionManager.setDefaultMaxPerRoute(this.connectionPolicy.getMaxPoolSize());
        this.connectionManager.closeIdleConnections(this.connectionPolicy.getIdleConnectionTimeout(), TimeUnit.SECONDS);
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

    public DocumentServiceResponse doSQLQuery(DocumentServiceRequest request)
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
            if (this.mediaHttpClient == null) this.mediaHttpClient = this.createHttpClient(isForMedia);
            return this.mediaHttpClient;
        } else {
            if (this.httpClient == null) this.httpClient = this.createHttpClient(isForMedia);
            return this.httpClient;
        }
    }

    /**
     * Only one instance is created for the httpClient for optimization.
     * A PoolingClientConnectionManager is used with the Http client
     * to be able to reuse connections and execute requests concurrently.
     * A timeout for closing each connection is set so that connections don't leak.
     * A timeout is set for requests to avoid deadlocks.
     * @return the created HttpClient
     */
    private HttpClient createHttpClient(boolean isForMedia) {
        ProxySelectorRoutePlanner ps = new ProxySelectorRoutePlanner(
                SchemeRegistryFactory.createDefault(), ProxySelector.getDefault());

        DefaultHttpClient defaultHttpClient = new DefaultHttpClient(this.connectionManager);
        defaultHttpClient.setRoutePlanner(ps);
        
        HttpClient httpClient = defaultHttpClient;
        HttpParams httpParams = httpClient.getParams();
        
        // Setting Cookie policy on httpclient to ignoring server cookies as we don't use them
        httpParams.setParameter("http.protocol.single-cookie-header", true);
        httpParams.setParameter(ClientPNames.COOKIE_POLICY, CookiePolicy.IGNORE_COOKIES);

        if (isForMedia) {
            HttpConnectionParams.setConnectionTimeout(httpParams,
                                                      this.connectionPolicy.getMediaRequestTimeout() * 1000);
            HttpConnectionParams.setSoTimeout(httpParams, this.connectionPolicy.getMediaRequestTimeout() * 1000);
        } else {
            HttpConnectionParams.setConnectionTimeout(httpParams, this.connectionPolicy.getRequestTimeout() * 1000);
            HttpConnectionParams.setSoTimeout(httpParams, this.connectionPolicy.getRequestTimeout() * 1000);
        }

        return httpClient;
    }

    private void putMoreContentIntoDocumentServiceRequest(
        DocumentServiceRequest request,
        String httpMethod) {
        if (this.masterKey != null) {
            final Date currentTime = new Date();
            final SimpleDateFormat sdf =
                new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);
            sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
            String xDate = sdf.format(currentTime);

            request.getHeaders().put(HttpConstants.HttpHeaders.X_DATE, xDate);
        }

        if (this.masterKey != null || this.resourceTokens != null) {
            String path = request.getPath();
            path = Utils.trimBeginingAndEndingSlashes(path);
            
            String resourceName = "";
            
            if(Utils.isNameBased(path)) {
                String[] segments = path.split("/");
                
                if (segments.length % 2 == 0) {
                    // if path has even segments, it is the individual resource like dbs/db1/colls/coll1
                    if (Utils.IsResourceType(segments[segments.length-2])) {
                        resourceName = path;
                    }
                }
                else {
                    // if path has odd segments, get the parent(dbs/db1 from dbs/db1/colls)
                    if (Utils.IsResourceType(segments[segments.length - 1])) {
                        resourceName = path.substring(0, path.lastIndexOf("/"));
                    }
                }
            }
            else {
                resourceName = request.getResourceId().toLowerCase();
            }
            
            String authorization =
                this.getAuthorizationToken(resourceName,
                                           request.getPath(),
                                           request.getResourceType(),
                                           httpMethod,
                                           request.getHeaders(),
                                           this.masterKey,
                                           this.resourceTokens);
            try {
                authorization = URLEncoder.encode(authorization, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new IllegalStateException("Failed to encode authtoken.", e);
            }
            request.getHeaders().put(HttpConstants.HttpHeaders.AUTHORIZATION,
                                     authorization);
        }

        if ((httpMethod == HttpConstants.HttpMethods.POST ||
             httpMethod ==  HttpConstants.HttpMethods.PUT) &&
            !request.getHeaders().containsKey(
                HttpConstants.HttpHeaders.CONTENT_TYPE)) {
            request.getHeaders().put(HttpConstants.HttpHeaders.CONTENT_TYPE,
                                     RuntimeConstants.MediaTypes.JSON);
        }

        if (!request.getHeaders().containsKey(
                HttpConstants.HttpHeaders.ACCEPT)) {
            request.getHeaders().put(HttpConstants.HttpHeaders.ACCEPT,
                                     RuntimeConstants.MediaTypes.JSON);
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

    private void maybeThrowException(HttpResponse response) throws DocumentClientException {
        int statusCode = response.getStatusLine().getStatusCode();

        if (statusCode >= HttpConstants.StatusCodes.MINIMUM_STATUSCODE_AS_ERROR_GATEWAY) {
            HttpEntity httpEntity = response.getEntity();
            String body = "";
            if (httpEntity != null) {
                try {
                    body = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                    EntityUtils.consume(response.getEntity());
                } catch (ParseException | IOException e) {
                    // body is empty.
                    throw new IllegalStateException("Failed to get content from the http response", e);
                }
            }

            Map<String, String> responseHeaders = new HashMap<String, String>();
            for (Header header : response.getAllHeaders()) {
                responseHeaders.put(header.getName(), header.getValue());
            }

            throw new DocumentClientException(statusCode, new Error(body), responseHeaders);
        }
    }

    private DocumentServiceResponse performDeleteRequest(
            DocumentServiceRequest request) throws DocumentClientException {
        putMoreContentIntoDocumentServiceRequest(
            request,
            HttpConstants.HttpMethods.DELETE);
        URI uri;
        try {
            uri = new URI("https",
                          null,
                          this.serviceEndpoint.getHost(),
                          this.serviceEndpoint.getPort(),
                          request.getPath(),
                          null,  // Query string not used.
                          null);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Incorrect uri from request.",
                                               e);
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

        this.maybeThrowException(response);

        // No content in delete request, we can release the connection directly;
        httpDelete.releaseConnection();
        return new DocumentServiceResponse(response);
    }

    private DocumentServiceResponse performGetRequest(DocumentServiceRequest request) throws DocumentClientException {
        putMoreContentIntoDocumentServiceRequest(request,
                                                 HttpConstants.HttpMethods.GET);
        URI uri;
        try {
            uri = new URI("https",
                          null,
                          this.serviceEndpoint.getHost(),
                          this.serviceEndpoint.getPort(),
                          request.getPath(),
                          null,  // Query string not used.
                          null);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Incorrect uri from request.",
                                               e);
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

        this.maybeThrowException(response);
        return new DocumentServiceResponse(response);
    }

    private DocumentServiceResponse performPostRequest(DocumentServiceRequest request) throws DocumentClientException {
        putMoreContentIntoDocumentServiceRequest(
            request,
            HttpConstants.HttpMethods.POST);
        URI uri;
        try {
            uri = new URI("https",
                          null,
                          this.serviceEndpoint.getHost(),
                          this.serviceEndpoint.getPort(),
                          request.getPath(),
                          null,  // Query string not used.
                          null);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Incorrect uri from request.",
                                               e);
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

        this.maybeThrowException(response);
        return new DocumentServiceResponse(response);
    }

    private DocumentServiceResponse performPutRequest(DocumentServiceRequest request) throws DocumentClientException {
        putMoreContentIntoDocumentServiceRequest(request,
                                                 HttpConstants.HttpMethods.PUT);
        URI uri;
        try {
            uri = new URI("https",
                          null,
                          this.serviceEndpoint.getHost(),
                          this.serviceEndpoint.getPort(),
                          request.getPath(),
                          null,  // Query string not used.
                          null);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Incorrect uri from request.",
                                               e);
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

        this.maybeThrowException(response);
        return new DocumentServiceResponse(response);
    }

    private String getAuthorizationToken(String resourceOrOwnerId,
                                         String path,
                                         ResourceType resourceType,
                                         String requestVerb,
                                         Map<String, String> headers,
                                         String masterKey,
                                         Map<String, String> resourceTokens) {
        if (masterKey != null) {
            return AuthorizationHelper.GenerateKeyAuthorizationSignature(
                requestVerb,
                resourceOrOwnerId,
                resourceType,
                headers,
                masterKey);
        } else if (resourceTokens != null) {
            return AuthorizationHelper.GetAuthorizationTokenUsingResourceTokens(
                resourceTokens, path, resourceOrOwnerId);
        }

        return null;
    }
}
