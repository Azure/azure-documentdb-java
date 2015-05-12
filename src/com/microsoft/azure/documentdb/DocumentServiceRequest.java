/* 
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 */

package com.microsoft.azure.documentdb;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;

/**
 * This is core Transport/Connection agnostic request to DocumentService. It is marked internal today. If needs arises
 * for client to do no-serialized processing we can open this up to public.
 */
final class DocumentServiceRequest {

    /**
     * Creates a DocumentServiceRequest with an HttpEntity.
     *
     * @param resourceId the resource Id.
     * @param resourceType the resource type.
     * @param httpEntity the HTTP entity.
     * @param headers the request headers.
     */
    private DocumentServiceRequest(String resourceId,
                                   ResourceType resourceType,
                                   HttpEntity body,
                                   Map<String, String> headers) {
        this.resourceType = resourceType;
        this.path = null;
        this.resourceId = resourceId;

        if (resourceType == ResourceType.Media) {
            this.resourceId = getAttachmentIdFromMediaId(this.resourceId);
        }

        this.body = body;
        this.headers = headers != null ? headers : new HashMap<String, String>();
    }

    /**
     * Creates a DocumentServiceRequest with an HttpEntity.
     * 
     * @param resourceType the resource type.
     * @param relativePath the relative URI path.
     * @param httpEntity the HTTP entity.
     * @param headers the request headers.
     */
    private DocumentServiceRequest(ResourceType resourceType,
                                   String path,
                                   HttpEntity body,
                                   Map<String, String> headers) {
        this.resourceType = resourceType;
        this.path = path;
        this.resourceId = extractIdFromUri(path);

        if (resourceType == ResourceType.Media) {
            this.resourceId = getAttachmentIdFromMediaId(this.resourceId);
        }

        this.body = body;
        this.headers = headers != null ? headers : new HashMap<String, String>();
    }

    /**
     * Creates a DocumentServiceRequest with a stream.
     * 
     * @param resourceType the resource type.
     * @param relativePath the relative URI path.
     * @param stream the input stream of the request.
     * @param headers the request headers.
     * @return the created document service request.
     */
    public static DocumentServiceRequest create(ResourceType resourceType,
                                                String relativePath,
                                                InputStream stream,
                                                Map<String, String> headers) {
        HttpEntity body = new InputStreamEntity(stream, Constants.StreamApi.STREAM_LENGTH_EOF);
        return new DocumentServiceRequest(resourceType, relativePath, body, headers);
    }

    /**
     * Creates a DocumentServiceRequest with a resource.
     * 
     * @param resourceType the resource type.
     * @param relativePath the relative URI path.
     * @param resource the resource of the request.
     * @param headers the request headers.
     * @return the created document service request.
     */
    public static DocumentServiceRequest create(ResourceType resourceType,
                                                String relativePath,
                                                Resource resource,
                                                Map<String, String> headers) {
        HttpEntity body = new StringEntity(resource.toString(), StandardCharsets.UTF_8);
        return new DocumentServiceRequest(resourceType, relativePath, body, headers);
    }

    /**
     * Creates a DocumentServiceRequest with a query.
     * 
     * @param resourceType the resource type.
     * @param relativePath the relative URI path.
     * @param query the query.
     * @param headers the request headers.
     * @return the created document service request.
     */
    public static DocumentServiceRequest create(ResourceType resourceType,
                                                String relativePath,
                                                String query,
                                                Map<String, String> headers) {
        HttpEntity body = new StringEntity(query, StandardCharsets.UTF_8);
        return new DocumentServiceRequest(resourceType, relativePath, body, headers);
    }

    /**
     * Creates a DocumentServiceRequest with a query.
     * 
     * @param resourceType the resource type.
     * @param relativePath the relative URI path.
     * @param query the query.
     * @param headers the request headers.
     * @return the created document service request.
     */
    public static DocumentServiceRequest create(ResourceType resourceType,
                                                String relativePath,
                                                SqlQuerySpec querySpec,
                                                DocumentClient.QueryCompatibilityMode queryCompatibilityMode,
                                                Map<String, String> headers) {
        String queryText;
        switch (queryCompatibilityMode) {
            case SqlQuery:
                if (querySpec.getParameters() != null && querySpec.getParameters().size() > 0) {
                    throw new IllegalArgumentException(
                        String.format("Unsupported argument in query compatibility mode '{%s}'",
                                      queryCompatibilityMode.name()));
                }

                queryText = querySpec.getQueryText();
                break;

            case Default:
            case Query:
            default:
                queryText = querySpec.toString();
                break;
        }

        HttpEntity body = new StringEntity(queryText, StandardCharsets.UTF_8);
        return new DocumentServiceRequest(resourceType, relativePath, body, headers);
    }

    /**
     * Creates a DocumentServiceRequest without body.
     * 
     * @param resourceType the resource type.
     * @param relativePath the relative URI path.
     * @param headers the request headers.
     * @return the created document service request.
     */
    public static DocumentServiceRequest create(ResourceType resourceType,
                                                String relativePath,
                                                Map<String, String> headers) {
        return new DocumentServiceRequest(resourceType, relativePath, null, headers);
    }

    /**
     * Creates a DocumentServiceRequest with a resourceId.
     *
     * @param resourceId the resource id.
     * @param resourceType the resource type.
     * @param headers the request headers.
     * @return the created document service request.
     */
    public static DocumentServiceRequest create(String resourceId,
                                                ResourceType resourceType,
                                                Map<String, String> headers) {
        return new DocumentServiceRequest(resourceId, resourceType, null, headers);
    }

    static String extractIdFromUri(String path) {
        if (path.length() == 0) {
            return path;
        }
        
        if (path.charAt(path.length() - 1) != '/') {
            path = path + '/';
        }

        if (path.charAt(0) != '/') {
            path = '/' + path;
        }
        // This is a hack. We need a padding '=' so that path.split("/")
        // returns even number of string pieces.
        // TODO(pushi): Improve the code and remove the hack.
        path = path + '=';

        // The path will be in the form of 
        // /[resourceType]/[resourceId]/ or
        // /[resourceType]/[resourceId]/[resourceType]/
        // The result of split will be in the form of
        // [[[resourceType], [resourceId] ... ,[resourceType], ""]
        // In the first case, to extract the resourceId it will the element
        // before last ( at length -2 ) and the the type will before it
        // ( at length -3 )
        // In the second case, to extract the resource type it will the element
        // before last ( at length -2 )
        String[] pathParts = path.split("/");
        if (pathParts.length % 2 == 0) {
            // request in form /[resourceType]/[resourceId]/.
            return pathParts[pathParts.length - 2];
        } else {
            // request in form /[resourceType]/[resourceId]/[resourceType]/.
            return pathParts[pathParts.length - 3];
        }
    }

    static String getAttachmentIdFromMediaId(String mediaId) {
        // '/' was replaced with '-'.
        byte[] buffer = Base64.decodeBase64(mediaId.replace('-', '/').getBytes());

        final int resoureIdLength = 20;
        String attachmentId;

        if (buffer.length > resoureIdLength) {
            // We are cuting off the storage index.
            byte[] newBuffer = new byte[resoureIdLength];
            System.arraycopy(buffer, 0, newBuffer, 0, resoureIdLength);
            attachmentId = Helper.encodeBase64String(newBuffer).replace('/', '-');
        } else {
            attachmentId = mediaId;
        }

        return attachmentId;
    }
    
    private String resourceId;

    /**
     * Gets the resource id.
     * 
     * @return the resource id.
     */
    public String getResourceId() {
        return this.resourceId;
    }

    void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

    private ResourceType resourceType;

    /**
     * Gets the resource type.
     * 
     * @return the resource type.
     */
    public ResourceType getResourceType() {
        return this.resourceType;
    }

    private String path;

    /**
     * Gets the path.
     * 
     * @return the path.
     */
    public String getPath() {
        return this.path;
    }

    private String queryString;

    /**
     * Gets the query string.
     * 
     * @return the query string.
     */
    public String getQueryString() {
        return this.queryString;
    }

    /**
     * Sets the query string.
     * 
     * @param queryString the query string.
     */
    public void setQueryString(String queryString) {
        this.queryString = queryString;
    }

    private HttpEntity body;

    /**
     * Gets the HTTP entity body.
     * 
     * @return the HTTP entity body.
     */
    public HttpEntity getBody() {
        return this.body;
    }

    private Map<String, String> headers;

    /**
     * Gets the request headers.
     * 
     * @return the request headers.
     */
    public Map<String, String> getHeaders() {
        return this.headers;
    }

    private String continuation;

    /**
     * Gets the continuation.
     * 
     * @return the continuation.
     */
    public String getContinuation() {
        return this.continuation;
    }

    public void setContinuation(String continuation) {
        this.continuation = continuation;
    }

    private boolean isMedia = false;

    public void setIsMedia(boolean isMedia) {
        this.isMedia = isMedia;
    }

    public boolean getIsMedia() {
        return this.isMedia;
    }
}
