/* 
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 */

package com.microsoft.azure.documentdb.internal;

import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;

import com.microsoft.azure.documentdb.Resource;
import com.microsoft.azure.documentdb.SqlQuerySpec;
import com.microsoft.azure.documentdb.directconnectivity.StoreReadResult;
import com.microsoft.azure.documentdb.internal.routing.PartitionKeyRangeIdentity;

/**
 * This is core Transport/Connection agnostic request to DocumentService. It is marked internal today. If needs arises
 * for client to do no-serialized processing we can open this up to public.
 */
public final class DocumentServiceRequest {

    private String resourceId;
    private ResourceType resourceType;
    private String path;
    private String queryString;
    private HttpEntity body;
    private Map<String, String> headers;
    private String continuation;
    private boolean isMedia = false;
    private boolean isNameBased = false;
    private OperationType operationType;
    private String resourceAddress;
    private boolean forceNameCacheRefresh;
    private boolean forceAddressRefresh;
    private long sessionLsn;
    private URI endpointOverride = null;
    private String activityId;
    private RequestChargeTracker requestChargeTracker;
    private String resourceFullName;
    private StoreReadResult quorumSelectedStoreResponse;
    private long quorumSelectedLSN;
    private String resolvedCollectionRid;
    private PartitionKeyRangeIdentity partitionKeyRangeIdentity;
    private String resolvedPartitionKeyRangeId;

    /**
     * Creates a DocumentServiceRequest with an HttpEntity.
     *
     * @param resourceId   the resource Id.
     * @param resourceType the resource type.
     * @param body         the HTTP entity.
     * @param headers      the request headers.
     */
    private DocumentServiceRequest(OperationType operationType,
            String resourceId,
            ResourceType resourceType,
            HttpEntity body,
            String path,
            Map<String, String> headers) {
        this.operationType = operationType;
        this.resourceType = resourceType;
        this.path = path;
        this.sessionLsn = -1;
        this.body = body;
        this.headers = headers != null ? headers : new HashMap<String, String>();
        this.isNameBased = Utils.isNameBased(path);
        this.activityId = UUID.randomUUID().toString();
        if (!this.isNameBased) {
            if (resourceType == ResourceType.Media) {
                this.resourceId = getAttachmentIdFromMediaId(resourceId);
            } else {
                this.resourceId = resourceId;
            }

            this.resourceAddress = resourceId;
        } else {
            this.resourceAddress = this.path;
        }
    }

    /**
     * Creates a DocumentServiceRequest with an HttpEntity.
     *
     * @param resourceType the resource type.
     * @param path         the relative URI path.
     * @param body         the HTTP entity.
     * @param headers      the request headers.
     */
    private DocumentServiceRequest(OperationType operationType,
            ResourceType resourceType,
            String path,
            HttpEntity body,
            Map<String, String> headers) {
        this(operationType, extractIdFromUri(path), resourceType, body, path, headers);
    }

    /**
     * Creates a DocumentServiceRequest with a stream.
     *
     * @param operation    the operation type.
     * @param resourceType the resource type.
     * @param relativePath the relative URI path.
     * @param stream       the input stream of the request.
     * @param headers      the request headers.
     * @return the created document service request.
     */
    public static DocumentServiceRequest create(OperationType operation,
            ResourceType resourceType,
            String relativePath,
            InputStream stream,
            Map<String, String> headers) {
        HttpEntity body = new InputStreamEntity(stream, InternalConstants.StreamApi.STREAM_LENGTH_EOF);
        return new DocumentServiceRequest(operation, resourceType, relativePath, body, headers);
    }

    /**
     * Creates a DocumentServiceRequest with a resource.
     *
     * @param operation    the operation type.
     * @param resourceType the resource type.
     * @param relativePath the relative URI path.
     * @param resource     the resource of the request.
     * @param headers      the request headers.
     * @return the created document service request.
     */
    public static DocumentServiceRequest create(OperationType operation,
            ResourceType resourceType,
            String relativePath,
            Resource resource,
            Map<String, String> headers) {
        HttpEntity body = new StringEntity(resource.toString(), StandardCharsets.UTF_8);
        return new DocumentServiceRequest(operation, resourceType, relativePath, body, headers);
    }

    /**
     * Creates a DocumentServiceRequest with a query.
     *
     * @param operation    the operation type.
     * @param resourceType the resource type.
     * @param relativePath the relative URI path.
     * @param query        the query.
     * @param headers      the request headers.
     * @return the created document service request.
     */
    public static DocumentServiceRequest create(OperationType operation,
            ResourceType resourceType,
            String relativePath,
            String query,
            Map<String, String> headers) {
        HttpEntity body = new StringEntity(query, StandardCharsets.UTF_8);
        return new DocumentServiceRequest(operation, resourceType, relativePath, body, headers);
    }

    /**
     * Creates a DocumentServiceRequest with a query.
     *
     * @param resourceType           the resource type.
     * @param relativePath           the relative URI path.
     * @param querySpec              the query.
     * @param queryCompatibilityMode the QueryCompatibilityMode mode.
     * @param headers                the request headers.
     * @return the created document service request.
     */
    public static DocumentServiceRequest create(ResourceType resourceType,
            String relativePath,
            SqlQuerySpec querySpec,
            QueryCompatibilityMode queryCompatibilityMode,
            Map<String, String> headers) {
        OperationType operation;
        String queryText;
        switch (queryCompatibilityMode) {
        case SqlQuery:
            if (querySpec.getParameters() != null && querySpec.getParameters().size() > 0) {
                throw new IllegalArgumentException(
                        String.format("Unsupported argument in query compatibility mode '{%s}'",
                                queryCompatibilityMode.name()));
            }

            operation = OperationType.SqlQuery;
            queryText = querySpec.getQueryText();
            break;

        case Default:
        case Query:
        default:
            operation = OperationType.Query;
            queryText = querySpec.toString();
            break;
        }

        HttpEntity body = new StringEntity(queryText, StandardCharsets.UTF_8);
        return new DocumentServiceRequest(operation, resourceType, relativePath, body, headers);
    }

    /**
     * Creates a DocumentServiceRequest without body.
     *
     * @param operation    the operation type.
     * @param resourceType the resource type.
     * @param relativePath the relative URI path.
     * @param headers      the request headers.
     * @return the created document service request.
     */
    public static DocumentServiceRequest create(OperationType operation,
            ResourceType resourceType,
            String relativePath,
            Map<String, String> headers) {
        return new DocumentServiceRequest(operation, resourceType, relativePath, null, headers);
    }

    /**
     * Creates a DocumentServiceRequest with a resourceId.
     *
     * @param operation    the operation type.
     * @param resourceId   the resource id.
     * @param resourceType the resource type.
     * @param headers      the request headers.
     * @return the created document service request.
     */
    public static DocumentServiceRequest create(OperationType operation,
            String resourceId,
            ResourceType resourceType,
            Map<String, String> headers) {
        String path = PathsHelper.generatePath(resourceType, resourceId, Utils.isFeedRequest(operation));
        return new DocumentServiceRequest(operation, resourceId, resourceType, null, path, headers);
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
            attachmentId = Utils.encodeBase64String(newBuffer).replace('/', '-');
        } else {
            attachmentId = mediaId;
        }

        return attachmentId;
    }

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

    /**
     * Gets the resource type.
     *
     * @return the resource type.
     */
    public ResourceType getResourceType() {
        return this.resourceType;
    }

    /**
     * Gets the path.
     *
     * @return the path.
     */
    public String getPath() {
        return this.path;
    }

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

    /**
     * Gets the HTTP entity body.
     *
     * @return the HTTP entity body.
     */
    public HttpEntity getBody() {
        return this.body;
    }

    /**
     * Gets the request headers.
     *
     * @return the request headers.
     */
    public Map<String, String> getHeaders() {
        return this.headers;
    }

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

    public boolean getIsMedia() {
        return this.isMedia;
    }

    public void setIsMedia(boolean isMedia) {
        this.isMedia = isMedia;
    }

    public boolean getIsNameBased() {
        return this.isNameBased;
    }

    void setIsNameBased(boolean isNameBased) {
        this.isNameBased = isNameBased;
    }

    public OperationType getOperationType() {
        return this.operationType;
    }

    void setOperationType(OperationType operationType) {
        this.operationType = operationType;
    }

    public String getResourceAddress() {
        return resourceAddress;
    }

    public void setResourceAddress(String resourceAddress) {
        this.resourceAddress = resourceAddress;
    }

    public boolean isForceNameCacheRefresh() {
        return forceNameCacheRefresh;
    }

    public void setForceNameCacheRefresh(boolean forceNameCacheRefresh) {
        this.forceNameCacheRefresh = forceNameCacheRefresh;
    }

    public boolean isForceAddressRefresh() {
        return forceAddressRefresh;
    }

    public void setForceAddressRefresh(boolean forceAddressRefresh) {
        this.forceAddressRefresh = forceAddressRefresh;
    }

    public long getSessionLsn() {
        return this.sessionLsn;
    }

    public void setSessionLsn(long sessionLsn) {
        this.sessionLsn = sessionLsn;
    }

    public URI getEndpointOverride() {
        return this.endpointOverride;
    }

    public void setEndpointOverride(URI endpointOverride) {
        this.endpointOverride = endpointOverride;
    }

    public String getActivityId() {
        return this.activityId;
    }

    public RequestChargeTracker getRequestChargeTracker() {
        return requestChargeTracker;
    }

    public void setRequestChargeTracker(RequestChargeTracker requestChargeTracker) {
        this.requestChargeTracker = requestChargeTracker;
    }

    public String getResourceFullName() {
        if (this.isNameBased) {
            String trimmedPath = Utils.trimBeginingAndEndingSlashes(this.path);
            String[] segments = trimmedPath.split("/");

            if (segments.length % 2 == 0) {
                // if path has even segments, it is the individual resource
                // like dbs/db1/colls/coll1
                if (Utils.IsResourceType(segments[segments.length - 2])) {
                    this.resourceFullName = trimmedPath;
                }
            } else {
                // if path has odd segments, get the parent(dbs/db1 from
                // dbs/db1/colls)
                if (Utils.IsResourceType(segments[segments.length - 1])) {
                    this.resourceFullName = trimmedPath.substring(0, trimmedPath.lastIndexOf("/"));
                }
            }
        } else {
            this.resourceFullName = this.getResourceId().toLowerCase();
        }

        return this.resourceFullName;
    }

    public String getResolvedCollectionRid() {
        return resolvedCollectionRid;
    }

    public void setResolvedCollectionRid(String resolvedCollectionRid) {
        this.resolvedCollectionRid = resolvedCollectionRid;
    }

    public PartitionKeyRangeIdentity getPartitionKeyRangeIdentity() {
        return partitionKeyRangeIdentity;
    }

    public void routeTo(PartitionKeyRangeIdentity partitionKeyRangeIdentity) {
        this.setPartitionKeyRangeIdentity(partitionKeyRangeIdentity);
    }

    private void setPartitionKeyRangeIdentity(PartitionKeyRangeIdentity partitionKeyRangeIdentity) {
        this.partitionKeyRangeIdentity = partitionKeyRangeIdentity;
        if (partitionKeyRangeIdentity != null) {
            this.headers.put(HttpConstants.HttpHeaders.PARTITION_KEY_RANGE_ID, partitionKeyRangeIdentity.toHeader());
        } else {
            this.headers.remove(HttpConstants.HttpHeaders.PARTITION_KEY_RANGE_ID);
        }
    }

    public String getResolvedPartitionKeyRangeId() {
        return resolvedPartitionKeyRangeId;
    }

    public void setResolvedPartitionKeyRangeId(String resolvedPartitionKeyRangeId) {
        this.resolvedPartitionKeyRangeId = resolvedPartitionKeyRangeId;
    }

    public long getQuorumSelectedLSN() {
        return quorumSelectedLSN;
    }

    public void setQuorumSelectedLSN(long quorumSelectedLSN) {
        this.quorumSelectedLSN = quorumSelectedLSN;
    }

    public StoreReadResult getQuorumSelectedStoreResponse() {
        return quorumSelectedStoreResponse;
    }

    public void setQuorumSelectedStoreResponse(StoreReadResult quorumSelectedStoreResponse) {
        this.quorumSelectedStoreResponse = quorumSelectedStoreResponse;
    }
}
