package com.microsoft.azure.documentdb.internal.query;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.DocumentQueryClient;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.PartitionKeyRange;
import com.microsoft.azure.documentdb.Resource;
import com.microsoft.azure.documentdb.SqlQuerySpec;
import com.microsoft.azure.documentdb.internal.DocumentServiceRequest;
import com.microsoft.azure.documentdb.internal.DocumentServiceResponse;
import com.microsoft.azure.documentdb.internal.HttpConstants;
import com.microsoft.azure.documentdb.internal.OperationType;
import com.microsoft.azure.documentdb.internal.ResourceType;
import com.microsoft.azure.documentdb.internal.routing.PartitionKeyInternal;
import com.microsoft.azure.documentdb.internal.routing.PartitionKeyRangeIdentity;
import com.microsoft.azure.documentdb.internal.routing.Range;
import com.microsoft.azure.documentdb.internal.routing.RoutingMapProviderHelper;

abstract class AbstractQueryExecutionContext<T extends Resource> implements QueryExecutionContext<T> {
    protected final DocumentQueryClient client;
    protected final ResourceType resourceType;
    protected final Class<T> classT;
    protected final SqlQuerySpec querySpec;
    protected final FeedOptions options;
    protected final String resourceLink;

    protected Map<String, String> responseHeaders;

    protected AbstractQueryExecutionContext(DocumentQueryClient client, ResourceType resourceType, Class<T> classT,
            SqlQuerySpec querySpec, FeedOptions options, String resourceLink) {
        this.client = client;
        this.resourceType = resourceType;
        this.classT = classT;
        this.querySpec = querySpec;
        this.options = options;
        this.resourceLink = resourceLink;
        this.responseHeaders = null;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove");
    }

    @Override
    public Map<String, String> getResponseHeaders() {
        return this.responseHeaders;
    }

    protected boolean hasNextInternal() {
        return this.responseHeaders == null || !StringUtils.isEmpty(this.getContinuationToken());
    }

    public boolean shouldExecuteQuery() {
        return this.querySpec != null;
    }

    protected String getContinuationToken() {
        return this.responseHeaders == null ? null : this.responseHeaders.get(HttpConstants.HttpHeaders.CONTINUATION);
    }

    protected Collection<PartitionKeyRange> getTargetPartitionKeyRanges(List<Range<String>> providedRanges) {
        return RoutingMapProviderHelper.getOverlappingRanges(this.client.getPartitionKeyRangeCache(), this.resourceLink,
                providedRanges);
    }

    protected Map<String, String> getFeedHeaders(FeedOptions options) {
        if (options == null)
            return new HashMap<>();

        Map<String, String> headers = new HashMap<>();

        if (options.getPageSize() != null) {
            headers.put(HttpConstants.HttpHeaders.PAGE_SIZE, options.getPageSize().toString());
        }

        if (options.getRequestContinuation() != null) {
            headers.put(HttpConstants.HttpHeaders.CONTINUATION, options.getRequestContinuation());
        }

        if (options.getSessionToken() != null) {
            headers.put(HttpConstants.HttpHeaders.SESSION_TOKEN, options.getSessionToken());
        }

        if (options.getEnableScanInQuery() != null) {
            headers.put(HttpConstants.HttpHeaders.ENABLE_SCAN_IN_QUERY, options.getEnableScanInQuery().toString());
        }

        if (options.getEmitVerboseTracesInQuery() != null) {
            headers.put(HttpConstants.HttpHeaders.EMIT_VERBOSE_TRACES_IN_QUERY,
                    options.getEmitVerboseTracesInQuery().toString());
        }

        if (options.getEnableCrossPartitionQuery() != null) {
            headers.put(HttpConstants.HttpHeaders.ENABLE_CROSS_PARTITION_QUERY,
                    options.getEnableCrossPartitionQuery().toString());
        }

        if (options.getMaxDegreeOfParallelism() != 0) {
            headers.put(HttpConstants.HttpHeaders.PARALLELIZE_CROSS_PARTITION_QUERY, Boolean.TRUE.toString());
        }

        return headers;
    }

    private void populatePartitionKeyInfo(DocumentServiceRequest request, PartitionKeyInternal partitionKey) {
        if (request == null) {
            throw new IllegalArgumentException("request");
        }

        if (request.getResourceType().isPartitioned()) {
            if (partitionKey != null) {
                request.getHeaders().put(HttpConstants.HttpHeaders.PARTITION_KEY, partitionKey.toString());
            }
        }
    }

    private void populatePartitionKeyRangeInfo(DocumentServiceRequest request, PartitionKeyRange range) {
        if (request == null) {
            throw new IllegalArgumentException("request");
        }

        if (range == null) {
            throw new IllegalArgumentException("range");
        }

        DocumentCollection collection = this.client.getCollectionCache().resolveCollection(request);

        if (request.getResourceType().isPartitioned()) {
            request.routeTo(new PartitionKeyRangeIdentity(collection.getResourceId(), range.getId()));
        }
    }

    DocumentServiceRequest createRequest(SqlQuerySpec querySpec, PartitionKeyInternal partitionKeyInternal) {
        Map<String, String> requestHeaders = this.getFeedHeaders(options);

        DocumentServiceRequest request;

        if (querySpec == null) {
            request = DocumentServiceRequest.create(
                    OperationType.ReadFeed, this.resourceType, this.resourceLink, requestHeaders);
        } else {
            request = DocumentServiceRequest.create(this.resourceType, this.resourceLink, querySpec,
                    this.client.getQueryCompatiblityMode(), requestHeaders);
        }

        this.populatePartitionKeyInfo(request, partitionKeyInternal);

        return request;
    }

    DocumentServiceRequest createRequest(SqlQuerySpec querySpec, PartitionKeyRange targetRange) {
        Map<String, String> requestHeaders = this.getFeedHeaders(options);

        DocumentServiceRequest request;

        if (querySpec == null) {
            request = DocumentServiceRequest.create(
                    OperationType.ReadFeed, this.resourceType, this.resourceLink, requestHeaders);
        } else {
            request = DocumentServiceRequest.create(this.resourceType, this.resourceLink, querySpec,
                    this.client.getQueryCompatiblityMode(), requestHeaders);
        }

        this.populatePartitionKeyRangeInfo(request, targetRange);

        return request;
    }

    PartitionKeyInternal getPartitionKeyInternal() {
        return this.options.getPartitionKey() == null ? null : this.options.getPartitionKey().getInternalPartitionKey();
    }

    DocumentServiceResponse executeRequest(DocumentServiceRequest request) throws DocumentClientException {
        return this.shouldExecuteQuery()
                ? this.client.doQuery(request)
                : this.client.doReadFeed(request);
    }
}
