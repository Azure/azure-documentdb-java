package com.microsoft.azure.documentdb.internal.query;

import java.util.*;

import org.apache.commons.lang3.StringUtils;

import com.microsoft.azure.documentdb.*;
import com.microsoft.azure.documentdb.internal.*;
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

    public DocumentServiceResponse executeQueryRequest(DocumentServiceRequest request) throws DocumentClientException {
        return this.client.doQuery(request);
    }

    public DocumentServiceResponse executeReadFeedRequest(DocumentServiceRequest request)
            throws DocumentClientException {
        return this.client.doReadFeed(request);
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
            return null;

        Map<String, String> headers = new HashMap<String, String>();

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

        if (options.getPartitionKey() != null) {
            headers.put(HttpConstants.HttpHeaders.PARTITION_KEY, options.getPartitionKey().toString());
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

    protected DocumentServiceRequest createRequest(Map<String, String> requestHeaders, SqlQuerySpec querySpec) {
        if (querySpec == null) {
            return DocumentServiceRequest.create(OperationType.ReadFeed, this.resourceType, this.resourceLink,
                    requestHeaders);
        } else {
            return DocumentServiceRequest.create(this.resourceType, this.resourceLink, querySpec,
                    this.client.getQueryCompatiblityMode(), requestHeaders);
        }
    }
}
