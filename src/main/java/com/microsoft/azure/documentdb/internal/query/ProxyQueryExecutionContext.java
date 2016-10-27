package com.microsoft.azure.documentdb.internal.query;

import java.util.*;

import com.microsoft.azure.documentdb.*;
import com.microsoft.azure.documentdb.internal.*;
import com.microsoft.azure.documentdb.internal.routing.CollectionCache;

final class ProxyQueryExecutionContext<T extends Resource> implements QueryExecutionContext<T> {
    private final DocumentQueryClient client;
    private final ResourceType resourceType;
    private final Class<T> classT;
    private final SqlQuerySpec querySpec;
    private final FeedOptions options;
    private final String resourceLink;
    private final QueryExecutionContext<T> queryExecutionContext;
    private T prefetchedResource;
    private boolean hasPrefetchedResource;

    @SuppressWarnings("unchecked")
    public ProxyQueryExecutionContext(DocumentQueryClient client, ResourceType resourceType, Class<T> classT,
            SqlQuerySpec querySpec, FeedOptions options, String resourceLink) {
        this.client = client;
        this.resourceType = resourceType;
        this.classT = classT;
        this.querySpec = querySpec;
        this.options = options;
        this.resourceLink = resourceLink;

        QueryExecutionContext<T> currentQueryExecutionContext = null;

        // Prefer to get query execution information from ServiceJNI if it's available.
        // Otherwise, fallback to Gateway as usual.
        // Keep the query execution info to use it in DefaultQueryExecutionContext
        // in case query is not executed with ParallelQueryExecutionContext
        PartitionedQueryExecutionInfo partitionedQueryExecutionInfo = null;
        if (ServiceJNIWrapper.isServiceJNIAvailable()
                && Utils.isCollectionChild(resourceType) && resourceType.isPartitioned()
                && options != null
                && options.getEnableCrossPartitionQuery() != null
                && options.getEnableCrossPartitionQuery().booleanValue()) {

            DocumentCollection collection = null;
            if (Utils.isCollectionChild(resourceType)) {
                DocumentServiceRequest request = DocumentServiceRequest.create(
                        OperationType.Query, resourceType, resourceLink, null);
                CollectionCache collectionCache = this.client.getCollectionCache();
                collection = collectionCache.resolveCollection(request);
            }

            QueryPartitionProvider queryPartitionProvider = this.client.getQueryPartitionProvider();
            partitionedQueryExecutionInfo = queryPartitionProvider.getPartitionQueryExcecutionInfo(
                    querySpec, collection.getPartitionKey());

            if (shouldCreateSpecializedDocumentQueryExecutionContext(resourceType, options, partitionedQueryExecutionInfo)) {
                currentQueryExecutionContext = (QueryExecutionContext<T>) new PipelinedQueryExecutionContext(this.client,
                        this.querySpec, this.options, this.resourceLink, partitionedQueryExecutionInfo);
            }
        }

        if (currentQueryExecutionContext == null) {
            currentQueryExecutionContext = new DefaultQueryExecutionContext<T>(
                    client, resourceType, classT, querySpec, partitionedQueryExecutionInfo, options, resourceLink);

            // To be able to answer hasNext reliably, we have to call next now.
            try {
                if (currentQueryExecutionContext.hasNext()) {
                    this.prefetchedResource = currentQueryExecutionContext.next();

                    if (this.prefetchedResource != null) {
                        this.hasPrefetchedResource = true;
                    }
                }
            } catch (IllegalStateException e) {
                DocumentClientException dce;
                if (!(e.getCause() instanceof DocumentClientException) || !this
                        .shouldCreatePipelinedQueryExecutionContext((dce = (DocumentClientException) e.getCause()))) {
                    throw e;
                }

                currentQueryExecutionContext = (QueryExecutionContext<T>) new PipelinedQueryExecutionContext(this.client,
                        this.querySpec, this.options, this.resourceLink,
                        new PartitionedQueryExecutionInfo(dce.getError().getPartitionedQueryExecutionInfo()));
            }
        }

        this.queryExecutionContext = currentQueryExecutionContext;
    }

    private static boolean shouldCreateSpecializedDocumentQueryExecutionContext(
            ResourceType resourceType,
            FeedOptions feedOptions,
            PartitionedQueryExecutionInfo partitionedQueryExecutionInfo) {
        return isCrossPartitionQuery(resourceType, feedOptions, partitionedQueryExecutionInfo)
                && (isTopOrderByQuery(partitionedQueryExecutionInfo) || isParallelQuery(feedOptions));
    }

    private static boolean isParallelQuery(FeedOptions feedOptions) {
        return (feedOptions.getMaxDegreeOfParallelism() != 0);
    }

    private static boolean isTopOrderByQuery(PartitionedQueryExecutionInfo partitionedQueryExecutionInfo) {
        return partitionedQueryExecutionInfo.getQueryInfo() != null
                && (partitionedQueryExecutionInfo.getQueryInfo().hasOrderBy()
                    || partitionedQueryExecutionInfo.getQueryInfo().hasTop());
    }

    private static boolean isCrossPartitionQuery(
            ResourceType resourceType,
            FeedOptions feedOptions,
            PartitionedQueryExecutionInfo partitionedQueryExecutionInfo) {
        return resourceType.isPartitioned()
                && (feedOptions.getPartitionKey() == null
                    && feedOptions.getEnableCrossPartitionQuery() != null && feedOptions.getEnableCrossPartitionQuery().booleanValue())
                && !(partitionedQueryExecutionInfo.getQueryRanges().size() == 1
                    && partitionedQueryExecutionInfo.getQueryRanges().get(0).isSingleValue());
    }

    @Override
    public boolean hasNext() {
        return this.hasPrefetchedResource || this.queryExecutionContext.hasNext();
    }

    @Override
    public T next() {
        T item = null;
        if (this.hasPrefetchedResource) {
            synchronized (this) {
                if (this.hasPrefetchedResource) {
                    T result = this.prefetchedResource;
                    this.hasPrefetchedResource = false;
                    item = result;
                }
            }
        }

        while (item == null && this.queryExecutionContext.hasNext()) {
            item = this.queryExecutionContext.next();
        }

        return item;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove");
    }

    @Override
    public Map<String, String> getResponseHeaders() {
        return this.queryExecutionContext.getResponseHeaders();
    }

    @Override
    public List<T> fetchNextBlock() throws DocumentClientException {
        if (this.hasPrefetchedResource) {
            synchronized (this) {
                if (this.hasPrefetchedResource) {
                    List<T> result = new ArrayList<T>();
                    result.add(this.prefetchedResource);
                    result.addAll(this.queryExecutionContext.fetchNextBlock());
                    this.hasPrefetchedResource = false;
                    return result;
                }
            }
        }

        return this.queryExecutionContext.fetchNextBlock();
    }

    @Override
    public void onNotifyStop() {
        this.queryExecutionContext.onNotifyStop();
    }

    private boolean shouldCreatePipelinedQueryExecutionContext(DocumentClientException e) {
        return !(this.queryExecutionContext instanceof PipelinedQueryExecutionContext)
                && this.resourceType == ResourceType.Document && Document.class.equals(this.classT)
                && (e.getStatusCode() == HttpConstants.StatusCodes.BADREQUEST && e.getSubStatusCode() != null
                        && e.getSubStatusCode() == HttpConstants.SubStatusCodes.CROSS_PARTITION_QUERY_NOT_SERVABLE);
    }
}
