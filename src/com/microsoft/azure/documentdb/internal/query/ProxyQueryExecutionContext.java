package com.microsoft.azure.documentdb.internal.query;

import java.util.*;

import com.microsoft.azure.documentdb.*;
import com.microsoft.azure.documentdb.internal.*;

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

        QueryExecutionContext<T> currentQueryExecutionContext = new DefaultQueryExecutionContext<T>(client,
                resourceType, classT, querySpec, options, resourceLink);

        // To be able to answer hasNext reliably, we have to call next now.
        try {
            if (currentQueryExecutionContext.hasNext()) {
                this.prefetchedResource = currentQueryExecutionContext.next();
            }

            this.hasPrefetchedResource = true;
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

        this.queryExecutionContext = currentQueryExecutionContext;
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
