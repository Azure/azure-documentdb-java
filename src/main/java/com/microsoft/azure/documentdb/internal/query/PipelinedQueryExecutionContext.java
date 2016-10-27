package com.microsoft.azure.documentdb.internal.query;

import java.util.*;

import com.microsoft.azure.documentdb.*;

final class PipelinedQueryExecutionContext implements QueryExecutionContext<Document> {
    private static final int DEFAULT_PAGE_SIZE = 1000;
    private final QueryExecutionComponent queryExecutionComponent;
    private final int actualPageSize;

    public PipelinedQueryExecutionContext(DocumentQueryClient client, SqlQuerySpec querySpec, FeedOptions options,
            String resourceLink, PartitionedQueryExecutionInfo partitionedQueryExecutionInfo) {
        QueryExecutionContext<Document> queryExecutionContext = new ParallelQueryExecutionContext(client, querySpec,
                options, resourceLink, partitionedQueryExecutionInfo);

        QueryExecutionComponent currentQueryExecutionComponent = new DefaultQueryExecutionComponent(
                queryExecutionContext);

        QueryInfo queryInfo = partitionedQueryExecutionInfo.getQueryInfo();
        if (queryInfo.hasOrderBy()) {
            currentQueryExecutionComponent = new OrderByQueryExecutionComponent(currentQueryExecutionComponent);
        }

        if (queryInfo.hasTop()) {
            currentQueryExecutionComponent = new TopQueryExecutionComponent(currentQueryExecutionComponent,
                    queryInfo.getTop());
        }

        this.queryExecutionComponent = currentQueryExecutionComponent;

        Integer optionsPageSize = options.getPageSize();
        this.actualPageSize = (optionsPageSize != null && optionsPageSize > 0 ? optionsPageSize : DEFAULT_PAGE_SIZE);
    }

    @Override
    public List<Document> fetchNextBlock() throws DocumentClientException {
        List<Document> result = null;
        while ((result == null || result.size() < this.actualPageSize) && this.hasNext()) {
            if (result == null) {
                result = new ArrayList<Document>(this.actualPageSize);
            }

            Document document = this.next();
            if (document != null) {
                result.add(document);
            }
        }

        return result;
    }

    @Override
    public boolean hasNext() {
        return this.queryExecutionComponent.hasNext();
    }

    @Override
    public Document next() {
        return this.queryExecutionComponent.next();
    }

    @Override
    public void remove() {
        this.queryExecutionComponent.remove();
    }

    @Override
    public Map<String, String> getResponseHeaders() {
        return this.queryExecutionComponent.getResponseHeaders();
    }

    @Override
    public void onNotifyStop() {
        this.queryExecutionComponent.onNotifyStop();
    }
}