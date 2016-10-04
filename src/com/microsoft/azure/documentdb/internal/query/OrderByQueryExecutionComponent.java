package com.microsoft.azure.documentdb.internal.query;

import java.util.Map;

import com.microsoft.azure.documentdb.*;

final class OrderByQueryExecutionComponent extends QueryExecutionComponent {
    protected OrderByQueryExecutionComponent(QueryExecutionContext<Document> source) {
        super(source);
    }

    @Override
    public Map<String, String> getResponseHeaders() {
        return super.source.getResponseHeaders();
    }

    @Override
    public boolean hasNext() {
        return super.source.hasNext();
    }

    @Override
    protected Document next(Document document) {
        return ((DocumentQueryResult) document).getPayload();
    }
}
