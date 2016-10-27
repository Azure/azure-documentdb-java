package com.microsoft.azure.documentdb.internal.query;

import java.util.*;

import com.microsoft.azure.documentdb.*;

final class DefaultQueryExecutionComponent extends QueryExecutionComponent {

    protected DefaultQueryExecutionComponent(QueryExecutionContext<Document> source) {
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
        return document;
    }
}
