package com.microsoft.azure.documentdb.internal.query;

import java.util.*;

import com.microsoft.azure.documentdb.*;

final class TopQueryExecutionComponent extends QueryExecutionComponent {
    private int topCount;

    protected TopQueryExecutionComponent(QueryExecutionContext<Document> source, int top) {
        super(source);
        this.topCount = top;
    }

    @Override
    public Map<String, String> getResponseHeaders() {
        return super.source.getResponseHeaders();
    }

    @Override
    public boolean hasNext() {
        return super.source.hasNext() && this.topCount > 0;
    }

    @Override
    protected Document next(Document document) {
        --this.topCount;
        if (this.topCount <= 0) {
            super.source.onNotifyStop();
        }

        return document;
    }
}
