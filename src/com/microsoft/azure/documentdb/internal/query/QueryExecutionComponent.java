package com.microsoft.azure.documentdb.internal.query;

import java.util.*;

import com.microsoft.azure.documentdb.*;

abstract class QueryExecutionComponent implements QueryExecutionContext<Document> {
    protected final QueryExecutionContext<Document> source;

    protected QueryExecutionComponent(QueryExecutionContext<Document> source) {
        this.source = source;
    }

    @Override
    public List<Document> fetchNextBlock() throws DocumentClientException {
        throw new UnsupportedOperationException("fetchNextBlock");
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove");
    }

    @Override
    public Document next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException("next");
        }

        Document document = this.source.next();
        return document == null ? document : this.next(document);
    }

    @Override
    public void onNotifyStop() {
        this.source.onNotifyStop();
    }

    abstract protected Document next(Document document);
}
