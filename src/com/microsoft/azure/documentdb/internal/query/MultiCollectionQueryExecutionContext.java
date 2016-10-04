package com.microsoft.azure.documentdb.internal.query;

import java.util.*;

import com.microsoft.azure.documentdb.*;
import com.microsoft.azure.documentdb.internal.*;

final class MultiCollectionQueryExecutionContext<T extends Resource> implements QueryExecutionContext<T> {
    private final List<QueryExecutionContext<T>> childQueryExecutionContexts;
    private int currentChildQueryExecutionContextIndex;
    private Map<String, String> responseHeaders;

    public MultiCollectionQueryExecutionContext(DocumentQueryClient client, ResourceType resourceType, Class<T> classT,
            SqlQuerySpec querySpec, FeedOptions options, Iterable<String> documentFeedLinks) {
        this.childQueryExecutionContexts = new ArrayList<QueryExecutionContext<T>>();
        this.currentChildQueryExecutionContextIndex = 0;

        for (String collectionLink : documentFeedLinks) {
            String path = Utils.joinPath(collectionLink, Paths.DOCUMENTS_PATH_SEGMENT);
            childQueryExecutionContexts.add(QueryExecutionContextFactory.createQueryExecutionContext(client,
                    resourceType, classT, querySpec, options, path));
        }
    }

    @Override
    public boolean hasNext() {
        while (this.currentChildQueryExecutionContextIndex < this.childQueryExecutionContexts.size()
                && !this.childQueryExecutionContexts.get(this.currentChildQueryExecutionContextIndex).hasNext()) {
            ++this.currentChildQueryExecutionContextIndex;
        }

        return this.currentChildQueryExecutionContextIndex < this.childQueryExecutionContexts.size();
    }

    @Override
    public T next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException("next");
        }

        QueryExecutionContext<T> currentQueryExecutionContext = this.childQueryExecutionContexts
                .get(this.currentChildQueryExecutionContextIndex);

        T result = currentQueryExecutionContext.next();
        this.responseHeaders = currentQueryExecutionContext.getResponseHeaders();

        return result;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove");
    }

    @Override
    public Map<String, String> getResponseHeaders() {
        return this.responseHeaders;
    }

    @Override
    public List<T> fetchNextBlock() throws DocumentClientException {
        if (!this.hasNext()) {
            return null;
        }

        QueryExecutionContext<T> currentQueryExecutionContext = this.childQueryExecutionContexts
                .get(this.currentChildQueryExecutionContextIndex);

        List<T> result = currentQueryExecutionContext.fetchNextBlock();
        this.responseHeaders = currentQueryExecutionContext.getResponseHeaders();

        return result;
    }

    @Override
    public void onNotifyStop() {
        for (QueryExecutionContext<T> context : this.childQueryExecutionContexts) {
            context.onNotifyStop();
        }
    }
}
