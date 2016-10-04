package com.microsoft.azure.documentdb.internal.query;

import java.util.*;

import com.microsoft.azure.documentdb.*;
import com.microsoft.azure.documentdb.internal.*;

final class DefaultQueryExecutionContext<T extends Resource> extends AbstractQueryExecutionContext<T> {
    private final Queue<T> buffer;
    private final DocumentServiceRequest request;

    public DefaultQueryExecutionContext(DocumentQueryClient client, ResourceType resourceType, Class<T> classT,
            SqlQuerySpec querySpec, FeedOptions options, String resourceLink) {
        super(client, resourceType, classT, querySpec, options, resourceLink);
        this.buffer = new ArrayDeque<T>();
        this.request = super.createRequest(super.getFeedHeaders(options), querySpec);
    }

    @Override
    public List<T> fetchNextBlock() throws DocumentClientException {
        if (!this.hasNext()) {
            return null;
        }

        this.fillBuffer();
        List<T> result = new ArrayList<T>(buffer);
        buffer.clear();
        return result;
    }

    @Override
    public boolean hasNext() {
        return super.hasNextInternal() || !this.buffer.isEmpty();
    }

    @Override
    public T next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException("next");
        }

        try {
            this.fillBuffer();
        } catch (DocumentClientException e) {
            throw new IllegalStateException(e);
        }

        return this.buffer.poll();
    }

    @Override
    public void onNotifyStop() {
        // Nothing to do
    }

    private void fillBuffer() throws DocumentClientException {
        while (super.hasNextInternal() && this.buffer.isEmpty()) {
            DocumentServiceResponse response;
            if (super.shouldExecuteQuery()) {
                response = super.executeQueryRequest(this.request);
            } else {
                response = super.executeReadFeedRequest(this.request);
            }

            super.responseHeaders = response.getResponseHeaders();
            buffer.addAll(response.getQueryResponse(this.classT));
            this.request.getHeaders().put(HttpConstants.HttpHeaders.CONTINUATION, super.getContinuationToken());
        }
    }
}
