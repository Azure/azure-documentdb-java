package com.microsoft.azure.documentdb.internal.query;

import java.util.*;
import java.util.concurrent.*;

import org.apache.commons.lang3.StringUtils;

import com.microsoft.azure.documentdb.*;
import com.microsoft.azure.documentdb.internal.*;

final class DocumentProducer implements Iterator<Document> {
    private final LinkedBlockingQueue<Document> documentBuffer;
    private final ParallelQueryExecutionContext parentQueryExecutionContext;
    private final DocumentServiceRequest request;
    private final PartitionKeyRange targetRange;
    private final Class<? extends Document> deserializationClass;
    private Document currentDocument;
    private boolean hasStarted;
    private int previousResponseItemCount;
    private Map<String, String> previousResponseHeaders;

    public DocumentProducer(ParallelQueryExecutionContext parentQueryExecutionContext, DocumentServiceRequest request,
            PartitionKeyRange targetRange, Class<? extends Document> deserializationClass) {
        this.documentBuffer = new LinkedBlockingQueue<Document>();
        this.parentQueryExecutionContext = parentQueryExecutionContext;
        this.request = request;
        this.targetRange = targetRange;
        this.deserializationClass = deserializationClass;
        this.currentDocument = null;
        this.hasStarted = false;

        this.request.getHeaders().put(HttpConstants.HttpHeaders.PARTITION_KEY_RANGE_ID, this.targetRange.getId());
    }

    @Override
    public boolean hasNext() {
        return !this.isFinished() || this.currentDocument != null || !this.documentBuffer.isEmpty();
    }

    @Override
    public Document next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException("next");
        }

        synchronized (this) {
            try {
                if (this.currentDocument == null) {
                    this.currentDocument = this.documentBuffer.take();
                }

                Document result = this.currentDocument;
                this.currentDocument = null;
                return result;

            } catch (InterruptedException e) {
                throw new IllegalStateException("Failed to take Document from buffer", e);
            }
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove");
    }

    public boolean hasStarted() {
        return this.hasStarted;
    }

    public String getId() {
        return this.targetRange.getId();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DocumentProducer))
            return false;

        return this.getId().compareTo(((DocumentProducer) obj).getId()) == 0;
    }

    @Override
    public int hashCode() {
        return this.getId().hashCode();
    }

    public boolean isFinished() {
        return this.hasStarted
                && StringUtils.isEmpty(this.request.getHeaders().get(HttpConstants.HttpHeaders.CONTINUATION));
    }

    public Document peek() {
        if (!this.hasNext()) {
            throw new IllegalStateException("peek should not be called when hasNext is false");
        }

        synchronized (this) {
            if (this.currentDocument == null) {
                try {
                    this.currentDocument = this.documentBuffer.take();
                } catch (InterruptedException e) {
                    throw new IllegalStateException("Failed to peek Document from buffer", e);
                }
            }

            return this.currentDocument;
        }
    }

    public int size() {
        return this.documentBuffer.size();
    }

    public int getPreviousResponseItemCount() {
        return this.previousResponseItemCount;
    }

    public Map<String, String> getPreviousResponseHeaders() {
        return this.previousResponseHeaders;
    }

    public PartitionKeyRange getTargetRange() {
        return this.targetRange;
    }

    public DocumentProducer produce() throws DocumentClientException {
        if (this.isFinished()) {
            throw new IllegalStateException("produce should not be called when it is finished.");
        }

        DocumentServiceResponse response = parentQueryExecutionContext.executeRequest(request);

        List<? extends Document> items = response.getQueryResponse(this.deserializationClass);
        this.previousResponseItemCount = items.size();
        documentBuffer.addAll(items);

        this.previousResponseHeaders = response.getResponseHeaders();

        this.request.getHeaders().put(HttpConstants.HttpHeaders.CONTINUATION,
                response.getResponseHeaders().get(HttpConstants.HttpHeaders.CONTINUATION));

        this.hasStarted = true;

        return this;
    }
}