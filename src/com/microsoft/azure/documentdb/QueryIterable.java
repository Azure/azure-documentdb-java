package com.microsoft.azure.documentdb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;


/**
 * 
 * The template class for iterable resources.
 *
 * @param <T> the resource type of the query iterable.
 */
public class QueryIterable<T extends Resource> implements Iterable<T> {

    private DocumentClient client = null;
    private ResourceThrottleRetryPolicy retryPolicy = null;
    private DocumentServiceRequest request = null;
    private ReadType readType;
    private Class<T> classT;
    private String initialContinuation = null;
    private String continuation = null;
    private boolean hasStarted = false;
    private List<T> items = new ArrayList<T>();
    private Map<String, String> responseHeaders;
    private int currentIndex = 0;
    private boolean hasNext = true;

    QueryIterable(DocumentClient client,
                  DocumentServiceRequest request,
                  ReadType readType,
                  Class<T> classT) {
        this.client = client;
        this.retryPolicy = new ResourceThrottleRetryPolicy(
                client.getRetryPolicy().getMaxRetryAttemptsOnQuery());
        this.request = request;
        this.readType = readType;
        this.classT = classT;

        if (this.request != null && this.request.getHeaders() != null) {
            String continuationToken = this.request.getHeaders().get(HttpConstants.HttpHeaders.CONTINUATION);
            if (!QueryIterable.isNullEmptyOrFalse(continuationToken)) {
                this.initialContinuation = continuationToken;
            }
        }

        this.reset();
    }

    /**
     * Gets the response headers.
     * 
     * @return the response headers.
     */
    Map<String, String> getResponseHeaders() {
        return this.responseHeaders;
    }

    /**
     * Gets the continuation token.
     * 
     * @return the continuation token.
     */
    String getContinuation() {
        return this.continuation;
    }

    /**
     * Gets the iterator of the iterable.
     * 
     * @return the iterator.
     */
    @Override
    public Iterator<T> iterator() {
        Iterator<T> it = new Iterator<T>() {

            private BackoffRetryUtilityDelegate delegate = new BackoffRetryUtilityDelegate() {

                @Override
                public void apply() throws Exception {
                    List<T> results = fetchNextBlock();
                    if (results == null || results.size() <= 0) {
                        hasNext = false;
                    }
                }
            };

            /**
             * Returns true if the iterator has a next value.
             * 
             * @return true if the iterator has a next value.
             */
            @Override
            public boolean hasNext() {
                if (currentIndex >= items.size() && hasNext) {
                    BackoffRetryUtility.execute(this.delegate, retryPolicy);
                }

                return hasNext;
            }

            /**
             * Gets and moves to the next value.
             * 
             * @return the current value.
             */
            @Override
            public T next() {
                if (currentIndex >= items.size() && hasNext) {
                    BackoffRetryUtility.execute(this.delegate, retryPolicy);
                }

                if (!hasNext) return null;
                return items.get(currentIndex++);
            }

            /**
             * Removes the current value.
             */
            @Override
            public void remove() {
                if (!hasNext()) throw new NoSuchElementException();
                if (currentIndex < items.size()) {
                    items.remove(currentIndex);
                }
            }

        };
        return it;
    }

    /**
     * Get the list of the iterable resources.
     * 
     * @return the list of the iterable resources.
     */
    public List<T> toList() {
        ArrayList<T> list = new ArrayList<T>();
        for (T t : this) {
            list.add(t);
        }
        return list;
    }

    /**
     * Resets the iterable.
     */
    public void reset() {
        this.hasStarted = false;
        this.continuation = this.initialContinuation;
        this.items.clear();
        this.currentIndex = 0;
        this.hasNext = true;
    }

    /**
     * Fetch the next block of query results.
     * 
     * @return the list of fetched resources.
     * @throws DocumentClientException the document client exception.
     */
    public List<T> fetchNextBlock()
        throws DocumentClientException {
        DocumentServiceResponse response = null;
        List<T> fetchedItems = null;

        while (!QueryIterable.isNullEmptyOrFalse(this.continuation) || !this.hasStarted) {
            if (!QueryIterable.isNullEmptyOrFalse(this.continuation)) {
                this.request.getHeaders().put(HttpConstants.HttpHeaders.CONTINUATION, this.continuation);
            } else {
                this.request.getHeaders().remove(HttpConstants.HttpHeaders.CONTINUATION);
            }

            if (this.readType == ReadType.Feed) {
                response = this.client.doReadFeed(this.request);
            } else {
                response = this.client.doQuery(this.request);
            }

            // A retriable exception may happen. "this.hasStarted" and "this.continuation" must not be set
            // value before this line.

            if (!this.hasStarted) {
                this.hasStarted = true;
            }

            this.responseHeaders = response.getResponseHeaders();
            this.continuation = this.responseHeaders.get(HttpConstants.HttpHeaders.CONTINUATION);

            fetchedItems = response.getQueryResponse(this.classT);
            this.items.clear();
            this.currentIndex = 0;
            this.items.addAll(fetchedItems);

            if (fetchedItems != null && fetchedItems.size() > 0) {
                break;
            }
        }

        return fetchedItems;
    }

    private static boolean isNullEmptyOrFalse(String s) {
        return s == null || s.isEmpty() || s == "false" || s == "False";
    }
}
