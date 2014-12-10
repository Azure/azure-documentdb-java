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
    private String continuation = null;
    private boolean hasStarted = false;
    private List<T> items = new ArrayList<T>();
    private Map<String, String> responseHeaders;

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
     * Gets the iterator of the iterable.
     * 
     * @return the iterator.
     */
    @Override
    public Iterator<T> iterator() {
        Iterator<T> it = new Iterator<T>() {

            private int currentIndex = 0;
            private boolean hasNext = true;

            private BackoffRetryUtilityDelegate delegate = new BackoffRetryUtilityDelegate() {

                @Override
                public void apply() throws Exception {
                    if (fetchNextBlock() <= 0) {
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
                if (this.currentIndex >= items.size() && this.hasNext) {
                    BackoffRetryUtility.execute(this.delegate, retryPolicy);
                }

                return this.hasNext;
            }

            /**
             * Gets and moves to the next value.
             * 
             * @return the current value.
             */
            @Override
            public T next() {
                if (this.currentIndex >= items.size() && this.hasNext) {
                    BackoffRetryUtility.execute(this.delegate, retryPolicy);
                }

                if (!this.hasNext) return null;
                return items.get(this.currentIndex++);
            }

            /**
             * Removes the current value.
             */
            @Override
            public void remove() {
                if (!hasNext()) throw new NoSuchElementException();
                if (this.currentIndex < items.size() - 1) {
                    items.remove(this.currentIndex);
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

    private int fetchNextBlock()
        throws DocumentClientException {
        DocumentServiceResponse response = null;
        List<T> fetchedItems = null;

        while (!this.isNullEmptyOrFalse(this.continuation) ||
               !this.hasStarted) {
            if (!this.isNullEmptyOrFalse(this.continuation)) {
                request.getHeaders().put(HttpConstants.HttpHeaders.CONTINUATION,
                                         this.continuation);
            }

            if (this.readType == ReadType.Feed) {
                response = this.client.doReadFeed(request);
            } else {
                response = this.client.doQuery(request);
            }

            // A retriable exception may happen. "this.hasStarted" and "this.continuation" must not be set
            // value before this line.

            if (!this.hasStarted) {
                this.hasStarted = true;
            }

            this.responseHeaders = response.getResponseHeaders();
            this.continuation = this.responseHeaders.get(HttpConstants.HttpHeaders.CONTINUATION);

            fetchedItems = response.getQueryResponse(this.classT);
            this.items.addAll(fetchedItems);

            if (fetchedItems != null && fetchedItems.size() > 0) {
                break;
            }
        }

        return fetchedItems != null ? fetchedItems.size() : 0;
    }

    private boolean isNullEmptyOrFalse(String s) {
        return s == null || s.isEmpty() || s == "false" || s == "False";
    }
}
