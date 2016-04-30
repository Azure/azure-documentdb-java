package com.microsoft.azure.documentdb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

/**
 * 
 * The template class for iterable resources.
 *
 * @param <T> the resource type of the query iterable.
 */
public class QueryIterable<T extends Resource> implements Iterable<T> {

    private DocumentClient client = null;
    private DocumentServiceRequest request = null;
    private ReadType readType;
    private Class<T> classT;
    private String initialContinuation = null;
    private String continuation = null;
    private boolean hasStarted = false;
    private List<T> items = new ArrayList<T>();
    private Map<String, String> requestHeaders;
    private Map<String, String> responseHeaders;
    private int currentIndex = 0;
    private boolean hasNext = true;
    private SqlQuerySpec querySpec = null;
    private ArrayList<String> documentCollectionLinks = new ArrayList<String>();
    private int currentCollectionIndex = 0;

    /**
     * QueryIterable constructor taking in the DocumentServiceRequest(for non-partitioning scenarios)
     */
    protected QueryIterable(DocumentClient client,
                  DocumentServiceRequest request,
                  ReadType readType,
                  Class<T> classT) {
        this.initialize(client, readType, classT);
        this.request = request;
        this.initializeContinuationToken();
        this.reset();
    }
    
    /**
     * QueryIterable constructor taking in the individual parameters for creating a DocumentServiceRequest
     * This constructor is used for partitioning scenarios when multiple DocumentServiceRequests need to be created
     */
    protected QueryIterable(DocumentClient client,
            String databaseOrDocumentCollectionLink,
            SqlQuerySpec querySpec,
            FeedOptions options,
            Object partitionKey,
            ReadType readType,
            Class<T> classT) {
        this.initialize(client, readType, classT);
        this.querySpec = querySpec;
        
        if(Utils.isDatabaseLink(databaseOrDocumentCollectionLink)) {
            // Gets the partition resolver(if it exists) for the specified database link
            PartitionResolver partitionResolver = this.client.getPartitionResolver(databaseOrDocumentCollectionLink);
            
            // If the partition resolver exists, get the list of collections(from resolveForRead passing in the partitionKey) which we need to query against
            if(partitionResolver != null) {
                for(String collectionLink : partitionResolver.resolveForRead(partitionKey)) {
                    this.documentCollectionLinks.add(collectionLink);
                }
            }
            else {
                throw new IllegalArgumentException(DocumentClient.PartitionResolverErrorMessage);
            }
        }
        else {
            this.documentCollectionLinks.add(databaseOrDocumentCollectionLink);
        }
        
        // Create the request for the first collection to be queried
        if(this.documentCollectionLinks != null && this.documentCollectionLinks.size() > 0) {
            String path = Utils.joinPath(this.documentCollectionLinks.get(this.currentCollectionIndex), Paths.DOCUMENTS_PATH_SEGMENT);
            this.currentCollectionIndex++;
            
            this.requestHeaders = this.client.getFeedHeaders(options);
            this.request = DocumentServiceRequest.create(ResourceType.Document,
                                                                           path,
                                                                           this.querySpec,
                                                                           this.client.queryCompatibilityMode,
                                                                           this.requestHeaders);
            
            this.initializeContinuationToken();
        }
        
        this.reset();
    }
    
    /**
     * Initialize the common fields to both QueryIterable constructors
     */
    private void initialize(DocumentClient client,
            ReadType readType,
            Class<T> classT) {
        this.client = client;        
        this.readType = readType;
        this.classT = classT;
    }
    
    /**
     * Initialize the continuation token from the request header
     */
    private void initializeContinuationToken() {
        if (this.request != null && this.request.getHeaders() != null) {
            String continuationToken = this.request.getHeaders().get(HttpConstants.HttpHeaders.CONTINUATION);
            if (!QueryIterable.isNullEmptyOrFalse(continuationToken)) {
                this.initialContinuation = continuationToken;
            }
        }
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

            private QueryBackoffRetryUtilityDelegate delegate = new QueryBackoffRetryUtilityDelegate() {

                @Override
                public void apply() throws Exception {
                    List<T> results = fetchNextBlock();
                    if (results == null) {
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
                    BackoffRetryUtility.execute(this.delegate, client.getConnectionPolicy().getMaxRetryOnThrottledAttempts());
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
                    BackoffRetryUtility.execute(this.delegate, client.getConnectionPolicy().getMaxRetryOnThrottledAttempts());
                }
                
                if (!hasNext) return null;
                return items.get(currentIndex++);
            }

            /**
             * Remove not supported.
             */
            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove");
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

        // Fetch next block of results by executing the query against the current document collection
        List<T> fetchedItems = this.fetchItems();
        
        // If there are multiple document collections to query for(in case of partitioning), keep looping through each one of them,
        // creating separate requests for each collection and execute it
        while(fetchedItems == null) {
            if(this.documentCollectionLinks != null && this.currentCollectionIndex < this.documentCollectionLinks.size()) {
                String path = Utils.joinPath(this.documentCollectionLinks.get(this.currentCollectionIndex), Paths.DOCUMENTS_PATH_SEGMENT);
                this.request = DocumentServiceRequest.create(ResourceType.Document,
                        path,
                        this.querySpec,
                        this.client.queryCompatibilityMode,
                        this.requestHeaders);
                this.reset();
                fetchedItems = this.fetchItems();
                this.currentCollectionIndex++;
            }
            else {
                break;
            }
        }

        return fetchedItems;
    }
    
    /**
     * Fetch items from query results for the current document collection.
     * 
     * @return the list of fetched resources.
     * @throws DocumentClientException the document client exception.
     */
    private List<T> fetchItems() 
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
        
        // Returning null if there are no items in the fetched collection instead of returning an empty list
        // This makes fetchNextBlock method usage much better which now, just needs to check for nullability
        if (fetchedItems != null && fetchedItems.size() <= 0) {
            return null;
        }
        
        return fetchedItems;
    }

    private static boolean isNullEmptyOrFalse(String s) {
        return StringUtils.isEmpty(s) || s.equalsIgnoreCase("false");
    }
}
