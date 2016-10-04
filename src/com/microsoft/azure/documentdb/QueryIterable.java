package com.microsoft.azure.documentdb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.microsoft.azure.documentdb.internal.*;
import com.microsoft.azure.documentdb.internal.query.*;

/**
 * The template class for iterable resources.
 *
 * @param <T>
 *            the resource type of the query iterable.
 */
public class QueryIterable<T extends Resource> implements Iterable<T> {
    private final DocumentClient client;
    private final ResourceType resourceType;
    private final Class<T> classT;
    private final SqlQuerySpec querySpec;
    private final FeedOptions options;
    private final String resourceLink;
    private final Object partitionKey;
    private QueryExecutionContext<T> queryExecutionContext;

    protected QueryIterable(DocumentClient client, ResourceType resourceType, Class<T> classT, String resourceLink,
            FeedOptions options) {
        this(client, resourceType, classT, resourceLink, null, options, null);
    }

    protected QueryIterable(DocumentClient client, ResourceType resourceType, Class<T> classT, String resourceLink,
            SqlQuerySpec querySpec, FeedOptions options) {
        this(client, resourceType, classT, resourceLink, querySpec, options, null);
    }
    
    protected QueryIterable(DocumentClient client, ResourceType resourceType, Class<T> classT, String resourceLink,
            FeedOptions options, Object partitionKey) {
        this(client, resourceType, classT, resourceLink, null, options, partitionKey);
                }
        
    protected QueryIterable(DocumentClient client, ResourceType resourceType, Class<T> classT, String resourceLink,
            SqlQuerySpec querySpec, FeedOptions options, Object partitionKey) {
        this.client = client;
        this.resourceType = resourceType;
        this.classT = classT;
        this.querySpec = querySpec;
        if (options == null) {
            options = new FeedOptions();
        }
        
        this.options = options;
        this.resourceLink = resourceLink;
        this.partitionKey = partitionKey;
        this.reset();
    }
    
    @SuppressWarnings("deprecation")
    private QueryExecutionContext<T> createQueryExecutionContext(DocumentClient client, ResourceType resourceType,
            Class<T> classT, String resourceLink, SqlQuerySpec querySpec, FeedOptions options, Object partitionKey) {
        PartitionResolver partitionResolver;
        if (resourceType == ResourceType.Document && Utils.isDatabaseLink(resourceLink)
                && ((partitionResolver = client.getPartitionResolver(resourceLink)) != null)) {
            return QueryExecutionContextFactory.createQueryExecutionContext(new DocumentQueryClient(client),
                    resourceType, classT, querySpec, options, partitionResolver.resolveForRead(partitionKey));
    }
    
        return QueryExecutionContextFactory.createQueryExecutionContext(new DocumentQueryClient(client), resourceType,
                classT, querySpec, options, resourceLink);
            }

    /**
     * Gets the response headers.
     * 
     * @return the response headers.
     */
    public Map<String, String> getResponseHeaders() {
        return this.queryExecutionContext.getResponseHeaders();
    }

    /**
     * Gets the iterator of the iterable.
     * 
     * @return the iterator.
     */
    @Override
    public Iterator<T> iterator() {
        return this.queryExecutionContext;
    }

    /**
     * Get the list of the iterable resources.
     * 
     * @return the list of the iterable resources.
     */
    public List<T> toList() {
        List<T> list = new ArrayList<T>();
        for (T t : this) {
            if (t == null) {
                continue;
            }

            list.add(t);
        }

        return list;
    }

    /**
     * Resets the iterable.
     */
    public void reset() {
        this.queryExecutionContext = this.createQueryExecutionContext(this.client, this.resourceType, this.classT,
                this.resourceLink, this.querySpec, this.options, this.partitionKey);
    }

    /**
     * Fetch the next block of query results.
     * 
     * @return the list of fetched resources.
     * @throws DocumentClientException
     *             the document client exception.
     */
    public List<T> fetchNextBlock() throws DocumentClientException {
        return this.queryExecutionContext.fetchNextBlock();
    }
}
