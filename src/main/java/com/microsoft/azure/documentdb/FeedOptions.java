/* 
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 */

package com.microsoft.azure.documentdb;

/**
 * Specifies the options associated with feed methods (enumeration operations).
 */
public final class FeedOptions {
    private Integer pageSize;
    private String requestContinuation;
    private String sessionToken;
    private Boolean enableScanInQuery;
    private Boolean emitVerboseTracesInQuery;
    private PartitionKey partitionkey;
    private Boolean enableCrossPartitionQuery;
    private int maxDegreeOfParallelism;
    private int maxBufferedItemCount;

    /**
     * Gets the maximum number of items to be returned in the enumeration
     * operation.
     * 
     * @return the page size.
     */
    public Integer getPageSize() {
        return this.pageSize;
    }

    /**
     * Sets the maximum number of items to be returned in the enumeration
     * operation.
     * 
     * @param pageSize
     *            the page size.
     */
    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }

    /**
     * Gets the request continuation token.
     * 
     * @return the request continuation.
     */
    public String getRequestContinuation() {
        return this.requestContinuation;
    }

    /**
     * Sets the request continuation token.
     * 
     * @param requestContinuation
     *            the request continuation.
     */
    public void setRequestContinuation(String requestContinuation) {
        this.requestContinuation = requestContinuation;
    }

    /**
     * Gets the session token for use with session consistency.
     * 
     * @return the session token.
     */
    public String getSessionToken() {
        return this.sessionToken;
    }

    /**
     * Sets the session token for use with session consistency.
     * 
     * @param sessionToken
     *            the session token.
     */
    public void setSessionToken(String sessionToken) {
        this.sessionToken = sessionToken;
    }

    /**
     * Gets the option to allow scan on the queries which couldn't be served as
     * indexing was opted out on the requested paths.
     * 
     * @return the option of enable scan in query.
     */
    public Boolean getEnableScanInQuery() {
        return this.enableScanInQuery;
    }

    /**
     * Sets the option to allow scan on the queries which couldn't be served as
     * indexing was opted out on the requested paths.
     * 
     * @param enableScanInQuery
     *            the option of enable scan in query.
     */
    public void setEnableScanInQuery(Boolean enableScanInQuery) {
        this.enableScanInQuery = enableScanInQuery;
    }

    /**
     * Gets the option to allow queries to emit out verbose traces for
     * investigation.
     * 
     * @return the emit verbose traces in query.
     */
    public Boolean getEmitVerboseTracesInQuery() {
        return this.emitVerboseTracesInQuery;
    }

    /**
     * Sets the option to allow queries to emit out verbose traces for
     * investigation.
     * 
     * @param emitVerboseTracesInQuery
     *            the emit verbose traces in query.
     */
    public void setEmitVerboseTracesInQuery(Boolean emitVerboseTracesInQuery) {
        this.emitVerboseTracesInQuery = emitVerboseTracesInQuery;
    }
    
    /**
     * Gets the partition key used to identify the current request's target
     * partition.
     * 
     * @return the partition key.
     */
    public PartitionKey getPartitionKey() {
        return this.partitionkey;
    }

    /**
     * Sets the partition key used to identify the current request's target
     * partition.
     * 
     * @param partitionkey
     *            the partition key value.
     */
    public void setPartitionKey(PartitionKey partitionkey) {
        this.partitionkey = partitionkey;
    }
    
    /**
     * Gets the option to allow queries to run across all partitions of the
     * collection.
     * 
     * @return whether to allow queries to run across all partitions of the
     *         collection.
     */
    public Boolean getEnableCrossPartitionQuery() {
        return this.enableCrossPartitionQuery;
    }

    /**
     * Sets the option to allow queries to run across all partitions of the
     * collection.
     * 
     * @param enableCrossPartitionQuery
     *            whether to allow queries to run across all partitions of the
     *            collection.
     */
    public void setEnableCrossPartitionQuery(Boolean enableCrossPartitionQuery) {
        this.enableCrossPartitionQuery = enableCrossPartitionQuery;
    }

    /**
     * Gets the number of concurrent operations run client side during parallel
     * query execution.
     * 
     * @return number of concurrent operations run client side during parallel
     *         query execution.
     */
    public int getMaxDegreeOfParallelism() {
        return maxDegreeOfParallelism;
    }

    /**
     * Sets the number of concurrent operations run client side during parallel
     * query execution.
     * 
     * @param maxDegreeOfParallelism
     *            number of concurrent operations.
     */
    public void setMaxDegreeOfParallelism(int maxDegreeOfParallelism) {
        this.maxDegreeOfParallelism = maxDegreeOfParallelism;
    }

    /**
     * Gets the maximum number of items that can be buffered client side during
     * parallel query execution.
     * 
     * @return maximum number of items that can be buffered client side during
     *         parallel query execution.
     */
    public int getMaxBufferedItemCount() {
        return maxBufferedItemCount;
    }

    /**
     * Sets the maximum number of items that can be buffered client side during
     * parallel query execution.
     * 
     * @param maxBufferedItemCount
     *            maximum number of items.
     */
    public void setMaxBufferedItemCount(int maxBufferedItemCount) {
        this.maxBufferedItemCount = maxBufferedItemCount;
    }
}
