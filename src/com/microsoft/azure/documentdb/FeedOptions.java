/* 
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 */

package com.microsoft.azure.documentdb;

/**
 * Specifies the options associated with feed methods (enumeration operations).
 */
public final class FeedOptions {

    private Integer pageSize;

    /**
     * Gets the maximum number of items to be returned in the enumeration operation.
     * 
     * @return the page size.
     */
    public Integer getPageSize() {
        return this.pageSize;
    }

    /**
     * Sets the maximum number of items to be returned in the enumeration operation.
     * 
     * @param pageSize the page size.
     */
    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }

    private String requestContinuation;

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
     * @param requestContinuation the request continuation.
     */
    public void setRequestContinuation(String requestContinuation) {
        this.requestContinuation = requestContinuation;
    }

    private String sessionToken;

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
     * @param sessionToken the session token.
     */
    public void setSessionToken(String sessionToken) {
        this.sessionToken = sessionToken;
    }

    private Boolean enableScanInQuery;

    /**
     * Gets the option to allow scan on the queries which couldn't be served as indexing was opted out on the requested
     * paths.
     * 
     * @return the option of enable scan in query.
     */
    public Boolean getEnableScanInQuery() {
        return this.enableScanInQuery;
    }

    /**
     * Sets the option to allow scan on the queries which couldn't be served as indexing was opted out on the requested
     * paths.
     * 
     * @param enableScanInQuery the option of enable scan in query.
     */
    public void setEnableScanInQuery(Boolean enableScanInQuery) {
        this.enableScanInQuery = enableScanInQuery;
    }

    private Boolean emitVerboseTracesInQuery;

    /**
     * Gets the option to allow queries to emit out verbose traces for investigation.
     * 
     * @return the emit verbose traces in query.
     */
    public Boolean getEmitVerboseTracesInQuery() {
        return this.emitVerboseTracesInQuery;
    }

    /**
     * Sets the option to allow queries to emit out verbose traces for investigation.
     * 
     * @param emitVerboseTracesInQuery the emit verbose traces in query.
     */
    public void setEmitVerboseTracesInQuery(Boolean emitVerboseTracesInQuery) {
        this.emitVerboseTracesInQuery = emitVerboseTracesInQuery;
    }
}
