/* 
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 */

package com.microsoft.azure.documentdb;

/**
 * Represents the Connection policy associated with a DocumentClient.
 */
public final class ConnectionPolicy {

    private static final int DEFAULT_REQUEST_TIMEOUT = 60;
    // defaultMediaRequestTimeout is based upon the blob client timeout and the
    // retry policy.
    private static final int DEFAULT_MEDIA_REQUEST_TIMEOUT = 300;
    private static final int DEFAULT_MAX_POOL_SIZE = 100;
    private static final int DEFAULT_IDLE_CONNECTION_TIMEOUT = 60;
    private static final int MAX_RETRY_ATTEMPTS = 3;

    private static ConnectionPolicy default_policy = null;

    /**
     * Constructor.
     */
    public ConnectionPolicy() {
        this.requestTimeout = ConnectionPolicy.DEFAULT_REQUEST_TIMEOUT;
        this.mediaRequestTimeout = ConnectionPolicy.DEFAULT_MEDIA_REQUEST_TIMEOUT;
        this.connectionMode = ConnectionMode.Gateway;
        this.mediaReadMode = MediaReadMode.Buffered;
        this.maxPoolSize = DEFAULT_MAX_POOL_SIZE;
        this.idleConnectionTimeout = DEFAULT_IDLE_CONNECTION_TIMEOUT;
        this.userAgentSuffix = "";
        this.maxRetryOnThrottledAttempts = MAX_RETRY_ATTEMPTS;
    }

    private int requestTimeout;

    /**
     * Gets the request timeout (time to wait for response from network peer) in
     * seconds.
     * 
     * @return the request timeout in seconds.
     */
    public int getRequestTimeout() {
        return this.requestTimeout;
    }

    /**
     * Sets the request timeout (time to wait for response from network peer) in
     * seconds.
     * 
     * @param requestTimeout
     *            the request timeout in seconds.
     */
    public void setRequestTimeout(int requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    private int mediaRequestTimeout;

    /**
     * Gets or sets Time to wait for response from network peer for attachment
     * content (aka media) operations.
     * 
     * @return the media request timeout in seconds.
     */
    public int getMediaRequestTimeout() {
        return this.mediaRequestTimeout;
    }

    /**
     * Gets or sets Time to wait for response from network peer for attachment
     * content (aka media) operations.
     * 
     * @param mediaRequestTimeout
     *            the media request timeout in seconds.
     */
    public void setMediaRequestTimeout(int mediaRequestTimeout) {
        this.mediaRequestTimeout = mediaRequestTimeout;
    }

    private ConnectionMode connectionMode;

    /**
     * Gets the connection mode used in the client. Currently only Gateway is
     * supported.
     * 
     * @return the connection mode.
     */
    public ConnectionMode getConnectionMode() {
        return this.connectionMode;
    }

    /**
     * Sets the connection mode used in the client. Currently only Gateway is
     * supported.
     * 
     * @param connectionMode
     *            the connection mode.
     */
    public void setConnectionMode(ConnectionMode connectionMode) {
        this.connectionMode = connectionMode;
    }

    private MediaReadMode mediaReadMode;

    /**
     * Gets the attachment content (aka media) download mode.
     * 
     * @return the media read mode.
     */
    public MediaReadMode getMediaReadMode() {
        return this.mediaReadMode;
    }

    /**
     * Sets the attachment content (aka media) download mode.
     * 
     * @param mediaReadMode
     *            the media read mode.
     */
    public void setMediaReadMode(MediaReadMode mediaReadMode) {
        this.mediaReadMode = mediaReadMode;
    }

    private int maxPoolSize;

    /**
     * Gets the value of the connection pool size the client is using.
     * 
     * @return connection pool size.
     */
    public int getMaxPoolSize() {
        return this.maxPoolSize;
    }

    /**
     * Sets the value of the connection pool size of the httpclient, the default
     * is 100.
     * 
     * @param maxPoolSize
     *            The value of the connection pool size the httpclient is using.
     */
    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    private int idleConnectionTimeout;

    /**
     * Gets the value of the timeout for an idle connection, the default is 60
     * seconds.
     * 
     * @return Idle connection timeout.
     */
    public int getIdleConnectionTimeout() {
        return this.idleConnectionTimeout;
    }

    /**
     * sets the value of the timeout for an idle connection. After that time,
     * the connection will be automatically closed.
     * 
     * @param idleConnectionTimeout
     *            the timeout for an idle connection in seconds.
     */
    public void setIdleConnectionTimeout(int idleConnectionTimeout) {
        this.idleConnectionTimeout = idleConnectionTimeout;
    }

    private String userAgentSuffix;

    /**
     * sets the value of the user-agent suffix.
     * 
     * @param userAgentSuffix
     *            The value to be appended to the user-agent header, this is
     *            used for monitoring purposes.
     */
    public void setUserAgentSuffix(String userAgentSuffix) {
        this.userAgentSuffix = userAgentSuffix;
    }

    /**
     * Gets the value of user-agent suffix.
     * 
     * @return the value of user-agent suffix.
     */
    public String getUserAgentSuffix() {
        return this.userAgentSuffix;
    }

    /**
     * Gets the default connection policy.
     * 
     * @return the default connection policy.
     */
    public static ConnectionPolicy GetDefault() {
        if (ConnectionPolicy.default_policy == null) {
            ConnectionPolicy.default_policy = new ConnectionPolicy();
        }
        return ConnectionPolicy.default_policy;
    }
    
    private Integer maxRetryOnThrottledAttempts;

    /**
     * Sets the maximum number of retries in the case where the request fails
     * due to a throttle error.
     * <p>
     * The default value is 3. This means in the case where the request is throttled,
     * the same request will be issued for a maximum of 4 times to the server before 
     * an error is returned to the application.
     * 
     * @param maxRetryOnThrottledAttempts
     *            the max number of retry attempts on failed requests.
     */
    public void setMaxRetryOnThrottledAttempts(Integer maxRetryOnThrottledAttempts) {
        this.maxRetryOnThrottledAttempts = maxRetryOnThrottledAttempts;
    }

    /**
     * Gets the maximum number of retries in the case where the request fails
     * due to a throttle error.
     * 
     * @return maximum number of retry attempts.
     */
    public Integer getMaxRetryOnThrottledAttempts() {
        return this.maxRetryOnThrottledAttempts;
    }
    
}
