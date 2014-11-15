/* 
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 */

package com.microsoft.azure.documentdb;

/**
 * Represents the Connection policy associated with a DocumentClient.
 */
public final class ConnectionPolicy {

    private static final int DEFAULT_MAX_CONNECTIONS = 20;
    private static final int DEFAULT_MAX_CONCURRENT_CALLS_PER_CONNECTION = 50;
    private static final int DEFAULT_REQUEST_TIMEOUT = 60;
    // defaultMediaRequestTimeout is based upon the blob client timeout and the retry policy.
    private static final int DEFAULT_MEDIA_REQUEST_TIMEOUT = 300;
    private static final int DEFAULT_MAX_CONCURRENT_FANOUT_REQUESTS = 32;
    private static final int DEFAULT_MAX_POOL_SIZE = 100;
    private static final int DEFAULT_IDLE_CONNECTION_TIMEOUT = 60;
    
    private static ConnectionPolicy default_policy = null;

    /**
     * Constructor.
     */
    public ConnectionPolicy() {
        this.maxConnections = ConnectionPolicy.DEFAULT_MAX_CONNECTIONS;
        this.requestTimeout = ConnectionPolicy.DEFAULT_REQUEST_TIMEOUT;
        this.mediaRequestTimeout = ConnectionPolicy.DEFAULT_MEDIA_REQUEST_TIMEOUT;
        this.connectionMode = ConnectionMode.Gateway;
        this.maxCallsPerConnection = ConnectionPolicy.DEFAULT_MAX_CONCURRENT_CALLS_PER_CONNECTION;
        this.maxConcurrentFanoutRequests = DEFAULT_MAX_CONCURRENT_FANOUT_REQUESTS;
        this.mediaReadMode = MediaReadMode.Buffered;
        this.maxPoolSize = DEFAULT_MAX_POOL_SIZE;
        this.idleConnectionTimeout = DEFAULT_IDLE_CONNECTION_TIMEOUT;
    }

    private int maxConnections;

    /**
     * Gets the maximum number of simultaneous network connections with a specific data partition. Currently used only
     * for Protocol.Tcp.
     * 
     * @return the max connections.
     */
    public int getMaxConnections() {
        return this.maxConnections;
    }

    /**
     * Sets the maximum number of simultaneous network connections with a specific data partition. Currently used only
     * for Protocol.Tcp.
     * 
     * @param maxConnections the max connections.
     */
    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    private int maxCallsPerConnection;

    /**
     * Gets the number of maximum simultaneous calls permitted on a single data connection. Currently used only for
     * Protocol.Tcp.
     * 
     * @return the max calls per connection.
     */
    public int getMaxCallsPerConnection() {
        return this.maxCallsPerConnection;
    }

    /**
     * Sets the number of maximum simultaneous calls permitted on a single data connection. Currently used only for
     * Protocol.Tcp.
     * 
     * @param maxCallsPerConnection the max calls per connection.
     */
    public void setMaxCallsPerConnection(int maxCallsPerConnection) {
        this.maxCallsPerConnection = maxCallsPerConnection;
    }

    private int maxConcurrentFanoutRequests;

    /**
     * Gets the maximum number of concurrent fanout requests.
     * 
     * @return the maximum number of concurrent fanout requests.
     */
    public int getMaxConcurrentFanoutRequest() {
        return this.maxConcurrentFanoutRequests;
    }

    /**
     * Sets the maximum number of concurrent fanout requests.
     * 
     * @param maxConcurrentFanoutRequests the max concurrent fanout requests.
     */
    public void setMaxConcurrentFanoutRequest(int maxConcurrentFanoutRequests) {
        this.maxConcurrentFanoutRequests = maxConcurrentFanoutRequests;
    }

    private int requestTimeout;

    /**
     * Gets the request timeout (time to wait for response from network peer) in seconds.
     * 
     * @return the request timeout in seconds.
     */
    public int getRequestTimeout() {
        return this.requestTimeout;
    }

    /**
     * Sets the request timeout (time to wait for response from network peer) in seconds.
     * 
     * @param requestTimeout the request timeout in seconds.
     */
    public void setRequestTimeout(int requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    private int mediaRequestTimeout;

    /**
     * Gets or sets Time to wait for response from network peer for attachment content (aka media) operations.
     * 
     * @return the media request timeout in seconds.
     */
    public int getMediaRequestTimeout() {
        return this.mediaRequestTimeout;
    }

    /**
     * Gets or sets Time to wait for response from network peer for attachment content (aka media) operations.
     * 
     * @param mediaRequestTimeout the media request timeout in seconds.
     */
    public void setMediaRequestTimeout(int mediaRequestTimeout) {
        this.mediaRequestTimeout = mediaRequestTimeout;
    }

    private ConnectionMode connectionMode;

    /**
     * Gets the connection mode used the client Gateway or Direct.
     * 
     * @return the connection mode.
     */
    public ConnectionMode getConnectionMode() {
        return this.connectionMode;
    }

    /**
     * Sets the connection mode used the client Gateway or Direct
     * 
     * @param connectionMode the connection mode.
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
     * @param mediaReadMode the media read mode.
     */
    public void setMediaReadMode(MediaReadMode mediaReadMode) {
        this.mediaReadMode = mediaReadMode;
    }

    private String connectBindingConfigName;

    /**
     * Gets the connection bindign config name. Ignored if ConnectionMode is Gateway or ConnectionProtocol is not TCP.
     * 
     * @return the connect binding config name.
     */
    public String getConnectBindingConfigName() {
        return this.connectBindingConfigName;
    }

    /**
     * Sets the connection binding config name. Ignored if ConnectionMode is Gateway or ConnectionProtocol is not TCP.
     * 
     * @param connectBindingConfigName the connect binding config name.
     */
    public void setConnectBindingConfigName(String connectBindingConfigName) {
        this.connectBindingConfigName = connectBindingConfigName;
    }

    private int maxPoolSize;
    
    /**
     * Gets the value of the connection pool size the client is using.
     * @return connection pool size.
     */
    public int getMaxPoolSize() {
        return this.maxPoolSize;
    }
    
    /**
     * Sets the value of the connection pool size of the httpclient, the default is 100.
     * @param maxPoolSize The value of the connection pool size the httpclient is using.
     */
    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }
    
    private int idleConnectionTimeout;
    
    /**
     * Gets the value of the timeout for an idle connection, the default is 60 seconds.
     * @return Idle connection timeout.
     */
    public int getIdleConnectionTimeout() {
        return this.idleConnectionTimeout;
    }
    
    /**
     * sets the value of the timeout for an idle connection. After that time, the connection will be automatically closed.
     * @param idleConnectionTimeout the timeout for an idle connection in seconds.
     */
    public void setIdleConnectionTimeout(int idleConnectionTimeout) {
        this.idleConnectionTimeout = idleConnectionTimeout;
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
}
