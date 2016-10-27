/* 
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 */

package com.microsoft.azure.documentdb;

import java.util.Collection;

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

    private static ConnectionPolicy default_policy = null;
    private int requestTimeout;
    private int mediaRequestTimeout;
    private ConnectionMode connectionMode;
    private MediaReadMode mediaReadMode;
    private int maxPoolSize;
    private int idleConnectionTimeout;
    private String userAgentSuffix;
    private RetryOptions retryOptions;
    private boolean enableEndpointDiscovery = true;
    private Collection<String> preferredLocations;

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
        this.retryOptions = new RetryOptions();
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
     * @param requestTimeout the request timeout in seconds.
     */
    public void setRequestTimeout(int requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

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
     * @param mediaRequestTimeout the media request timeout in seconds.
     */
    public void setMediaRequestTimeout(int mediaRequestTimeout) {
        this.mediaRequestTimeout = mediaRequestTimeout;
    }

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
     * @param connectionMode the connection mode.
     */
    public void setConnectionMode(ConnectionMode connectionMode) {
        this.connectionMode = connectionMode;
    }

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
     * @param maxPoolSize The value of the connection pool size the httpclient is using.
     */
    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

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
     * @param idleConnectionTimeout the timeout for an idle connection in seconds.
     */
    public void setIdleConnectionTimeout(int idleConnectionTimeout) {
        this.idleConnectionTimeout = idleConnectionTimeout;
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
     * sets the value of the user-agent suffix.
     *
     * @param userAgentSuffix The value to be appended to the user-agent header, this is
     *                        used for monitoring purposes.
     */
    public void setUserAgentSuffix(String userAgentSuffix) {
        this.userAgentSuffix = userAgentSuffix;
    }

    /**
     * Gets the maximum number of retries in the case where the request fails
     * due to a throttle error.
     * <p>
     * This property is deprecated. Please use
     * connectionPolicy.getRetryOptions().getMaxRetryAttemptsOnThrottledRequests() for equivalent
     * functionality.
     *
     * @return maximum number of retry attempts.
     */
    @Deprecated
    public Integer getMaxRetryOnThrottledAttempts() {
        return this.retryOptions.getMaxRetryAttemptsOnThrottledRequests();
    }

    /**
     * Sets the maximum number of retries in the case where the request fails
     * due to a throttle error.
     * <p>
     * When a client is sending request faster than the request rate limit imposed by the service,
     * the service will return HttpStatusCode 429 (Too Many Request) to throttle the client. The current
     * implementation in the SDK will then wait for the amount of time the service tells it to wait and
     * retry after the time has elapsed.
     * <p>
     * The default value is 9. This means in the case where the request is throttled,
     * the same request will be issued for a maximum of 10 times to the server before
     * an error is returned to the application.
     * <p>
     * This property is deprecated. Please use
     * connectionPolicy.getRetryOptions().setMaxRetryAttemptsOnThrottledRequests() for equivalent
     * functionality.
     *
     * @param maxRetryOnThrottledAttempts the max number of retry attempts on failed requests.
     */
    @Deprecated
    public void setMaxRetryOnThrottledAttempts(Integer maxRetryOnThrottledAttempts) {
        int maxAttempts = 0;
        if (maxRetryOnThrottledAttempts != null) {
            maxAttempts = maxRetryOnThrottledAttempts;
        }

        this.retryOptions.setMaxRetryAttemptsOnThrottledRequests(maxAttempts);
    }

    /**
     * Gets the retry policy options associated with the DocumentClient instance.
     *
     * @return the RetryOptions instance.
     */
    public RetryOptions getRetryOptions() {
        return this.retryOptions;
    }

    /**
     * Sets the retry policy options associated with the DocumentClient instance.
     * <p>
     * Properties in the RetryOptions class allow application to customize the built-in
     * retry policies. This property is optional. When it's not set, the SDK uses the
     * default values for configuring the retry policies.  See RetryOptions class for
     * more details.
     *
     * @param retryOptions the RetryOptions instance.
     */
    public void setRetryOptions(RetryOptions retryOptions) {
        if (retryOptions == null) {
            throw new IllegalArgumentException("retryOptions value must not be null.");
        }

        this.retryOptions = retryOptions;
    }

    /**
     * Gets the flag to enable endpoint discovery for geo-replicated database accounts.
     *
     * @return whether endpoint discovery is enabled.
     */
    public boolean getEnableEndpointDiscovery() {
        return this.enableEndpointDiscovery;
    }

    /**
     * Sets the flag to enable endpoint discovery for geo-replicated database accounts.
     * <p>
     * When EnableEndpointDiscovery is true, the SDK will automatically discover the
     * current write and read regions to ensure requests are sent to the correct region
     * based on the capability of the region and the user's preference.
     * <p>
     * The default value for this property is true indicating endpoint discovery is enabled.
     *
     * @param enableEndpointDiscovery true if EndpointDiscovery is enabled.
     */
    public void setEnableEndpointDiscovery(boolean enableEndpointDiscovery) {
        this.enableEndpointDiscovery = enableEndpointDiscovery;
    }

    /**
     * Gets the preferred locations for geo-replicated database accounts
     *
     * @return the list of preferred location.
     */
    public Collection<String> getPreferredLocations() {
        return this.preferredLocations;
    }

    /**
     * Sets the preferred locations for geo-replicated database accounts. For example,
     * "East US" as the preferred location.
     * <p>
     * When EnableEndpointDiscovery is true and PreferredRegions is non-empty,
     * the SDK will prefer to use the locations in the collection in the order
     * they are specified to perform operations.
     * <p>
     * If EnableEndpointDiscovery is set to false, this property is ignored.
     *
     * @param preferredLocations the list of preferred locations.
     */
    public void setPreferredLocations(Collection<String> preferredLocations) {
        this.preferredLocations = preferredLocations;
    }
}
