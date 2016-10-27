package com.microsoft.azure.documentdb.internal;

import java.net.URI;

import com.microsoft.azure.documentdb.DatabaseAccount;

/**
 * Defines an interface used to manage endpoint selection for geo-distributed
 * database accounts.
 */
public interface EndpointManager {

    /**
     * Returns the current write region endpoint.
     *
     * @return the write endpoint URI
     */
    public URI getWriteEndpoint();

    /**
     * Returns the current read region endpoint.
     *
     * @return the read endpoint URI
     */
    public URI getReadEndpoint();

    /**
     * Returns the target endpoint for a given request.
     *
     * @param operationType the operation type
     * @return the service endpoint URI
     */
    public URI resolveServiceEndpoint(OperationType operationType);

    /**
     * Refreshes the client side endpoint cache.
     */
    public void refreshEndpointList();

    /**
     * Gets the Database Account resource
     *
     * @return the database account
     */
    public DatabaseAccount getDatabaseAccountFromAnyEndpoint();
}
