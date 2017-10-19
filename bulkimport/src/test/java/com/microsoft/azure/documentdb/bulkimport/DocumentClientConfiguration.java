package com.microsoft.azure.documentdb.bulkimport;

import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;

/**
 * This is used in parameterized tests. Contains information to construct a DocumentClient object
 */
public class DocumentClientConfiguration {
    private String endpoint;
    private String masterKey;
    private ConnectionPolicy connectionPolicy;
    private ConsistencyLevel consistencyLevel;

    public DocumentClientConfiguration(
            String endpoint, String masterKey, ConnectionPolicy connectionPolicy, ConsistencyLevel consistencyLevel) {
        this.endpoint = endpoint;
        this.masterKey = masterKey;
        this.connectionPolicy = connectionPolicy;
        this.consistencyLevel = consistencyLevel;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getMasterKey() {
        return masterKey;
    }

    public ConnectionPolicy getConnectionPolicy() {
        return connectionPolicy;
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }
}
