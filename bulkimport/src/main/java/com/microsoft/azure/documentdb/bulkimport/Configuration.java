package com.microsoft.azure.documentdb.bulkimport;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.beust.jcommander.Parameter;
import com.microsoft.azure.documentdb.ConnectionMode;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.RetryOptions;

public class Configuration {

    @Parameter(names = "-serviceEndpoint", description = "Service Endpoint", required = true)
    private String serviceEndpoint;

    @Parameter(names = "-masterKey", description = "Master Key", required = true)
    private String masterKey;

    @Parameter(names = "-databaseId", description = "Database ID", required = true)
    private String databaseId;

    @Parameter(names = "-collectionId", description = "Collection ID", required = true)
    private String collectionId;

//    @Parameter(names = "-documentDataFieldSize", description = "Length of a document data field in characters (16-bit)")
//    private int documentDataFieldSize = 1;

    @Parameter(names = "-maxConnectionPoolSize", description = "Max Connection Pool Size")
    private Integer maxConnectionPoolSize = 200;

    @Parameter(names = "-consistencyLevel", description = "Consistency Level")
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.Session;

    @Parameter(names = "-connectionMode", description = "Connection Mode")
    private ConnectionMode connectionMode = ConnectionMode.Gateway;

//    @Parameter(names = "-concurrency", description = "Degree of Concurrency in Inserting Documents (only applies to blocking client)."
//            + " If this value is not specified, the max connection pool size will be used as the concurrency level.")
//    private Integer concurrency;

    @Parameter(names = "-numberOfDocumentsForEachCheckpoint", description = "Number of documents in each checkpoint.")
    private int numberOfDocumentsForEachCheckpoint = 100000;

    @Parameter(names = "-numberOfCheckpoints", description = "Number of checkpoints.")
    private int numberOfCheckpoints = 10;

    @Parameter(names = {"-h", "-help", "--help"}, description = "Help", help = true)
    private boolean help = false;

    public int getNumberOfCheckpoints() {
        return numberOfCheckpoints;
    }
    
    public int getNumberOfDocumentsForEachCheckpoint() {
        return numberOfDocumentsForEachCheckpoint;
    }
    
    public String getServiceEndpoint() {
        return serviceEndpoint;
    }

    public String getMasterKey() {
        return masterKey;
    }

    public boolean isHelp() {
        return help;
    }
//
//    public int getDocumentDataFieldSize() {
//        return documentDataFieldSize;
//    }

    public ConnectionPolicy getConnectionPolicy() {
        ConnectionPolicy policy = new ConnectionPolicy();
        policy.setConnectionMode(connectionMode);
        policy.setMaxPoolSize(maxConnectionPoolSize);
        return policy;
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public String getDatabaseId() {
        return databaseId;
    }

    public String getCollectionId() {
        return collectionId;
    }

//    public int getConcurrency() {
//        if (this.concurrency != null) {
//            return concurrency;
//        } else {
//            return this.maxConnectionPoolSize;
//        }
//    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
    }
}
