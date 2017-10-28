package com.microsoft.azure.documentdb.bulkimport;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.beust.jcommander.Parameter;
import com.microsoft.azure.documentdb.ConnectionMode;
import com.microsoft.azure.documentdb.ConsistencyLevel;

class CmdLineConfiguration {

    @Parameter(names = "-serviceEndpoint", description = "Service Endpoint", required = true)
    private String serviceEndpoint;

    @Parameter(names = "-masterKey", description = "Master Key", required = true)
    private String masterKey;

    @Parameter(names = "-databaseId", description = "Database ID", required = true)
    private String databaseId;

    @Parameter(names = "-collectionId", description = "Collection ID", required = true)
    private String collectionId;

    @Parameter(names = "-maxConnectionPoolSize", description = "Max Connection Pool Size")
    private int maxConnectionPoolSize = 200;

    @Parameter(names = "-consistencyLevel", description = "Consistency Level")
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.Session;

    @Parameter(names = "-connectionMode", description = "Connection Mode")
    private ConnectionMode connectionMode = ConnectionMode.Gateway;

    @Parameter(names = "-numberOfDocumentsForEachCheckpoint", description = "Number of documents in each checkpoint.")
    private int numberOfDocumentsForEachCheckpoint = 500000;

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

    public Integer getMaxConnectionPoolSize() {
        return maxConnectionPoolSize;
    }

    public ConnectionMode getConnectionMode() {
        return connectionMode;
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

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
    }
}
