package com.microsoft.azure.documentdb.internal;

import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.DatabaseAccount;

public class BaseDatabaseAccountConfigurationProvider implements DatabaseAccountConfigurationProvider {
    private ConsistencyLevel desiredConsistencyLevel;
    private DatabaseAccount databaseAccount;

    public BaseDatabaseAccountConfigurationProvider(DatabaseAccount databaseAccount, ConsistencyLevel desiredConsistencyLevel) {
        this.databaseAccount = databaseAccount;
        this.desiredConsistencyLevel = desiredConsistencyLevel;
    }

    public ConsistencyLevel getStoreConsistencyPolicy() {
        ConsistencyLevel databaseAccountConsistency = this.databaseAccount.getConsistencyPolicy().getDefaultConsistencyLevel();
        if (this.desiredConsistencyLevel == null) {
            return databaseAccountConsistency;
        } else if (!Utils.isValidConsistency(databaseAccountConsistency, this.desiredConsistencyLevel)) {
            throw new IllegalArgumentException(String.format(
                    "ConsistencyLevel %1s specified in the request is invalid when service is configured with consistency level %2s. Ensure the request consistency level is not stronger than the service consistency level.",
                    this.desiredConsistencyLevel.toString(),
                    databaseAccountConsistency.toString()));
        } else {
            return this.desiredConsistencyLevel;
        }
    }

    public int getMaxReplicaSetSize() {
        return this.databaseAccount.getReplicationPolicy().getMaxReplicaSetSize();
    }
}
