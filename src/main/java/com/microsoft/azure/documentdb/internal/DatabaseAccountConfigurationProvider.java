package com.microsoft.azure.documentdb.internal;

import com.microsoft.azure.documentdb.ConsistencyLevel;

public interface DatabaseAccountConfigurationProvider {
    ConsistencyLevel getStoreConsistencyPolicy();

    int getMaxReplicaSetSize();
}
