/* 
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 */

package com.microsoft.azure.documentdb.internal;

public enum ResourceType {
    Attachment,
    Conflict,
    Database,
    DatabaseAccount,
    Document,
    DocumentCollection,
    Media,  // Media doesn't have a corresponding resource class.
    Offer,
    PartitionKeyRange,
    Permission,
    StoredProcedure,
    Trigger,
    User,
    UserDefinedFunction,
    MasterPartition,
    ServerPartition,
    Topology,
    Schema;

    public boolean isScript() {
        return this == ResourceType.UserDefinedFunction
                || this == ResourceType.Trigger
                || this == ResourceType.StoredProcedure;
    }

    public boolean isPartitioned() {
        return this == ResourceType.Document ||
                this == ResourceType.Attachment ||
                this == ResourceType.Conflict ||
                this == ResourceType.Schema;
    }
}
