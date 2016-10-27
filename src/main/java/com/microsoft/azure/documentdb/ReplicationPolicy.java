package com.microsoft.azure.documentdb;

import org.json.JSONObject;

import com.microsoft.azure.documentdb.internal.Constants;

public class ReplicationPolicy extends JsonSerializable {
    private static final int DEFAULT_MAX_REPLICA_SET_SIZE = 4;
    private static final int DEFAULT_MIN_REPLICA_SET_SIZE = 3;

    ReplicationPolicy() {
    }

    /**
     * Constructor.
     *
     * @param jsonString the json string that represents the replication policy.
     */
    public ReplicationPolicy(String jsonString) {
        super(jsonString);
    }

    /**
     * Constructor.
     *
     * @param jsonObject the json object that represents the replication policy.
     */
    public ReplicationPolicy(JSONObject jsonObject) {
        super(jsonObject);
    }


    public int getMaxReplicaSetSize() {
        Integer maxReplicaSetSize = super.getInt(Constants.Properties.MAX_REPLICA_SET_SIZE);
        if (maxReplicaSetSize == null) {
            return DEFAULT_MAX_REPLICA_SET_SIZE;
        }

        return maxReplicaSetSize;
    }

    public int getMinReplicaSetSize() {
        Integer minReplicaSetSize = super.getInt(Constants.Properties.MIN_REPLICA_SET_SIZE);
        if (minReplicaSetSize == null) {
            return DEFAULT_MIN_REPLICA_SET_SIZE;
        }

        return minReplicaSetSize;
    }
}
