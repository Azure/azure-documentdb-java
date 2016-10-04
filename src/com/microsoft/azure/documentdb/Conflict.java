package com.microsoft.azure.documentdb;

import org.json.JSONObject;

import com.microsoft.azure.documentdb.internal.Constants;

/**
 * Represents a conflict in the version of a particular resource.
 * <p>
 * During rare failure scenarios, conflicts are generated for the documents in transit. Clients can inspect the
 * respective conflict instances  for resources and operations in conflict.
 */
public final class Conflict extends Resource {
    /**
     * Initialize a conflict object.
     */
    public Conflict() {
        super();
    }

    /**
     * Initialize a conflict object from json string.
     *
     * @param jsonString the json string that represents the conflict.
     */
    public Conflict(String jsonString) {
        super(jsonString);
    }

    /**
     * Initialize a conflict object from json object.
     *
     * @param jsonObject the json object that represents the conflict.
     */
    public Conflict(JSONObject jsonObject) {
        super(jsonObject);
    }

    /**
     * Gets the operation kind.
     *
     * @return the operation kind.
     */
    public String getOperationKind() {
        return super.getString(Constants.Properties.OPERATION_TYPE);
    }

    /**
     * Gets the type of the conflicting resource.
     *
     * @return the resource type.
     */
    public String getResouceType() {
        return super.getString(Constants.Properties.RESOURCE_TYPE);
    }
}
