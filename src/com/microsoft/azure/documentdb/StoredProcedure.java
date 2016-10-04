package com.microsoft.azure.documentdb;

import org.json.JSONObject;

import com.microsoft.azure.documentdb.internal.Constants;

/**
 * Represents a stored procedure.
 * <p>
 * DocumentDB allows stored procedures to be executed in the storage tier, directly against a document collection. The
 * script gets executed under ACID transactions on the primary storage partition of the specified collection. For
 * additional details, refer to the server-side JavaScript API documentation.
 */
public class StoredProcedure extends Resource {

    /**
     * Constructor.
     */
    public StoredProcedure() {
        super();
    }

    /**
     * Constructor.
     *
     * @param jsonString the json string that represents the stored procedure.
     */
    public StoredProcedure(String jsonString) {
        super(jsonString);
    }

    /**
     * Constructor.
     *
     * @param jsonObject the json object that represents the stored procedure.
     */
    public StoredProcedure(JSONObject jsonObject) {
        super(jsonObject);
    }

    /**
     * Get the body of the stored procedure.
     *
     * @return the body of the stored procedure.
     */
    public String getBody() {
        return super.getString(Constants.Properties.BODY);
    }

    /**
     * Set the body of the stored procedure.
     *
     * @param body the body of the stored procedure.
     */
    public void setBody(String body) {
        super.set(Constants.Properties.BODY, body);
    }
}

