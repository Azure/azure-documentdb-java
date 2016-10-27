package com.microsoft.azure.documentdb;

import org.json.JSONObject;

import com.microsoft.azure.documentdb.internal.Constants;

public class DatabaseAccountLocation extends JsonSerializable {

    /**
     * Default Constructor. Creates a new instance of the
     * DatabaseAccountLocation object.
     */
    DatabaseAccountLocation() {
        super();
    }

    /**
     * Creates a new instance of the DatabaseAccountLocation object from a JSON
     * string.
     *
     * @param jsonString the JSON string that represents the DatabaseAccountLocation object.
     */
    public DatabaseAccountLocation(String jsonString) {
        super(jsonString);
    }

    /**
     * Creates a new instance of the DatabaseAccountLocation object from a
     * JSON object.
     *
     * @param jsonObject the JSON object that represents the DatabaseAccountLocation object.
     */
    public DatabaseAccountLocation(JSONObject jsonObject) {
        super(jsonObject);
    }

    /**
     * Gets The name of the database account location.
     *
     * @return the name of the database account location.
     */
    public String getName() {
        return super.getString(Constants.Properties.Name);
    }

    /**
     * Sets the name of the database account location.
     *
     * @param name the name of the database account location.
     */
    public void setName(String name) {
        super.set(Constants.Properties.Name, name);
    }

    /**
     * Gets The endpoint (the URI) of the database account location.
     *
     * @return the endpoint of the database account location.
     */
    public String getEndpoint() {
        return super.getString(Constants.Properties.DATABASE_ACCOUNT_ENDPOINT);
    }

    /**
     * Sets the endpoint (the URI) of the database account location.
     *
     * @param endpoint the endpoint of the database account location.
     */
    public void setEndpoint(String endpoint) {
        super.set(Constants.Properties.DATABASE_ACCOUNT_ENDPOINT, endpoint);
    }
}
