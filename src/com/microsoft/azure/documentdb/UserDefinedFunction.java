package com.microsoft.azure.documentdb;

import org.json.JSONObject;

import com.microsoft.azure.documentdb.internal.Constants;

/**
 * Represents a user defined function.
 * <p>
 * DocumentDB supports JavaScript UDFs which can be used inside queries, stored procedures and triggers. For additional
 * details, refer to the server-side JavaScript API documentation.
 */
public class UserDefinedFunction extends Resource {

    /**
     * Constructor.
     */
    public UserDefinedFunction() {
        super();
    }

    /**
     * Constructor.
     *
     * @param jsonString the json string that represents the user defined function.
     */
    public UserDefinedFunction(String jsonString) {
        super(jsonString);
    }

    /**
     * Constructor.
     *
     * @param jsonObject the json object that represents the user defined function.
     */
    public UserDefinedFunction(JSONObject jsonObject) {
        super(jsonObject);
    }

    /**
     * Get the body of the user defined function.
     *
     * @return the body.
     */
    public String getBody() {
        return super.getString(Constants.Properties.BODY);
    }

    /**
     * Set the body of the user defined function.
     *
     * @param body the body.
     */
    public void setBody(String body) {
        super.set(Constants.Properties.BODY, body);
    }
}

