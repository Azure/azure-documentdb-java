package com.microsoft.azure.documentdb;

import org.json.JSONObject;

import com.microsoft.azure.documentdb.internal.Constants;

/**
 * Represents a database user.
 */
public class User extends Resource {

    /**
     * Initialize a user object.
     */
    public User() {
        super();
    }

    /**
     * Initialize a user object from json string.
     *
     * @param jsonString the json string that represents the database user.
     */
    public User(String jsonString) {
        super(jsonString);
    }

    /**
     * Initialize a user object from json object.
     *
     * @param jsonObject the json object that represents the database user.
     */
    public User(JSONObject jsonObject) {
        super(jsonObject);
    }

    /**
     * Gets the self-link of the permissions associated with the user.
     *
     * @return the permissions link.
     */
    public String getPermissionsLink() {
        String selfLink = this.getSelfLink();
        if (selfLink.endsWith("/")) {
            return selfLink + super.getString(Constants.Properties.PERMISSIONS_LINK);
        } else {
            return selfLink + "/" + super.getString(Constants.Properties.PERMISSIONS_LINK);
        }
    }
}
