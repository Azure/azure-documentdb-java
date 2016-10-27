/*
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 */

package com.microsoft.azure.documentdb;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

import com.microsoft.azure.documentdb.internal.Constants;

/**
 * Represents a Database. A database manages users, permissions and a set of collections
 * <p>
 * Each Azure DocumentDB Service is able to support multiple independent named databases, with the database being the
 * logical container for data. Each Database consists of one or more collections, each of which in turn contain one or
 * more documents. Since databases are an an administrative resource and the Service Master Key will be required in
 * order to access and successfully complete any action using the User APIs.
 */
public final class Database extends Resource {

    /**
     * Initialize a database object.
     */
    public Database() {
        super();
    }

    /**
     * Initialize a database object from json string.
     *
     * @param jsonString the json string.
     */
    public Database(String jsonString) {
        super(jsonString);
    }

    /**
     * Initialize a database object from json string.
     *
     * @param jsonObject the json object.
     */
    public Database(JSONObject jsonObject) {
        super(jsonObject);
    }

    /**
     * Gets the self-link for collections in the database
     *
     * @return the collections link.
     */
    public String getCollectionsLink() {
        return String.format("%s/%s",
                StringUtils.stripEnd(super.getSelfLink(), "/"),
                super.getString(Constants.Properties.COLLECTIONS_LINK));
    }

    /**
     * Gets the self-link for users in the database.
     *
     * @return the users link.
     */
    public String getUsersLink() {
        return String.format("%s/%s",
                StringUtils.stripEnd(super.getSelfLink(), "/"),
                super.getString(Constants.Properties.USERS_LINK));
    }
}
