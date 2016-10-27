/*
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 */

package com.microsoft.azure.documentdb;

import org.apache.commons.lang3.text.WordUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import com.microsoft.azure.documentdb.internal.Constants;

/**
 * Represents a per-User Permission to access a specific resource e.g. Document or Collection
 */
public class Permission extends Resource {
    /**
     * Initialize a permission object.
     */
    public Permission() {
        super();
    }

    /**
     * Initialize a permission object from json string.
     *
     * @param jsonString the json string that represents the permission.
     */
    public Permission(String jsonString) {
        super(jsonString);
    }

    /**
     * Initialize a permission object from json object.
     *
     * @param jsonObject the json object that represents the permission.
     */
    public Permission(JSONObject jsonObject) {
        super(jsonObject);
    }

    /**
     * Gets the self-link of resource to which the permission applies.
     *
     * @return the resource link.
     */
    public String getResourceLink() {
        return super.getString(Constants.Properties.RESOURCE_LINK);
    }

    /**
     * Sets the self-link of resource to which the permission applies.
     *
     * @param resourceLink the resource link.
     */
    public void setResourceLink(String resourceLink) {
        super.set(Constants.Properties.RESOURCE_LINK, resourceLink);
    }

    /**
     * Gets the permission mode.
     *
     * @return the permission mode.
     */
    public PermissionMode getPermissionMode() {
        String value = super.getString(Constants.Properties.PERMISSION_MODE);
        return PermissionMode.valueOf(WordUtils.capitalize(value));
    }

    /**
     * Sets the permission mode.
     *
     * @param permissionMode the permission mode.
     */
    public void setPermissionMode(PermissionMode permissionMode) {
        this.set(Constants.Properties.PERMISSION_MODE,
                permissionMode.name().toLowerCase());
    }

    /**
     * Gets the access token granting the defined permission.
     *
     * @return the access token.
     */
    public String getToken() {
        return super.getString(Constants.Properties.TOKEN);
    }

    /**
     * Gets the resource partition key associated with this permission object.
     *
     * @return the partition key.
     */
    public PartitionKey getResourcePartitionKey() {
        PartitionKey key = null;
        Object value = super.get(Constants.Properties.RESOURCE_PARTITION_KEY);
        if (value != null) {
            JSONArray arrayValue = (JSONArray) value;
            key = new PartitionKey(arrayValue.get(0));
        }

        return key;
    }

    /**
     * Sets the resource partition key associated with this permission object.
     *
     * @param partitionkey the partition key.
     */
    public void setResourcePartitionKey(PartitionKey partitionkey) {
        super.set(Constants.Properties.RESOURCE_PARTITION_KEY, partitionkey.getKey());
    }
}
