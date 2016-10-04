/* 
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 */

package com.microsoft.azure.documentdb;

import java.util.Date;

import org.json.JSONObject;

import com.microsoft.azure.documentdb.internal.Constants;

/**
 * Resource type.
 */
public class Resource extends JsonSerializable {

    /**
     * Constructor.
     */
    protected Resource() {
        super();
    }

    /**
     * Constructor.
     *
     * @param jsonString the json string that represents the resource.
     */
    protected Resource(String jsonString) {
        super(jsonString);
    }

    /**
     * Constructor.
     *
     * @param jsonObject the json object that represents the resource.
     */
    protected Resource(JSONObject jsonObject) {
        super(jsonObject);
    }

    /**
     * Gets the name of the resource.
     *
     * @return the name of the resource.
     */
    public String getId() {
        return super.getString(Constants.Properties.ID);
    }

    /**
     * Sets the name of the resource.
     *
     * @param id the name of the resource.
     */
    public void setId(String id) {
        super.set(Constants.Properties.ID, id);
    }

    /**
     * Gets the ID associated with the resource.
     *
     * @return the ID associated with the resource.
     */
    public String getResourceId() {
        return super.getString(Constants.Properties.R_ID);
    }

    /**
     * Set the ID associated with the resource.
     *
     * @param resourceId the ID associated with the resource.
     */
    public void setResourceId(String resourceId) {
        super.set(Constants.Properties.R_ID, resourceId);
    }

    /**
     * Get the self-link associated with the resource.
     *
     * @return the self link.
     */
    public String getSelfLink() {
        return super.getString(Constants.Properties.SELF_LINK);
    }

    /**
     * Set the self-link associated with the resource.
     *
     * @param selfLink the self link.
     */
    void setSelfLink(String selfLink) {
        super.set(Constants.Properties.SELF_LINK, selfLink);
    }

    /**
     * Get the last modified timestamp associated with the resource.
     *
     * @return the timestamp.
     */
    public Date getTimestamp() {
        Double millisec = super.getDouble(Constants.Properties.LAST_MODIFIED);
        if (millisec == null) return null;
        return new Date(millisec.longValue());
    }

    /**
     * Set the last modified timestamp associated with the resource.
     *
     * @param timestamp the timestamp.
     */
    void setTimestamp(Date timestamp) {
        double millisec = timestamp.getTime();
        super.set(Constants.Properties.LAST_MODIFIED, millisec);
    }

    /**
     * Get the entity tag associated with the resource.
     *
     * @return the e tag.
     */
    public String getETag() {
        return super.getString(Constants.Properties.E_TAG);
    }

    /**
     * Set the self-link associated with the resource.
     *
     * @param eTag the e tag.
     */
    void setETag(String eTag) {
        super.set(Constants.Properties.E_TAG, eTag);
    }
}
