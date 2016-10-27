package com.microsoft.azure.documentdb.directconnectivity;

import org.json.JSONObject;

import com.microsoft.azure.documentdb.Resource;
import com.microsoft.azure.documentdb.internal.Constants;

public class Address extends Resource {
    /**
     * Initialize an offer object.
     */
    public Address() {
        super();
    }

    /**
     * Initialize an address object from json string.
     *
     * @param jsonString the json string that represents the address.
     */
    public Address(String jsonString) {
        super(jsonString);
    }

    /**
     * Initialize an address object from json object.
     *
     * @param jsonObject the json object that represents the address.
     */
    public Address(JSONObject jsonObject) {
        super(jsonObject);
    }

    public boolean IsPrimary() {
        return super.getBoolean(Constants.Properties.IS_PRIMARY);
    }

    void setIsPrimary(boolean isPrimary) {
        super.set(Constants.Properties.IS_PRIMARY, isPrimary);
    }

    public String getProtocol() {
        return super.getString(Constants.Properties.PROTOCOL);
    }

    void setProtocol(String protocol) {
        super.set(Constants.Properties.PROTOCOL, protocol);
    }

    public String getLogicalUri() {
        return super.getString(Constants.Properties.LOGICAL_URI);
    }

    void setLogicalUri(String logicalUri) {
        super.set(Constants.Properties.LOGICAL_URI, logicalUri);
    }

    public String getPhyicalUri() {
        return super.getString(Constants.Properties.PHYISCAL_URI);
    }

    void setPhysicalUri(String phyicalUri) {
        super.set(Constants.Properties.PHYISCAL_URI, phyicalUri);
    }

    public String getPartitionIndex() {
        return super.getString(Constants.Properties.PARTITION_INDEX);
    }

    void setPartitionIndex(String partitionIndex) {
        super.set(Constants.Properties.PARTITION_INDEX, partitionIndex);
    }

    public String getParitionKeyRangeId() {
        return super.getString(Constants.Properties.PARTITION_KEY_RANGE_ID);
    }

    public void setPartitionKeyRangeId(String partitionKeyRangeId) {
        super.set(Constants.Properties.PARTITION_KEY_RANGE_ID, partitionKeyRangeId);
    }
}
