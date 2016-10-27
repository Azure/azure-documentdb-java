package com.microsoft.azure.documentdb;

import org.json.JSONObject;

import com.microsoft.azure.documentdb.internal.routing.Range;

public class PartitionKeyRange extends Resource {
    public static final String MINIMUM_INCLUSIVE_EFFECTIVE_PARTITION_KEY = "";
    public static final String MAXIMUM_EXCLUSIVE_EFFECTIVE_PARTITION_KEY = "FF";

    /**
     * Initialize a partition key range object.
     */
    public PartitionKeyRange() {
        super();
    }

    /**
     * Initialize a partition key range object from json string.
     * 
     * @param jsonString
     *            the json string that represents the partition key range
     *            object.
     */
    public PartitionKeyRange(String jsonString) {
        super(jsonString);
    }

    /**
     * Initialize a partition key range object from json object.
     * 
     * @param jsonObject
     *            the json object that represents the partition key range
     *            object.
     */
    public PartitionKeyRange(JSONObject jsonObject) {
        super(jsonObject);
    }

    public PartitionKeyRange(String id, String minInclusive, String maxExclusive) {
        super();
        this.setId(id);
        this.setMinInclusive(minInclusive);
        this.setMaxExclusive(maxExclusive);
    }

    public String getMinInclusive() {
        return super.getString("minInclusive");
    }

    public void setMinInclusive(String minInclusive) {
        super.set("minInclusive", minInclusive);
    }

    public String getMaxExclusive() {
        return super.getString("maxExclusive");
    }

    public void setMaxExclusive(String maxExclusive) {
        super.set("maxExclusive", maxExclusive);
    }

    public Range<String> toRange() {
        return new Range<String>(this.getMinInclusive(), this.getMaxExclusive(), true, false);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PartitionKeyRange)) {
            return false;
        }

        PartitionKeyRange otherRange = (PartitionKeyRange) obj;

        return this.getId().compareTo(otherRange.getId()) == 0
                && this.getMinInclusive().compareTo(otherRange.getMinInclusive()) == 0
                && this.getMaxExclusive().compareTo(otherRange.getMaxExclusive()) == 0;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        hash = (hash * 397) ^ this.getId().hashCode();
        hash = (hash * 397) ^ this.getMinInclusive().hashCode();
        hash = (hash * 397) ^ this.getMaxExclusive().hashCode();
        return hash;
    }
}
