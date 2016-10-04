package com.microsoft.azure.documentdb;

import org.json.JSONArray;

/**
 * Represents a partition key value. A partition key identifies the partition
 * where the document is stored in.
 */
public class PartitionKey {

    private final Object[] key;
    private final String keyString;

    /**
     * Constructor. Create a new instance of the PartitionKey object.
     *
     * @param key the value of the partition key.
     */
    public PartitionKey(Object key) {
        this.key = new Object[] {key};
        JSONArray array = new JSONArray(this.key);
        this.keyString = array.toString();
    }

    /**
     * Create a new instance of the PartitionKey object from a serialized JSON string.
     *
     * @param jsonString the JSON string representation of this PartitionKey object.
     * @return the PartitionKey instance.
     */
    public static PartitionKey FromJsonString(String jsonString) {
        JSONArray array = new JSONArray(jsonString);
        PartitionKey key = new PartitionKey(array.get(0));

        return key;
    }

    /**
     * Gets the Key property.
     *
     * @return the value of the partition key.
     */
    Object[] getKey() {
        return this.key;
    }

    /**
     * Serialize the PartitionKey object to a JSON string.
     *
     * @return the string representation of this PartitionKey object.
     */
    public String toString() {
        return this.keyString;
    }
}
