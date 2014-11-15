/* 
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 */

package com.microsoft.azure.documentdb;

import org.apache.commons.lang3.text.WordUtils;
import org.json.JSONObject;

/**
 * ConsistencyPolicy
 */
public final class ConsistencyPolicy extends JsonSerializable {
    private static final ConsistencyLevel DEFAULT_DEFAULT_CONSISTENCY_LEVEL =
            ConsistencyLevel.Session;

    private static final int DEFAULT_MAX_STALENESS_INTERVAL = 5;
    private static final int DEFAULT_MAX_STALENESS_PREFIX = 100;

    /*
    private static final int MAX_STALENESS_INTERVAL_IN_SECONDS_MIN_VALUE = 5;
    private static final int MAX_STALENESS_INTERVAL_IN_SECONDS_MAX_VALUE = 600;

    private static final int MAX_STALENESS_PREFIX_MIN_VALUE = 10;
    private static final int MAX_STALENESS_PREFIX_MAX_VALUE = 1000;
    */

    /**
     * Constructor.
     */
    ConsistencyPolicy() {
        super();
    }

    /**
     * Constructor.
     * 
     * @param jsonString the json string that represents the consistency policy.
     */
    public ConsistencyPolicy(String jsonString) {
        super(jsonString);
    }

    /**
     * Constructor.
     * 
     * @param jsonObject the json object that represents the consistency policy.
     */
    public ConsistencyPolicy(JSONObject jsonObject) {
        super(jsonObject);
    }

    /**
     * Get the name of the resource.
     * 
     * @return the default consistency level.
     */
    public ConsistencyLevel getDefaultConsistencyLevel() {

        ConsistencyLevel result = ConsistencyPolicy.DEFAULT_DEFAULT_CONSISTENCY_LEVEL;
        try {
            result = ConsistencyLevel.valueOf(
                    WordUtils.capitalize(super.getString(Constants.Properties.DEFAULT_CONSISTENCY_LEVEL)));
        } catch (IllegalArgumentException e) {
            // ignore the exception and return the default
            e.printStackTrace();
        }
        return result;
    }

    /**
     * Set the name of the resource.
     * 
     * @param level the consistency level.
     */
    public void setDefaultConsistencyLevel(ConsistencyLevel level) {
        super.set(Constants.Properties.DEFAULT_CONSISTENCY_LEVEL, level.name());
    }

    /**
     * Gets the bounded staleness consistency, the maximum allowed staleness in terms difference in sequence numbers
     * (aka version).
     * 
     * @return the max staleness prefix.
     */
    public int getMaxStalenessPrefix() {
        Integer value = super.getInt(Constants.Properties.MAX_STALENESS_PREFIX);
        if (value == null) {
            return ConsistencyPolicy.DEFAULT_MAX_STALENESS_PREFIX;
        }
        return value;
    }

    /**
     * Sets the bounded staleness consistency, the maximum allowed staleness in terms difference in sequence numbers
     * (aka version).
     * 
     * @param maxStalenessPrefix the max staleness prefix.
     */
    public void setMaxStalenessPrefix(int maxStalenessPrefix) {
        super.set(Constants.Properties.MAX_STALENESS_PREFIX, maxStalenessPrefix);
    }

    /**
     * Gets the in bounded staleness consistency, the maximum allowed staleness in terms time interval.
     * 
     * @return the max staleness prefix.
     */
    public int getMaxStalenessIntervalInSeconds() {
        Integer value = super.getInt(Constants.Properties.MAX_STALENESS_INTERVAL_IN_SECONDS);
        if (value == null) {
            return ConsistencyPolicy.DEFAULT_MAX_STALENESS_INTERVAL;
        }
        return value;
    }

    /**
     * Sets the in bounded staleness consistency, the maximum allowed staleness in terms time interval.
     * 
     * @param maxStalenessIntervalInSeconds the max staleness interval in seconds.
     */
    public void setMaxStalenessIntervalInSeconds(int maxStalenessIntervalInSeconds) {
        super.set(Constants.Properties.MAX_STALENESS_INTERVAL_IN_SECONDS, maxStalenessIntervalInSeconds);
    }

    void Validate() {
    }
}
