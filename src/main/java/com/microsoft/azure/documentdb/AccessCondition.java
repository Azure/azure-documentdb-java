/* 
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 */

package com.microsoft.azure.documentdb;

/**
 * Represents a set of access conditions to be used for operations against DocumentDB.
 */
public final class AccessCondition {

    private AccessConditionType type = AccessConditionType.IfMatch;
    private String condition;

    /**
     * Gets the condition type.
     *
     * @return the condition type.
     */
    public AccessConditionType getType() {
        return this.type;
    }

    /**
     * Sets the condition type.
     *
     * @param type the condition type to use.
     */
    public void setType(AccessConditionType type) {
        this.type = type;
    }

    /**
     * Gets the value of the condition - for AccessConditionType IfMatchs and IfNotMatch, this is the ETag that has to
     * be compared to.
     *
     * @return the condition.
     */
    public String getCondition() {
        return this.condition;
    }

    /**
     * Sets the value of the condition - for AccessConditionType IfMatchs and IfNotMatch, this is the ETag that has to
     * be compared to.
     *
     * @param condition the condition to use.
     */
    public void setCondition(String condition) {
        this.condition = condition;
    }
}
