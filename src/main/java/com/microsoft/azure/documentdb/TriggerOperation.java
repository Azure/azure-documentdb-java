package com.microsoft.azure.documentdb;

/**
 * Specifies the operations on which a trigger should be executed.
 */
public enum TriggerOperation {
    /**
     * All operations.
     */
    All(0x0),

    /**
     * Create operations only.
     */
    Create(0x1),

    /**
     * Update operations only.
     */
    Update(0x2),

    /**
     * Delete operations only.
     */
    Delete(0x3),

    /**
     * Replace operations only.
     */
    Replace(0x4);

    private int value;

    private TriggerOperation(int value) {
        this.value = value;
    }

    /**
     * Gets the numerical value of the trigger operation.
     *
     * @return the numerical value.
     */
    public int getValue() {
        return value;
    }
}
