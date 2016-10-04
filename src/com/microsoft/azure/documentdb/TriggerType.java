package com.microsoft.azure.documentdb;

/**
 * The trigger type.
 */
public enum TriggerType {
    /**
     * Trigger should be executed before the associated operation(s).
     */
    Pre(0x0),

    /**
     * Trigger should be executed after the associated operation(s).
     */
    Post(0x1);

    private int value;

    private TriggerType(int value) {
        this.value = value;
    }

    /**
     * Gets the numerical value of the trigger type.
     *
     * @return the numerical value.
     */
    public int getValue() {
        return value;
    }
}
