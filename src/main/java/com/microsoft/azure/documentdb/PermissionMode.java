package com.microsoft.azure.documentdb;

/**
 * Enumeration specifying applicability of permission.
 */
public enum PermissionMode {
    /**
     * Permission applicable for read operations only.
     */
    Read(0x1),

    /**
     * Permission applicable for all operations.
     */
    All(0x2);

    private int value;

    private PermissionMode(int value) {
        this.value = value;
    }

    /**
     * Gets the numerical value of the permission mode.
     *
     * @return the numerical value.
     */
    public int getValue() {
        return value;
    }
}
