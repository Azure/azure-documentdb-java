/* 
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 */

package com.microsoft.azure.documentdb;

/**
 * Specifies the set of access condition types that can be used for operations.
 */
public enum AccessConditionType {
    /**
     * Check if the resource's ETag value matches the ETag value performed.
     */
    IfMatch,

    /**
     * Check if the resource's ETag value does not match ETag value performed.
     */
    IfNoneMatch
}
