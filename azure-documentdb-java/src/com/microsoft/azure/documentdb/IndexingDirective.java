/* 
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 */

package com.microsoft.azure.documentdb;

/**
 * Specifies whether or not the resource is to be indexed.
 */
public enum IndexingDirective {

    /**
     * Use any pre-defined/pre-configured defaults.
     */
    Default,

    /**
     * Index the resource.
     */
    Include,

    /**
     * Do not index the resource.
     */
    Exclude
}
