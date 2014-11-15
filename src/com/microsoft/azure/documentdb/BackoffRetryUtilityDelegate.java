package com.microsoft.azure.documentdb;

/**
 * The delegate class of the BackoffRetryUtility class. 
 *
 */
abstract interface BackoffRetryUtilityDelegate {
    abstract void apply() throws Exception;
}
