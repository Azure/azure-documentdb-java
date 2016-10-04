package com.microsoft.azure.documentdb.internal;

/**
 * A client query compatibility mode when making query request. Can be used to force a specific query request
 * format.
 */
public enum QueryCompatibilityMode {
    Default,
    Query,
    SqlQuery
}