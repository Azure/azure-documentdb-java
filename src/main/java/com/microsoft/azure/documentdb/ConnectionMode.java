/* 
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 */

package com.microsoft.azure.documentdb;

/**
 * Represents the connection mode to be used by the client.
 * <p>
 * Direct and Gateway connectivity modes are supported. Gateway is the default.
 * Refer to &lt;see&gt;http://azure.microsoft.com/documentation/articles/documentdb-
 * interactions-with-resources/#connectivity-options&lt;/see&gt; for additional
 * details.
 */
public enum ConnectionMode {

    /**
     * Specifies that requests to server resources are made through a gateway proxy using HTTPS.
     * <p>
     * In Gateway mode, all requests are made through a gateway proxy.
     */
    Gateway,

    /**
     * Specifies that requests to server resources are made directly to the data nodes through HTTPS.
     * <p>
     * In DirectHttps mode, all requests to server resources within a collection, such as documents, stored procedures
     * and user-defined functions, etc., are made directly to the data nodes within
     * the target DocumentDB cluster using the HTTPS transport protocol.  DirectHttps is less efficient than Direct but more
     * efficient than Gateway.
     * <p>
     * Certain operations on account or database level resources, such as databases, collections and users, etc.,
     * are always routed through the gateway using HTTPS.
     */
    DirectHttps
}
