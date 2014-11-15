/* 
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 */

package com.microsoft.azure.documentdb;

/**
 * Represents the connection mode to be used by the client Direct and Gateway connectivity modes are supported.
 * Gateway is the default.
 */
public enum ConnectionMode {

    /**
     * Use the DocumentDB gateway to route all requests. The gateway proxies requests to the right data partition.
     */
    Gateway

    // TODO(pushi): Implement other connection modes.
}
