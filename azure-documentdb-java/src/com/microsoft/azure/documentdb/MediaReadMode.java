/* 
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 */

package com.microsoft.azure.documentdb;

/**
 * Represents the mode for use with downloading attachment content (aka media).
 */
public enum MediaReadMode {

    /**
     * Content is buffered at the client and not directly streamed from the
     * content store. Use Buffered to reduce the time taken to read and write
     * media files.
     */
    Buffered,

    /**
     * Content is directly streamed from the content store without any buffering
     * at the client. Use Streamed to reduce the client memory overhead of
     * reading and writing media files.
     */
    Streamed
}
