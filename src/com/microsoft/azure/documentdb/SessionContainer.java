/* 
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 */

package com.microsoft.azure.documentdb;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;

final class SessionContainer {
    private final ConcurrentHashMap<Long, String> sessionTokens;
    private final String hostName;

    public SessionContainer(final String hostName) {
        this.hostName = hostName;
        this.sessionTokens = new ConcurrentHashMap<Long, String>();
    }

    public String getHostName() {
        return this.hostName;
    }

    public String resolveSessionToken(final ResourceId resourceId) {
        if (resourceId.getDocumentCollection() != 0) // One token per collection.
        {
            return this.sessionTokens.get(resourceId.getUniqueDocumentCollectionId());

        }
        return null;
    }

    public void clearToken(final ResourceId resourceId) {
        if (resourceId.getDocumentCollection() != 0) {
            this.sessionTokens.remove(resourceId.getUniqueDocumentCollectionId());
        }
    }

    public void setSessionToken(ResourceId resourceId, String token) {
        if (resourceId.getDocumentCollection() != 0) {
            long currentTokenValue = !StringUtils.isEmpty(token) ? Long.parseLong(token) : 0;

            String oldToken = this.sessionTokens.get(resourceId.getUniqueDocumentCollectionId());

            if (oldToken == null) {
                this.sessionTokens.putIfAbsent(resourceId.getUniqueDocumentCollectionId(), token);
            } else {
                long existingValue = Long.parseLong(oldToken);
                if (existingValue < currentTokenValue) {
                    this.sessionTokens.put(resourceId.getUniqueDocumentCollectionId(), token);
                }
            }

        }
    }

}
