/* 
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 */

package com.microsoft.azure.documentdb;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;

final class SessionContainer {
    private final ConcurrentHashMap<Long, String> sessionTokens;
    private final ConcurrentHashMap<String, String> sessionTokensNameBased;
    private final String hostName;

    public SessionContainer(final String hostName) {
        this.hostName = hostName;
        this.sessionTokens = new ConcurrentHashMap<Long, String>();
        this.sessionTokensNameBased = new ConcurrentHashMap<String, String>();
    }

    public String getHostName() {
        return this.hostName;
    }

    public String resolveSessionToken(final DocumentServiceRequest request) {
        if(!request.getIsNameBased()) {
            if(!StringUtils.isEmpty(request.getResourceId())) {
                ResourceId resourceId = ResourceId.parse(request.getResourceId());
                if (resourceId.getDocumentCollection() != 0) {// One token per collection.
                    return this.sessionTokens.get(resourceId.getUniqueDocumentCollectionId());
                }
            }
        }
        else {
            String collectionName = getCollectionName(request.getPath());
            if(!StringUtils.isEmpty(collectionName)) {
                return this.sessionTokensNameBased.get(collectionName);
            }
        }
        return null;
    }

    public void clearToken(final DocumentServiceRequest request, final DocumentServiceResponse response)
    {
        String ownerFullName = response.getResponseHeaders().get(HttpConstants.HttpHeaders.OWNER_FULL_NAME);
        String ownerId = response.getResponseHeaders().get(HttpConstants.HttpHeaders.OWNER_ID);

        String collectionName = getCollectionName(ownerFullName);

        if (!request.getIsNameBased()) {
            ownerId = request.getResourceId();
        }

        if (!StringUtils.isEmpty(ownerId)) {
            ResourceId resourceId = ResourceId.parse(ownerId);
            if (resourceId.getDocumentCollection() != 0 && !StringUtils.isEmpty(collectionName)) {
                this.sessionTokens.remove(resourceId.getUniqueDocumentCollectionId());
                this.sessionTokensNameBased.remove(collectionName);
            }
        }
    }

    public void setSessionToken(DocumentServiceRequest request, DocumentServiceResponse response)
    {
        String sessionToken = response.getResponseHeaders().get(HttpConstants.HttpHeaders.SESSION_TOKEN);

        if (!StringUtils.isEmpty(sessionToken)) {
            String ownerFullName = response.getResponseHeaders().get(HttpConstants.HttpHeaders.OWNER_FULL_NAME);
            String ownerId = response.getResponseHeaders().get(HttpConstants.HttpHeaders.OWNER_ID);

            String collectionName = getCollectionName(ownerFullName);

            if (!request.getIsNameBased()) {
                ownerId = request.getResourceId();
            }

            if (!StringUtils.isEmpty(ownerId)) {
                ResourceId resourceId = ResourceId.parse(ownerId);

                if (resourceId.getDocumentCollection() != 0 && !StringUtils.isEmpty(collectionName)) {
                    long currentTokenValue = !StringUtils.isEmpty(sessionToken) ? Long.parseLong(sessionToken) : 0;

                    String oldToken = this.sessionTokens.get(resourceId.getUniqueDocumentCollectionId());

                    if (oldToken == null) {
                        this.sessionTokens.putIfAbsent(resourceId.getUniqueDocumentCollectionId(), sessionToken);
                    } 
                    else {
                        long existingValue = Long.parseLong(oldToken);
                        if (existingValue < currentTokenValue) {
                            this.sessionTokens.put(resourceId.getUniqueDocumentCollectionId(), sessionToken);
                        }
                    }

                    String oldTokenNameBased = this.sessionTokensNameBased.get(collectionName);

                    if (oldTokenNameBased == null) {
                        this.sessionTokensNameBased.putIfAbsent(collectionName, sessionToken);
                    } 
                    else {
                        long existingValue = Long.parseLong(oldTokenNameBased);
                        if (existingValue < currentTokenValue) {
                            this.sessionTokensNameBased.put(collectionName, sessionToken);
                        }
                    }
                }
            }
        }
    }        

    private String getCollectionName(String resourceFullName)
    {   
        if (resourceFullName != null)
        {
            resourceFullName = Utils.trimBeginingAndEndingSlashes(resourceFullName);

            int slashCount=0;
            for(int i=0; i< resourceFullName.length(); i++)
            {
                if(resourceFullName.charAt(i) == '/') {
                    slashCount++;
                    if(slashCount == 4) {
                        return resourceFullName.substring(0, i);
                    }
                }
            }
        }
        return resourceFullName;
    }
}
