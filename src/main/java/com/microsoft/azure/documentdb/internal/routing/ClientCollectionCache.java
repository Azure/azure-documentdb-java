package com.microsoft.azure.documentdb.internal.routing;

import java.util.logging.Logger;

import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.internal.AuthorizationTokenProvider;
import com.microsoft.azure.documentdb.internal.PathsHelper;
import com.microsoft.azure.documentdb.internal.ResourceType;
import com.microsoft.azure.documentdb.internal.StoreModel;
import com.microsoft.azure.documentdb.internal.Utils;

/**
 * Caches collection information
 */
public class ClientCollectionCache extends CollectionCache {
    private DocumentClient documentClient;

    public ClientCollectionCache(DocumentClient documentClient) {
        this.documentClient = documentClient;
    }

    @Override
    protected DocumentCollection getByRid(String collectionRid) {
        return this.readCollection(
                Utils.joinPath(PathsHelper.generatePath(ResourceType.DocumentCollection, collectionRid, false), ""));
    }

    private DocumentCollection readCollection(String collectionLink) {
        String collectionName = Utils.getCollectionName(collectionLink);
        try {
            return documentClient.readCollection(collectionName, null).getResource();
        } catch (DocumentClientException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected DocumentCollection getByName(String resourceAddress) {
        return this.readCollection(resourceAddress);
    }
}
