package com.microsoft.azure.documentdb.internal.routing;

import java.util.concurrent.Callable;

import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.internal.AsyncCache;
import com.microsoft.azure.documentdb.internal.DocumentServiceRequest;
import com.microsoft.azure.documentdb.internal.PathsHelper;
import com.microsoft.azure.documentdb.internal.ResourceId;

/**
 * Cache to provide resource id lookup based on resource name
 */
public abstract class CollectionCache {
    private final AsyncCache<String, DocumentCollection> collectionInfoByNameCache;
    private final AsyncCache<String, DocumentCollection> collectionInfoByIdCache;

    CollectionCache() {
        collectionInfoByNameCache = new AsyncCache<>();
        collectionInfoByIdCache = new AsyncCache<>();
    }

    /**
     * Resolves a request to a collection in a sticky manner.
     * Unless request.ForceNameCacheRefresh is equal to true, it will return the same collection.
     *
     * @param request Request to resolve
     * @return Instance of DocumentCollection
     */
    public DocumentCollection resolveCollection(DocumentServiceRequest request) {
        if (request.getIsNameBased()) {
            if (request.isForceNameCacheRefresh()) {
                this.refresh(request);
                request.setForceNameCacheRefresh(false);
            }

            if (request.getResolvedCollectionRid() == null) {
                DocumentCollection collectionInfo = this.resolveByName(request.getResourceAddress());
                request.setResolvedCollectionRid(collectionInfo.getResourceId());
                return collectionInfo;
            } else {
                return this.resolveByRid(request.getResolvedCollectionRid());
            }
        } else {
            return this.resolveByRid(request.getResourceAddress());
        }
    }

    private void refresh(final DocumentServiceRequest request) {
        final String resourceFullName = PathsHelper.getCollectionPath(request.getResourceAddress());
        final CollectionCache collectionCache = this;

        if (request.getResolvedCollectionRid() != null) {
            // Here we will issue backend call only if cache wasn't already refreshed
            // (if whatever is there corresponds to previously resolved collection rid).
            DocumentCollection obsoleteValue = new DocumentCollection();
            obsoleteValue.setResourceId(request.getResolvedCollectionRid());
            try {
                this.collectionInfoByNameCache.get(
                        resourceFullName,
                        obsoleteValue,
                        new Callable<DocumentCollection>() {
                            @Override
                            public DocumentCollection call() throws Exception {
                                DocumentCollection collection = collectionCache.getByName(resourceFullName);
                                collectionCache.collectionInfoByIdCache.put(collection.getResourceId(), collection);
                                return collection;
                            }
                        }).get();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        } else {
            // In case of ForceRefresh directive coming from client, there will be no ResolvedCollectionRid, so we
            // need to refresh unconditionally.
            this.refresh(request.getResourceAddress());
        }

        request.setResolvedCollectionRid(null);
    }

    /**
     * This method is only used in client SDK in retry policy as it doesn't have request handy.
     *
     * @param resourceAddress a string represents resource address
     */
    public void refresh(String resourceAddress) {
        if (PathsHelper.isNameBased(resourceAddress)) {
            final String resourceFullName = PathsHelper.getCollectionPath(resourceAddress);
            final CollectionCache collectionCache = this;

            this.collectionInfoByNameCache.refresh(
                    resourceFullName,
                    new Callable<DocumentCollection>() {
                        @Override
                        public DocumentCollection call() throws Exception {
                            DocumentCollection collection = collectionCache.getByName(resourceFullName);
                            collectionCache.collectionInfoByIdCache.put(collection.getResourceId(), collection);
                            return collection;
                        }
                    }
            );
        }
    }

    private DocumentCollection resolveByRid(String resourceId) {
        ResourceId resourceIdParsed = ResourceId.parse(resourceId);
        final String collectionResourceId = resourceIdParsed.getDocumentCollectionId().toString();
        final CollectionCache collectionCache = this;

        try {
            return this.collectionInfoByIdCache.get(
                    collectionResourceId,
                    null,
                    new Callable<DocumentCollection>() {
                        @Override
                        public DocumentCollection call() throws Exception {
                            return collectionCache.getByRid(collectionResourceId);
                        }
                    }).get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private DocumentCollection resolveByName(final String resourceAddress) {
        String resourceFullName = PathsHelper.getCollectionPath(resourceAddress);
        final CollectionCache collectionCache = this;

        try {
            return this.collectionInfoByNameCache.get(
                    resourceFullName,
                    null,
                    new Callable<DocumentCollection>() {
                        @Override
                        public DocumentCollection call() throws Exception {
                            DocumentCollection collection = collectionCache.getByName(resourceAddress);
                            collectionCache.collectionInfoByIdCache.put(collection.getResourceId(), collection);
                            return collection;
                        }
                    }
            ).get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

    }

    protected abstract DocumentCollection getByRid(String collectionRid);

    protected abstract DocumentCollection getByName(String resourceAddress);
}
