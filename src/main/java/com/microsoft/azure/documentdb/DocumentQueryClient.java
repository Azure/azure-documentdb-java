package com.microsoft.azure.documentdb;

import com.microsoft.azure.documentdb.internal.DocumentServiceRequest;
import com.microsoft.azure.documentdb.internal.DocumentServiceResponse;
import com.microsoft.azure.documentdb.internal.QueryCompatibilityMode;
import com.microsoft.azure.documentdb.internal.ServiceJNIWrapper;
import com.microsoft.azure.documentdb.internal.query.QueryPartitionProvider;
import com.microsoft.azure.documentdb.internal.routing.CollectionCache;
import com.microsoft.azure.documentdb.internal.routing.RoutingMapProvider;

public final class DocumentQueryClient {
    private final DocumentClient innerClient;
    private QueryPartitionProvider queryPartitionProvider;

    public DocumentQueryClient(DocumentClient innerClient) {
        this.innerClient = innerClient;
    }

    public QueryCompatibilityMode getQueryCompatiblityMode() {
        return this.innerClient.getQueryCompatiblityMode();
    }

    public RoutingMapProvider getPartitionKeyRangeCache() {
        return this.innerClient.getPartitionKeyRangeCache();
    }

    public FeedResponse<PartitionKeyRange> readPartitionKeyRanges(String collectionLink, FeedOptions options) {
        return this.innerClient.readPartitionKeyRanges(collectionLink, options);
    }

    public DocumentServiceResponse doReadFeed(DocumentServiceRequest request) throws DocumentClientException {
        return this.innerClient.doReadFeed(request);
    }

    public DocumentServiceResponse doQuery(DocumentServiceRequest request) throws DocumentClientException {
        return this.innerClient.doQuery(request);
    }

    public CollectionCache getCollectionCache() {
        return innerClient.getCollectionCache();
    }

    public QueryPartitionProvider getQueryPartitionProvider() {
        if (this.queryPartitionProvider == null) {
           synchronized (this) {
               if (this.queryPartitionProvider == null) {
                   this.queryPartitionProvider =
                           new QueryPartitionProvider();
               }
           }
        }

        return this.queryPartitionProvider;
    }
}
