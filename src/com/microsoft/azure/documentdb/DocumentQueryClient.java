package com.microsoft.azure.documentdb;

import com.microsoft.azure.documentdb.internal.*;
import com.microsoft.azure.documentdb.internal.routing.RoutingMapProvider;

public final class DocumentQueryClient {
    private final DocumentClient innerClient;

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
}
