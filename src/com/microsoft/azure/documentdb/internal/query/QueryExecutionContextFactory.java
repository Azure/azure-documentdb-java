package com.microsoft.azure.documentdb.internal.query;

import com.microsoft.azure.documentdb.*;
import com.microsoft.azure.documentdb.internal.ResourceType;

public final class QueryExecutionContextFactory {
    private QueryExecutionContextFactory() {
    }

    public static <T extends Resource> QueryExecutionContext<T> createQueryExecutionContext(DocumentQueryClient client,
            ResourceType resourceType, Class<T> classT, SqlQuerySpec querySpec, FeedOptions options,
            Iterable<String> documentFeedLinks) {
        return new MultiCollectionQueryExecutionContext<T>(client, resourceType, classT, querySpec, options,
                documentFeedLinks);
    }

    public static <T extends Resource> QueryExecutionContext<T> createQueryExecutionContext(DocumentQueryClient client,
            ResourceType resourceType, Class<T> classT, SqlQuerySpec querySpec, FeedOptions options,
            String resourceLink) {
        return new ProxyQueryExecutionContext<T>(client, resourceType, classT, querySpec, options, resourceLink);
    }
}