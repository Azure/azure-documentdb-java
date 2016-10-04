package com.microsoft.azure.documentdb.internal.routing;

import java.util.*;
import java.util.concurrent.*;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import com.microsoft.azure.documentdb.*;
import com.microsoft.azure.documentdb.internal.*;

public final class PartitionKeyRangeCache implements RoutingMapProvider {
    private final DocumentQueryClient client;
    private final AsyncCache<String, CollectionRoutingMap> routingMapCache;

    public PartitionKeyRangeCache(DocumentQueryClient client) {
        this.client = client;
        this.routingMapCache = new AsyncCache<String, CollectionRoutingMap>();
    }

    @Override
    public Collection<PartitionKeyRange> getOverlappingRanges(final String collectionIdOrNameBasedLink,
            Range<String> range) {
        CollectionRoutingMap routingMap;
        try {
            routingMap = this.routingMapCache
                    .get(collectionIdOrNameBasedLink, null, new Callable<CollectionRoutingMap>() {
                        @Override
                        public CollectionRoutingMap call() throws Exception {
                            return PartitionKeyRangeCache.this.getRoutingMapForCollection(collectionIdOrNameBasedLink);
                        }
                    }).get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

        return routingMap.getOverlappingRanges(range);
    }

    public void Refresh(String collectionLink) {
        this.routingMapCache.put(collectionLink, this.getRoutingMapForCollection(collectionLink));
    }

    private CollectionRoutingMap getRoutingMapForCollection(String collectionAddress) {
        PathInfo pathInfo = PathsHelper.parsePathSegments(collectionAddress);
        if (pathInfo == null) {
            throw new IllegalArgumentException(String.format("Unable to parse path %s", collectionAddress));
        }
        String collectionLink;
        if (pathInfo.isFeed) {
            collectionLink = PathsHelper.generatePath(ResourceType.DocumentCollection, pathInfo.resourceIdOrFullName,
                    false);
        } else {
            collectionLink = collectionAddress;
        }

        List<ImmutablePair<PartitionKeyRange, Boolean>> ranges = new ArrayList<ImmutablePair<PartitionKeyRange, Boolean>>();
        for (PartitionKeyRange range : client.readPartitionKeyRanges(collectionLink, null).getQueryIterable()) {
            ranges.add(new ImmutablePair<PartitionKeyRange, Boolean>(range, true));
        }

        CollectionRoutingMap routingMap = InMemoryCollectionRoutingMap.tryCreateCompleteRoutingMap(ranges,
                StringUtils.EMPTY);

        if (routingMap == null) {
            throw new IllegalStateException("Cannot create complete routing map");
        }

        return routingMap;
    }
}