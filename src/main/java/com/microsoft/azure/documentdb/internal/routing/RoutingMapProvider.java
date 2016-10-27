package com.microsoft.azure.documentdb.internal.routing;

import java.util.Collection;

import com.microsoft.azure.documentdb.PartitionKeyRange;

public interface RoutingMapProvider {
    Collection<PartitionKeyRange> getOverlappingRanges(String collectionIdOrNameBasedLink, Range<String> range);

    PartitionKeyRange tryGetRangeByEffectivePartitionKey(String collectionLink, String effectivePartitionKey);
}
