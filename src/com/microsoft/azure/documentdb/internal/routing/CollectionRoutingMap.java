package com.microsoft.azure.documentdb.internal.routing;

import java.util.Collection;
import java.util.List;

import com.microsoft.azure.documentdb.PartitionKeyRange;

public interface CollectionRoutingMap {
    List<PartitionKeyRange> getOrderedPartitionKeyRanges();

    PartitionKeyRange getRangeByEffectivePartitionKey(String effectivePartitionKeyValue);

    PartitionKeyRange getRangeByPartitionKeyRangeId(String partitionKeyRangeId);

    Collection<PartitionKeyRange> getOverlappingRanges(Range<String> range);

    Collection<PartitionKeyRange> getOverlappingRanges(Collection<Range<String>> providedPartitionKeyRanges);
}
