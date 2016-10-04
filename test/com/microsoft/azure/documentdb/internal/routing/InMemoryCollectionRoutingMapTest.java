package com.microsoft.azure.documentdb.internal.routing;

import java.util.*;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Assert;
import org.junit.Test;

import com.microsoft.azure.documentdb.PartitionKeyRange;

public class InMemoryCollectionRoutingMapTest {
    @Test
    public void testCollectionRoutingMap() {
        InMemoryCollectionRoutingMap<Integer> routingMap = InMemoryCollectionRoutingMap
                .tryCreateCompleteRoutingMap(Arrays.asList(
                        new ImmutablePair<PartitionKeyRange, Integer>(
                                new PartitionKeyRange("2", "0000000050", "0000000070"), 2),
                        new ImmutablePair<PartitionKeyRange, Integer>(new PartitionKeyRange("0", "", "0000000030"), 0),
                        new ImmutablePair<PartitionKeyRange, Integer>(
                                new PartitionKeyRange("1", "0000000030", "0000000050"), 1),
                        new ImmutablePair<PartitionKeyRange, Integer>(new PartitionKeyRange("3", "0000000070", "FF"),
                                3)),
                        StringUtils.EMPTY);

        Assert.assertEquals("0", routingMap.getOrderedPartitionKeyRanges().get(0).getId());
        Assert.assertEquals("1", routingMap.getOrderedPartitionKeyRanges().get(1).getId());
        Assert.assertEquals("2", routingMap.getOrderedPartitionKeyRanges().get(2).getId());
        Assert.assertEquals("3", routingMap.getOrderedPartitionKeyRanges().get(3).getId());

        Assert.assertEquals((Integer) 0, routingMap.getOrderedPartitionInfo().get(0));
        Assert.assertEquals((Integer) 1, routingMap.getOrderedPartitionInfo().get(1));
        Assert.assertEquals((Integer) 2, routingMap.getOrderedPartitionInfo().get(2));
        Assert.assertEquals((Integer) 3, routingMap.getOrderedPartitionInfo().get(3));

        Assert.assertEquals("0", routingMap.getRangeByEffectivePartitionKey("").getId());
        Assert.assertEquals("0", routingMap.getRangeByEffectivePartitionKey("0000000000").getId());
        Assert.assertEquals("1", routingMap.getRangeByEffectivePartitionKey("0000000030").getId());
        Assert.assertEquals("1", routingMap.getRangeByEffectivePartitionKey("0000000031").getId());
        Assert.assertEquals("3", routingMap.getRangeByEffectivePartitionKey("0000000071").getId());

        Assert.assertEquals("0", routingMap.getRangeByPartitionKeyRangeId("0").getId());
        Assert.assertEquals("1", routingMap.getRangeByPartitionKeyRangeId("1").getId());

        Assert.assertEquals(4,
                routingMap
                        .getOverlappingRanges(Arrays
                                .asList(new Range<String>(PartitionKeyRange.MINIMUM_INCLUSIVE_EFFECTIVE_PARTITION_KEY,
                                        PartitionKeyRange.MAXIMUM_EXCLUSIVE_EFFECTIVE_PARTITION_KEY, true, false)))
                        .size());
        Assert.assertEquals(0,
                routingMap
                        .getOverlappingRanges(Arrays
                                .asList(new Range<String>(PartitionKeyRange.MINIMUM_INCLUSIVE_EFFECTIVE_PARTITION_KEY,
                                        PartitionKeyRange.MINIMUM_INCLUSIVE_EFFECTIVE_PARTITION_KEY, false, false)))
                        .size());

        Collection<PartitionKeyRange> partitionKeyRanges = routingMap
                .getOverlappingRanges(Arrays.asList(new Range<String>("0000000040", "0000000040", true, true)));

        Assert.assertEquals(1, partitionKeyRanges.size());
        Iterator<PartitionKeyRange> iterator = partitionKeyRanges.iterator();
        Assert.assertEquals("1", iterator.next().getId());

        Collection<PartitionKeyRange> partitionKeyRanges1 = routingMap
                .getOverlappingRanges(Arrays.asList(new Range<String>("0000000040", "0000000045", true, true),
                        new Range<String>("0000000045", "0000000046", true, true),
                        new Range<String>("0000000046", "0000000050", true, true)));

        Assert.assertEquals(2, partitionKeyRanges1.size());
        Iterator<PartitionKeyRange> iterator1 = partitionKeyRanges1.iterator();
        Assert.assertEquals("1", iterator1.next().getId());
        Assert.assertEquals("2", iterator1.next().getId());
    }

    @Test(expected = IllegalStateException.class)
    public void testInvalidRoutingMap() {
        InMemoryCollectionRoutingMap.tryCreateCompleteRoutingMap(Arrays.asList(
                new ImmutablePair<PartitionKeyRange, Integer>(new PartitionKeyRange("1", "0000000020", "0000000030"),
                        2),
                new ImmutablePair<PartitionKeyRange, Integer>(new PartitionKeyRange("2", "0000000025", "0000000035"),
                        2)),
                StringUtils.EMPTY);
    }

    @Test
    public void testIncompleteRoutingMap() {
        InMemoryCollectionRoutingMap<Integer> routingMap = InMemoryCollectionRoutingMap
                .tryCreateCompleteRoutingMap(Arrays.asList(
                        new ImmutablePair<PartitionKeyRange, Integer>(new PartitionKeyRange("2", "", "0000000030"), 2),
                        new ImmutablePair<PartitionKeyRange, Integer>(new PartitionKeyRange("3", "0000000031", "FF"),
                                2)),
                        StringUtils.EMPTY);

        Assert.assertNull(routingMap);

        routingMap = InMemoryCollectionRoutingMap.tryCreateCompleteRoutingMap(Arrays.asList(
                new ImmutablePair<PartitionKeyRange, Integer>(new PartitionKeyRange("2", "", "0000000030"), 2),
                new ImmutablePair<PartitionKeyRange, Integer>(new PartitionKeyRange("2", "0000000030", "FF"), 2)),
                StringUtils.EMPTY);

        Assert.assertNotNull(routingMap);
    }
}
