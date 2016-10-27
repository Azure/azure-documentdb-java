package com.microsoft.azure.documentdb.internal.routing;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Assert;
import org.junit.Test;

import com.microsoft.azure.documentdb.PartitionKeyRange;

public class RoutingMapProviderHelperTest {
    private static final MockRoutingMapProvider ROUTING_MAP_PROVIDER = new MockRoutingMapProvider(
            Arrays.asList(new PartitionKeyRange("0", "", "000A"), new PartitionKeyRange("1", "000A", "000D"),
                    new PartitionKeyRange("2", "000D", "0012"), new PartitionKeyRange("3", "0012", "0015"),
                    new PartitionKeyRange("4", "0015", "0020"), new PartitionKeyRange("5", "0020", "0040"),
                    new PartitionKeyRange("6", "0040", "FF")));

    private static class MockRoutingMapProvider implements RoutingMapProvider {
        private final CollectionRoutingMap routingMap;

        public MockRoutingMapProvider(Collection<PartitionKeyRange> ranges) {
            List<ImmutablePair<PartitionKeyRange, Boolean>> pairs = new ArrayList<ImmutablePair<PartitionKeyRange, Boolean>>(
                    ranges.size());
            for (PartitionKeyRange range : ranges) {
                pairs.add(new ImmutablePair<PartitionKeyRange, Boolean>(range, true));
            }

            this.routingMap = InMemoryCollectionRoutingMap.tryCreateCompleteRoutingMap(pairs, StringUtils.EMPTY);
        }

        @Override
        public Collection<PartitionKeyRange> getOverlappingRanges(String collectionIdOrNameBasedLink,
                Range<String> range) {
            return this.routingMap.getOverlappingRanges(range);
        }

        @Override
        public PartitionKeyRange tryGetRangeByEffectivePartitionKey(String collectionRid, String effectivePartitionKey) {
            return null;
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNonSortedRanges() {
        RoutingMapProviderHelper.getOverlappingRanges(ROUTING_MAP_PROVIDER, "dbs/db1/colls/coll1",
                Arrays.asList(new Range<String>("0B", "0B", true, true), new Range<String>("0A", "0A", true, true)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testOverlappingRanges1() {
        RoutingMapProviderHelper.getOverlappingRanges(ROUTING_MAP_PROVIDER, "dbs/db1/colls/coll1",
                Arrays.asList(new Range<String>("0A", "0D", true, true), new Range<String>("0B", "0E", true, true)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testOverlappingRanges2() {
        RoutingMapProviderHelper.getOverlappingRanges(ROUTING_MAP_PROVIDER, "dbs/db1/colls/coll1",
                Arrays.asList(new Range<String>("0A", "0D", true, true), new Range<String>("0D", "0E", true, true)));
    }

    @Test
    public void testGetOverlappingRanges() {
        Collection<PartitionKeyRange> ranges = RoutingMapProviderHelper.getOverlappingRanges(ROUTING_MAP_PROVIDER,
                "dbs/db1/colls/coll1",
                Arrays.asList(new Range<String>("000B", "000E", true, false),
                        new Range<String>("000E", "000F", true, false), new Range<String>("000F", "0010", true, true),
                        new Range<String>("0015", "0015", true, true)));

        Function<PartitionKeyRange, String> func = new Function<PartitionKeyRange, String>() {
            @Override
            public String apply(PartitionKeyRange range) {
                return range.getId();
            }
        };

        Assert.assertEquals("1,2,4", ranges.stream().map(func).collect(Collectors.joining(",")));

        // query for minimal point
        ranges = RoutingMapProviderHelper.getOverlappingRanges(ROUTING_MAP_PROVIDER, "dbs/db1/colls/coll1",
                Arrays.asList(new Range<String>("", "", true, true)));

        Assert.assertEquals("0", ranges.stream().map(func).collect(Collectors.joining(",")));

        // query for empty range
        ranges = RoutingMapProviderHelper.getOverlappingRanges(ROUTING_MAP_PROVIDER, "dbs/db1/colls/coll1",
                Arrays.asList(new Range<String>("", "", true, false)));

        Assert.assertEquals(0, ranges.size());

        // entire range
        ranges = RoutingMapProviderHelper.getOverlappingRanges(ROUTING_MAP_PROVIDER, "dbs/db1/colls/coll1",
                Arrays.asList(new Range<String>("", "FF", true, false)));

        Assert.assertEquals("0,1,2,3,4,5,6", ranges.stream().map(func).collect(Collectors.joining(",")));

        // matching range
        ranges = RoutingMapProviderHelper.getOverlappingRanges(ROUTING_MAP_PROVIDER, "dbs/db1/colls/coll1",
                Arrays.asList(new Range<String>("0012", "0015", true, false)));

        Assert.assertEquals("3", ranges.stream().map(func).collect(Collectors.joining(",")));

        // matching range with empty ranges
        ranges = RoutingMapProviderHelper.getOverlappingRanges(ROUTING_MAP_PROVIDER, "dbs/db1/colls/coll1",
                Arrays.asList(new Range<String>("", "", true, false), new Range<String>("0012", "0015", true, false)));

        Assert.assertEquals("3", ranges.stream().map(func).collect(Collectors.joining(",")));

        // matching range and a little bit more.
        ranges = RoutingMapProviderHelper.getOverlappingRanges(ROUTING_MAP_PROVIDER, "dbs/db1/colls/coll1",
                Arrays.asList(new Range<String>("0012", "0015", false, true)));

        Assert.assertEquals("3,4", ranges.stream().map(func).collect(Collectors.joining(",")));
    }
}
