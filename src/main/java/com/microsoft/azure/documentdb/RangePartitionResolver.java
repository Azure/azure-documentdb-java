/*
The MIT License (MIT)
Copyright (c) 2014 Microsoft Corporation

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

package com.microsoft.azure.documentdb;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * RangePartitionResolver implements partitioning based on the ranges, allowing you to
 * distribute requests and data across a number of partitions by implementing PartitionResolver interface.
 */
@Deprecated
public class RangePartitionResolver<T extends Comparable<T>> implements PartitionResolver {
    private PartitionKeyExtractor partitionKeyExtractor;
    private Map<Range<T>, String> partitionMap;

    /**
     * RangePartitionResolver constructor taking in the PartitionKeyExtractor, a map of Ranges to collection links.
     *
     * @param partitionKeyExtractor an instance of class that implements PartitionKeyExtractor interface.
     * @param partitionMap          the map of ranges to collection links.
     */
    public RangePartitionResolver(PartitionKeyExtractor partitionKeyExtractor, Map<Range<T>, String> partitionMap) {
        if (partitionKeyExtractor == null) {
            throw new IllegalArgumentException("partitionKeyExtractor");
        }

        if (partitionMap == null) {
            throw new IllegalArgumentException("partitionMap");
        }

        this.partitionKeyExtractor = partitionKeyExtractor;
        this.partitionMap = partitionMap;
    }

    /**
     * Resolves the collection for creating the document based on the partition key.
     *
     * @param document the document to be created.
     * @return collection Self link or Name based link which should handle the Create operation.
     */
    @Override
    public String resolveForCreate(Object document) {
        if (document == null) {
            throw new IllegalArgumentException("document");
        }

        Object partitionKey = this.partitionKeyExtractor.getPartitionKey(document);
        Range<T> containingRange = this.getContainingRange(partitionKey);

        if (containingRange == null) {
            throw new UnsupportedOperationException(String.format("A containing range for {0} doesn't exist in the partition map.", partitionKey));
        }

        return this.partitionMap.get(containingRange);
    }

    /**
     * Resolves the collection for reading/querying the documents based on the partition key.
     *
     * @param partitionKey the partition key value.
     * @return collection Self link(s) or Name based link(s) which should handle the Read operation.
     */
    @Override
    public Iterable<String> resolveForRead(Object partitionKey) {
        Set<Range<T>> intersectingRanges = this.getIntersectingRanges(partitionKey);

        ArrayList<String> collectionsLinks = new ArrayList<String>();
        for (Range<T> range : intersectingRanges) {
            collectionsLinks.add(this.partitionMap.get(range));
        }

        return collectionsLinks;
    }

    /**
     * Gets the containing range based on the partition key.
     */
    @SuppressWarnings("unchecked")
    private Range<T> getContainingRange(Object partitionKey) {
        try {
            T TPartitionKey = (T) partitionKey;

            for (Range<T> range : this.partitionMap.keySet()) {
                if (range.contains(TPartitionKey)) {
                    return range;
                }
            }
        } catch (ClassCastException ex) {
            throw new UnsupportedOperationException(String.format("Unsupported type %s for partitionKey.", partitionKey.getClass()), ex.getCause());
        }

        return null;
    }

    /**
     * Gets the intersecting ranges based on the partition key.
     */
    @SuppressWarnings("unchecked")
    private Set<Range<T>> getIntersectingRanges(Object partitionKey) {
        Set<Range<T>> intersectingRanges = new HashSet<Range<T>>();
        Set<Range<T>> partitionKeyRanges = new HashSet<Range<T>>();

        if (partitionKey == null) {
            return this.partitionMap.keySet();
        }

        try {
            // Check the type of partitionKey to be Range<T>. In Java the type information is erased at runtime due to type erasure.
            // We can only check for Range<?> in this case
            if (partitionKey instanceof Range<?>) {
                Range<T> RangeTPartitionKey = (Range<T>) partitionKey;
                partitionKeyRanges.add(RangeTPartitionKey);
            }
            // Check the type of partitionKey to be Iterable<T>. We can only check for Iterable<?> in this case
            else if (partitionKey instanceof Iterable<?>) {
                Iterable<T> IterableTPartitionKey = (Iterable<T>) partitionKey;
                for (T TPartitionKey : IterableTPartitionKey) {
                    // If any of the partition key values is null, we return the complete collection set 
                    if (TPartitionKey == null) {
                        return this.partitionMap.keySet();
                    } else {
                        partitionKeyRanges.add(new Range<T>(TPartitionKey));
                    }
                }
            }
            // Check the type of partitionKey to be T. We can only check for Comparable<?> in this case since T extends Comparable<T>
            else if (partitionKey instanceof Comparable<?>) {
                T TPartitionKey = (T) partitionKey;
                partitionKeyRanges.add(new Range<T>(TPartitionKey));
            } else {
                throw new UnsupportedOperationException(String.format("Unsupported type %s for partitionKey.", partitionKey.getClass()));
            }

            for (Range<T> partitionKeyRange : partitionKeyRanges) {
                for (Range<T> range : this.partitionMap.keySet()) {
                    if (range.intersect(partitionKeyRange)) {
                        intersectingRanges.add(range);
                    }
                }
            }
        } catch (ClassCastException ex) {
            throw new UnsupportedOperationException(String.format("The type parameter of RangePartitionResolver doesn't matches with the type of the partition key passed."), ex.getCause());
        }

        return intersectingRanges;
    }
}
