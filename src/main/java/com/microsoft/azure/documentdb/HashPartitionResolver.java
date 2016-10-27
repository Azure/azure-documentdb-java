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
import java.util.List;
import java.util.Map;

/**
 * HashPartitionResolver implements partitioning based on the value of a hash function, allowing you to evenly
 * distribute requests and data across a number of partitions by implementing PartitionResolver interface.
 */
@Deprecated
public class HashPartitionResolver implements PartitionResolver {
    // 128 virtual nodes per collections seems to produce good distribution of nodes, so choosing that as default  
    private static final int defaultNumberOfVirtualNodesPerCollection = 128;

    private ConsistentHashRing consistentHashRing;
    private ArrayList<String> collectionLinks = new ArrayList<String>();
    private PartitionKeyExtractor partitionKeyExtractor;

    /**
     * HashPartitionResolver constructor taking in the PartitionKeyExtractor and collection links
     * with default number of virtual nodes per collection(128) and default hash generator(MurmurHash3)
     *
     * @param partitionKeyExtractor an instance of class that implements PartitionKeyExtractor interface
     * @param collectionLinks       the links of collections participating in partitioning
     */
    public HashPartitionResolver(PartitionKeyExtractor partitionKeyExtractor, Iterable<String> collectionLinks) {
        this(partitionKeyExtractor, collectionLinks, defaultNumberOfVirtualNodesPerCollection, null);
    }

    /**
     * HashPartitionResolver constructor taking in the PartitionKeyExtractor, collection Links,
     * number of virtual nodes per collection and default hash generator(MurmurHash3)
     *
     * @param partitionKeyExtractor             an instance of class that implements PartitionKeyExtractor interface
     * @param collectionLinks                   the links of collections participating in partitioning
     * @param numberOfVirtualNodesPerCollection number of virtual nodes per collection
     */
    public HashPartitionResolver(PartitionKeyExtractor partitionKeyExtractor, Iterable<String> collectionLinks, int numberOfVirtualNodesPerCollection) {
        this(partitionKeyExtractor, collectionLinks, numberOfVirtualNodesPerCollection, null);
    }

    /**
     * HashPartitionResolver constructor taking in the PartitionKeyExtractor, collection Links,
     * hash generator with default number of virtual nodes per collection(128)
     *
     * @param partitionKeyExtractor an instance of class that implements PartitionKeyExtractor interface
     * @param collectionLinks       the links of collections participating in partitioning
     * @param hashGenerator         hash generator used for hashing
     */
    public HashPartitionResolver(PartitionKeyExtractor partitionKeyExtractor, Iterable<String> collectionLinks, HashGenerator hashGenerator) {
        this(partitionKeyExtractor, collectionLinks, defaultNumberOfVirtualNodesPerCollection, hashGenerator);
    }

    /**
     * HashPartitionResolver constructor taking in the PartitionKeyExtractor, collection Links,
     * hash generator and number of virtual nodes per collection
     *
     * @param partitionKeyExtractor             an instance of class that implements PartitionKeyExtractor interface
     * @param collectionLinks                   the links of collections participating in partitioning
     * @param numberOfVirtualNodesPerCollection number of virtual nodes per collection
     * @param hashGenerator                     hash generator used for hashing
     */
    public HashPartitionResolver(PartitionKeyExtractor partitionKeyExtractor, Iterable<String> collectionLinks, int numberOfVirtualNodesPerCollection, HashGenerator hashGenerator) {
        if (partitionKeyExtractor == null) {
            throw new IllegalArgumentException("partitionKeyExtractor");
        }

        if (collectionLinks == null) {
            throw new IllegalArgumentException("collectionLinks");
        }

        if (numberOfVirtualNodesPerCollection <= 0) {
            throw new IllegalArgumentException("The number of virtual nodes per collection must greater than 0.");
        }

        this.partitionKeyExtractor = partitionKeyExtractor;

        for (String collectionLink : collectionLinks) {
            this.collectionLinks.add(collectionLink);
        }

        if (hashGenerator == null) {
            hashGenerator = new MurmurHash();
        }

        // Initialize the consistent ring to be used for the hashing algorithm by placing the virtual nodes along the ring
        this.consistentHashRing = new ConsistentHashRing(this.collectionLinks, numberOfVirtualNodesPerCollection, hashGenerator);
    }

    /**
     * Resolves the collection for creating the document based on the partition key.
     *
     * @param document the document to be created
     * @return collection Self link or Name based link which should handle the Create operation.
     */
    @Override
    public String resolveForCreate(Object document) {
        if (document == null) {
            throw new IllegalArgumentException("document");
        }

        Object partitionKey = this.partitionKeyExtractor.getPartitionKey(document);
        return this.consistentHashRing.getCollectionNode(partitionKey);
    }

    /**
     * Resolves the collection for reading/querying the documents based on the partition key.
     *
     * @param partitionKey the partition key value
     * @return collection Self link(s) or Name based link(s) which should handle the Read operation
     */
    @Override
    public Iterable<String> resolveForRead(Object partitionKey) {
        // For Read operations, partitionKey can be null in which case we return all collection links
        if (partitionKey == null) {
            return this.collectionLinks;
        }

        ArrayList<String> collectionLinks = new ArrayList<String>();
        collectionLinks.add(this.consistentHashRing.getCollectionNode(partitionKey));
        return collectionLinks;
    }

    /**
     * Gets the serialized version of the consistentRing. Added this helper for the test code.
     */
    @SuppressWarnings("unused") // used only by test code
    private List<Map.Entry<String, Long>> getSerializedPartitionList() {
        return this.consistentHashRing.getSerializedPartitionList();
    }
}