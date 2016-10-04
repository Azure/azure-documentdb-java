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

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * The ConsistentHashRing class implements a consistent hash ring using the hash function specified
 */
@Deprecated
final class ConsistentHashRing {
    private HashGenerator hashGenerator;
    private Partition[] partitions;
    private ArrayList<String> collectionLinks = new ArrayList<String>();

    /**
     * ConsistentHashRing constructor taking in the collection links, number of partitions per node
     * and hash generator to initialize the ring.
     *
     * @param collectionLinks   the links of collections participating in partitioning.
     * @param partitionsPerNode number of partitions per node.
     * @param hashGenerator     the hash generator to be used for hashing algorithm.
     */
    public ConsistentHashRing(Iterable<String> collectionLinks, int partitionsPerNode, HashGenerator hashGenerator) {
        if (collectionLinks == null) {
            throw new IllegalArgumentException("collectionLinks");
        }

        for (String collectionLink : collectionLinks) {
            this.collectionLinks.add(collectionLink);
        }

        if (partitionsPerNode <= 0) {
            throw new IllegalArgumentException("The partitions per node must greater than 0.");
        }

        if (hashGenerator == null) {
            throw new IllegalArgumentException("hashGenerator");
        }

        this.hashGenerator = hashGenerator;
        this.partitions = this.constructPartitions(this.collectionLinks, partitionsPerNode);
    }

    /**
     * Gets the bytes representing the value of the partition key.
     */
    private static byte[] getBytes(Object partitionKey) {
        byte[] bytes = null;

        // Currently we only support partition key to be of type String
        if (partitionKey instanceof String) {
            String str = (String) partitionKey;
            try {
                bytes = str.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                // If we get this exception, we return bytes as null(as initialized)
            }
        } else {
            throw new UnsupportedOperationException(String.format("Unsupported type %s for partitionKey.", partitionKey.getClass()));
        }

        return bytes;
    }

    /**
     * Searches the partition in the partition array using hashValue
     * and returns the lower bound index
     */
    private static int lowerBoundSearch(Partition[] partitions, byte[] hashValue) {
        for (int i = 0; i < partitions.length - 1; i++) {
            if (partitions[i].compareTo(hashValue) <= 0 && partitions[i + 1].compareTo(hashValue) > 0) {
                return i;
            }
        }

        return partitions.length - 1;
    }

    /**
     * Gets the SelfLink/ID based link of the collection node that maps to the partition key
     * based on the hashing algorithm used for finding the node in the ring.
     *
     * @param partitionKey partition key to be used for finding the node in the ring.
     * @return collection node SelfLink/ID based link mapped to the partition key.
     */
    public String getCollectionNode(Object partitionKey) {
        if (partitionKey == null) {
            throw new IllegalArgumentException("partitionKey");
        }

        // Find the the partition from the total partitions created for the given partition key
        int partition = this.findPartition(getBytes(partitionKey));

        // Returns the name of the collection mapped to that partition
        return this.partitions[partition].getNode();
    }

    /**
     * Constructs the partitions in the consistent ring by assigning them to collection nodes
     * using the hashing algorithm and then finally sorting the partitions based on the hash value.
     */
    private Partition[] constructPartitions(ArrayList<String> collectionLinks, int partitionsPerNode) {
        int collectionsNodeCount = collectionLinks.size();
        Partition[] partitions = new Partition[partitionsPerNode * collectionsNodeCount];

        int index = 0;
        for (String collectionNode : collectionLinks) {
            byte[] hashValue = this.hashGenerator.computeHash(getBytes(collectionNode));

            for (int i = 0; i < partitionsPerNode; ++i) {
                partitions[index++] = new Partition(hashValue, collectionNode);
                hashValue = this.hashGenerator.computeHash(hashValue);
            }
        }

        Arrays.sort(partitions);
        return partitions;
    }

    /**
     * Finds the partition from the byte array representation of the partition key.
     */
    private int findPartition(byte[] key) {
        byte[] hashValue = this.hashGenerator.computeHash(key);
        return lowerBoundSearch(this.partitions, hashValue);
    }

    /**
     * Gets the serialized version of the consistentRing. Added this helper for the test code.
     */
    List<Map.Entry<String, Long>> getSerializedPartitionList() {
        List<Map.Entry<String, Long>> partitionList = new ArrayList<>();

        for (int i = 0; i < this.partitions.length; i++) {
            ByteBuffer wrapped = ByteBuffer.wrap(partitions[i].getHashValue()).order(ByteOrder.LITTLE_ENDIAN);
            int num = wrapped.getInt();
            partitionList.add(new AbstractMap.SimpleEntry<>(partitions[i].getNode(), (long) num & 0x0FFFFFFFFL));
        }

        return partitionList;
    }
}
