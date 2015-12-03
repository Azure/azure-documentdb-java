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

final class Partition implements Comparable<Partition> {
    private byte[] hashValue;
    private String node;
     
    public Partition(byte[] hashValue, String node) {
        if(hashValue == null) {
            throw new IllegalArgumentException("hashValue");
        }
        if(node == null) {
            throw new IllegalArgumentException("node");
        }
        
        this.hashValue = hashValue;
        this.node = node;
    }
    
    public byte[] getHashValue() {
        return this.hashValue;
    }
    
    public String getNode() {
        return this.node;
    }
    
    @Override
    public int compareTo(Partition otherPartition) {
        return compareTo(otherPartition.hashValue);
    }
    
    public int compareTo(byte[] otherHashValue) {
        return CompareHashValues(this.hashValue, otherHashValue);
    }
    
    private static int CompareHashValues(byte[] hash1, byte[] hash2) {
        if(hash1.length != hash2.length)
            throw new IllegalArgumentException("Length of hashes doesn't match.");
        
        for (int i = 0; i < hash1.length; i++) {
            if (hash1[i] < hash2[i]) {
                return -1;
            }
            else if (hash1[i] > hash2[i]) {
                return 1;
            }
        }

        return 0;
    }
}
