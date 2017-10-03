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

import org.junit.Assert;

import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.PartitionResolver;

/**
 * This is a test PartitionResolver used for resolving the collection(s) based on the partitionKey.
 * This class implements PartitionResolver interface.
 */
@Deprecated
public class TestPartitionResolver implements PartitionResolver {
    // Self Links or ID based Links for the collections participating in partitioning
    private ArrayList<String> collectionLinks = new ArrayList<String>();
    
    /**
     * Initializes the TestPartitionResolver with the list of collections participating in partitioning
     */
    public TestPartitionResolver(Iterable<String> collectionLinks) {
        if(collectionLinks == null) {
            throw new IllegalArgumentException("collectionLinks");
        }
       
        for(String collectionLink : collectionLinks) {
            this.collectionLinks.add(collectionLink);
        }
    }

    /**
     * Resolves the collection for creating the document based on the partition key
     * @return collection Self link or Name based link which should handle the Create operation
     */
    @Override
    public String resolveForCreate(Object document) {
        if(document == null) {
            throw new IllegalArgumentException("document");
        }
        
        String partitionKey = this.getPartitionKey(document);
        return this.getDocumentCollection(this.getHashCode(partitionKey));
    }

    /**
     * Resolves the collection for reading/querying the documents based on the partition key
     * @return collection Self link(s) or Name based link(s) which should handle the Read operation
     */
    @Override
    public Iterable<String> resolveForRead(Object partitionKey) {
        // For Read operations, partitionKey can be null in which case we return all collection links
        if(partitionKey == null) {
            return this.collectionLinks;
        }
        
        ArrayList<String> collectionLinks = new ArrayList<String>();
        collectionLinks.add(this.getDocumentCollection(this.getHashCode(partitionKey)));
        return collectionLinks;
    }
    
    /**
     * Calculates the partition key from the document passed
     * @return the partition key.
     */
    private String getPartitionKey(Object document) {
        // For this TestPartitionResolver, we expect document to be of type Document 
        Assert.assertTrue("document should be of type Document", document instanceof Document);
        
        // For this TestPartitionResolver, Id property of the document is the partition key
        return ((Document)document).getId();
    }
    
    /**
     * Calculates the hashCode from the partition key 
     * @return hashCode used to resolve the collection
     */
    private int getHashCode(Object partitionKey) {
        // For this TestPartitionResolver, we expect partitionKey to be of type String since we have chosen it to be Id property 
        Assert.assertTrue("partitionKey should be of type String", partitionKey instanceof String);
        
        // Test will fail here if we get a NumberFormatException and will display the exception message to the output console
        return Integer.parseInt((String)partitionKey);
    }
    
    /**
     * Gets the Document Collection from the hash code 
     * @return collection Self link or Name based link
     */
    private String getDocumentCollection(int hashCode) {
        return this.collectionLinks.get(Math.abs(hashCode) % this.collectionLinks.size());
    }
}
