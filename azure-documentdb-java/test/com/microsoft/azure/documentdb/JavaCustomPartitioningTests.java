package com.microsoft.azure.documentdb;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.HashPartitionResolver;
import com.microsoft.azure.documentdb.QueryIterable;
import com.microsoft.azure.documentdb.Range;
import com.microsoft.azure.documentdb.RangePartitionResolver;
import com.microsoft.azure.documentdb.SqlParameter;
import com.microsoft.azure.documentdb.SqlParameterCollection;
import com.microsoft.azure.documentdb.SqlQuerySpec;

@Deprecated
public class JavaCustomPartitioningTests extends GatewayTestBase {

    @Test
    public void testPartitioning() throws DocumentClientException {

        // Create bunch of collections participating in partitioning
        DocumentCollection collectionDefinition = new DocumentCollection();
        
        collectionDefinition.setId("coll_0");
        DocumentCollection collection0 = client.createCollection(getDatabaseLink(this.databaseForTest, true),
                collectionDefinition,
                null).getResource();
        
        collectionDefinition.setId("coll_1");
        DocumentCollection collection1 = client.createCollection(getDatabaseLink(this.databaseForTest, true),
                collectionDefinition,
                null).getResource();
        
        collectionDefinition.setId("coll_2");
        DocumentCollection collection2 = client.createCollection(getDatabaseLink(this.databaseForTest, true),
                collectionDefinition,
                null).getResource();
        
        // Iterator of ID based collection links
        ArrayList<String> collectionLinks = new ArrayList<String>();
        collectionLinks.add(getDocumentCollectionLink(this.databaseForTest, collection0, true));
        collectionLinks.add(getDocumentCollectionLink(this.databaseForTest, collection1, true));
        collectionLinks.add(getDocumentCollectionLink(this.databaseForTest, collection2, true));
        
        // Instantiate PartitionResolver to be used for partitioning 
        TestPartitionResolver partitionResolver = new TestPartitionResolver(collectionLinks);
        
        // Register the test database with partitionResolver created above
        client.registerPartitionResolver(getDatabaseLink(this.databaseForTest, true), partitionResolver);
        
        // Create document definition used to create documents
        Document documentDefinition = new Document(
                "{" + 
                        "  'id': '0'," +
                        "  'name': 'sample document'," +
                        "  'key': 'value'" + 
                "}");
        
        documentDefinition.setId("0");
        client.createDocument(getDatabaseLink(this.databaseForTest, true),
                documentDefinition,
                null,
                false).getResource();
        
        // Read the documents in collection0 and verify that the count is 1 now
        List<Document> list = client.readDocuments(getDocumentCollectionLink(this.databaseForTest, collection0, true), null).getQueryIterable().toList();
        Assert.assertEquals(1, list.size());
        
        // Verify that it contains the document with Id 0
        Assert.assertEquals("0", list.get(0).getId());
        
        documentDefinition.setId("1");
        client.createDocument(getDatabaseLink(this.databaseForTest, true),
                documentDefinition,
                null,
                false).getResource();
        
        // Read the documents in collection1 and verify that the count is 1 now
        list = client.readDocuments(getDocumentCollectionLink(this.databaseForTest, collection1, true), null).getQueryIterable().toList();
        Assert.assertEquals(1, list.size());
        
        // Verify that it contains the document with Id 1
        Assert.assertEquals("1", list.get(0).getId());
        
        documentDefinition.setId("2");
        client.createDocument(getDatabaseLink(this.databaseForTest, true),
                documentDefinition,
                null,
                false).getResource();
         
        // Read the documents in collection2 and verify that the count is 1 now
        list = client.readDocuments(getDocumentCollectionLink(this.databaseForTest, collection2, true), null).getQueryIterable().toList();
        Assert.assertEquals(1, list.size());
        
        // Verify that it contains the document with Id 2
        Assert.assertEquals("2", list.get(0).getId());
        
        // Updating the value of "key" property to test UpsertDocument(replace scenario)
        documentDefinition.setId("0");
        documentDefinition.set("key", "new value");
        
        client.upsertDocument(getDatabaseLink(this.databaseForTest, true),
                documentDefinition,
                null,
                false).getResource();
         
        // Read the documents in collection0 and verify that the count is still 1
        list = client.readDocuments(getDocumentCollectionLink(this.databaseForTest, collection0, true), null).getQueryIterable().toList();
        Assert.assertEquals(1, list.size());
        
        // Verify that it contains the document with new key value
        Assert.assertEquals(documentDefinition.getString("key"), list.get(0).getString("key"));
        
        // Query documents in all collections(since no partition key specified)
        list = client.queryDocuments(getDatabaseLink(this.databaseForTest, true),
                "SELECT * FROM root r WHERE r.id='0'",
                null).getQueryIterable().toList();
        Assert.assertEquals(1, list.size());
        
        // Updating the value of id property to test UpsertDocument(create scenario)
        documentDefinition.setId("4");
        
        client.upsertDocument(getDatabaseLink(this.databaseForTest, true),
                documentDefinition,
                null,
                false).getResource();
         
        // Read the documents in collection1 and verify that the count is 2 now
        list = client.readDocuments(getDocumentCollectionLink(this.databaseForTest, collection1, true), null).getQueryIterable().toList();
        Assert.assertEquals(2, list.size());
        
        // Query documents in all collections(since no partition key specified)
        list = client.queryDocuments(getDatabaseLink(this.databaseForTest, true),
                new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
                        new SqlParameterCollection(new SqlParameter(
                                "@id", documentDefinition.getId()))),
                null).getQueryIterable().toList();
        Assert.assertEquals(1, list.size());
        
        // Query documents in collection(with partition key of "4" specified) which resolves to collection1
        list = client.queryDocuments(getDatabaseLink(this.databaseForTest, true),
                "SELECT * FROM root r",
                null, documentDefinition.getId()).getQueryIterable().toList();
        Assert.assertEquals(2, list.size());
        
        // Query documents in collection(with partition key "5" specified) which resolves to collection2 but non existent document in that collection
        list = client.queryDocuments(getDatabaseLink(this.databaseForTest, true),
                new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
                        new SqlParameterCollection(new SqlParameter(
                                "@id", documentDefinition.getId()))),
                null, "5").getQueryIterable().toList();
        Assert.assertEquals(0, list.size());
    }
    
    @Test
    public void testPartitionPaging() throws DocumentClientException {
        // Create bunch of collections participating in partitioning
        DocumentCollection collectionDefinition = new DocumentCollection();
        
        String collectionId0 = GatewayTests.getUID();
        collectionDefinition.setId(collectionId0);
        DocumentCollection collection0 = client.createCollection(getDatabaseLink(this.databaseForTest, true),
                collectionDefinition,
                null).getResource();
        
        String collectionId1 = GatewayTests.getUID();
        collectionDefinition.setId(collectionId1);
        DocumentCollection collection1 = client.createCollection(getDatabaseLink(this.databaseForTest, true),
                collectionDefinition,
                null).getResource();
        
        // Iterator of ID based collection links
        ArrayList<String> collectionLinks = new ArrayList<String>();
        collectionLinks.add(getDocumentCollectionLink(this.databaseForTest, collection0, true));
        collectionLinks.add(getDocumentCollectionLink(this.databaseForTest, collection1, true));
        
        // Instantiate PartitionResolver to be used for partitioning 
        TestPartitionResolver partitionResolver = new TestPartitionResolver(collectionLinks);
        
        // Register the test database with partitionResolver created above
        client.registerPartitionResolver(getDatabaseLink(this.databaseForTest, true), partitionResolver);
        
        // Create document definition used to create documents
        Document documentDefinition = new Document(
                "{" + 
                        "  'id': '0'," +
                        "  'name': 'sample document'," +
                        "  'key': 'value'" + 
                "}");
        
        // Create 10 documents each with a different id starting from 0 to 9
        for(int i=0; i<10; i++) {
            documentDefinition.setId(Integer.toString(i));
            try {
                client.createDocument(getDatabaseLink(this.databaseForTest, true),
                        documentDefinition,
                        null,
                        false).getResource();    
                Thread.sleep(1500);
            } catch (InterruptedException ie) {
                System.out.println(ie.getMessage());
            }
        }
        
        // Query the documents to ensure that you get the correct count(no paging)
        List<Document> list = client.queryDocuments(getDatabaseLink(this.databaseForTest, true),
                "SELECT * FROM root r WHERE r.id < '7'",
                null).getQueryIterable().toList();
        
        Assert.assertEquals(7, list.size());
        
        // Setting PageSize to restrict the max number of documents returned
        FeedOptions feedOptions = new FeedOptions();
        feedOptions.setPageSize(3);
        
        QueryIterable<Document> query = client.queryDocuments(getDatabaseLink(this.databaseForTest, true),
                "SELECT * FROM root r WHERE r.id < '7'",
                feedOptions).getQueryIterable();

        // Query again and use the hasNext and next APIs to count the number of documents(with paging)
        int docCount = 0;
        while(query.iterator().hasNext()) {
            if(query.iterator().next() != null)
                docCount++;
        }
        
        Assert.assertEquals(7, docCount);
        
        // Query again to test fetchNextBlock API to ensure that it returns the correct number of documents everytime it's called
        query = client.queryDocuments(getDatabaseLink(this.databaseForTest, true),
                "SELECT * FROM root r WHERE r.id < '7'",
                feedOptions).getQueryIterable();
        
        // Documents with id 0, 2, 4(in collection0)
        Assert.assertEquals(3, query.fetchNextBlock().size());
        // Documents with id 6(in collection0)
        Assert.assertEquals(1, query.fetchNextBlock().size());
        // Documents with id 1, 3, 5(in collection1)
        Assert.assertEquals(3, query.fetchNextBlock().size());
        // No more documents
        Assert.assertNull(query.fetchNextBlock());
        
        // Set PageSzie to -1 to lift the limit on max documents returned by the query
        feedOptions.setPageSize(-1);
        // Query again to test fetchNextBlock API to ensure that it returns the correct number of documents from each collection
        query = client.queryDocuments(getDatabaseLink(this.databaseForTest, true),
                "SELECT * FROM root r WHERE r.id < '7'",
                feedOptions).getQueryIterable();
        
        // Documents with id 0, 2, 4, 6(all docs in collection0 adhering to query condition)
        Assert.assertEquals(4, query.fetchNextBlock().size());
        // Documents with id 1, 3, 5(all docs in collection1 adhering to query condition)
        Assert.assertEquals(3, query.fetchNextBlock().size());
        // No more documents
        Assert.assertNull(query.fetchNextBlock());
    }
    
    @Test
    public void testHashPartitionResolver() {
        ArrayList<String> collectionLinks = new ArrayList<String>();
        DocumentCollection collectionDefinition = new DocumentCollection();
        
        // Create collectionLinks for the hash partition resolver
        collectionDefinition.setId("coll_0");
        collectionLinks.add(getDocumentCollectionLink(this.databaseForTest, collectionDefinition, true));
        
        collectionDefinition.setId("coll_1");
        collectionLinks.add(getDocumentCollectionLink(this.databaseForTest, collectionDefinition, true));
        
        // Instantiating PartitionKeyExtractor which will be used to get the partition key
        // (Id in this case, which is of type String) from the document
        TestIdPartitionKeyExtractor testIdPartitionKeyExtractor = new TestIdPartitionKeyExtractor();
        
        // Instantiate HashPartitionResolver to be used for partitioning 
        HashPartitionResolver hashPartitionResolver = new HashPartitionResolver(testIdPartitionKeyExtractor, collectionLinks);
        
        // Create document definition used to create documents
        Document documentDefinition = new Document(
                "{" + 
                        "  'id': '0'," +
                        "  'name': 'sample document'," +
                        "  'val': 10" + 
                "}");
        
        documentDefinition.setId("2");
        // Get the collection link in which this document will be created
        String createCollectionLink = hashPartitionResolver.resolveForCreate(documentDefinition);
        
        // Get the collection links in which this document is present
        Iterable<String> readCollectionLinks = hashPartitionResolver.resolveForRead(documentDefinition.getId());
        
        // This document can be present only in one collection in which it was created
        ArrayList<String> readCollections = new ArrayList<String>();
        for(String link : readCollectionLinks) {
            readCollections.add(link);
        }
        
        Assert.assertEquals(1, readCollections.size());
        Assert.assertEquals(createCollectionLink, readCollections.get(0));
    }
    
    @Test
    public void testConsistentRing() {
        final int TotalCollectionsCount = 2;
        
        ArrayList<String> collectionLinks = new ArrayList<String>();
        DocumentCollection coll = new DocumentCollection();
        String collLink = StringUtils.EMPTY;
        
        Database databaseDefinition = new Database();
        databaseDefinition.setId("db");
        
        // Populate the collections links for constructing the ring and initialize the hashMap
        for(int i=0; i<TotalCollectionsCount; i++) {
            coll.setId("coll" + i);
            collLink = getDocumentCollectionLink(databaseDefinition, coll, true);
            collectionLinks.add(collLink);
        }
        
        List<Map.Entry<String,Long>> expectedPartitionList= new ArrayList<>();
        expectedPartitionList.add(new AbstractMap.SimpleEntry<>("dbs/db/colls/coll0",1076200484L));
        expectedPartitionList.add(new AbstractMap.SimpleEntry<>("dbs/db/colls/coll0",1302652881L));
        expectedPartitionList.add(new AbstractMap.SimpleEntry<>("dbs/db/colls/coll0",2210251988L));
        expectedPartitionList.add(new AbstractMap.SimpleEntry<>("dbs/db/colls/coll1",2341558382L));
        expectedPartitionList.add(new AbstractMap.SimpleEntry<>("dbs/db/colls/coll0",2348251587L));
        expectedPartitionList.add(new AbstractMap.SimpleEntry<>("dbs/db/colls/coll0",2887945459L));
        expectedPartitionList.add(new AbstractMap.SimpleEntry<>("dbs/db/colls/coll1",2894403633L));
        expectedPartitionList.add(new AbstractMap.SimpleEntry<>("dbs/db/colls/coll1",3031617259L));
        expectedPartitionList.add(new AbstractMap.SimpleEntry<>("dbs/db/colls/coll1",3090861424L));
        expectedPartitionList.add(new AbstractMap.SimpleEntry<>("dbs/db/colls/coll1",4222475028L));
        
        
        // Instantiating PartitionKeyExtractor which will be used to get the partition key
        // (Id in this case) from the document
        TestIdPartitionKeyExtractor testIdPartitionKeyExtractor = new TestIdPartitionKeyExtractor();
        
        // Instantiate HashPartitionResolver to be used for partitioning 
        HashPartitionResolver hashPartitionResolver = new HashPartitionResolver(testIdPartitionKeyExtractor, collectionLinks, 5);
        
        Method method = null;
        
        try {
            Class<?> c = Class.forName("com.microsoft.azure.documentdb.HashPartitionResolver");
            method = c.getDeclaredMethod("getSerializedPartitionList");
            method.setAccessible(true);
        } catch (ClassNotFoundException | NoSuchMethodException | SecurityException e) {
            e.printStackTrace();
        }
        
        try {
            @SuppressWarnings("unchecked")
            List<Map.Entry<String,Long>> actualPartitionMap = (List<Map.Entry<String,Long>>)method.invoke(hashPartitionResolver);
            
            Assert.assertEquals(actualPartitionMap.size(), expectedPartitionList.size());
            
            for(int i=0; i < actualPartitionMap.size(); i++) {
                Assert.assertEquals(actualPartitionMap.get(i).getKey(), expectedPartitionList.get(i).getKey());
                Assert.assertEquals(actualPartitionMap.get(i).getValue(), expectedPartitionList.get(i).getValue());
            }
            
            
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            e.printStackTrace();
        }
        
        Iterable<String> readCollectionLink = hashPartitionResolver.resolveForRead("beadledom");
        ArrayList<String> list = new ArrayList<String>();
        for(String collectionLink : readCollectionLink)
            list.add(collectionLink);
        
        Assert.assertEquals(1, list.size());
        
        coll.setId("coll1");
        collLink = getDocumentCollectionLink(databaseDefinition, coll, true);
        
        Assert.assertEquals(collLink, list.get(0));
        
        // Querying for a document and verifying that it's in the expected collection
        readCollectionLink = hashPartitionResolver.resolveForRead("999");
        list = new ArrayList<String>();
        for(String collectionLink : readCollectionLink)
            list.add(collectionLink);
        
        Assert.assertEquals(1, list.size());
        
        coll.setId("coll0");
        collLink = getDocumentCollectionLink(databaseDefinition, coll, true);
        
        Assert.assertEquals(collLink, list.get(0));
    }
    
    @Test
    public void testMurmurHash() throws UnsupportedEncodingException {
        Method method = null;
        
        // MurmurHash is a package-private class with a private computeHash method and hence using reflection to 
        // call the method and unit testing it
        try {
            Class<?> c = Class.forName("com.microsoft.azure.documentdb.MurmurHash");
            method = c.getDeclaredMethod("computeHash", byte[].class, Integer.TYPE, Integer.TYPE);
            method.setAccessible(true);
        } catch (ClassNotFoundException | NoSuchMethodException | SecurityException e) {
            e.printStackTrace();
        }
        
        try {
            // The following tests are the unit tests for ensuring that MurmurHash
            // returns the same hash across SDKs since we have the same set of tests
            // in .NET and Nodejs checking for the same hash value
            String str = "afdgdd";
            byte[] strBytes = str.getBytes("UTF-8");
            int strHashValue = (int)method.invoke(null, strBytes, strBytes.length, 0);
        
            // Java doesn't has unsigned types and since we need to compare the unsigned 32 bit
            // representation of the returned hash, up-casting the returned value to long 
            // and chopping of everything but the last 32 bits and then comparing the value
            Assert.assertEquals(1099701186L, (long)strHashValue & 0x0FFFFFFFFL);
            
            double num = 374;
            // Java's default "Endianess" is BigEndian but the MurmurHash we are using 
            // across all SDKs assumes the byte order to be LittleEndian, so changing the ByteOrder
            // so that we can compare the hashes as is
            byte[] numBytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(num).array();
            int numHashValue = (int)method.invoke(null, numBytes, numBytes.length, 0);
            
            Assert.assertEquals(3717946798L, (long)numHashValue & 0x0FFFFFFFFL);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            e.printStackTrace();
        }
        
        validate("", 0x1B873593, new byte[]{(byte) 0x67, (byte) 0xA2, (byte) 0xA8, (byte) 0xEE}, 1738713326L, method);
        validate("1", 0xE82562E4, new byte[]{(byte) 0xED, (byte) 0x24, (byte) 0x92, (byte) 0xD0}, 3978597072L, method);
        validate("00", 0xB4C39035, new byte[]{(byte) 0x1B, (byte) 0x64, (byte) 0x09, (byte) 0xFA}, 459540986L, method);
        validate("eyetooth", 0x8161BD86, new byte[]{(byte) 0x6F, (byte) 0x1C, (byte) 0x62, (byte) 0x98}, 1864131224L, method);
        validate("acid", 0x4DFFEAD7, new byte[]{(byte) 0xB9, (byte) 0xC0, (byte) 0x92, (byte) 0x36}, 3116405302L, method);
        validate("elevation", 0x1A9E1828, new byte[]{(byte) 0xDF, (byte) 0x40, (byte) 0xB6, (byte) 0xA9}, 3745560233L, method);
        validate("dent", 0xE73C4579, new byte[]{(byte) 0xD3, (byte) 0xE1, (byte) 0x59, (byte) 0xD4}, 3554761172L, method);
        validate("homeland", 0xB3DA72CA, new byte[]{(byte) 0xBB, (byte) 0x72, (byte) 0x4D, (byte) 0x06}, 3144830214L, method);
        validate("glamor", 0x8078A01B, new byte[]{(byte) 0xA7, (byte) 0xA2, (byte) 0x89, (byte) 0x89}, 2812447113L, method);
        validate("flags", 0x4D16CD6C, new byte[]{(byte) 0x02, (byte) 0x66, (byte) 0x87, (byte) 0x52}, 40273746L, method);
        validate("democracy", 0x19B4FABD, new byte[]{(byte) 0xB0, (byte) 0xD6, (byte) 0x55, (byte) 0xE4}, 2966836708L, method);
        validate("bumble", 0xE653280E, new byte[]{(byte) 0x0C, (byte) 0xC3, (byte) 0xD7, (byte) 0xFE}, 214161406L, method);
        validate("catch", 0xB2F1555F, new byte[]{(byte) 0xCD, (byte) 0xB6, (byte) 0x4B, (byte) 0x98}, 3451276184L, method);
        validate("omnomnomnivore", 0x7F8F82B0, new byte[]{(byte) 0xFF, (byte) 0xCD, (byte) 0xC4, (byte) 0x38}, 4291675192L, method);
        validate("The quick brown fox jumps over the lazy dog", 0x4C2DB001, new byte[]{(byte) 0xC9, (byte) 0x8D, (byte) 0xAB, (byte) 0x6D}, 3381504877L, method);
    }
    
    @Test
    public void testGetBytes() throws UnsupportedEncodingException {
        Method method = null;
        
        // ConsistentHashRing is a package-private class with a private getBytes method and hence using reflection to 
        // call the method and unit testing it
        try {
            Class<?> c = Class.forName("com.microsoft.azure.documentdb.ConsistentHashRing");    
            method = c.getDeclaredMethod("getBytes", Object.class);
            method.setAccessible(true);
        } catch (ClassNotFoundException | NoSuchMethodException | SecurityException e) {
            e.printStackTrace();
        }
        
        try {
            byte[] actualBytes = (byte[])method.invoke(null, "documentdb");
            byte[] expectedBytes = new byte[]{(byte) 0x64, (byte) 0x6F, (byte) 0x63, (byte) 0x75, (byte) 0x6D, (byte) 0x65, (byte) 0x6E, (byte) 0x74, (byte) 0x64, (byte) 0x62};
            Assert.assertTrue(Arrays.equals(expectedBytes, actualBytes));
            
            actualBytes = (byte[])method.invoke(null, "azure");
            expectedBytes = new byte[]{(byte) 0x61, (byte) 0x7A, (byte) 0x75, (byte) 0x72, (byte) 0x65};
            Assert.assertTrue(Arrays.equals(expectedBytes, actualBytes));
            
            actualBytes = (byte[])method.invoke(null, "json");
            expectedBytes = new byte[]{(byte) 0x6A, (byte) 0x73, (byte) 0x6F, (byte) 0x6E};
            Assert.assertTrue(Arrays.equals(expectedBytes, actualBytes));
            
            actualBytes = (byte[])method.invoke(null, "nosql");
            expectedBytes = new byte[]{(byte) 0x6E, (byte) 0x6F, (byte) 0x73, (byte) 0x71, (byte) 0x6C};
            Assert.assertTrue(Arrays.equals(expectedBytes, actualBytes));
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            e.printStackTrace();
        }
    }
    
    @Test
    public void testRangePartitionResolver() {
        ArrayList<String> collectionLinks = new ArrayList<String>();
        DocumentCollection collectionDefinition = new DocumentCollection();
        
        collectionDefinition.setId("coll_0");
        collectionLinks.add(getDocumentCollectionLink(this.databaseForTest, collectionDefinition, true));

        collectionDefinition.setId("coll_1");
        collectionLinks.add(getDocumentCollectionLink(this.databaseForTest, collectionDefinition, true));

        collectionDefinition.setId("coll_2");
        collectionLinks.add(getDocumentCollectionLink(this.databaseForTest, collectionDefinition, true));
        
        // Instantiating PartitionKeyExtractor which will be used to get the partition key
        // (Val in this case) from the document
        TestValPartitionKeyExtractor testValPartitionKeyExtractor = new TestValPartitionKeyExtractor();
        
        // Creating a Map of Ranges with the associated collection to be used in Range partitioning
        Map<Range<Integer>, String> partitionMap = new HashMap<Range<Integer>, String>();
        partitionMap.put(new Range<Integer>(0,400), collectionLinks.get(0));
        partitionMap.put(new Range<Integer>(401,800), collectionLinks.get(1));
        partitionMap.put(new Range<Integer>(501,1200), collectionLinks.get(2));
        
        // Instantiate RangePartitionResolver to be used for partitioning
        // The last parameter Integer.class represents the type of the partition key
        RangePartitionResolver<Integer> rangePartitionResolver = new RangePartitionResolver<Integer>(testValPartitionKeyExtractor, partitionMap);
        
        // Create document definition used to create documents
        Document documentDefinition = new Document(
                "{" + 
                        "  'id': '0'," +
                        "  'name': 'sample document'," +
                        "  'val': 0 " + 
                "}");
        
        // Create document by setting the val property
        documentDefinition.set("val", 400);
        
        // Verify that partition key 400 will fall under collection associated with range (0,400)
        String collectionLink = rangePartitionResolver.resolveForCreate(documentDefinition);
        Assert.assertEquals(collectionLinks.get(0), collectionLink);
        
        Iterable<String> readCollectionLinks = rangePartitionResolver.resolveForRead(600);
        ArrayList<String> list = new ArrayList<String>();
        for(String collLink : readCollectionLinks)
            list.add(collLink);
        
        // Verify that partition key 600 will fall under collection associated with range (401,800) and (401,1200)
        Assert.assertEquals(2, list.size());
        Assert.assertTrue(list.contains(collectionLinks.get(1)));
        Assert.assertTrue(list.contains(collectionLinks.get(2)));
        
        readCollectionLinks = rangePartitionResolver.resolveForRead(new Range<Integer>(250, 500));
        list = new ArrayList<String>();
        for(String collLink : readCollectionLinks)
            list.add(collLink);
        
        // Verify that partition key range (250, 500) will fall under collection associated with range (0,400) and (401,800)
        Assert.assertEquals(2, list.size());
        Assert.assertTrue(list.contains(collectionLinks.get(0)));
        Assert.assertTrue(list.contains(collectionLinks.get(1)));
        
        ArrayList<Integer> partitionKeyList = new ArrayList<Integer>();
        partitionKeyList.add(50);
        partitionKeyList.add(100);
        partitionKeyList.add(600);
        partitionKeyList.add(1000);
        
        readCollectionLinks = rangePartitionResolver.resolveForRead(partitionKeyList);
        list = new ArrayList<String>();
        for(String collLink : readCollectionLinks)
            list.add(collLink);
        
        // Verify that partition key values 50,100,600,1000 will fall under collection associated with range (0,400), (401,800) and (501, 1200)
        Assert.assertEquals(3, list.size());
        Assert.assertTrue(list.contains(collectionLinks.get(0)));
        Assert.assertTrue(list.contains(collectionLinks.get(1)));
        Assert.assertTrue(list.contains(collectionLinks.get(2)));
        
        partitionKeyList = new ArrayList<Integer>();
        partitionKeyList.add(100);
        partitionKeyList.add(null);
        
        readCollectionLinks = rangePartitionResolver.resolveForRead(partitionKeyList);
        list = new ArrayList<String>();
        for(String collLink : readCollectionLinks)
            list.add(collLink);
        
        // Since one of the partition keys is null, we return the complete collection set
        Assert.assertEquals(3, list.size());
        Assert.assertTrue(list.contains(collectionLinks.get(0)));
        Assert.assertTrue(list.contains(collectionLinks.get(1)));
        Assert.assertTrue(list.contains(collectionLinks.get(2)));
    }
    
    private static void validate(String str, int seed, byte[] expectedHashBytes, long expectedValue, Method method) throws UnsupportedEncodingException {
        byte[] bytes = str.getBytes("UTF-8");
        
        try {
            int hashValue = (int)method.invoke(null, bytes, bytes.length, seed);
            Assert.assertEquals(expectedValue, (long)hashValue & 0x0FFFFFFFFL);
            
            byte[] actualHashBytes = ByteBuffer.allocate(4).putInt(hashValue).array();
            Assert.assertTrue(Arrays.equals(expectedHashBytes, actualHashBytes));
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            e.printStackTrace();
        }
    }
}
