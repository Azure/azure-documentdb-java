package com.microsoft.azure.documentdb;

import org.junit.Assert;
import org.junit.Test;

import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.RequestOptions;

public class JavaTtlTests extends GatewayTestBase {

    @Test
    public void testCollectionDefaultTtl() throws DocumentClientException {
        String collectionId = GatewayTestBase.getUID();        
        RequestOptions options = new RequestOptions();
        options.setOfferThroughput(400);
        
        Integer ttl = 5;
        DocumentCollection collectionDefinition = new DocumentCollection();
        collectionDefinition.setDefaultTimeToLive(ttl);
        
        collectionDefinition.setId(collectionId);
        DocumentCollection createdCollection = this.client.createCollection(
                getDatabaseLink(this.databaseForTest, true),
                collectionDefinition,
                options).getResource();
        Assert.assertEquals(ttl, createdCollection.getDefaultTimeToLive());
        
        String sampleDocumentTemplate =
                "{" +
                        "  'id': '%s'," +
                        "  'name': 'sample document %s'," +
                "}";
        String documentId = GatewayTests.getUID();
        Document sampleDocument = new Document(String.format(sampleDocumentTemplate, documentId, documentId));
        Document createdDocument = this.client.createDocument(
                createdCollection.getSelfLink(),
                sampleDocument,
                null,
                false).getResource();
        
        Document document = this.client.readDocument(createdDocument.getSelfLink(), null).getResource();
        Assert.assertEquals(documentId, document.getString("id"));
        
        // Wait for document to expired and be deleted in the background.       
        try {
            Thread.sleep(25000);
        } catch (InterruptedException e) {
            Assert.assertTrue("Sleep interrupted.", false);
        }
        
        Boolean readFail = false;
        try {
            document = this.client.readDocument(createdDocument.getSelfLink(), null).getResource();
        } catch (DocumentClientException exp) {
            Assert.assertEquals(404, exp.getStatusCode());
            Assert.assertEquals("NotFound", exp.getError().getCode());
            readFail = true;
        }
        Assert.assertTrue(readFail);
    }
    
    @Test
    public void testDocumentOverrideCollectionTtlPositive() throws DocumentClientException {
        this.testDocumentOverride(60, 5, 6, false);
        this.testDocumentOverride(5, 60, 6, true);
        this.testDocumentOverride(5, -1, 6, true);
        this.testDocumentOverride(5, null, 6, false);
    }
    
    @Test
    public void testDocumentOverrideCollectionTtlNegative() throws DocumentClientException {
        this.testDocumentOverride(-1, 5, 6, false);
        this.testDocumentOverride(-1, null, 6, true);
    }
    
    @Test
    public void testDocumentOverrideCollectionTtlNull() throws DocumentClientException {
        this.testDocumentOverride(null, 2, 5, true);
        this.testDocumentOverride(null, -1, 5, true);
    }
    
    @Test
    public void testRemoveDefaultTtl() throws DocumentClientException {
        String collectionId = GatewayTestBase.getUID();        
        RequestOptions options = new RequestOptions();
        options.setOfferThroughput(400);
        
        Integer defaultTtl = 5;
        DocumentCollection collectionDefinition = new DocumentCollection();
        collectionDefinition.setDefaultTimeToLive(defaultTtl);
        
        collectionDefinition.setId(collectionId);
        DocumentCollection createdCollection = this.client.createCollection(
                getDatabaseLink(this.databaseForTest, true),
                collectionDefinition,
                options).getResource();
        Assert.assertEquals(collectionId, createdCollection.getString("id"));
        Assert.assertEquals(defaultTtl, createdCollection.getDefaultTimeToLive());

        // Remove the default ttl by setting it to null.
        createdCollection.setDefaultTimeToLive(null);
        createdCollection = this.client.replaceCollection(createdCollection, null).getResource();
        Assert.assertNull(createdCollection.getDefaultTimeToLive());
        
        String sampleDocumentTemplate =
                "{" +
                        "  'id': '%s'," +
                        "  'name': 'sample document %s'," +
                "}";
        String documentId = GatewayTests.getUID();
        Document sampleDocument = new Document(String.format(sampleDocumentTemplate, documentId, documentId));
        Document createdDocument = this.client.createDocument(
                createdCollection.getSelfLink(),
                sampleDocument,
                null,
                false).getResource();
        
        try {
            Thread.sleep(6000);
        } catch (InterruptedException e) {
            Assert.assertTrue("Sleep interrupted.", false);
        }        
        
        // Trigger TTL evaluation by adding another document to the collection.
        String documentId2 = GatewayTests.getUID();
        Document sampleDocument2 = new Document(String.format(sampleDocumentTemplate, documentId2, documentId2));
        this.client.createDocument(
                createdCollection.getSelfLink(),
                sampleDocument2,
                null,
                false).getResource();
        
        createdDocument = this.client.readDocument(createdDocument.getSelfLink(), null).getResource();
        Assert.assertNotNull(createdDocument);
    }
    
    @Test
    public void testRemoveDocumentTtl() throws DocumentClientException {
        String collectionId = GatewayTestBase.getUID();        
        RequestOptions options = new RequestOptions();
        options.setOfferThroughput(400);
        
        DocumentCollection collectionDefinition = new DocumentCollection();
        collectionDefinition.setDefaultTimeToLive(-1);
        collectionDefinition.setId(collectionId);
        DocumentCollection createdCollection = this.client.createCollection(
                getDatabaseLink(this.databaseForTest, true),
                collectionDefinition,
                options).getResource();

        String sampleDocumentTemplate =
                "{" +
                        "  'id': '%s'," +
                        "  'name': 'sample document %s'," +
                "}";
        String documentId = GatewayTests.getUID();
        Document sampleDocument = new Document(String.format(sampleDocumentTemplate, documentId, documentId));
        Integer ttl = 5;
        sampleDocument.setTimeToLive(ttl);
        Document createdDocument = this.client.createDocument(
                createdCollection.getSelfLink(),
                sampleDocument,
                null,
                false).getResource();
        Assert.assertEquals(ttl, createdDocument.getTimeToLive());
        
        // Remove document ttl by setting it to null.
        createdDocument.setTimeToLive(null);
        createdDocument = this.client.replaceDocument(createdDocument, null).getResource();
        Assert.assertNull(createdDocument.getTimeToLive());
        
        try {
            Thread.sleep(6000);
        } catch (InterruptedException e) {
            Assert.assertTrue("Sleep interrupted.", false);
        }        
        
        // Trigger TTL evaluation by adding another document to the collection.
        String documentId2 = GatewayTests.getUID();
        Document sampleDocument2 = new Document(String.format(sampleDocumentTemplate, documentId2, documentId2));
        this.client.createDocument(
                createdCollection.getSelfLink(),
                sampleDocument2,
                null,
                false).getResource();
        
        createdDocument = this.client.readDocument(createdDocument.getSelfLink(), null).getResource();
        Assert.assertNotNull(createdDocument);
    }
    
    private void testDocumentOverride(
            Integer collectionDefaultTtl, 
            Integer documentTtl,
            Integer sleepSeconds,
            Boolean documentExists) throws DocumentClientException {
        String collectionId = GatewayTestBase.getUID();        
        RequestOptions options = new RequestOptions();
        options.setOfferThroughput(400);
        
        DocumentCollection collectionDefinition = new DocumentCollection();
        collectionDefinition.setDefaultTimeToLive(collectionDefaultTtl);
        
        collectionDefinition.setId(collectionId);
        DocumentCollection createdCollection = this.client.createCollection(
                getDatabaseLink(this.databaseForTest, true),
                collectionDefinition,
                options).getResource();
        
        String sampleDocumentTemplate =
                "{" +
                        "  'id': '%s'," +
                        "  'name': 'sample document %s'," +
                "}";
        String documentId = GatewayTests.getUID();
        Document sampleDocument = new Document(String.format(sampleDocumentTemplate, documentId, documentId));
        sampleDocument.setTimeToLive(documentTtl);
        Document createdDocument = this.client.createDocument(
                createdCollection.getSelfLink(),
                sampleDocument,
                null,
                false).getResource();
        
        Document document = this.client.readDocument(createdDocument.getSelfLink(), null).getResource();
        Assert.assertEquals(documentId, document.getString("id"));
        
        try {
            Thread.sleep(sleepSeconds * 1000);
        } catch (InterruptedException e) {
            Assert.assertTrue("Sleep interrupted.", false);
        }
        
        // Trigger TTL evaluation by adding another document to the collection.
        String anotherDocumentId = GatewayTests.getUID();
        Document anotherDocument = new Document(String.format(sampleDocumentTemplate, anotherDocumentId, anotherDocumentId));
        anotherDocument = this.client.createDocument(
                createdCollection.getSelfLink(),
                anotherDocument,
                null,
                false).getResource();
        
        Boolean documentFound = false;
        try {
            document = this.client.readDocument(createdDocument.getSelfLink(), null).getResource();
            Assert.assertNotNull(document);
            documentFound = true;
        } catch (DocumentClientException exp) {
            Assert.assertEquals(404, exp.getStatusCode());
            Assert.assertEquals("NotFound", exp.getError().getCode());
        }
        Assert.assertEquals(documentExists, documentFound);
    }
}