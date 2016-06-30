package com.microsoft.azure.documentdb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.FeedResponse;
import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.PartitionKey;
import com.microsoft.azure.documentdb.PartitionKind;
import com.microsoft.azure.documentdb.Permission;
import com.microsoft.azure.documentdb.PermissionMode;
import com.microsoft.azure.documentdb.RequestOptions;
import com.microsoft.azure.documentdb.ResourceResponse;
import com.microsoft.azure.documentdb.SqlParameter;
import com.microsoft.azure.documentdb.SqlParameterCollection;
import com.microsoft.azure.documentdb.SqlQuerySpec;
import com.microsoft.azure.documentdb.StoredProcedure;
import com.microsoft.azure.documentdb.StoredProcedureResponse;
import com.microsoft.azure.documentdb.Undefined;
import com.microsoft.azure.documentdb.User;

public class JavaMultiPartitionCollectionTests extends GatewayTestBase {
    
    @Test
    public void testCollectionCreate() throws DocumentClientException {
        
        String collectionId = GatewayTestBase.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/id");
        Assert.assertNotNull(this.client.getPartitionKeyDefinitionMap().getPartitionKeyDefinition(createdCollection.getSelfLink()));
        
        Assert.assertEquals(collectionId, createdCollection.getId());
        PartitionKeyDefinition partitionKeyDef = createdCollection.getPartitionKey();
        Assert.assertNotNull(partitionKeyDef);
        Collection<String> paritionKeyPaths = partitionKeyDef.getPaths();
        Assert.assertNotNull(paritionKeyPaths);
        String path = paritionKeyPaths.iterator().next();
        Assert.assertEquals(path.toLowerCase(), "/id");
        Assert.assertEquals(PartitionKind.Hash, partitionKeyDef.getKind());

        // Read collection and validate partition key definition.
        DocumentCollection readCollection = client.readCollection(createdCollection.getSelfLink(), null).getResource();
        Assert.assertEquals(collectionId, readCollection.getId());
        Assert.assertNotNull(readCollection.getPartitionKey());
        paritionKeyPaths = readCollection.getPartitionKey().getPaths();
        Assert.assertNotNull(paritionKeyPaths);
        path = paritionKeyPaths.iterator().next();
        Assert.assertEquals("/id", path.toLowerCase());
        
        // Create collection using JSON constructor
        collectionId = GatewayTestBase.getUID();
        String collectionDefinition = String.format(
                "{" +
                        " 'id': '%s'," +
                        " 'partitionKey': { 'paths': [ '/id' ], 'kind': 'Hash' } " +
                "}", collectionId);
        
        DocumentCollection collectionDef = new DocumentCollection(collectionDefinition);
        DocumentCollection createdCollection2 = this.client.createCollection(
                getDatabaseLink(this.databaseForTest, true),
                collectionDef,
                null).getResource();
        
        Assert.assertEquals(collectionId, createdCollection2.getId());
        Assert.assertNotNull(createdCollection2.getPartitionKey());
        paritionKeyPaths = createdCollection2.getPartitionKey().getPaths();
        Assert.assertNotNull(paritionKeyPaths);
        path = paritionKeyPaths.iterator().next();
        Assert.assertEquals(path.toLowerCase(), "/id");
    }
    
    @Test
    public void testCollectionDelete() throws DocumentClientException {

        String collectionId = GatewayTestBase.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/id");
        
        this.client.readCollection(createdCollection.getSelfLink(), null);
        this.client.deleteCollection(createdCollection.getSelfLink(), null);
        
        Boolean deleteFail = false;
        try {
            this.client.readCollection(createdCollection.getSelfLink(), null);
        } catch (DocumentClientException e) {
            Assert.assertEquals(404, e.getStatusCode());
            Assert.assertEquals("NotFound", e.getError().getCode());
            deleteFail = true;
        }
        Assert.assertTrue(deleteFail);
    }
    
    @Test
    public void testDocumentCrud() throws DocumentClientException {
        
        String collectionId = GatewayTests.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/id");
        
        String sampleDocumentTemplate =
                "{" +
                        "  'id': '%s'," +
                        "  'name': 'sample document %s'," +
                "}";

        // specify partition key in RequestOptions.
        String documentId = GatewayTests.getUID();
        Document sampleDocument = new Document(String.format(sampleDocumentTemplate, documentId, documentId));
        RequestOptions requestOptions = new RequestOptions();
        requestOptions.setPartitionKey(new PartitionKey(documentId));
        Document document = client.createDocument(
                createdCollection.getSelfLink(),
                sampleDocument,
                requestOptions,
                false).getResource();
        
        Assert.assertEquals(documentId, document.getString("id"));
        Assert.assertEquals(sampleDocument.getString("name"), document.getString("name"));
        Assert.assertNotNull(document.getId());

        // Read document without partition key should fail
        Boolean readFail = false;
        try {
            this.client.readDocument(document.getSelfLink(), null).getResource();
        } catch (DocumentClientException e) {
            readFail = true;
            Assert.assertEquals(400, e.getStatusCode());
            Assert.assertEquals("BadRequest", e.getError().getCode());
        }
        Assert.assertTrue(readFail);
        
        // Read document1 with partition key.
        document = this.client.readDocument(document.getSelfLink(), requestOptions).getResource();
        Assert.assertEquals(documentId, document.getString("id"));
        
        // Update document without partition key.
        document.set("name", "updated name");
        document = this.client.replaceDocument(document, null).getResource();
        Assert.assertEquals("updated name", document.getString("name"));
        
        // Update document with partition key.
        document.set("name", "updated name 2");
        document = this.client.replaceDocument(document, requestOptions).getResource();
        Assert.assertEquals("updated name 2", document.getString("name"));
        
        // Delete document without partition key should fail
        Boolean deleteFail = false;
        try {
            this.client.deleteDocument(document.getSelfLink(), null);
        } catch (DocumentClientException e) {
            deleteFail = true;
            Assert.assertEquals(400, e.getStatusCode());
            Assert.assertEquals("BadRequest", e.getError().getCode());
        }
        Assert.assertTrue(deleteFail);
        
        // Delete document with partition key.
        this.client.deleteDocument(document.getSelfLink(), requestOptions);
        
        readFail = false;
        try {
            this.client.readDocument(document.getSelfLink(), requestOptions).getResource();
        } catch (DocumentClientException e) {
            readFail = true;
            Assert.assertEquals(404, e.getStatusCode());
            Assert.assertEquals("NotFound", e.getError().getCode());
        }
        Assert.assertTrue(readFail);
    }
    
    @Test
    public void testDocumentKeyExtraction() throws DocumentClientException {
        
        String collectionId = GatewayTests.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/id");
        
        String sampleDocumentTemplate =
                "{" +
                        "  'id': '%s'," +
                        "  'name': 'sample document %s'," +
                "}";

        // create document without partition key
        String documentId = GatewayTests.getUID();
        Document sampleDocument = new Document(String.format(sampleDocumentTemplate, documentId, documentId));
        Document document = this.client.createDocument(
                createdCollection.getSelfLink(),
                sampleDocument,
                null,
                false).getResource();
        
        Assert.assertEquals(documentId, document.getString("id"));
        Assert.assertEquals(sampleDocument.getString("name"), document.getString("name"));
        Assert.assertNotNull(document.getId());
        
        // Update document without partition key.
        document.set("name", "updated name");
        document = this.client.replaceDocument(document, null).getResource();
        Assert.assertEquals("updated name", document.getString("name"));
    }
    
    @Test
    public void testDocumentKeyExtractionNestedProperty() throws DocumentClientException {
        
        String collectionId = GatewayTests.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/level1/level2/level3");
        
        String sampleDocumentTemplate =
                "{" +
                        "  'id': '%s'," +
                        "  'level1': { 'level2': { 'level3': %s, 'someother': 2 }, 'someother': 'random value' }," +
                "}";

        String documentId = GatewayTests.getUID();
        Document sampleDocument = new Document(String.format(sampleDocumentTemplate, documentId, documentId));
        Document document = this.client.createDocument(
                createdCollection.getSelfLink(),
                sampleDocument,
                null,
                false).getResource();
        
        Assert.assertEquals(documentId, document.getString("id"));
        Assert.assertNotNull(document.getId());
        
        RequestOptions options = new RequestOptions();
        options.setPartitionKey(new PartitionKey(documentId));
        document = this.client.readDocument(document.getSelfLink(), options).getResource();
        Assert.assertEquals(documentId, document.getString("id"));
    }
    
    @Test
    public void testDocumentKeyExtractionUndefined() throws DocumentClientException {
        
        String collectionId = GatewayTests.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/level1/level2/level3");
        
        String sampleDocumentTemplate =
                "{" +
                        "  'id': '%s'," +
                        "  'level1': { 'level2': { 'someother': 2 }, 'someother': 'random value' }," +
                "}";

        String documentId = GatewayTests.getUID();
        Document sampleDocument = new Document(String.format(sampleDocumentTemplate, documentId));
        Document document = this.client.createDocument(
                createdCollection.getSelfLink(),
                sampleDocument,
                null,
                false).getResource();
        
        Assert.assertEquals(documentId, document.getString("id"));
        Assert.assertNotNull(document.getId());
        
        RequestOptions options = new RequestOptions();
        options.setPartitionKey(new PartitionKey(Undefined.Value()));
        document = this.client.readDocument(document.getSelfLink(), options).getResource();
        Assert.assertEquals(documentId, document.getString("id"));
    }
    
    @Test
    public void testDocumentKeyExtractionComplextTypeAsUndefined() throws DocumentClientException {
        
        String collectionId = GatewayTests.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/level1/level2");
        
        String sampleDocumentTemplate =
                "{" +
                        "  'id': '%s'," +
                        "  'level1': { 'level2': { 'someother': 2 }, 'someother': 'random value' }," +
                "}";

        String documentId = GatewayTests.getUID();
        Document sampleDocument = new Document(String.format(sampleDocumentTemplate, documentId));

        Document document = this.client.createDocument(
                createdCollection.getSelfLink(),
                sampleDocument,
                null,
                false).getResource();
        
        Assert.assertEquals(documentId, document.getString("id"));
        Assert.assertNotNull(document.getId());
        
        RequestOptions options = new RequestOptions();
        options.setPartitionKey(new PartitionKey(Undefined.Value()));
        document = this.client.readDocument(document.getSelfLink(), options).getResource();
        Assert.assertEquals(documentId, document.getString("id"));
    }
    
    @Test
    public void testDocumentKeyExtractionWithEscapeCharacters1() throws DocumentClientException {
        
        String collectionId = GatewayTests.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/\"level' 1*()\"/\"le/vel2\"");
        
        String sampleDocumentTemplate =
                "{" +
                        "  'id': '%s'," +
                        "  \"level' 1*()\": { 'le/vel2': %s, 'someother': 2 }," +
                "}";

        String documentId = GatewayTests.getUID();
        Document sampleDocument = new Document(String.format(sampleDocumentTemplate, documentId, documentId));
        Document document = this.client.createDocument(
                createdCollection.getSelfLink(),
                sampleDocument,
                null,
                false).getResource();
        
        Assert.assertEquals(documentId, document.getString("id"));
        Assert.assertNotNull(document.getId());
        
        RequestOptions options = new RequestOptions();
        options.setPartitionKey(new PartitionKey(documentId));
        document = this.client.readDocument(document.getSelfLink(), options).getResource();
        Assert.assertEquals(documentId, document.getString("id"));        
    }
    
    @Test
    public void testDocumentKeyExtractionWithEscapeCharacters2() throws DocumentClientException {
        
        String collectionId = GatewayTests.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/\'level\" 1*()\'/\'le/vel2\'");
        
        String sampleDocumentTemplate =
                "{" +
                        "  'id': '%s'," +
                        "  'level\" 1*()': { 'le/vel2': %s, 'someother': 2 }," +
                "}";

        String documentId = GatewayTests.getUID();
        Document sampleDocument = new Document(String.format(sampleDocumentTemplate, documentId, documentId));
        Document document = this.client.createDocument(
                createdCollection.getSelfLink(),
                sampleDocument,
                null,
                false).getResource();
        
        Assert.assertEquals(documentId, document.getString("id"));
        Assert.assertNotNull(document.getId());
        
        RequestOptions options = new RequestOptions();
        options.setPartitionKey(new PartitionKey(documentId));
        document = this.client.readDocument(document.getSelfLink(), options).getResource();
        Assert.assertEquals(documentId, document.getString("id"));
    }
    
    @Test
    public void testNullPartitionKey() throws DocumentClientException {
        
        String collectionId = GatewayTests.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/name");
        
        String sampleDocumentTemplate =
                "{" +
                        "  'id': '%s'," +
                        "  'name': %s," +
                "}";

        // create document without partition key
        String documentId = GatewayTests.getUID();
        String name = JSONObject.NULL.toString();
        Document sampleDocument = new Document(String.format(sampleDocumentTemplate, documentId, name));
        Document document = this.client.createDocument(
                createdCollection.getSelfLink(),
                sampleDocument,
                null,
                false).getResource();
        
        Assert.assertEquals(documentId, document.getString("id"));
        Assert.assertEquals(sampleDocument.getString("name"), document.getString("name"));
        
        RequestOptions options = new RequestOptions();
        options.setPartitionKey(new PartitionKey(null));
        document = this.client.readDocument(document.getSelfLink(), options).getResource();
        Assert.assertEquals(documentId, document.getString("id"));
    }
    
    @Test
    public void testReadDocumentFeed() throws DocumentClientException {
        String collectionId = GatewayTests.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/id");
        
        String sampleDocumentTemplate = 
                "{" +
                        "  'id': '%s'," +
                        "  'name': 'sample document %s'," +
                "}";
        
        String documentId1 = GatewayTests.getUID();
        Document sampleDocument1 = new Document(String.format(sampleDocumentTemplate, documentId1, documentId1));
        RequestOptions requestOptions = new RequestOptions();
        requestOptions.setPartitionKey(new PartitionKey(documentId1));
        this.client.createDocument(
                createdCollection.getSelfLink(),
                sampleDocument1,
                requestOptions,
                false).getResource();
        
        String documentId2 = GatewayTests.getUID();
        Document sampleDocument2 = new Document(String.format(sampleDocumentTemplate, documentId2, documentId2));
        requestOptions = new RequestOptions();
        requestOptions.setPartitionKey(new PartitionKey(documentId2));
        this.client.createDocument(
                createdCollection.getSelfLink(),
                sampleDocument2,
                requestOptions,
                false).getResource();

        FeedOptions feedOptions = new FeedOptions();
        feedOptions.setEnableCrossPartitionQuery(true);
        List<Document> documents = this.client.readDocuments(createdCollection.getSelfLink(), feedOptions).getQueryIterable().toList();
        Assert.assertEquals(2, documents.size());
        
        // Read document feed with partition key.
        feedOptions = new FeedOptions();
        feedOptions.setEnableCrossPartitionQuery(true);
        feedOptions.setPartitionKey(new PartitionKey(documentId1));
        documents = this.client.readDocuments(createdCollection.getSelfLink(), feedOptions).getQueryIterable().toList();
        Assert.assertEquals(1, documents.size());
        Assert.assertEquals(sampleDocument1.getString("id"), documents.get(0).getString("id"));
        
        feedOptions = new FeedOptions();
        feedOptions.setPartitionKey(new PartitionKey(documentId2));
        documents = this.client.readDocuments(createdCollection.getSelfLink(), feedOptions).getQueryIterable().toList();
        Assert.assertEquals(1, documents.size());
        Assert.assertEquals(sampleDocument2.getString("id"), documents.get(0).getString("id"));
    }
    
    @Test
    public void testQueryDocuments() throws DocumentClientException {
        String collectionId = GatewayTests.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/id");
        
        String sampleDocumentTemplate = 
                "{" +
                        "  'id': '%s'," +
                        "  'name': 'sample document %s'," +
                "}";
        
        String documentId1 = GatewayTests.getUID();
        Document sampleDocument1 = new Document(String.format(sampleDocumentTemplate, documentId1, documentId1));
        RequestOptions requestOptions = new RequestOptions();
        requestOptions.setPartitionKey(new PartitionKey(documentId1));
        this.client.createDocument(
                createdCollection.getSelfLink(),
                sampleDocument1,
                requestOptions,
                false).getResource();
        
        String documentId2 = GatewayTests.getUID();
        Document sampleDocument2 = new Document(String.format(sampleDocumentTemplate, documentId2, documentId2));
        requestOptions = new RequestOptions();
        requestOptions.setPartitionKey(new PartitionKey(documentId2));
        this.client.createDocument(
                createdCollection.getSelfLink(),
                sampleDocument2,
                requestOptions,
                false).getResource();

        SqlQuerySpec queryWithPartitionKey = new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
                new SqlParameterCollection(new SqlParameter(
                        "@id", documentId1)));
        
        // Query documents should return 1 result - the partition key is part of the query
        FeedResponse<Document> feedResponse = this.client.queryDocuments(createdCollection.getSelfLink(), queryWithPartitionKey, null);
        List<Document> firstBatch = feedResponse.getQueryIterable().toList();
        Assert.assertEquals(1, firstBatch.size());

        // Query document with no partition key in the query.
        SqlQuerySpec queryNoPartitionKey = new SqlQuerySpec("SELECT * FROM root r", new SqlParameterCollection());

        // Query documents with correct partition key should return 1 document.
        FeedOptions options = new FeedOptions();
        options.setPartitionKey(new PartitionKey(documentId1));
        List<Document> documents = this.client.queryDocuments(createdCollection.getSelfLink(), queryNoPartitionKey, options).getQueryIterable().toList();
        Assert.assertEquals(1, documents.size());
        
        // Query documents with cross partition query enabled should return 2 documents.
        options = new FeedOptions();
        options.setEnableCrossPartitionQuery(true);
        documents = this.client.queryDocuments(createdCollection.getSelfLink(), queryNoPartitionKey, options).getQueryIterable().toList();
        Assert.assertEquals(2, documents.size());
        
        // Query documents with no partition key should fail.
        Boolean queryFail = false;
        try {
            this.client.queryDocuments(createdCollection.getSelfLink(), queryNoPartitionKey, null).getQueryIterable().toList();
        } catch (IllegalStateException e) {
            DocumentClientException exp = (DocumentClientException) e.getCause();
            Assert.assertEquals(400, exp.getStatusCode());
            Assert.assertEquals("BadRequest", exp.getError().getCode());
            queryFail = true;
        }
        Assert.assertTrue(queryFail);
    }

    @Test
    public void testQueryDocumentsCrossPartition() throws DocumentClientException {
        String collectionId = GatewayTests.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/id");
        
        String sampleDocumentTemplate = 
                "{" +
                        "  'id': '%s'," +
                        "  'name': 'sample document %s'," +
                "}";
        
        int numDocumentsCreated = 0;
        ArrayList<String> ranges = new ArrayList<String>();
        while (true) {
            Random randomGenerator = new Random();
            byte[] bytes = new byte[128];
            randomGenerator.nextBytes(bytes);
            
            String documentId = bytes.toString();
            
            Document sampleDocument = new Document(String.format(sampleDocumentTemplate, documentId, documentId));
            RequestOptions requestOptions = new RequestOptions();
            requestOptions.setPartitionKey(new PartitionKey(documentId));
            ResourceResponse<Document> createResponse = this.client.createDocument(
                    createdCollection.getSelfLink(),
                    sampleDocument,
                    requestOptions,
                    false);
            
            numDocumentsCreated++;
            String[] sessionTokenParts = createResponse.getSessionToken().split(":");
            if (!ranges.contains(sessionTokenParts[0])) {
                ranges.add(sessionTokenParts[0]);
            }
            
            if (ranges.size() > 1 && numDocumentsCreated > 100) {
                break;
            }
        }

        SqlQuerySpec query = new SqlQuerySpec("SELECT * FROM root r", new SqlParameterCollection());        
        FeedOptions options = new FeedOptions();
        options.setEnableCrossPartitionQuery(true);
        
        FeedResponse<Document> feedResponse = this.client.queryDocuments(createdCollection.getSelfLink(), query, options);
        List<Document> batch = feedResponse.getQueryIterable().toList();
        
        Assert.assertEquals(numDocumentsCreated, batch.size());
    }

    @Test
    public void testExecuteStoredProcedure() throws DocumentClientException {
        String collectionId = GatewayTests.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/id");
        
        String documentId = GatewayTests.getUID();
        String storedProcId1 = GatewayTests.getUID();
        StoredProcedure storedProcedure1 = new StoredProcedure();
        storedProcedure1.setId(storedProcId1);
        String body = String.format(
                "function() {" +
                    "var collection = getContext().getCollection();" +                
                    "collection.createDocument(collection.getSelfLink(), { id: '%s', a : 2 }, {}, function(err, docCreated, options) {" + 
                        "if(err) throw new Error('Error while creating document.');" + 
                        "else getContext().getResponse().setBody(docCreated); })" +  
                    "}", documentId);
        
        storedProcedure1.setBody(body);
                            
        StoredProcedure createdSproc = this.client.createStoredProcedure(
                createdCollection.getSelfLink(),
                storedProcedure1,
                null).getResource();
        
        // Execute stored procedure without partition key should fail.
        Boolean executeFail = false;
        try {
            this.client.executeStoredProcedure(createdSproc.getSelfLink(), null, null);
        } catch (DocumentClientException e) {
            executeFail = true;
            Assert.assertEquals(400, e.getStatusCode());
            Assert.assertEquals("BadRequest", e.getError().getCode());
        }
        Assert.assertTrue(executeFail);

        RequestOptions options = new RequestOptions();
        options.setPartitionKey(new PartitionKey(documentId));
        StoredProcedureResponse response = this.client.executeStoredProcedure(createdSproc.getSelfLink(), options, null);
        Document responseDoc = response.getResponseAsDocument();
        Assert.assertEquals(documentId, responseDoc.getString("id"));
        
        String queryBody =
                "function() {" +
                    "var collection = getContext().getCollection();" +                
                    "collection.queryDocuments(collection.getSelfLink(), 'SELECT * FROM root r', function(err, feed, options) {" + 
                        "if(err) throw new Error('Error while querying document.');" + 
                        "else getContext().getResponse().setBody(feed); })" +  
                    "}";
        
        String storedProcId2 = GatewayTests.getUID();
        StoredProcedure storedProcedure2 = new StoredProcedure();
        storedProcedure2.setId(storedProcId2);
        storedProcedure2.setBody(queryBody);
        StoredProcedure createdSproc2 = this.client.createStoredProcedure(
                createdCollection.getSelfLink(),
                storedProcedure2,
                null).getResource();
        
        StoredProcedureResponse response2 = this.client.executeStoredProcedure(createdSproc2.getSelfLink(), options, null);
        JSONArray array = new JSONArray((response2.getResponseAsString()));
        Assert.assertEquals(1, array.length());
        Assert.assertEquals(documentId, array.getJSONObject(0).getString("id"));
        
        // Execute stored procedure using a different partition key should result in 0 documents returned. 
        options.setPartitionKey(new PartitionKey("somevalue"));
        StoredProcedureResponse response3 = this.client.executeStoredProcedure(createdSproc2.getSelfLink(), options, null);
        JSONArray array2 = new JSONArray((response3.getResponseAsString()));
        Assert.assertEquals(0, array2.length());
    }

    @Test
    public void testCreatePermissionWithPartitionKey() throws DocumentClientException {
        // Create user.
        User user = this.client.createUser(
                getDatabaseLink(this.databaseForTest, true),
                new User("{ 'id': 'new user' }"),
                null).getResource();
        
        String collectionId = GatewayTests.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/id");
        
        String permissionId = GatewayTests.getUID();
        Permission permissionDefinition = new Permission();
        permissionDefinition.setId(permissionId);
        permissionDefinition.setPermissionMode(PermissionMode.Read);
        permissionDefinition.setResourceLink(createdCollection.getSelfLink());
        String partitionKeyValue = GatewayTests.getUID();
        permissionDefinition.setResourcePartitionKey(new PartitionKey(partitionKeyValue));
        Permission permission = this.client.createPermission(user.getSelfLink(), permissionDefinition, null).getResource();
        Assert.assertEquals(permissionId, permission.getId());

        // Read permission.
        permission = this.client.readPermission(permission.getSelfLink(), null).getResource();
        Assert.assertEquals(permissionId, permission.getId());
        PartitionKey partitionKey = permission.getResourcePartitionKey();
        Assert.assertNotNull(partitionKey);
        JSONArray key = new JSONArray(new Object[] {partitionKeyValue});
        Assert.assertEquals(key.toString(), (String)partitionKey.toString());
    }

    @Test
    public void testReadmeExample() throws DocumentClientException {
        String COLLECTION_ID_PARTITIONED = "TestCollection_Partitioned";
        
        PartitionKeyDefinition partitionKeyDef = new PartitionKeyDefinition();
        ArrayList<String> paths = new ArrayList<String>();
        paths.add("/id");
        partitionKeyDef.setPaths(paths);
        
        DocumentCollection myPartitionedCollection = new DocumentCollection();
        myPartitionedCollection.setId(COLLECTION_ID_PARTITIONED);
        myPartitionedCollection.setPartitionKey(partitionKeyDef);

        RequestOptions options = new RequestOptions();
        options.setOfferThroughput(10100);
        myPartitionedCollection = this.client.createCollection(
                this.databaseForTest.getSelfLink(),
                myPartitionedCollection, options).getResource();

        // Insert a document into the created collection.
        String document = "{ 'id': 'document1', 'description': 'this is a test document.' }";
        Document newDocument = new Document(document);
        newDocument = this.client.createDocument(myPartitionedCollection.getSelfLink(),
                newDocument, null, false).getResource();
                
        // Read the created document, specifying the required partition key in RequestOptions.
        options = new RequestOptions();
        options.setPartitionKey(new PartitionKey("document1"));
        newDocument = this.client.readDocument(newDocument.getSelfLink(), options).getResource();
    }
    
    @Test
    public void testPartitionKeyChanged() throws DocumentClientException {
        // Test create document on partition key change.
        String collectionId = GatewayTests.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/id");
        
        String documentDef1 = "{ 'id': 'document1', 'description': 'this is a test document.' }";
        Document newDocument1 = new Document(documentDef1);
        newDocument1 = this.client.createDocument(
                getDocumentCollectionLink(this.databaseForTest, createdCollection, true),
                newDocument1, null, false).getResource();
        
        this.client.deleteCollection(createdCollection.getSelfLink(), null);
        
        // Create a collection with the same id but different partition key.
        String documentDef2 = "{ 'order': 1, 'description': 'this is a test document.' }";
        createdCollection = this.createMultiPartitionCollection(collectionId, "/order");
        Document newDocument2 = new Document(documentDef2);
        
        newDocument2 = this.client.createDocument(
                getDocumentCollectionLink(this.databaseForTest, createdCollection, true),
                newDocument2, null, false).getResource();
        Assert.assertNotNull(newDocument2);
        
        // Test upsert document on partition key change.
        collectionId = GatewayTests.getUID();
        createdCollection = this.createMultiPartitionCollection(collectionId, "/id");
        
        documentDef1 = "{ 'id': 'document1', 'description': 'this is a test document.' }";
        newDocument1 = new Document(documentDef1);
        newDocument1 = this.client.upsertDocument(
                getDocumentCollectionLink(this.databaseForTest, createdCollection, true),
                newDocument1, null, false).getResource();
        
        this.client.deleteCollection(createdCollection.getSelfLink(), null);
        
        documentDef2 = "{ 'order': 1, 'description': 'this is a test document.' }";
        createdCollection = this.createMultiPartitionCollection(collectionId, "/order");
        newDocument2 = new Document(documentDef2);
        
        newDocument2 = this.client.upsertDocument(
                getDocumentCollectionLink(this.databaseForTest, createdCollection, true),
                newDocument2, null, false).getResource();
        Assert.assertNotNull(newDocument2);
    }
    
    @Test
    public void testPartitionKeyCache() throws DocumentClientException {
        String collectionId = GatewayTests.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/id");
        Assert.assertNotNull(this.client.getPartitionKeyDefinitionMap().getPartitionKeyDefinition(createdCollection.getSelfLink()));
        String collectionLink = getDocumentCollectionLink(this.databaseForTest, createdCollection, true);
        Assert.assertNotNull(this.client.getPartitionKeyDefinitionMap().getPartitionKeyDefinition(collectionLink));
        
        collectionId = GatewayTests.getUID();
        createdCollection = this.createMultiPartitionCollection(collectionId, "/id", false);
        Assert.assertNotNull(this.client.getPartitionKeyDefinitionMap().getPartitionKeyDefinition(createdCollection.getSelfLink()));
        collectionLink = getDocumentCollectionLink(this.databaseForTest, createdCollection, true);
        Assert.assertNotNull(this.client.getPartitionKeyDefinitionMap().getPartitionKeyDefinition(collectionLink));
    }
    
    @Test
	public void testIncorrectPartitionkeyValue() throws DocumentClientException {
		
        String collectionId = GatewayTests.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/id");
        
        String documentDef1 = "{ 'id': 'document1', 'description': 'this is a test document.' }";
        Document newDocument1 = new Document(documentDef1);
        
        // Creating a document with mismatched partition key should fail without retry
        try {
        	RequestOptions requestOptions = new RequestOptions();
            requestOptions.setPartitionKey(new PartitionKey("document2"));
            newDocument1 = this.client.createDocument(
            		GatewayTests.getDocumentCollectionLink(this.databaseForTest, createdCollection, true),
                    newDocument1, requestOptions, false).getResource();
            Assert.fail("Creating a document with mismatched partition key not fail");
        }
        catch (DocumentClientException e) {
	    	Assert.assertEquals(400, e.getStatusCode());
	        Assert.assertEquals("BadRequest", e.getError().getCode());
        }
	}
    
    private DocumentCollection createMultiPartitionCollection(String collectionId, String partitionKeyPath) 
            throws DocumentClientException {
        return this.createMultiPartitionCollection(collectionId, partitionKeyPath, true);
    }
    
    private DocumentCollection createMultiPartitionCollection(String collectionId, String partitionKeyPath, boolean isNameBased)
            throws DocumentClientException {
        PartitionKeyDefinition partitionKeyDef = new PartitionKeyDefinition();
        ArrayList<String> paths = new ArrayList<String>();
        paths.add(partitionKeyPath );
        partitionKeyDef.setPaths(paths);

        RequestOptions options = new RequestOptions();
        options.setOfferThroughput(10100);
        DocumentCollection collectionDefinition = new DocumentCollection();
        collectionDefinition.setId(collectionId);
        collectionDefinition.setPartitionKey(partitionKeyDef);
        DocumentCollection createdCollection = this.client.createCollection(
                getDatabaseLink(this.databaseForTest, isNameBased),
                collectionDefinition,
                options).getResource();

        return createdCollection;
    }
}