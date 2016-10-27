package com.microsoft.azure.documentdb;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import com.microsoft.azure.documentdb.internal.HttpConstants;
import com.microsoft.azure.documentdb.internal.ServiceJNIWrapper;
import com.microsoft.azure.documentdb.internal.routing.Range;
import com.microsoft.azure.documentdb.internal.routing.RoutingMapProvider;

public class JavaMultiPartitionCollectionTests extends ParameterizedGatewayTestBase {
    
    public JavaMultiPartitionCollectionTests(DocumentClient client) {
        super(client);
    }

    @Test
    public void testCollectionCreate() throws DocumentClientException {

        String collectionId = TestUtils.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/id");

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
        collectionId = TestUtils.getUID();
        String collectionDefinition = String.format(
                "{" + " 'id': '%s'," + " 'partitionKey': { 'paths': [ '/id' ], 'kind': 'Hash' } " + "}", collectionId);

        DocumentCollection collectionDef = new DocumentCollection(collectionDefinition);
        DocumentCollection createdCollection2 = this.client
                .createCollection(TestUtils.getDatabaseLink(this.databaseForTest, true), collectionDef, null)
                .getResource();

        Assert.assertEquals(collectionId, createdCollection2.getId());
        Assert.assertNotNull(createdCollection2.getPartitionKey());
        paritionKeyPaths = createdCollection2.getPartitionKey().getPaths();
        Assert.assertNotNull(paritionKeyPaths);
        path = paritionKeyPaths.iterator().next();
        Assert.assertEquals(path.toLowerCase(), "/id");
    }

    @Test
    public void testCollectionDelete() throws DocumentClientException {

        String collectionId = TestUtils.getUID();
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

        String collectionId = TestUtils.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/id");

        String sampleDocumentTemplate = "{" + "  'id': '%s'," + "  'name': 'sample document %s'," + "}";

        // specify partition key in RequestOptions.
        String documentId = TestUtils.getUID();
        Document sampleDocument = new Document(String.format(sampleDocumentTemplate, documentId, documentId));
        RequestOptions requestOptions = new RequestOptions();
        requestOptions.setPartitionKey(new PartitionKey(documentId));
        Document document = client
                .createDocument(createdCollection.getSelfLink(), sampleDocument, requestOptions, false).getResource();

        Assert.assertEquals(documentId, document.getString("id"));
        Assert.assertEquals(sampleDocument.getString("name"), document.getString("name"));
        Assert.assertNotNull(document.getId());

        // Read document without partition key should fail
        Boolean readFail = false;
        try {
            this.client.readDocument(document.getSelfLink(), null).getResource();
        } catch (UnsupportedOperationException e) {
            readFail = true;
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

        // Upsert document with wrong partition key
        RequestOptions options = new RequestOptions();
        options.setPartitionKey(new PartitionKey("wrong id"));
        document.set("name", "upsert with wrong partition key");
        Document upsertedDocument = null;
        try {
            upsertedDocument = this.client.upsertDocument(
                    createdCollection.getSelfLink(), document, options, false).getResource();
            Assert.fail("Upsert with wrong partition key should fail");
        } catch (DocumentClientException expected) {}
        Assert.assertNull(upsertedDocument);

        // Upsert document with correct partition key
        options.setPartitionKey(new PartitionKey(document.get("id")));
        document.set("name", "upsert with correct partition key");
        upsertedDocument = this.client.upsertDocument(
                createdCollection.getSelfLink(), document, options, false).getResource();
        Assert.assertEquals(document.get("name"), upsertedDocument.get("name"));
        document = upsertedDocument;

        // Upsert document without partition key
        document.set("name", "upsert without partition key");
        upsertedDocument = this.client.upsertDocument(
                createdCollection.getSelfLink(), document, null, false).getResource();
        Assert.assertEquals(document.get("name"), upsertedDocument.get("name"));
        document = upsertedDocument;

        // Delete document without partition key should fail
        Boolean deleteFail = false;
        try {
            this.client.deleteDocument(document.getSelfLink(), null);
        } catch (UnsupportedOperationException e) {
            deleteFail = true;
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

        String collectionId = TestUtils.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/id");

        String sampleDocumentTemplate = "{" + "  'id': '%s'," + "  'name': 'sample document %s'," + "}";

        // create document without partition key
        String documentId = TestUtils.getUID();
        Document sampleDocument = new Document(String.format(sampleDocumentTemplate, documentId, documentId));
        Document document = this.client.createDocument(createdCollection.getSelfLink(), sampleDocument, null, false)
                .getResource();

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

        String collectionId = TestUtils.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId,
                "/level1/level2/level3");

        String sampleDocumentTemplate = "{" + "  'id': '%s',"
                + "  'level1': { 'level2': { 'level3': %s, 'someother': 2 }, 'someother': 'random value' }," + "}";

        String documentId = TestUtils.getUID();
        Document sampleDocument = new Document(String.format(sampleDocumentTemplate, documentId, documentId));
        Document document = this.client.createDocument(createdCollection.getSelfLink(), sampleDocument, null, false)
                .getResource();

        Assert.assertEquals(documentId, document.getString("id"));
        Assert.assertNotNull(document.getId());

        RequestOptions options = new RequestOptions();
        options.setPartitionKey(new PartitionKey(documentId));
        document = this.client.readDocument(document.getSelfLink(), options).getResource();
        Assert.assertEquals(documentId, document.getString("id"));
    }

    @Test
    public void testDocumentKeyExtractionUndefined() throws DocumentClientException {

        String collectionId = TestUtils.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId,
                "/level1/level2/level3");

        String sampleDocumentTemplate = "{" + "  'id': '%s',"
                + "  'level1': { 'level2': { 'someother': 2 }, 'someother': 'random value' }," + "}";

        String documentId = TestUtils.getUID();
        Document sampleDocument = new Document(String.format(sampleDocumentTemplate, documentId));
        Document document = this.client.createDocument(createdCollection.getSelfLink(), sampleDocument, null, false)
                .getResource();

        Assert.assertEquals(documentId, document.getString("id"));
        Assert.assertNotNull(document.getId());

        RequestOptions options = new RequestOptions();
        options.setPartitionKey(new PartitionKey(Undefined.Value()));
        document = this.client.readDocument(document.getSelfLink(), options).getResource();
        Assert.assertEquals(documentId, document.getString("id"));
    }

    @Test
    public void testDocumentKeyExtractionComplextTypeAsUndefined() throws DocumentClientException {

        String collectionId = TestUtils.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/level1/level2");

        String sampleDocumentTemplate = "{" + "  'id': '%s',"
                + "  'level1': { 'level2': { 'someother': 2 }, 'someother': 'random value' }," + "}";

        String documentId = TestUtils.getUID();
        Document sampleDocument = new Document(String.format(sampleDocumentTemplate, documentId));

        Document document = this.client.createDocument(createdCollection.getSelfLink(), sampleDocument, null, false)
                .getResource();

        Assert.assertEquals(documentId, document.getString("id"));
        Assert.assertNotNull(document.getId());

        RequestOptions options = new RequestOptions();
        options.setPartitionKey(new PartitionKey(Undefined.Value()));
        document = this.client.readDocument(document.getSelfLink(), options).getResource();
        Assert.assertEquals(documentId, document.getString("id"));
    }

    @Test
    public void testDocumentKeyExtractionWithEscapeCharacters1() throws DocumentClientException {

        String collectionId = TestUtils.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId,
                "/\"level' 1*()\"/\"le/vel2\"");

        String sampleDocumentTemplate = "{" + "  'id': '%s'," + "  \"level' 1*()\": { 'le/vel2': %s, 'someother': 2 },"
                + "}";

        String documentId = TestUtils.getUID();
        Document sampleDocument = new Document(String.format(sampleDocumentTemplate, documentId, documentId));
        Document document = this.client.createDocument(createdCollection.getSelfLink(), sampleDocument, null, false)
                .getResource();

        Assert.assertEquals(documentId, document.getString("id"));
        Assert.assertNotNull(document.getId());

        RequestOptions options = new RequestOptions();
        options.setPartitionKey(new PartitionKey(documentId));
        document = this.client.readDocument(document.getSelfLink(), options).getResource();
        Assert.assertEquals(documentId, document.getString("id"));
    }

    @Test
    public void testDocumentKeyExtractionWithEscapeCharacters2() throws DocumentClientException {

        String collectionId = TestUtils.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId,
                "/\'level\" 1*()\'/\'le/vel2\'");

        String sampleDocumentTemplate = "{" + "  'id': '%s'," + "  'level\" 1*()': { 'le/vel2': %s, 'someother': 2 },"
                + "}";

        String documentId = TestUtils.getUID();
        Document sampleDocument = new Document(String.format(sampleDocumentTemplate, documentId, documentId));
        Document document = this.client.createDocument(createdCollection.getSelfLink(), sampleDocument, null, false)
                .getResource();

        Assert.assertEquals(documentId, document.getString("id"));
        Assert.assertNotNull(document.getId());

        RequestOptions options = new RequestOptions();
        options.setPartitionKey(new PartitionKey(documentId));
        document = this.client.readDocument(document.getSelfLink(), options).getResource();
        Assert.assertEquals(documentId, document.getString("id"));
    }

    @Test
    public void testNullPartitionKey() throws DocumentClientException {

        String collectionId = TestUtils.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/name");

        String sampleDocumentTemplate = "{" + "  'id': '%s'," + "  'name': %s," + "}";

        // create document without partition key
        String documentId = TestUtils.getUID();
        String name = JSONObject.NULL.toString();
        Document sampleDocument = new Document(String.format(sampleDocumentTemplate, documentId, name));
        Document document = this.client.createDocument(createdCollection.getSelfLink(), sampleDocument, null, false)
                .getResource();

        Assert.assertEquals(documentId, document.getString("id"));
        Assert.assertEquals(sampleDocument.getString("name"), document.getString("name"));

        RequestOptions options = new RequestOptions();
        options.setPartitionKey(new PartitionKey(null));
        document = this.client.readDocument(document.getSelfLink(), options).getResource();
        Assert.assertEquals(documentId, document.getString("id"));
    }

    @Test
    public void testReadDocumentFeed() throws DocumentClientException {
        String collectionId = TestUtils.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/id");

        String sampleDocumentTemplate = "{" + "  'id': '%s'," + "  'name': 'sample document %s'," + "}";

        String documentId1 = TestUtils.getUID();
        Document sampleDocument1 = new Document(String.format(sampleDocumentTemplate, documentId1, documentId1));
        RequestOptions requestOptions = new RequestOptions();
        requestOptions.setPartitionKey(new PartitionKey(documentId1));
        this.client.createDocument(createdCollection.getSelfLink(), sampleDocument1, requestOptions, false)
                .getResource();

        String documentId2 = TestUtils.getUID();
        Document sampleDocument2 = new Document(String.format(sampleDocumentTemplate, documentId2, documentId2));
        requestOptions = new RequestOptions();
        requestOptions.setPartitionKey(new PartitionKey(documentId2));
        this.client.createDocument(createdCollection.getSelfLink(), sampleDocument2, requestOptions, false)
                .getResource();

        FeedOptions feedOptions = new FeedOptions();
        feedOptions.setEnableCrossPartitionQuery(true);
        List<Document> documents = this.client.readDocuments(createdCollection.getSelfLink(), feedOptions)
                .getQueryIterable().toList();
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
        String collectionId = TestUtils.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/id");

        String sampleDocumentTemplate = "{" + "  'id': '%s'," + "  'name': 'sample document %s'," + "}";

        // String documentId1 = TestUtils.getUID();
        String documentId1 = "1";
        Document sampleDocument1 = new Document(String.format(sampleDocumentTemplate, documentId1, documentId1));
        RequestOptions requestOptions = new RequestOptions();
        requestOptions.setPartitionKey(new PartitionKey(documentId1));
        this.client.createDocument(createdCollection.getSelfLink(), sampleDocument1, requestOptions, false)
                .getResource();

        // String documentId2 = TestUtils.getUID();
        String documentId2 = "2";
        Document sampleDocument2 = new Document(String.format(sampleDocumentTemplate, documentId2, documentId2));
        requestOptions = new RequestOptions();
        requestOptions.setPartitionKey(new PartitionKey(documentId2));
        this.client.createDocument(createdCollection.getSelfLink(), sampleDocument2, requestOptions, false)
                .getResource();

        SqlQuerySpec queryWithPartitionKey = new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
                new SqlParameterCollection(new SqlParameter("@id", documentId1)));

        // Query documents should return 1 result - the partition key is part of
        // the query
        FeedResponse<Document> feedResponse = this.client.queryDocuments(createdCollection.getSelfLink(),
                queryWithPartitionKey, null);
        List<Document> firstBatch = feedResponse.getQueryIterable().toList();
        Assert.assertEquals(1, firstBatch.size());

        // Query document with no partition key in the query.
        SqlQuerySpec queryNoPartitionKey = new SqlQuerySpec("SELECT * FROM root r", new SqlParameterCollection());

        // Query documents with correct partition key should return 1 document.
        FeedOptions options = new FeedOptions();
        options.setPartitionKey(new PartitionKey(documentId1));
        List<Document> documents = this.client
                .queryDocuments(createdCollection.getSelfLink(), queryNoPartitionKey, options).getQueryIterable()
                .toList();
        Assert.assertEquals(1, documents.size());

        // Query documents with cross partition query enabled should return 2
        // documents.
        options = new FeedOptions();
        options.setEnableCrossPartitionQuery(true);
        documents = this.client.queryDocuments(createdCollection.getSelfLink(), queryNoPartitionKey, options)
                .getQueryIterable().toList();
        Assert.assertEquals(2, documents.size());

        // Query documents with no partition key should fail.
        Boolean queryFail = false;
        try {
            this.client.queryDocuments(createdCollection.getSelfLink(), queryNoPartitionKey, null).getQueryIterable()
                    .toList();
        } catch (IllegalStateException e) {
            DocumentClientException exp = (DocumentClientException) e.getCause();
            Assert.assertEquals(400, exp.getStatusCode());
            queryFail = true;
        }
        Assert.assertTrue(queryFail);
    }

    @Test
    public void testQueryDocumentsCrossPartition() throws DocumentClientException {
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        String collectionId = TestUtils.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/id");

        String sampleDocumentTemplate = "{" + "  'id': '%s'," + "  'name': 'sample document %s'," + "}";

        int numDocumentsCreated = 0;
        ArrayList<String> ranges = new ArrayList<String>();
        while (true) {
            byte[] bytes = new byte[128];
            rand.nextBytes(bytes);

            String documentId = bytes.toString();

            Document sampleDocument = new Document(String.format(sampleDocumentTemplate, documentId, documentId));
            RequestOptions requestOptions = new RequestOptions();
            requestOptions.setPartitionKey(new PartitionKey(documentId));
            ResourceResponse<Document> createResponse = this.client.createDocument(createdCollection.getSelfLink(),
                    sampleDocument, requestOptions, false);

            numDocumentsCreated++;
            String[] sessionTokenParts = createResponse.getSessionToken().split(":");
            if (!ranges.contains(sessionTokenParts[0])) {
                ranges.add(sessionTokenParts[0]);
            }

            // if (ranges.size() > 1 && numDocumentsCreated > 100) {
            if (ranges.size() > 1 && numDocumentsCreated > 5) {
                break;
            }
        }

        SqlQuerySpec query = new SqlQuerySpec("SELECT * FROM root r", new SqlParameterCollection());
        FeedOptions options = new FeedOptions();
        options.setEnableCrossPartitionQuery(true);

        FeedResponse<Document> feedResponse = this.client.queryDocuments(createdCollection.getSelfLink(), query,
                options);
        List<Document> batch = feedResponse.getQueryIterable().toList();

        Assert.assertEquals(String.format("Failed with seed %d", seed), numDocumentsCreated, batch.size());
    }

    @Test
    public void testQueryDocumentsCrossPartitionTopOrderByDifferentDimensions() throws DocumentClientException {
        String collectionId = TestUtils.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/key");

        String[] documents = new String[] { "{\"id\":\"documentId1\",\"key\":\"A\"}",
                "{\"id\":\"documentId2\",\"key\":\"A\",\"prop\":3}", "{\"id\":\"documentId3\",\"key\":\"A\"}",
                "{\"id\":\"documentId4\",\"key\":5}", "{\"id\":\"documentId5\",\"key\":5,\"prop\":2}",
                "{\"id\":\"documentId6\",\"key\":5}", "{\"id\":\"documentId7\",\"key\":null}",
                "{\"id\":\"documentId8\",\"key\":null,\"prop\":1}", "{\"id\":\"documentId9\",\"key\":null}", };

        for (String document : documents) {
            this.client
                    .createDocument(createdCollection.getSelfLink(), new Document(document), new RequestOptions(), true)
                    .getResource();
        }

        SqlQuerySpec query = new SqlQuerySpec("SELECT r.id FROM r ORDER BY r.prop DESC", new SqlParameterCollection());
        FeedOptions options = new FeedOptions();
        options.setEnableCrossPartitionQuery(true);
        options.setPageSize(1);

        FeedResponse<Document> feedResponse = this.client.queryDocuments(createdCollection.getSelfLink(), query,
                options);
        Assert.assertEquals(String.join(",", Arrays.asList("documentId2", "documentId5", "documentId8")),
                feedResponse.getQueryIterable().toList().stream().map(new Function<Document, String>() {
                    @Override
                    public String apply(Document doc) {
                        return doc.getId();
                    }
                }).collect(Collectors.joining(",")));
    }

    @Test
    public void testQueryDocumentsCrossPartitionTopOrderByDifferentTypes() throws DocumentClientException {
        String collectionId = TestUtils.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/key");

        String[] documents = new String[] { "{\"id\":\"documentId1\",\"key\":\"A\",\"prop\":\"3\"}",
                "{\"id\":\"documentId2\",\"key\":\"A\",\"prop\":3}",
                "{\"id\":\"documentId3\",\"key\":\"A\",\"prop\":true}",
                "{\"id\":\"documentId4\",\"key\":5,\"prop\":\"2\"}", "{\"id\":\"documentId5\",\"key\":5,\"prop\":2}",
                "{\"id\":\"documentId6\",\"key\":5,\"prop\":false}",
                "{\"id\":\"documentId7\",\"key\":null,\"prop\":\"1\"}",
                "{\"id\":\"documentId8\",\"key\":null,\"prop\":1}",
                "{\"id\":\"documentId9\",\"key\":null,\"prop\":null}",
                "{\"id\":\"documentId10\",\"key\":null,\"prop\":\"2010-01-02T09:56:46.0903751Z\"}",
                "{\"id\":\"documentId11\",\"key\":null,\"prop\":\"2010-01-01T09:56:46.0903751Z\"}", };

        List<Document> documentList = new ArrayList<Document>();
        for (String document : documents) {
            documentList.add(this.client
                    .createDocument(createdCollection.getSelfLink(), new Document(document), new RequestOptions(), true)
                    .getResource());
        }

        FeedOptions options = new FeedOptions();
        options.setEnableCrossPartitionQuery(true);
        options.setPageSize(1);

        try {
            this.client.queryDocuments(createdCollection.getSelfLink(),
                    new SqlQuerySpec("SELECT r.id FROM r WHERE r.key IN (\"A\", 5, null) ORDER BY r.prop",
                            new SqlParameterCollection()),
                    options).getQueryIterable().toList();
        } catch (UnsupportedOperationException ex) {
        }

        Function<Document, String> func = new Function<Document, String>() {
            @Override
            public String apply(Document doc) {
                return doc.getId();
            }
        };

        // String
        Assert.assertEquals(
                String.join(",",
                        Arrays.asList("documentId1", "documentId10", "documentId11", "documentId4", "documentId7")),
                this.client
                        .queryDocuments(createdCollection.getSelfLink(),
                                new SqlQuerySpec(
                                        "SELECT r.id FROM r WHERE r.key IN (\"A\", 5, null) AND IS_STRING(r.prop) ORDER BY r.prop DESC",
                                        new SqlParameterCollection()),
                                options)
                        .getQueryIterable().toList().stream().map(func).collect(Collectors.joining(",")));
        // Number
        Assert.assertEquals(String.join(",", Arrays.asList("documentId8", "documentId5", "documentId2")),
                this.client
                        .queryDocuments(createdCollection.getSelfLink(),
                                new SqlQuerySpec(
                                        "SELECT r.id FROM r WHERE r.key IN (\"A\", 5, null) AND IS_NUMBER(r.prop) ORDER BY r.prop",
                                        new SqlParameterCollection()),
                                options)
                        .getQueryIterable().toList().stream().map(func).collect(Collectors.joining(",")));
        // Boolean
        Assert.assertEquals(String.join(",", Arrays.asList("documentId3", "documentId6")),
                this.client
                        .queryDocuments(createdCollection.getSelfLink(),
                                new SqlQuerySpec(
                                        "SELECT r.id FROM r WHERE r.key IN (\"A\", 5, null) AND IS_BOOL(r.prop) ORDER BY r.prop DESC",
                                        new SqlParameterCollection()),
                                options)
                        .getQueryIterable().toList().stream().map(func).collect(Collectors.joining(",")));
    }

    @Test
    public void testCrossPartitionDocumentQueryIterable() throws Exception {
        long seed = System.currentTimeMillis();
        System.out.println(String.format("Starting testQueryDocumentsCrossPartitionTopOrderBy with seed %d", seed));
        Random rand = new Random(seed);
        int numberOfDocuments = 2000;
        double[] uniquenessFactors = new double[] { 0.01, 0.02, 0.04, 0.08, 0.16, 0.32, 0.64, 0.80, 0.90, 1, 1 };
        String[] fieldNames = new String[uniquenessFactors.length];
        for (int i = 0; i < fieldNames.length; ++i) {
            fieldNames[i] = "field_" + i;
        }

        String partitionKey = fieldNames[0];

        String collectionId = TestUtils.getUID();
        final DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId,
                "/" + partitionKey);

        Range<String> fullRange = new Range<String>(PartitionKeyRange.MINIMUM_INCLUSIVE_EFFECTIVE_PARTITION_KEY,
                PartitionKeyRange.MAXIMUM_EXCLUSIVE_EFFECTIVE_PARTITION_KEY, true, false);
        RoutingMapProvider routingMapProvider = this.client.getPartitionKeyRangeCache();
        Assert.assertEquals(5,
                routingMapProvider.getOverlappingRanges(createdCollection.getSelfLink(), fullRange).size());

        Document[] documents = new Document[numberOfDocuments];
        Map<String, String> idToPartitionKeyRangeIdMap = new HashMap<String, String>();

        long start = System.currentTimeMillis();
        for (int i = 0; i < numberOfDocuments; ++i) {
            final Document doc = new Document();
            doc.setId(Integer.toString(i + 1));
            for (int j = 0; j < fieldNames.length; ++j) {
                doc.set(fieldNames[j], rand.nextInt((int) (numberOfDocuments * uniquenessFactors[j])));
            }

            ResourceResponse<Document> response = this.client.createDocument(createdCollection.getSelfLink(), doc,
                    new RequestOptions(), true);
            String sessionToken = response.getSessionToken();
            documents[i] = response.getResource();
            idToPartitionKeyRangeIdMap.put(documents[i].getId(), sessionToken.split(":")[0]);
        }

        System.out.println(String.format("Finished populating %d documents in %d ms", numberOfDocuments,
                System.currentTimeMillis() - start));

        testCrossPartitionDocumentQueryIterable(createdCollection, documents, partitionKey, idToPartitionKeyRangeIdMap,
                seed);
    }

    private void testCrossPartitionDocumentQueryIterable(DocumentCollection coll, Document[] documents,
            String partitionKey, final Map<String, String> idToPartitionKeyRangeIdMap, long seed) throws Exception {
        final Random rand = new Random(seed);
        Set<Integer> uniquePartitionKeyValues = new HashSet<Integer>();
        for (Document doc : documents) {
            uniquePartitionKeyValues.add(doc.getInt(partitionKey));
        }
        Function<Integer, String> getIntStringFunc = new Function<Integer, String>() {
            @Override
            public String apply(Integer i) {
                return i.toString();
            }
        };
        Function<Document, String> getDocIdFunc = new Function<Document, String>() {
            @Override
            public String apply(Document doc) {
                return doc.getId();
            }
        };
        final List<Integer> partitionKeyValues = new ArrayList<Integer>(uniquePartitionKeyValues);

        // Default FeedOptions
        FeedOptions defaultFeedOptions = new FeedOptions();
        defaultFeedOptions.setEnableCrossPartitionQuery(true);

        // Serial FeedOptions
        FeedOptions serialFeedOptions = new FeedOptions();
        serialFeedOptions.setEnableCrossPartitionQuery(true);
        serialFeedOptions.setPageSize(-1);
        serialFeedOptions.setMaxDegreeOfParallelism(1);

        // High Parallelized FeedOptions
        FeedOptions parallelFeedOptions = new FeedOptions();
        parallelFeedOptions.setEnableCrossPartitionQuery(true);
        parallelFeedOptions.setPageSize(100);
        parallelFeedOptions.setMaxBufferedItemCount(Integer.MAX_VALUE);
        parallelFeedOptions.setMaxDegreeOfParallelism(Integer.MAX_VALUE);

        // Dynamic FeedOptions
        FeedOptions dynamicFeedOptions = new FeedOptions();
        dynamicFeedOptions.setEnableCrossPartitionQuery(true);
        dynamicFeedOptions.setPageSize(-1);
        dynamicFeedOptions.setMaxBufferedItemCount(-1);
        dynamicFeedOptions.setMaxDegreeOfParallelism(-1);

        // Random FeedOptions
        FeedOptions randomFeedOptions = new FeedOptions();
        randomFeedOptions.setEnableCrossPartitionQuery(true);
        randomFeedOptions.setPageSize(rand.nextInt(2) == 0 ? -1 : 1 + rand.nextInt(documents.length));
        randomFeedOptions.setMaxBufferedItemCount(Math.min(100, documents.length) + rand.nextInt(documents.length + 1));
        randomFeedOptions.setMaxDegreeOfParallelism(rand.nextInt(2) == 0 ? -1 : rand.nextInt(25));

        for (int trial = 0; trial < 1; ++trial) {
            for (boolean useFetchNextBlock : Arrays.asList(true, false)) {
                for (FeedOptions feedOptions : Arrays.asList(defaultFeedOptions, serialFeedOptions, parallelFeedOptions,
                        dynamicFeedOptions, randomFeedOptions)) {
                    testCrossPartitionDocumentQueryIterable(coll, documents, partitionKey, idToPartitionKeyRangeIdMap,
                            feedOptions, partitionKeyValues, getIntStringFunc, getDocIdFunc, trial, true,
                            useFetchNextBlock, false, false, false, false, "", seed);
                    for (boolean fanOut : Arrays.asList(true, false)) {
                        for (boolean isParametrized : Arrays.asList(false, true)) {
                            for (boolean hasTop : Arrays.asList(false, true)) {
                                for (boolean hasOrderBy : Arrays.asList(false, true)) {
                                    for (String sortOrder : Arrays.asList("", "ASC", "DESC")) {
                                        testCrossPartitionDocumentQueryIterable(coll, documents, partitionKey,
                                                idToPartitionKeyRangeIdMap, feedOptions, partitionKeyValues,
                                                getIntStringFunc, getDocIdFunc, trial, false, useFetchNextBlock, fanOut,
                                                isParametrized, hasTop, hasOrderBy, sortOrder, seed);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private void testCrossPartitionDocumentQueryIterable(DocumentCollection coll, Document[] documents,
            String partitionKey, final Map<String, String> idToPartitionKeyRangeIdMap, FeedOptions feedOptions,
            final List<Integer> partitionKeyValues, Function<Integer, String> getIntStringFunc,
            Function<Document, String> getDocIdFunc, int trial, boolean isReadFeed, boolean useFetchNextBlock,
            boolean fanOut, final boolean isParametrized, final boolean hasTop, final boolean hasOrderBy,
            final String sortOrder, long seed) throws Exception {
        final Random rand = new Random(seed);
        List<Document> filteredDocuments;
        QueryIterable<Document> queryIterable;
        String startTraceMessage;
        String assertMessage;

        Comparator<Document> defaultComparator = new Comparator<Document>() {
            @Override
            public int compare(Document doc1, Document doc2) {
                int cmp = Integer.compare(Integer.parseInt(idToPartitionKeyRangeIdMap.get(doc1.getId())),
                        Integer.parseInt(idToPartitionKeyRangeIdMap.get(doc2.getId())));
                if (cmp != 0)
                    return cmp;
                return Integer.compare(Integer.parseInt(doc1.getId()), Integer.parseInt(doc2.getId()));
            }
        };

        if (isReadFeed) {
            filteredDocuments = Arrays.asList(documents);

            Collections.sort(filteredDocuments, defaultComparator);

            queryIterable = this.client.readDocuments(coll.getSelfLink(), feedOptions).getQueryIterable();

            startTraceMessage = String.format(
                    "Start: readDocuments, <Use fetchNextBlock>: %s, <PageSize>: %d, <MaxBufferedItemCount>: %d, <MaxDegreeOfParallelism>: %d",
                    String.valueOf(useFetchNextBlock), feedOptions.getPageSize(), feedOptions.getMaxBufferedItemCount(),
                    feedOptions.getMaxDegreeOfParallelism());

            assertMessage = String.format(
                    "readDocuments, seed: %d, trial: %d, fanOut: %s, useFetchNextBlock: %s, hasTop: %s, hasOrderBy: %s, sortOrder: %s",
                    seed, trial, String.valueOf(fanOut), String.valueOf(useFetchNextBlock), String.valueOf(hasTop),
                    String.valueOf(hasOrderBy), sortOrder);

        } else {
            final int top = rand.nextInt(10) * (1 + rand.nextInt(Math.min(partitionKeyValues.size(), 100)));
            String queryText;
            final String orderByField = "field_" + rand.nextInt(10);
            final String topValueName = "@topValue";
            SqlParameterCollection parameters = new SqlParameterCollection();
            if (isParametrized && hasTop) {
                parameters.add(new SqlParameter(topValueName, top));
            }

            Callable<String> getTop = new Callable<String>() {
                @Override
                public String call() throws Exception {
                    return hasTop ? String.format("TOP %s ", isParametrized ? topValueName : String.valueOf(top))
                            : StringUtils.EMPTY;
                }
            };

            Callable<String> getOrderBy = new Callable<String>() {
                @Override
                public String call() throws Exception {
                    return hasOrderBy ? String.format(" ORDER BY r.%s %s", orderByField, sortOrder) : StringUtils.EMPTY;
                }
            };

            if (fanOut) {
                queryText = String.format("SELECT %sr.id, r.%s FROM r%s", getTop.call(), partitionKey,
                        getOrderBy.call());

                filteredDocuments = Arrays.asList(documents);
            } else {
                Collections.shuffle(partitionKeyValues, rand);
                Set<Integer> selectedPartitionKeyValues = new HashSet<Integer>();
                for (Integer i : partitionKeyValues.subList(0,
                        1 + rand.nextInt(Math.min(100, partitionKeyValues.size())))) {
                    selectedPartitionKeyValues.add(i);
                }

                queryText = String.format("SELECT %sr.id, r.%s FROM r WHERE r.%s IN (%s)%s", getTop.call(),
                        partitionKey, partitionKey,
                        selectedPartitionKeyValues.stream().map(getIntStringFunc).collect(Collectors.joining(",")),
                        getOrderBy.call());

                filteredDocuments = new ArrayList<Document>();
                for (Document doc : documents) {
                    if (selectedPartitionKeyValues.contains(doc.getInt(partitionKey))) {
                        filteredDocuments.add(doc);
                    }
                }
            }

            Comparator<Document> comparator = null;
            if (hasOrderBy) {
                switch (sortOrder) {
                case "":
                case "ASC":
                    comparator = new Comparator<Document>() {
                        @Override
                        public int compare(Document doc1, Document doc2) {
                            int cmp = Integer.compare(doc1.getInt(orderByField), doc2.getInt(orderByField));
                            if (cmp != 0)
                                return cmp;
                            cmp = Integer.compare(Integer.parseInt(idToPartitionKeyRangeIdMap.get(doc1.getId())),
                                    Integer.parseInt(idToPartitionKeyRangeIdMap.get(doc2.getId())));
                            if (cmp != 0)
                                return cmp;
                            return Integer.compare(Integer.parseInt(doc1.getId()), Integer.parseInt(doc2.getId()));
                        }
                    };
                    break;
                case "DESC":
                    comparator = new Comparator<Document>() {
                        @Override
                        public int compare(Document doc1, Document doc2) {
                            int cmp = Integer.compare(doc1.getInt(orderByField), doc2.getInt(orderByField));
                            if (cmp != 0)
                                return -cmp;
                            cmp = Integer.compare(Integer.parseInt(idToPartitionKeyRangeIdMap.get(doc1.getId())),
                                    Integer.parseInt(idToPartitionKeyRangeIdMap.get(doc2.getId())));
                            if (cmp != 0)
                                return cmp;
                            return -Integer.compare(Integer.parseInt(doc1.getId()), Integer.parseInt(doc2.getId()));
    }
                    };
                    break;
                }
            } else {
                comparator = defaultComparator;
            }

            Collections.sort(filteredDocuments, comparator);

            if (hasTop) {
                filteredDocuments = filteredDocuments.subList(0, Math.min(top, filteredDocuments.size()));
    }

            SqlQuerySpec querySpec = new SqlQuerySpec(queryText, parameters);

            startTraceMessage = String.format(
                    "Start: <Query>: %s, <Use fetchNextBlock>: %s, <PageSize>: %d, <MaxBufferedItemCount>: %d, <MaxDegreeOfParallelism>: %d",
                    queryText, String.valueOf(useFetchNextBlock), feedOptions.getPageSize(),
                    feedOptions.getMaxBufferedItemCount(), feedOptions.getMaxDegreeOfParallelism());

            assertMessage = String.format(
                    "query: %s, seed: %d, trial: %d, fanOut: %s, useFetchNextBlock: %s, isParametrized: %s, hasTop: %s, hasOrderBy: %s, sortOrder: %s",
                    queryText, seed, trial, String.valueOf(fanOut), String.valueOf(useFetchNextBlock),
                    String.valueOf(isParametrized), String.valueOf(hasTop), String.valueOf(hasOrderBy), sortOrder);

            queryIterable = client.queryDocuments(coll.getSelfLink(), querySpec, feedOptions).getQueryIterable();
        }

        System.out.println(startTraceMessage);
        List<Document> actualDocuments;
        long startTime = System.currentTimeMillis();
        if (useFetchNextBlock) {
            List<Document> result = new ArrayList<Document>();

            List<Document> nextBlock;
            while ((nextBlock = queryIterable.fetchNextBlock()) != null) {
                result.addAll(nextBlock);
            }

            actualDocuments = result;
        } else {
            actualDocuments = queryIterable.toList();
        }

        System.out.println(String.format("End: <Document Count>: %d, <Time>: %d ms", actualDocuments.size(),
                System.currentTimeMillis() - startTime));

        Assert.assertEquals(assertMessage,
                filteredDocuments.stream().map(getDocIdFunc).collect(Collectors.joining(",")),
                actualDocuments.stream().map(getDocIdFunc).collect(Collectors.joining(",")));
    }

    @Test
    public void testExecuteStoredProcedure() throws DocumentClientException {
        String collectionId = TestUtils.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/id");

        String documentId = TestUtils.getUID();
        String storedProcId1 = TestUtils.getUID();
        StoredProcedure storedProcedure1 = new StoredProcedure();
        storedProcedure1.setId(storedProcId1);
        String body = String.format("function() {" + "var collection = getContext().getCollection();"
                + "collection.createDocument(collection.getSelfLink(), { id: '%s', a : 2 }, {}, function(err, docCreated, options) {"
                + "if(err) throw new Error('Error while creating document.');"
                + "else getContext().getResponse().setBody(docCreated); })" + "}", documentId);

        storedProcedure1.setBody(body);

        StoredProcedure createdSproc = this.client
                .createStoredProcedure(createdCollection.getSelfLink(), storedProcedure1, null).getResource();

        // Execute stored procedure without partition key should fail.
        Boolean executeFail = false;
        try {
            this.client.executeStoredProcedure(createdSproc.getSelfLink(), null, null);
        } catch (UnsupportedOperationException e) {
            executeFail = true;
        }
        Assert.assertTrue(executeFail);

        RequestOptions options = new RequestOptions();
        options.setPartitionKey(new PartitionKey(documentId));
        StoredProcedureResponse response = this.client.executeStoredProcedure(createdSproc.getSelfLink(), options,
                null);
        Document responseDoc = response.getResponseAsDocument();
        Assert.assertEquals(documentId, responseDoc.getString("id"));

        String queryBody = "function() {" + "var collection = getContext().getCollection();"
                + "collection.queryDocuments(collection.getSelfLink(), 'SELECT * FROM root r', function(err, feed, options) {"
                + "if(err) throw new Error('Error while querying document.');"
                + "else getContext().getResponse().setBody(feed); })" + "}";

        String storedProcId2 = TestUtils.getUID();
        StoredProcedure storedProcedure2 = new StoredProcedure();
        storedProcedure2.setId(storedProcId2);
        storedProcedure2.setBody(queryBody);
        StoredProcedure createdSproc2 = this.client
                .createStoredProcedure(createdCollection.getSelfLink(), storedProcedure2, null).getResource();

        StoredProcedureResponse response2 = this.client.executeStoredProcedure(createdSproc2.getSelfLink(), options,
                null);
        JSONArray array = new JSONArray((response2.getResponseAsString()));
        Assert.assertEquals(1, array.length());
        Assert.assertEquals(documentId, array.getJSONObject(0).getString("id"));

        // Execute stored procedure using a different partition key should
        // result in 0 documents returned.
        options.setPartitionKey(new PartitionKey("somevalue"));
        StoredProcedureResponse response3 = this.client.executeStoredProcedure(createdSproc2.getSelfLink(), options,
                null);
        JSONArray array2 = new JSONArray((response3.getResponseAsString()));
        Assert.assertEquals(0, array2.length());
    }

    @Test
    public void testCreatePermissionWithPartitionKey() throws DocumentClientException {
        // Create user.
        User user = this.client.createUser(TestUtils.getDatabaseLink(this.databaseForTest, true),
                new User("{ 'id': 'new user' }"), null).getResource();

        String collectionId = TestUtils.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/id");

        String permissionId = TestUtils.getUID();
        Permission permissionDefinition = new Permission();
        permissionDefinition.setId(permissionId);
        permissionDefinition.setPermissionMode(PermissionMode.Read);
        permissionDefinition.setResourceLink(createdCollection.getSelfLink());
        String partitionKeyValue = TestUtils.getUID();
        permissionDefinition.setResourcePartitionKey(new PartitionKey(partitionKeyValue));
        Permission permission = this.client.createPermission(user.getSelfLink(), permissionDefinition, null)
                .getResource();
        Assert.assertEquals(permissionId, permission.getId());

        // Read permission.
        permission = this.client.readPermission(permission.getSelfLink(), null).getResource();
        Assert.assertEquals(permissionId, permission.getId());
        PartitionKey partitionKey = permission.getResourcePartitionKey();
        Assert.assertNotNull(partitionKey);
        JSONArray key = new JSONArray(new Object[] {partitionKeyValue});
        Assert.assertEquals(key.toString(), (String) partitionKey.toString());
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
        myPartitionedCollection = this.client
                .createCollection(this.databaseForTest.getSelfLink(), myPartitionedCollection, options).getResource();

        // Insert a document into the created collection.
        String document = "{ 'id': 'document1', 'description': 'this is a test document.' }";
        Document newDocument = new Document(document);
        newDocument = this.client.createDocument(myPartitionedCollection.getSelfLink(), newDocument, null, false)
                .getResource();

        // Read the created document, specifying the required partition key in
        // RequestOptions.
        options = new RequestOptions();
        options.setPartitionKey(new PartitionKey("document1"));
        newDocument = this.client.readDocument(newDocument.getSelfLink(), options).getResource();
    }

    @Test
    public void testPartitionKeyChanged() throws DocumentClientException {
        // Test create document on partition key change.
        String collectionId = TestUtils.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/id");

        String documentDef1 = "{ 'id': 'document1', 'description': 'this is a test document.' }";
        Document newDocument1 = new Document(documentDef1);
        newDocument1 = this.client
                .createDocument(TestUtils.getDocumentCollectionLink(this.databaseForTest, createdCollection, true),
                        newDocument1, null, false)
                .getResource();

        this.client.deleteCollection(createdCollection.getSelfLink(), null);

        // Create a document with the same id but different partition key.
        String documentDef2 = "{ 'order': 1, 'description': 'this is a test document.' }";
        createdCollection = this.createMultiPartitionCollection(collectionId, "/order");
        Document newDocument2 = new Document(documentDef2);

        newDocument2 = this.client
                .createDocument(TestUtils.getDocumentCollectionLink(this.databaseForTest, createdCollection, true),
                        newDocument2, null, false)
                .getResource();
        Assert.assertNotNull(newDocument2);

        // Test upsert document on partition key change.
        collectionId = TestUtils.getUID();
        createdCollection = this.createMultiPartitionCollection(collectionId, "/id");

        documentDef1 = "{ 'id': 'document1', 'description': 'this is a test document.' }";
        newDocument1 = new Document(documentDef1);
        newDocument1 = this.client
                .upsertDocument(TestUtils.getDocumentCollectionLink(this.databaseForTest, createdCollection, true),
                        newDocument1, null, false)
                .getResource();

        this.client.deleteCollection(createdCollection.getSelfLink(), null);

        documentDef2 = "{ 'order': 1, 'description': 'this is a test document.' }";
        createdCollection = this.createMultiPartitionCollection(collectionId, "/order");
        newDocument2 = new Document(documentDef2);

        newDocument2 = this.client
                .upsertDocument(TestUtils.getDocumentCollectionLink(this.databaseForTest, createdCollection, true),
                        newDocument2, null, false)
                .getResource();
        Assert.assertNotNull(newDocument2);
    }

    @Test
    public void testIncorrectPartitionkeyValue() throws DocumentClientException {

        String collectionId = TestUtils.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/id");

        String documentDef1 = "{ 'id': 'document1', 'description': 'this is a test document.' }";
        Document newDocument1 = new Document(documentDef1);

        // Creating a document with mismatched partition key should fail without
        // retry
        try {
            RequestOptions requestOptions = new RequestOptions();
            requestOptions.setPartitionKey(new PartitionKey("document2"));
            newDocument1 = this.client
                    .createDocument(TestUtils.getDocumentCollectionLink(this.databaseForTest, createdCollection, true),
                            newDocument1, requestOptions, false)
                    .getResource();
            Assert.fail("Creating a document with mismatched partition key not fail");
        } catch (DocumentClientException e) {
            Assert.assertEquals(400, e.getStatusCode());
        }
    }

    @Test
    public void testConflictCrud_SelfLink() throws DocumentClientException {
        boolean isNameBased = false;
        testConflictCrud(isNameBased);
    }

    @Test
    public void testConflictCrud_NameBased() throws DocumentClientException {
        boolean isNameBased = true;
        testConflictCrud(isNameBased);
    }

    private void testConflictCrud(boolean isNameBased) throws DocumentClientException {
        String collectionId = TestUtils.getUID();
        DocumentCollection createdCollection = this.createMultiPartitionCollection(collectionId, "/id");
        this.collectionForTest = createdCollection;

        // Read conflicts.
        client.readConflicts(TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased), null).getQueryIterable().toList();

        // Query for conflicts.
        client.queryConflicts(
                TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
                        new SqlParameterCollection(new SqlParameter("@id", "FakeId"))),
                null).getQueryIterable().toList();
    }

    private DocumentCollection createMultiPartitionCollection(String collectionId, String partitionKeyPath)
            throws DocumentClientException {
        return this.createMultiPartitionCollection(collectionId, partitionKeyPath, true);
    }

    private DocumentCollection createMultiPartitionCollection(String collectionId, String partitionKeyPath,
            boolean isNameBased) throws DocumentClientException {
        PartitionKeyDefinition partitionKeyDef = new PartitionKeyDefinition();
        ArrayList<String> paths = new ArrayList<String>();
        paths.add(partitionKeyPath);
        partitionKeyDef.setPaths(paths);

        RequestOptions options = new RequestOptions();
        options.setOfferThroughput(10100);
        DocumentCollection collectionDefinition = new DocumentCollection();
        collectionDefinition.setId(collectionId);
        collectionDefinition.setPartitionKey(partitionKeyDef);
        DocumentCollection createdCollection = this.client
                .createCollection(TestUtils.getDatabaseLink(this.databaseForTest, isNameBased), collectionDefinition,
                        options)
                .getResource();

        return createdCollection;
    }

    @Test
    public void testServiceJNIWrapper_GatewayFallback()
            throws Exception {
        List<ImmutablePair<String, String>> osAndArchPairList = new ArrayList<ImmutablePair<String, String>>() {{
            add(ImmutablePair.of((String) null, (String) null));
            add(ImmutablePair.of("Linux", "i386"));
        }};

        Field osNameField = ServiceJNIWrapper.class.getDeclaredField("osName");
        osNameField.setAccessible(true);
        Field archField = ServiceJNIWrapper.class.getDeclaredField("systemArchitecture");
        archField.setAccessible(true);
        Method loadServiceJNIMethod = ServiceJNIWrapper.class.getDeclaredMethod("loadServiceJNI");
        loadServiceJNIMethod.setAccessible(true);

        String previousOsName = (String) osNameField.get(null);
        String previousArch = (String) archField.get(null);

        for (ImmutablePair<String, String> osAndArch : osAndArchPairList) {
            System.out.println(String.format("Testing: osName = '%s', systemArchitecture = '%s'",
                    osAndArch.getLeft(), osAndArch.getRight()));
            osNameField.set(null, osAndArch.getLeft());
            archField.set(null, osAndArch.getRight());
            loadServiceJNIMethod.invoke(null);

            testCrossPartitionDocumentQueryIterable();
        }

        osNameField.set(null, previousOsName);
        archField.set(null, previousArch);
        loadServiceJNIMethod.invoke(null);
    }
}