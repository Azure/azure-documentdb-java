package com.microsoft.azure.documentdb;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.http.HttpStatus;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.cglib.transform.ClassEmitterTransformer;

final class AnotherPOJO {
    public String pojoProp = "789";
}

public final class GatewayTests extends ParameterizedGatewayTestBase {

    public GatewayTests(DocumentClient client) {
        super(client);
    }

    private static void checkDefaultPolicyPaths(IndexingPolicy indexingPolicy) {
        // no excluded paths
        Assert.assertEquals(0, indexingPolicy.getExcludedPaths().size());
        // included path should be 1 '/'
        Assert.assertEquals(1, indexingPolicy.getIncludedPaths().size());

        // check default path and ts path
        IncludedPath rootIncludedPath = null;
        for (IncludedPath path : indexingPolicy.getIncludedPaths()) {
            if (path.getPath().equals("/*")) {
                rootIncludedPath = path;
            }
        }

        // root path should exist.
        Assert.assertNotNull(rootIncludedPath);
        // RangeIndex for Numbers and HashIndex for Strings.
        Assert.assertEquals(2, rootIncludedPath.getIndexes().size());

        // There exists one HashIndex and one RangeIndex out of these 2 indexes.
        HashIndex hashIndex = null;
        RangeIndex rangeIndex = null;
        for (Index index : rootIncludedPath.getIndexes()) {
            if (index.getKind() == IndexKind.Hash) {
                hashIndex = (HashIndex) index;
            } else if (index.getKind() == IndexKind.Range) {
                rangeIndex = (RangeIndex) index;
            }
        }
        Assert.assertNotNull(hashIndex);
        Assert.assertNotNull(rangeIndex);

        // HashIndex for Strings, skipping checking for precision as that default is set at backend and can change
        Assert.assertEquals(DataType.String, hashIndex.getDataType());
        // RangeIndex for Numbers, skipping checking for precision as that default is set at backend and can change
        Assert.assertEquals(DataType.Number, rangeIndex.getDataType());
    }

    @Test
    public void testJsonSerialization() {
        Document document = new Document();
        document.set("prop0", null);
        document.set("prop1", "abc");
        Document childDocument = new Document("{'child1Prop1': 500}");
        document.set("child1", childDocument);
        document.set("child2", new JSONObject("{'child2Prop1': '800'}"));

        document.set("child3", new StaticPOJOForTest());
        document.set("child4", new AnotherPOJO());
        // Collection of numbers.
        Collection<Integer> collection1 = new ArrayList<Integer>();
        collection1.add(101);
        collection1.add(102);
        document.set("collection1", collection1);
        // Collection of documents.
        Collection<Document> collection2 = new ArrayList<Document>();
        collection2.add(new Document("{'foo': 'bar'}"));
        document.set("collection2", collection2);
        // Collection of POJO.
        Collection<StaticPOJOForTest> collection3 = new ArrayList<StaticPOJOForTest>();
        collection3.add(new StaticPOJOForTest());
        document.set("collection3", collection3);
        // Collection of collections.
        Collection<Collection<Collection<String>>> collection4 = new ArrayList<Collection<Collection<String>>>();
        Collection<Collection<String>> collection5 = new ArrayList<Collection<String>>();
        Collection<String> collection6 = new ArrayList<String>();
        collection6.add("ABCD");
        collection5.add(collection6);
        collection4.add(collection5);
        document.set("collection4", collection4);

        Document expectedDocument = new Document(
                "{" +
                        "  'prop0': null," +
                        "  'prop1': 'abc'," +
                        "  'child1': {" +
                        "    'child1Prop1': 500" +
                        "  }," +
                        "  'child2': {" +
                        "    'child2Prop1': '800'" +
                        "  }," +
                        "  'child3': {" +
                        "    'pojoProp': '456'" +
                        "  }," +
                        "  'child4': {" +
                        "    'pojoProp': '789'" +
                        "  }," +
                        "  'collection1': [101, 102]," +
                        "  'collection2': [{'foo': 'bar'}]," +
                        "  'collection3': [{'pojoProp': '456'}]," +
                        "  'collection4': [[['ABCD']]]" +
                        "}");
        Assert.assertEquals(expectedDocument.toString(), document.toString());

        Assert.assertEquals("456", document.getObject("child3", StaticPOJOForTest.class).pojoProp);
        Assert.assertEquals("789", document.getObject("child4", AnotherPOJO.class).pojoProp);
        Assert.assertEquals("456", document.getCollection("collection3",
                StaticPOJOForTest.class).iterator().next().pojoProp);

        document = new Document("{'pojoProp': '654'}");
        StaticPOJOForTest pojo = document.toObject(StaticPOJOForTest.class);
        Assert.assertEquals("654", pojo.pojoProp);
        JSONObject jsonObject = document.toObject(JSONObject.class);
        Assert.assertEquals("654", jsonObject.getString("pojoProp"));
    }

    @Test
    public void testSqlQuerySpecSerialization() {
        Assert.assertEquals("{}", (new SqlQuerySpec()).toString());
        Assert.assertEquals("{\"query\":\"SELECT 1\"}", (new SqlQuerySpec("SELECT 1")).toString());

        Assert.assertEquals("{\"query\":\"SELECT 1\",\"parameters\":[" +
                        "{\"name\":\"@p1\",\"value\":5}" +
                        "]}",
                (new SqlQuerySpec("SELECT 1",
                        new SqlParameterCollection(new SqlParameter("@p1", 5)))).toString());

        Assert.assertEquals("{\"query\":\"SELECT 1\",\"parameters\":[" +
                        "{\"name\":\"@p1\",\"value\":5}," +
                        "{\"name\":\"@p1\",\"value\":true}" +
                        "]}",
                (new SqlQuerySpec("SELECT 1",
                        new SqlParameterCollection(new SqlParameter("@p1", 5),
                                new SqlParameter("@p1", true)))).toString());

        Assert.assertEquals("{\"query\":\"SELECT 1\",\"parameters\":[" +
                        "{\"name\":\"@p1\",\"value\":\"abc\"}" +
                        "]}",
                (new SqlQuerySpec("SELECT 1",
                        new SqlParameterCollection(new SqlParameter("@p1", "abc")))).toString());

        Assert.assertEquals("{\"query\":\"SELECT 1\",\"parameters\":[" +
                        "{\"name\":\"@p1\",\"value\":[1,2,3]}" +
                        "]}",
                (new SqlQuerySpec("SELECT 1",
                        new SqlParameterCollection(
                                new SqlParameter("@p1", new int[] {1, 2, 3})))).toString());

        Assert.assertEquals("{\"query\":\"SELECT 1\",\"parameters\":[" +
                        "{\"name\":\"@p1\",\"value\":{\"a\":[1,2,3]}}" +
                        "]}",
                (new SqlQuerySpec("SELECT 1",
                        new SqlParameterCollection(new SqlParameter(
                                "@p1", new JSONObject("{\"a\":[1,2,3]}"))))).toString());
    }

    @Test
    public void testDatabaseCrud_SelfLink() throws DocumentClientException {
        boolean isNameBased = false;
        testDatabaseCrud(isNameBased);
    }

    @Test
    public void testDatabaseCrud_NameBased() throws DocumentClientException {
        boolean isNameBased = true;
        testDatabaseCrud(isNameBased);
    }

    private void testDatabaseCrud(boolean isNameBased) throws DocumentClientException {
        // Read databases.
        List<Database> databases = client.readDatabases(null).getQueryIterable().toList();
        // Create a database.
        int beforeCreateDatabasesCount = databases.size();
        Database databaseDefinition = new Database();
        databaseDefinition.setId(GatewayTests.databaseForTestAlternativeId1);

        Database createdDb = client.createDatabase(databaseDefinition, null).getResource();

        // Read a database
        String databaseLink = TestUtils.getDatabaseLink(createdDb, isNameBased);
        Database readDatabase = this.client.readDatabase(databaseLink, null)
                .getResource();
        Assert.assertEquals(createdDb.getId(), readDatabase.getId());
        Assert.assertEquals(createdDb.getCollectionsLink(), readDatabase.getCollectionsLink());
        Assert.assertEquals(createdDb.getUsersLink(), readDatabase.getUsersLink());

        Database databaseDefinition2 = new Database();
        databaseDefinition2.setId(GatewayTests.databaseForTestAlternativeId2);
        Database createdDb2 = client.createDatabase(databaseDefinition2, null).getResource();
        
        Assert.assertEquals(databaseDefinition2.getId(), createdDb2.getId());
        // Read databases after creation.
        databases = client.readDatabases(null).getQueryIterable().toList();
        // create should increase the number of databases.
        Assert.assertEquals(beforeCreateDatabasesCount + 2, databases.size());
        // query all databases.
        databases = client.queryDatabases("SELECT * FROM root", null).getQueryIterable().toList();
        Assert.assertEquals(beforeCreateDatabasesCount + 2, databases.size());
        // query databases.
        databases = client.queryDatabases(new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
                        new SqlParameterCollection(new SqlParameter(
                                "@id", databaseDefinition.getId()))),
                null).getQueryIterable().toList();
        // number of results for the query should be == 1.
        Assert.assertTrue(databases.size() == 1);

        // Delete database.
        client.deleteDatabase(databaseLink, null);

        // Read database after deletion.
        try {
            client.readDatabase(databaseLink, null);
            Assert.fail("Exception didn't happen.");
        } catch (DocumentClientException e) {
            Assert.assertEquals(404, e.getStatusCode());
            Assert.assertEquals("NotFound", e.getError().getCode());
        }
    }

    @Test
    public void testCollectionCrud_SelfLink() throws DocumentClientException {
        boolean isNameBased = false;
        testCollectionCrud(isNameBased);
    }

    @Test
    public void testCollectionCrud_NameBased() throws DocumentClientException {
        boolean isNameBased = true;
        testCollectionCrud(isNameBased);
    }

    private void testCollectionCrud(boolean isNameBased) throws DocumentClientException {
        List<DocumentCollection> collections = client.readCollections(TestUtils.getDatabaseLink(this.databaseForTest, isNameBased),
                null).getQueryIterable().toList();
        // Create a collection.
        int beforeCreateCollectionsCount = collections.size();

        IndexingPolicy consistentPolicy = new IndexingPolicy("{'indexingMode': 'Consistent'}");
        DocumentCollection collectionDefinition = new DocumentCollection();
        collectionDefinition.setId(TestUtils.getUID());
        collectionDefinition.setIndexingPolicy(consistentPolicy);
        DocumentCollection createdCollection = client.createCollection(TestUtils.getDatabaseLink(this.databaseForTest, isNameBased),
                collectionDefinition,
                null).getResource();
        Assert.assertEquals(collectionDefinition.getId(), createdCollection.getId());
        Assert.assertEquals(IndexingMode.Consistent, createdCollection.getIndexingPolicy().getIndexingMode());

        // Read collections after creation.
        collections = client.readCollections(TestUtils.getDatabaseLink(this.databaseForTest, isNameBased), null).getQueryIterable().toList();
        // Create should increase the number of collections.
        Assert.assertEquals(collections.size(), beforeCreateCollectionsCount + 1);
        // Query collections.
        collections = client.queryCollections(TestUtils.getDatabaseLink(this.databaseForTest, isNameBased),
                new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
                        new SqlParameterCollection(new SqlParameter(
                                "@id", collectionDefinition.getId()))),
                null).getQueryIterable().toList();
        Assert.assertTrue(collections.size() > 0);

        // Replacing indexing policy is allowed.
        IndexingPolicy lazyPolicy = new IndexingPolicy("{'indexingMode': 'Lazy'}");
        createdCollection.setIndexingPolicy(lazyPolicy);
        DocumentCollection replacedCollection = client.replaceCollection(createdCollection, null).getResource();
        Assert.assertEquals(IndexingMode.Lazy, replacedCollection.getIndexingPolicy().getIndexingMode());
        Assert.assertEquals(createdCollection.getDocumentsLink(), replacedCollection.getDocumentsLink());
        Assert.assertEquals(createdCollection.getConflictsLink(), replacedCollection.getConflictsLink());
        Assert.assertEquals(createdCollection.getTriggersLink(), replacedCollection.getTriggersLink());
        Assert.assertEquals(createdCollection.getUserDefinedFunctionsLink(), replacedCollection.getUserDefinedFunctionsLink());
        Assert.assertEquals(createdCollection.getStoredProceduresLink(), replacedCollection.getStoredProceduresLink());
        // Replacing collection Id should fail.
        try {
            createdCollection.setId(TestUtils.getUID());
            client.replaceCollection(createdCollection, null);
            Assert.fail("Exception didn't happen.");
        } catch (DocumentClientException e) {
            // Document collection properties other than indexing policy cannot be changed.
            Assert.assertEquals(400, e.getStatusCode());
            Assert.assertEquals("BadRequest", e.getError().getCode());
        }

        // Resetting the id of the createdCollection so that it can be deleted with the named based id
        createdCollection.setId(collectionDefinition.getId());

        // Delete collection.
        client.deleteCollection(TestUtils.getDocumentCollectionLink(this.databaseForTest, createdCollection, isNameBased), null);
        // Read collection after deletion.

        try {
            client.readCollection(TestUtils.getDocumentCollectionLink(this.databaseForTest, createdCollection, isNameBased), null);
            Assert.fail("Exception didn't happen.");
        } catch (DocumentClientException e) {
            Assert.assertEquals(404, e.getStatusCode());
            Assert.assertEquals("NotFound", e.getError().getCode());
        }
    }

    @Test
    public void testSpatialIndex() throws DocumentClientException {
        DocumentCollection collectionDefinition = new DocumentCollection();
        collectionDefinition.setId(TestUtils.getUID());
        IndexingPolicy indexingPolicy = new IndexingPolicy();
        IncludedPath includedPath1 = new IncludedPath();
        includedPath1.setPath("/\"Location\"/?");
        includedPath1.getIndexes().add(new SpatialIndex(DataType.Point));
        indexingPolicy.getIncludedPaths().add(includedPath1);
        IncludedPath includedPath2 = new IncludedPath();
        includedPath2.setPath("/");
        indexingPolicy.getIncludedPaths().add(includedPath2);
        collectionDefinition.setIndexingPolicy(indexingPolicy);

        DocumentCollection collection = client.createCollection(
                this.databaseForTest.getSelfLink(), collectionDefinition, null).getResource();

        Document location1 = new Document(
                "{ 'id': 'loc1', 'Location': { 'type': 'Point', 'coordinates': [ 20.0, 20.0 ] } }");
        client.createDocument(collection.getSelfLink(), location1, null, true);
        Document location2 = new Document(
                "{ 'id': 'loc2', 'Location': { 'type': 'Point', 'coordinates': [ 100.0, 100.0 ] } }");
        client.createDocument(collection.getSelfLink(), location2, null, true);

        List<Document> results = client.queryDocuments(
                collection.getSelfLink(),
                "SELECT * FROM root WHERE (ST_DISTANCE(root.Location, {type: 'Point', coordinates: [20.1, 20]}) < 20000) ",
                null).getQueryIterable().toList();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(location1.getId(), results.get(0).getId());
    }

    @Test
    public void testQueryIterableCrud() throws DocumentClientException {
        List<Document> documents = client.readDocuments(this.collectionForTest.getSelfLink(),
                null).getQueryIterable().toList();
        final int numOfDocuments = 10;

        // Create 10 documents.
        for (int i = 0; i < numOfDocuments; ++i) {
            Document documentDefinition = new Document("{ 'name': 'For paging test' }");
            client.createDocument(this.collectionForTest.getSelfLink(), documentDefinition, null, false);
        }

        int numOfDocumentsPerPage = numOfDocuments / 5;
        FeedOptions fo = new FeedOptions();
        fo.setPageSize(numOfDocumentsPerPage);
        FeedResponse<Document> feedResponse;
        int i = 0;
        String continuationToken = null;

        List<String> currentPage = new ArrayList<String>();
        List<String> previousPage = new ArrayList<String>();
        do {
            currentPage.clear();
            fo.setRequestContinuation(continuationToken);
            feedResponse = client.readDocuments(this.collectionForTest.getSelfLink(), fo);
            i = 0;
            for (Document document : feedResponse.getQueryIterable()) {
                i++;
                currentPage.add(document.getId());
                if (i == numOfDocumentsPerPage) {
                    break;
                }
            }
            continuationToken = feedResponse.getResponseContinuation();
            for (String idFromCurrentPage : currentPage) {
                if (previousPage.contains(idFromCurrentPage)) {
                    Assert.fail("Both pages contain same id " + idFromCurrentPage);
                }
            }
            previousPage.clear();
            previousPage.addAll(currentPage);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {

            }
        } while (continuationToken != null);

        documents = client.readDocuments(this.collectionForTest.getSelfLink(), null).getQueryIterable().toList();
        Assert.assertEquals(numOfDocuments, documents.size());

        // Test fetch next block.
        fo = new FeedOptions();
        fo.setPageSize(6);

        QueryIterable<Document> queryItr =
                client.readDocuments(this.collectionForTest.getSelfLink(), fo).getQueryIterable();
        Assert.assertEquals(6, queryItr.fetchNextBlock().size());
        Assert.assertEquals(4, queryItr.fetchNextBlock().size());

        // Reset the query iterable.
        queryItr.reset();
        Assert.assertEquals(6, queryItr.fetchNextBlock().size());
        Assert.assertEquals(4, queryItr.fetchNextBlock().size());
    }

    @Test
    public void testCollectionIndexingPolicy() throws DocumentClientException {
        // Default indexing mode should be consistent.
        Assert.assertEquals(IndexingMode.Consistent, this.collectionForTest.getIndexingPolicy().getIndexingMode());

        DocumentCollection lazyCollectionDefinition = new DocumentCollection(
                "{" +
                        "  'id': 'lazy collection'," +
                        "  'indexingPolicy': {" +
                        "    'indexingMode': 'lazy'" +
                        "  }" +
                        "}");
        client.deleteCollection(this.collectionForTest.getSelfLink(), null);
        DocumentCollection lazyCollection = client.createCollection(this.databaseForTest.getSelfLink(),
                lazyCollectionDefinition,
                null).getResource();
        // Indexing mode should be lazy.
        Assert.assertEquals(IndexingMode.Lazy, lazyCollection.getIndexingPolicy().getIndexingMode());

        DocumentCollection consistentCollectionDefinition = new DocumentCollection(
                "{" +
                        "  'id': 'lazy collection'," +
                        "  'indexingPolicy': {" +
                        "    'indexingMode': 'Consistent'" +
                        "  }" +
                        "}");
        client.deleteCollection(lazyCollection.getSelfLink(), null);
        DocumentCollection consistentCollection = client.createCollection(this.databaseForTest.getSelfLink(),
                consistentCollectionDefinition,
                null).getResource();
        // Indexing mode should be consistent.
        Assert.assertEquals(this.collectionForTest.getIndexingPolicy().getIndexingMode(),
                IndexingMode.Consistent);
        DocumentCollection collectionDefinition = new DocumentCollection(
                "{" +
                        "  'id': 'CollectionWithIndexingPolicy'," +
                        "  'indexingPolicy': {" +
                        "    'automatic': true," +
                        "    'indexingMode': 'Consistent'," +
                        "    'includedPaths': [" +
                        "      {" +
                        "        'path': '/'," +
                        "        'indexes': [" +
                        "          {" +
                        "            'kind': 'Hash'," +
                        "            'dataType': 'Number'," +
                        "            'precision': 2," +
                        "          }" +
                        "        ]" +
                        "      }" +
                        "    ]," +
                        "    'excludedPaths': [" +
                        "      {" +
                        "        'path': '/\"systemMetadata\"/*'," +
                        "      }" +
                        "    ]" +
                        "  }" +
                        "}");

        // Change the index using the setter.
        HashIndex indexToChange = (HashIndex) (
                collectionDefinition.getIndexingPolicy().getIncludedPaths().iterator().next().getIndexes().iterator().next());
        indexToChange.setDataType(DataType.String);
        indexToChange.setPrecision(3);

        client.deleteCollection(consistentCollection.getSelfLink(), null);
        DocumentCollection collectionWithSecondaryIndex = client.createCollection(this.databaseForTest.getSelfLink(),
                collectionDefinition,
                null).getResource();
        // Check the size of included and excluded paths.
        Assert.assertEquals(1, collectionWithSecondaryIndex.getIndexingPolicy().getIncludedPaths().size());
        IncludedPath includedPath = collectionWithSecondaryIndex.getIndexingPolicy().getIncludedPaths().iterator().next();
        Assert.assertEquals(
                IndexKind.Hash,
                includedPath.getIndexes().iterator().next().getKind());
        HashIndex hashIndex = (HashIndex) includedPath.getIndexes().iterator().next();
        Assert.assertEquals(
                DataType.String,
                hashIndex.getDataType());
        Assert.assertEquals(
                3,
                hashIndex.getPrecision());
        Assert.assertEquals(1, collectionWithSecondaryIndex.getIndexingPolicy().getExcludedPaths().size());
    }

    @Test
    public void testCreateDefaultPolicy() throws DocumentClientException {
        // no indexing policy specified
        DocumentCollection collectionDefinition = new DocumentCollection();
        collectionDefinition.setId("TestCreateDefaultPolicy" + TestUtils.getUID());
        DocumentCollection collection = client.createCollection(
                this.databaseForTest.getSelfLink(), collectionDefinition, null).getResource();
        GatewayTests.checkDefaultPolicyPaths(collection.getIndexingPolicy());

        // partial policy specified
        collectionDefinition = new DocumentCollection();
        collectionDefinition.setId("TestCreateDefaultPolicy" + TestUtils.getUID());
        collectionDefinition.getIndexingPolicy().setIndexingMode(IndexingMode.Lazy);
        collectionDefinition.getIndexingPolicy().setAutomatic(true);

        collection = client.createCollection(
                this.databaseForTest.getSelfLink(), collectionDefinition, null).getResource();
        GatewayTests.checkDefaultPolicyPaths(collection.getIndexingPolicy());

        // default policy
        collectionDefinition = new DocumentCollection();
        collectionDefinition.setId("TestCreateDefaultPolicy" + TestUtils.getUID());
        collectionDefinition.setIndexingPolicy(new IndexingPolicy());

        collection = client.createCollection(
                this.databaseForTest.getSelfLink(), collectionDefinition, null).getResource();
        GatewayTests.checkDefaultPolicyPaths(collection.getIndexingPolicy());

        // missing indexes
        collectionDefinition = new DocumentCollection();
        collectionDefinition.setId("TestCreateDefaultPolicy" + TestUtils.getUID());
        collectionDefinition.setIndexingPolicy(new IndexingPolicy());
        IncludedPath includedPath = new IncludedPath();
        includedPath.setPath("/*");
        collectionDefinition.getIndexingPolicy().getIncludedPaths().add(includedPath);

        collection = client.createCollection(
                this.databaseForTest.getSelfLink(), collectionDefinition, null).getResource();
        GatewayTests.checkDefaultPolicyPaths(collection.getIndexingPolicy());

        // missing precision
        collectionDefinition = new DocumentCollection();
        collectionDefinition.setId("TestCreateDefaultPolicy" + TestUtils.getUID());
        collectionDefinition.setIndexingPolicy(new IndexingPolicy());
        includedPath = new IncludedPath();
        includedPath.setPath("/*");
        Collection<Index> indexes = includedPath.getIndexes();
        indexes.add(new HashIndex(DataType.String));
        indexes.add(new RangeIndex(DataType.Number));

        collectionDefinition.getIndexingPolicy().getIncludedPaths().add(includedPath);

        collection = client.createCollection(
                this.databaseForTest.getSelfLink(), collectionDefinition, null).getResource();
        GatewayTests.checkDefaultPolicyPaths(collection.getIndexingPolicy());
    }

    @Test
    public void testIndexingPolicyOverrides() throws DocumentClientException {

        HashIndex hashIndexOverride = Index.Hash(DataType.String, 5);
        RangeIndex rangeIndexOverride = Index.Range(DataType.Number, 2);
        SpatialIndex spatialIndexOverride = Index.Spatial(DataType.Point);

        Index[] indexes = {hashIndexOverride, rangeIndexOverride, spatialIndexOverride};
        IndexingPolicy indexingPolicy = new IndexingPolicy(indexes);

        DocumentCollection collection = new DocumentCollection();
        collection.setId(TestUtils.getUID());
        collection.setIndexingPolicy(indexingPolicy);

        DocumentCollection createdCollection = client.createCollection(this.databaseForTest.getSelfLink(), collection, null).getResource();

        // check default path.
        IncludedPath rootIncludedPath = null;
        for (IncludedPath path : createdCollection.getIndexingPolicy().getIncludedPaths()) {
            if (path.getPath().equals("/*")) {
                rootIncludedPath = path;
            }
        }
        // root path should exist.
        Assert.assertNotNull(rootIncludedPath);

        Assert.assertEquals(3, rootIncludedPath.getIndexes().size());

        HashIndex hashIndex = null;
        RangeIndex rangeIndex = null;
        SpatialIndex spatialIndex = null;
        for (Index index : rootIncludedPath.getIndexes()) {
            if (index.getKind() == IndexKind.Hash) {
                hashIndex = (HashIndex) index;
            } else if (index.getKind() == IndexKind.Range) {
                rangeIndex = (RangeIndex) index;
            } else if (index.getKind() == IndexKind.Spatial) {
                spatialIndex = (SpatialIndex) index;
            }
        }
        Assert.assertNotNull(hashIndex);
        Assert.assertNotNull(rangeIndex);
        Assert.assertNotNull(spatialIndex);

        Assert.assertEquals(DataType.String, hashIndex.getDataType());
        Assert.assertEquals(5, hashIndex.getPrecision());

        Assert.assertEquals(DataType.Number, rangeIndex.getDataType());
        Assert.assertEquals(2, rangeIndex.getPrecision());

        Assert.assertEquals(DataType.Point, spatialIndex.getDataType());
    }

    @Test
    public void testIndexClassesSerialization() {
        HashIndex hashIndex = new HashIndex("{\"kind\": \"Hash\", \"dataType\": \"String\", \"precision\": 8}");
        Assert.assertEquals(IndexKind.Hash, hashIndex.getKind());
        Assert.assertEquals(DataType.String, hashIndex.getDataType());
        Assert.assertEquals(8, hashIndex.getPrecision());

        RangeIndex rangeIndex = new RangeIndex("{\"kind\": \"Range\", \"dataType\": \"Number\", \"precision\": 4}");
        Assert.assertEquals(IndexKind.Range, rangeIndex.getKind());
        Assert.assertEquals(DataType.Number, rangeIndex.getDataType());
        Assert.assertEquals(4, rangeIndex.getPrecision());

        SpatialIndex spatialIndex = new SpatialIndex("{\"kind\": \"Spatial\", \"dataType\": \"Point\"}");
        Assert.assertEquals(IndexKind.Spatial, spatialIndex.getKind());
        Assert.assertEquals(DataType.Point, spatialIndex.getDataType());
    }

    @Test
    public void testDocumentCrud_SelfLink() throws DocumentClientException {
        boolean isNameBased = false;
        testDocumentCrud(isNameBased);
    }

    @Test
    public void testDocumentCrud_NameBased() throws DocumentClientException {
        boolean isNameBased = true;
        testDocumentCrud(isNameBased);
    }

    private void testDocumentCrud(boolean isNameBased) throws DocumentClientException {

        // Read documents.
        String documentCollectionLink = TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased);
        List<Document> documents = this.client.readDocuments(documentCollectionLink,
                null).getQueryIterable().toList();
        Assert.assertEquals(0, documents.size());

        // create a document
        Document documentDefinition = new Document(
                "{" +
                        "  'name': 'sample document'," +
                        "  'foo': 'bar Ã¤Â½Â Ã¥Â¥Â½'," +  // foo contains some UTF-8 characters.
                        "  'key': 'value'" +
                        "}");

        // Should throw an error because automatic id generation is disabled.
        try {
            this.client.createDocument(documentCollectionLink, documentDefinition, null, true);
            Assert.fail("Exception didn't happen.");
        } catch (DocumentClientException e) {
            Assert.assertEquals(400, e.getStatusCode());
        }

        Document document = this.client.createDocument(documentCollectionLink,
                documentDefinition,
                null,
                false).getResource();
        Assert.assertEquals(documentDefinition.getString("name"), document.getString("name"));
        Assert.assertEquals(documentDefinition.getString("foo"), document.getString("foo"));
        Assert.assertEquals(documentDefinition.getString("bar"), document.getString("bar"));
        Assert.assertNotNull(document.getId());

        // create a document using upsert
        documentDefinition.setId(TestUtils.getUID());
        Document document2 = this.client.upsertDocument(documentCollectionLink,
                documentDefinition,
                null,
                true).getResource();
        Assert.assertEquals(documentDefinition.getString("name"), document2.getString("name"));
        Assert.assertEquals(documentDefinition.getString("foo"), document2.getString("foo"));
        Assert.assertEquals(documentDefinition.getString("bar"), document2.getString("bar"));
        Assert.assertNotNull(document2.getId());

        // Read documents after creation.
        documents = this.client.readDocuments(documentCollectionLink, null).getQueryIterable().toList();
        // Create should increase the number of documents.
        Assert.assertEquals(2, documents.size());

        // Query documents.
        documents = this.client.queryDocuments(documentCollectionLink,
                new SqlQuerySpec("SELECT * FROM root r WHERE r.name=@id",
                        new SqlParameterCollection(new SqlParameter(
                                "@id", documentDefinition.getString("name")))),
                null).getQueryIterable().toList();
        Assert.assertEquals(2, documents.size());

        // Replace document.
        document.set("name", "replaced document");
        document.set("foo", "not bar");
        Document replacedDocument = this.client.replaceDocument(document, null).getResource();
        // Document id property should change.
        Assert.assertEquals("replaced document", replacedDocument.getString("name"));
        // Property should have changed.
        Assert.assertEquals("not bar", replacedDocument.getString("foo"));
        // Document id should stay the same.
        Assert.assertEquals(document.getId(), replacedDocument.getId());
        // Read document.
        Document oneDocumentFromRead = this.client.readDocument(TestUtils.getDocumentLink(this.databaseForTest, this.collectionForTest, replacedDocument, isNameBased), null).getResource();
        Assert.assertEquals(replacedDocument.getId(), oneDocumentFromRead.getId());

        AccessCondition accessCondition = new AccessCondition();
        accessCondition.setCondition(oneDocumentFromRead.getETag());
        accessCondition.setType(AccessConditionType.IfNoneMatch);

        RequestOptions options = new RequestOptions();
        options.setAccessCondition(accessCondition);
        ResourceResponse<Document> rr = this.client.readDocument(TestUtils.getDocumentLink(this.databaseForTest, this.collectionForTest, oneDocumentFromRead, isNameBased), options);
        Assert.assertEquals(rr.getStatusCode(), HttpStatus.SC_NOT_MODIFIED);

        // delete document
        this.client.deleteDocument(TestUtils.getDocumentLink(this.databaseForTest, this.collectionForTest, replacedDocument, isNameBased), null);

        // read documents after deletion
        try {
            this.client.readDocument(TestUtils.getDocumentLink(this.databaseForTest, this.collectionForTest, replacedDocument, isNameBased), null);
            Assert.fail("Exception didn't happen.");
        } catch (DocumentClientException e) {
            Assert.assertEquals(404, e.getStatusCode());
            Assert.assertEquals("NotFound", e.getError().getCode());
        }
    }

    // Upsert test for Document resource - self link version
    @Test
    public void testDocumentUpsert_SelfLink() throws DocumentClientException {
        boolean isNameBased = false;
        testDocumentUpsert(isNameBased);
    }

    // Upsert test for Document resource - name based routing version
    @Test
    public void testDocumentUpsert_NameBased() throws DocumentClientException {
        boolean isNameBased = true;
        testDocumentUpsert(isNameBased);
    }

    private void testDocumentUpsert(boolean isNameBased) throws DocumentClientException {

        // Read documents and get initial count
        List<Document> documents = client
                .readDocuments(
                        TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased), null)
                .getQueryIterable().toList();

        int initialDocumentCount = documents.size();

        // Create a document definition
        Document documentDefinition = new Document(
                "{" +
                        "  'name': 'sample document'," +
                        "  'key': 'value'" +
                        "}");

        // Upsert should create the document with the definition
        Document createdDocument = client.upsertDocument(
                TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                documentDefinition, null, false).getResource();

        // Read documents to check the count and it should increase
        documents = client
                .readDocuments(
                        TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased), null)
                .getQueryIterable().toList();

        Assert.assertEquals(initialDocumentCount + 1, documents.size());

        // Update document.
        createdDocument.set("name", "replaced document");
        createdDocument.set("key", "new value");

        // Upsert should replace the existing document since Id exists
        Document upsertedDocument = client.upsertDocument(
                TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased), createdDocument,
                null, true).getResource();

        // Document id should stay the same.
        Assert.assertEquals(createdDocument.getId(), upsertedDocument.getId());

        // Property should have changed.
        Assert.assertEquals(createdDocument.getString("name"), upsertedDocument.getString("name"));
        Assert.assertEquals(createdDocument.getString("key"), upsertedDocument.getString("key"));

        // Documents count should remain the same
        documents = client
                .readDocuments(
                        TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased), null)
                .getQueryIterable().toList();

        Assert.assertEquals(initialDocumentCount + 1, documents.size());

        // Update document id
        createdDocument.setId(TestUtils.getUID());

        // Upsert should create new document
        Document newDocument = client.upsertDocument(
                TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased), createdDocument,
                null, true).getResource();

        // Verify id property
        Assert.assertEquals(createdDocument.getId(), newDocument.getId());

        // Read documents after upsert to check the count and it should increase
        documents = client
                .readDocuments(
                        TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased), null)
                .getQueryIterable().toList();

        Assert.assertEquals(initialDocumentCount + 2, documents.size());

        // Delete documents
        client.deleteDocument(
                TestUtils.getDocumentLink(this.databaseForTest, this.collectionForTest, upsertedDocument, isNameBased),
                null);
        client.deleteDocument(
                TestUtils.getDocumentLink(this.databaseForTest, this.collectionForTest, newDocument, isNameBased), null);

        // Read documents after delete to check the count and it should be back to original
        documents = client
                .readDocuments(
                        TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased), null)
                .getQueryIterable().toList();

        Assert.assertEquals(initialDocumentCount, documents.size());
    }

    @Test
    public void testPOJODocumentCrud() throws DocumentClientException {

        TestPOJO testPojo = new TestPOJO(10);
        testPojo.id = "MyPojoObejct" + TestUtils.getUID();
        testPojo.stringProperty = "testString";
        testPojo.objectProperty = new TestPOJOInner(100);
        testPojo.setPrivateStringProperty("testStringAccess");

        Document document = client.createDocument(this.collectionForTest.getSelfLink(),
                testPojo,
                null,
                false).getResource();

        Assert.assertEquals(document.getInt("intProperty").intValue(), testPojo.intProperty);

        Assert.assertEquals(document.getString("stringProperty"), testPojo.stringProperty);

        Assert.assertEquals(document.getString("privateStringProperty"), testPojo.getPrivateStringProperty());

        JSONObject jObject = document.getObject("objectProperty");

        Assert.assertEquals(jObject.getInt("intProperty"), testPojo.objectProperty.intProperty);

        Collection<String> coll1 = document.getCollection("stringList", String.class);
        Assert.assertEquals(coll1.size(), testPojo.stringList.size());
        Assert.assertEquals(coll1.iterator().next(), testPojo.stringList.get(0));

        Collection<String> coll2 = document.getCollection("stringArray", String.class);
        Assert.assertEquals(coll2.size(), testPojo.stringArray.length);
        Assert.assertEquals(coll2.iterator().next(), testPojo.stringArray[0]);

        // replace document with POJO
        testPojo.stringProperty = "updatedTestString";
        document = client.replaceDocument(document.getSelfLink(), testPojo, null).getResource();
        Assert.assertEquals(document.getString("stringProperty"), testPojo.stringProperty);

    }

    @Test
    public void testAttachmentCrud_SelfLink() throws DocumentClientException {
        boolean isNameBased = false;
        testAttachmentCrud(isNameBased);
    }

    @Test
    public void testAttachmentCrud_NameBased() throws DocumentClientException {
        boolean isNameBased = true;
        testAttachmentCrud(isNameBased);
    }

    private void testAttachmentCrud(boolean isNameBased) throws DocumentClientException {
        // Create document.
        Document documentDefinition = new Document(
                "{" +
                        "  'id': 'sample document'," +
                        "  'foo': 'bar'," +
                        "  'key': 'value'" +
                        "}");
        String documentCollectionLink = TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased);
        Document document = this.client.createDocument(documentCollectionLink,
                documentDefinition,
                null,
                false).getResource();
        // List all attachments.
        String documentLink = TestUtils.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased);
        List<Attachment> attachments = this.client.readAttachments(documentLink,
                null).getQueryIterable().toList();
        Assert.assertEquals(0, attachments.size());

        MediaOptions validMediaOptions = new MediaOptions();
        validMediaOptions.setSlug("attachment id");
        validMediaOptions.setContentType("application/text");
        MediaOptions invalidMediaOptions = new MediaOptions();
        invalidMediaOptions.setSlug("attachment id");
        invalidMediaOptions.setContentType("junt/test");

        // Create attachment with invalid content type.
        ReadableStream mediaStream = new ReadableStream("stream content.");
        try {
            this.client.createAttachment(documentLink, mediaStream, invalidMediaOptions);
            Assert.assertTrue(false);  // This line shouldn't execute.
        } catch (DocumentClientException e) {
            Assert.assertEquals(400, e.getStatusCode());
        }

        // Create attachment with valid content type.
        mediaStream = new ReadableStream("stream content.");
        Attachment validAttachment = this.client.createAttachment(documentLink,
                mediaStream,
                validMediaOptions).getResource();
        Assert.assertEquals("attachment id", validAttachment.getId());

        mediaStream = new ReadableStream("stream content");
        // Create colliding attachment.
        try {
            this.client.createAttachment(documentLink, mediaStream, validMediaOptions);
            Assert.fail("Exception didn't happen.");
        } catch (DocumentClientException e) {
            Assert.assertEquals(409, e.getStatusCode());
            Assert.assertEquals("Conflict", e.getError().getCode());
        }

        // Create attachment with media link.
        Attachment attachmentDefinition = new Attachment(
                "{" +
                        "  'id': 'dynamic attachment'," +
                        "  'media': 'http://xstore.'," +
                        "  'MediaType': 'Book'," +
                        "  'Author': 'My Book Author'," +
                        "  'Title': 'My Book Title'," +
                        "  'contentType': 'application/text'" +
                        "}");
        Attachment attachment = this.client.createAttachment(documentLink,
                attachmentDefinition,
                null).getResource();
        Assert.assertEquals("Book", attachment.getString("MediaType"));
        Assert.assertEquals("My Book Author", attachment.getString("Author"));

        // List all attachments.
        FeedOptions fo = new FeedOptions();
        fo.setPageSize(1);
        attachments = this.client.readAttachments(documentLink, fo).getQueryIterable().toList();
        Assert.assertEquals(2, attachments.size());

        // Query the attachments
        attachments = this.client.queryAttachments(documentLink, "SELECT * FROM root", null)
                .getQueryIterable().toList();
        Assert.assertEquals(2, attachments.size());

        // Query the attachments by Id
        attachments = this.client.queryAttachments(documentLink,
                new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
                        new SqlParameterCollection(new SqlParameter("@id", attachment.getId()))), null)
                .getQueryIterable().toList();
        Assert.assertEquals(1, attachments.size());

        // Read the attachment
        String attachmentLink = TestUtils.getAttachmentLink(this.databaseForTest, this.collectionForTest, document, attachment, isNameBased);
        Attachment readAttachment = this.client.readAttachment(attachmentLink, null).getResource();
        Assert.assertEquals(attachment.get("media"), readAttachment.get("media"));

        // Replace the attachment.
        attachment.set("Author", "new author");
        this.client.replaceAttachment(attachment, null);
        Assert.assertEquals("Book", attachment.getString("MediaType"));
        Assert.assertEquals("new author", attachment.getString("Author"));
        // Read attachment media.
        InputStream mediaResponse = this.client.readMedia(validAttachment.getMediaLink()).getMedia();
        Assert.assertEquals("stream content.", TestUtils.getStringFromInputStream(mediaResponse));

        mediaStream = new ReadableStream("updated stream content");
        // Update attachment media.
        this.client.updateMedia(validAttachment.getMediaLink(), mediaStream, validMediaOptions);

        // Read attachment media after update.
        // read media buffered (default).
        mediaResponse = this.client.readMedia(validAttachment.getMediaLink()).getMedia();
        Assert.assertEquals("updated stream content", TestUtils.getStringFromInputStream(mediaResponse));

        // read media streamed, should still work.
        ConnectionPolicy streamPolicy = new ConnectionPolicy();
        streamPolicy.setMediaReadMode(MediaReadMode.Streamed);
        DocumentClient mediaClient = new DocumentClient(HOST, MASTER_KEY, streamPolicy, ConsistencyLevel.Session);
        mediaResponse = mediaClient.readMedia(validAttachment.getMediaLink()).getMedia();
        Assert.assertEquals("updated stream content", TestUtils.getStringFromInputStream(mediaResponse));

        // Share attachment with a second document.
        documentDefinition = new Document("{'id': 'document 2'}");
        document = mediaClient.createDocument(documentCollectionLink,
                documentDefinition,
                null,
                false).getResource();
        String secondAttachmentJson = String.format("{" +
                        "  'id': '%s'," +
                        "  'contentType': '%s'," +
                        "  'media': '%s'," +
                        " }",
                validAttachment.getId(),
                validAttachment.getContentType(),
                validAttachment.getMediaLink());
        Attachment secondAttachmentDefinition = new Attachment(secondAttachmentJson);
        documentLink = TestUtils.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased);
        attachment = mediaClient.createAttachment(documentLink, secondAttachmentDefinition, null).getResource();
        Assert.assertEquals(validAttachment.getId(), attachment.getId());
        Assert.assertEquals(validAttachment.getMediaLink(), attachment.getMediaLink());
        Assert.assertEquals(validAttachment.getContentType(), attachment.getContentType());
        // Deleting attachment.
        attachmentLink = TestUtils.getAttachmentLink(this.databaseForTest, this.collectionForTest, document, attachment, isNameBased);
        mediaClient.deleteAttachment(attachmentLink, null);
        // read attachments after deletion
        attachments = mediaClient.readAttachments(documentLink, null).getQueryIterable().toList();
        Assert.assertEquals(0, attachments.size());
    }

    // Upsert test for Attachment resource - self link version
    @Test
    public void testAttachmentUpsert_SelfLink() throws DocumentClientException {
        boolean isNameBased = false;
        testAttachmentUpsert(isNameBased);
    }

    // Upsert test for Attachment resource - name based routing version
    @Test
    public void testAttachmentUpsert_NameBased() throws DocumentClientException {
        boolean isNameBased = true;
        testAttachmentUpsert(isNameBased);
    }

    private void testAttachmentUpsert(boolean isNameBased) throws DocumentClientException {
        // Implement a readable stream class
        class ReadableStream extends InputStream {

            byte[] bytes;
            int index;

            ReadableStream(String content) {
                this.bytes = content.getBytes();
                this.index = 0;
            }

            @Override
            public int read() throws IOException {
                if (this.index < this.bytes.length) {
                    return this.bytes[this.index++];
                }
                return -1;
            }
        }

        // Create document definition
        Document documentDefinition = new Document(
                "{" +
                        "  'name': 'document'," +
                        "  'key': 'value'" +
                        "}");

        // Create document
        Document document = client.createDocument(
                TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                documentDefinition, null, false).getResource();

        // Read attachments and get initial count
        List<Attachment> attachments = client
                .readAttachments(
                        TestUtils.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased), null)
                .getQueryIterable().toList();

        int initialAttachmentCount = attachments.size();

        // Set MediaOptions
        MediaOptions mediaOptions = new MediaOptions();
        mediaOptions.setSlug("attachment id");
        mediaOptions.setContentType("application/text");

        // Set Feed Options
        FeedOptions fo = new FeedOptions();
        fo.setPageSize(1);

        // Initialize media stream
        ReadableStream mediaStream = new ReadableStream("stream content.");

        // Upsert should create the attachment
        Attachment createdMediaStreamAttachment = client.upsertAttachment(
                TestUtils.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased), mediaStream,
                mediaOptions).getResource();

        Assert.assertEquals("attachment id", createdMediaStreamAttachment.getId());

        // Read attachments to check the count and it should increase
        attachments = client
                .readAttachments(TestUtils.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased), fo)
                .getQueryIterable().toList();

        Assert.assertEquals(initialAttachmentCount + 1, attachments.size());

        // Update MediaOptions
        mediaOptions.setSlug("new attachment id");
        mediaStream = new ReadableStream("stream content.");

        // Upsert should create new attachment
        Attachment newMediaStreamAttachment = client.upsertAttachment(
                TestUtils.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased), mediaStream,
                mediaOptions).getResource();

        Assert.assertEquals("new attachment id", newMediaStreamAttachment.getId());

        // Read attachments to check the count and it should increase
        attachments = client
                .readAttachments(TestUtils.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased), fo)
                .getQueryIterable().toList();

        Assert.assertEquals(initialAttachmentCount + 2, attachments.size());

        // Create attachment definition with media link.
        Attachment attachmentDefinition = new Attachment(
                "{" +
                        "  'id': 'dynamic attachment'," +
                        "  'media': 'http://xstore.'," +
                        "  'MediaType': 'Book'," +
                        "  'Author': 'My Book Author'," +
                        "  'Title': 'My Book Title'," +
                        "  'contentType': 'application/text'" +
                        "}");

        // Upsert should create the attachment
        Attachment createdDynamicAttachment = client.upsertAttachment(
                TestUtils.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased),
                attachmentDefinition, null).getResource();

        // Verify id property
        Assert.assertEquals(attachmentDefinition.getId(), createdDynamicAttachment.getId());

        // Read all attachments and verify the count increase
        attachments = client
                .readAttachments(TestUtils.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased), fo)
                .getQueryIterable().toList();

        Assert.assertEquals(initialAttachmentCount + 3, attachments.size());

        // Update attachment
        createdDynamicAttachment.set("Author", "new author");

        // Upsert should replace the existing attachment since Id exists
        Attachment upsertedDynamicAttachment = client.upsertAttachment(
                TestUtils.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased),
                createdDynamicAttachment, null).getResource();

        // Verify id property
        Assert.assertEquals(createdDynamicAttachment.getId(), upsertedDynamicAttachment.getId());

        // Verify property change
        Assert.assertEquals(createdDynamicAttachment.getString("Author"), upsertedDynamicAttachment.getString("Author"));

        // Read all attachments and verify the count remains the same
        attachments = client
                .readAttachments(TestUtils.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased), fo)
                .getQueryIterable().toList();

        Assert.assertEquals(initialAttachmentCount + 3, attachments.size());

        // Change id property
        createdDynamicAttachment.setId(TestUtils.getUID());

        // Upsert should create new attachment
        Attachment newDynamicAttachment = client.upsertAttachment(
                TestUtils.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased),
                createdDynamicAttachment, null).getResource();

        // Verify id property
        Assert.assertEquals(createdDynamicAttachment.getId(), newDynamicAttachment.getId());

        // Read all attachments and verify the count increases
        attachments = client
                .readAttachments(
                        TestUtils.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased), fo)
                .getQueryIterable().toList();

        Assert.assertEquals(initialAttachmentCount + 4, attachments.size());

        // Deleting attachments.
        client.deleteAttachment(TestUtils.getAttachmentLink(this.databaseForTest, this.collectionForTest, document,
                createdMediaStreamAttachment, isNameBased), null);
        client.deleteAttachment(TestUtils.getAttachmentLink(this.databaseForTest, this.collectionForTest, document,
                newMediaStreamAttachment, isNameBased), null);
        client.deleteAttachment(TestUtils.getAttachmentLink(this.databaseForTest, this.collectionForTest, document,
                upsertedDynamicAttachment, isNameBased), null);
        client.deleteAttachment(TestUtils.getAttachmentLink(this.databaseForTest, this.collectionForTest, document,
                newDynamicAttachment, isNameBased), null);

        // read attachments after deletion and verify count remains the same
        attachments = client
                .readAttachments(TestUtils.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased), null)
                .getQueryIterable().toList();

        Assert.assertEquals(initialAttachmentCount, attachments.size());
    }

    @Test
    public void testTriggerCrud_SelfLink() throws DocumentClientException {
        boolean isNameBased = false;
        testTriggerCrud(isNameBased);
    }

    @Test
    public void testTriggerCrud_NameBased() throws DocumentClientException {
        boolean isNameBased = true;
        testTriggerCrud(isNameBased);
    }

    private void testTriggerCrud(boolean isNameBased) throws DocumentClientException {
        // create..
        Trigger triggerDef = new Trigger();
        triggerDef.setId(TestUtils.getUID());
        triggerDef.setTriggerType(TriggerType.Pre);
        triggerDef.setTriggerOperation(TriggerOperation.All);
        triggerDef.setBody("function() {var x = 10;}");
        String documentCollectionLink = TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased);
        Trigger newTrigger = client.createTrigger(documentCollectionLink,
                triggerDef,
                null).getResource();
        Assert.assertNotNull(newTrigger.getBody());
        Assert.assertNotNull(newTrigger.getETag());

        RequestOptions options = new RequestOptions();
        List<String> preTriggerInclude = new ArrayList<String>();
        preTriggerInclude.add(newTrigger.getId());
        options.setPreTriggerInclude(preTriggerInclude);

        Document document = new Document();
        document.setId("noname");
        client.createDocument(documentCollectionLink, document, options, false);

        // replace...
        String id = TestUtils.getUID();
        newTrigger.setId(id);

        newTrigger = client.replaceTrigger(newTrigger, null).getResource();
        Assert.assertEquals(newTrigger.getId(), id);

        String triggerLink = TestUtils.getTriggerLink(this.databaseForTest, this.collectionForTest, newTrigger, isNameBased);
        Trigger readTrigger = client.readTrigger(triggerLink, null).getResource();
        Assert.assertEquals(newTrigger.getTriggerType(), readTrigger.getTriggerType());

        // read triggers:
        List<Trigger> triggers = client.readTriggers(documentCollectionLink,
                null).getQueryIterable().toList();
        Assert.assertEquals(1, triggers.size());

        // query all triggers
        triggers = client.queryTriggers(documentCollectionLink, "SELECT * FROM root", null)
                .getQueryIterable().toList();
        Assert.assertEquals(1, triggers.size());

        // query triggers by Id
        triggers = client.queryTriggers(documentCollectionLink,
                new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
                        new SqlParameterCollection(
                                new SqlParameter("@id", newTrigger.getId()))),
                null).getQueryIterable().toList();
        Assert.assertEquals(1, triggers.size());

        client.deleteTrigger(triggerLink, null);
    }

    // Upsert test for Trigger resource - self link version
    @Test
    public void testTriggerUpsert_SelfLink() throws DocumentClientException {
        boolean isNameBased = false;
        testTriggerUpsert(isNameBased);
    }

    // Upsert test for Trigger resource - name based routing version
    @Test
    public void testTriggerUpsert_NameBased() throws DocumentClientException {
        boolean isNameBased = true;
        testTriggerUpsert(isNameBased);
    }

    private void testTriggerUpsert(boolean isNameBased) throws DocumentClientException {

        // Read triggers and get initial count
        List<Trigger> triggers = client
                .readTriggers(TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                        null)
                .getQueryIterable().toList();

        int initialTriggerCount = triggers.size();

        // Create trigger definition
        Trigger triggerDefinition = new Trigger();
        triggerDefinition.setId(TestUtils.getUID());
        triggerDefinition.setTriggerType(TriggerType.Pre);
        triggerDefinition.setTriggerOperation(TriggerOperation.All);
        triggerDefinition.setBody("function() {var x = 10;}");

        // Upsert should create the trigger
        Trigger createdTrigger = client.upsertTrigger(
                TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                triggerDefinition, null).getResource();

        // Verify Id property
        Assert.assertEquals(triggerDefinition.getId(), createdTrigger.getId());

        // Read triggers to check the count and it should increase
        triggers = client.readTriggers(TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                null)
                .getQueryIterable().toList();

        Assert.assertEquals(initialTriggerCount + 1, triggers.size());

        // Update trigger
        createdTrigger.setTriggerOperation(TriggerOperation.Update);
        createdTrigger.setBody("function() {var x = 20;}");

        // Upsert should replace the trigger since it already exists
        Trigger upsertedTrigger = client.upsertTrigger(
                TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                createdTrigger, null).getResource();

        // Verify Id property
        Assert.assertEquals(createdTrigger.getId(), upsertedTrigger.getId());

        // Verify updated property
        Assert.assertEquals(createdTrigger.getTriggerOperation(), upsertedTrigger.getTriggerOperation());
        Assert.assertEquals(createdTrigger.getBody(), upsertedTrigger.getBody());

        // Read triggers to check the count and it should remain the same
        triggers = client.readTriggers(TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                null)
                .getQueryIterable().toList();

        Assert.assertEquals(initialTriggerCount + 1, triggers.size());

        // Update trigger id
        createdTrigger.setId(TestUtils.getUID());

        // Upsert should create new trigger since id is changed
        Trigger newTrigger = client.upsertTrigger(
                TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                createdTrigger, null).getResource();

        // Verify Id property
        Assert.assertEquals(createdTrigger.getId(), newTrigger.getId());

        // Read triggers to check the count and it should increase
        triggers = client.readTriggers(TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                null)
                .getQueryIterable().toList();

        Assert.assertEquals(initialTriggerCount + 2, triggers.size());

        // Delete triggers
        client.deleteTrigger(TestUtils.getTriggerLink(this.databaseForTest, this.collectionForTest, upsertedTrigger, isNameBased),
                null);
        client.deleteTrigger(TestUtils.getTriggerLink(this.databaseForTest, this.collectionForTest, newTrigger, isNameBased),
                null);

        // Read triggers to check the count and it should remain same
        triggers = client.readTriggers(TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                null)
                .getQueryIterable().toList();

        Assert.assertEquals(initialTriggerCount, triggers.size());
    }

    @Test
    public void testStoredProcedureCrud_SelfLink() throws DocumentClientException {
        boolean isNameBased = false;
        testStoredProcedureCrud(isNameBased);
    }

    @Test
    public void testStoredProcedureCrud_NameBased() throws DocumentClientException {
        boolean isNameBased = true;
        testStoredProcedureCrud(isNameBased);
    }

    private void testStoredProcedureCrud(boolean isNameBased) throws DocumentClientException {

        // create..
        StoredProcedure storedProcedureDef = new StoredProcedure();
        storedProcedureDef.setId(TestUtils.getUID());
        storedProcedureDef.setBody("function() {var x = 10;}");

        String documentCollectionLink = TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased);
        StoredProcedure newStoredProcedure = client.createStoredProcedure(documentCollectionLink,
                storedProcedureDef,
                null).getResource();
        Assert.assertNotNull(newStoredProcedure.getBody());
        Assert.assertNotNull(newStoredProcedure.getETag());

        // replace...
        String id = TestUtils.getUID();
        newStoredProcedure.setId(id);
        newStoredProcedure = client.replaceStoredProcedure(newStoredProcedure, null).getResource();
        Assert.assertEquals(newStoredProcedure.getId(), id);

        String storedProcedureLink = TestUtils.getStoredProcedureLink(this.databaseForTest, this.collectionForTest,
                newStoredProcedure, isNameBased);
        newStoredProcedure = client.readStoredProcedure(storedProcedureLink, null).getResource();

        // read storedProcedures:
        List<StoredProcedure> storedProcedures = client.readStoredProcedures(documentCollectionLink,
                null).getQueryIterable().toList();
        Assert.assertEquals(1, storedProcedures.size());

        // query all storedProcedure
        storedProcedures = client.queryStoredProcedures(documentCollectionLink, "SELECT * FROM root", null)
            .getQueryIterable().toList();
        Assert.assertEquals(1, storedProcedures.size());

        // query StoreProcedure by Id
        storedProcedures = client.queryStoredProcedures(documentCollectionLink,
                new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
                        new SqlParameterCollection(new SqlParameter(
                                "@id", newStoredProcedure.getId()))),
                null).getQueryIterable().toList();
        Assert.assertEquals(1, storedProcedures.size());

        client.deleteStoredProcedure(storedProcedureLink, null);
    }

    // Upsert test for StoredProcedure resource - self link version
    @Test
    public void testStoredProcedureUpsert_SelfLink() throws DocumentClientException {
        boolean isNameBased = false;
        testStoredProcedureUpsert(isNameBased);
    }

    // Upsert test for StoredProcedure resource - name based routing version
    @Test
    public void testStoredProcedureUpsert_NameBased() throws DocumentClientException {
        boolean isNameBased = true;
        testStoredProcedureUpsert(isNameBased);
    }

    private void testStoredProcedureUpsert(boolean isNameBased) throws DocumentClientException {

        // Read sprocs and get initial count
        List<StoredProcedure> sprocs = client
                .readStoredProcedures(TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                        null)
                .getQueryIterable().toList();

        int initialStoredProcedureCount = sprocs.size();

        // Create stored procedure definition
        StoredProcedure storedProcedureDefinition = new StoredProcedure();
        storedProcedureDefinition.setId(TestUtils.getUID());
        storedProcedureDefinition.setBody("function() {var x = 10;}");

        // Upsert should create the sproc
        StoredProcedure createdStoredProcedure = client.upsertStoredProcedure(
                TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                storedProcedureDefinition, null).getResource();

        // Verify Id property
        Assert.assertEquals(storedProcedureDefinition.getId(), createdStoredProcedure.getId());

        // Read sprocs to check the count and it should increase
        sprocs = client
                .readStoredProcedures(
                        TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased), null)
                .getQueryIterable().toList();

        Assert.assertEquals(initialStoredProcedureCount + 1, sprocs.size());

        // Update sproc
        createdStoredProcedure.setBody("function() {var x = 20;}");

        // Upsert should replace the sproc
        StoredProcedure upsertedStoredProcedure = client.upsertStoredProcedure(
                TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                createdStoredProcedure, null).getResource();

        // Verify Id property
        Assert.assertEquals(createdStoredProcedure.getId(), upsertedStoredProcedure.getId());

        // Verify updated property
        Assert.assertEquals(createdStoredProcedure.getBody(), upsertedStoredProcedure.getBody());

        // Read the sprocs and the count should remain the same
        sprocs = client
                .readStoredProcedures(
                        TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased), null)
                .getQueryIterable().toList();

        Assert.assertEquals(initialStoredProcedureCount + 1, sprocs.size());

        // Update sproc id
        createdStoredProcedure.setId(TestUtils.getUID());

        // Upsert should create the sproc since id is changed
        StoredProcedure newStoredProcedure = client.upsertStoredProcedure(
                TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                createdStoredProcedure, null).getResource();

        // Verify Id property
        Assert.assertEquals(createdStoredProcedure.getId(), newStoredProcedure.getId());

        // Read the sprocs and the count should increase
        sprocs = client
                .readStoredProcedures(
                        TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased), null)
                .getQueryIterable().toList();

        Assert.assertEquals(initialStoredProcedureCount + 2, sprocs.size());

        // Delete sprocs
        client.deleteStoredProcedure(TestUtils.getStoredProcedureLink(this.databaseForTest, this.collectionForTest,
                upsertedStoredProcedure, isNameBased), null);
        client.deleteStoredProcedure(TestUtils.getStoredProcedureLink(this.databaseForTest, this.collectionForTest,
                newStoredProcedure, isNameBased), null);

        // Read the sprocs and the count should remain the same
        sprocs = client
                .readStoredProcedures(
                        TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased), null)
                .getQueryIterable().toList();

        Assert.assertEquals(initialStoredProcedureCount, sprocs.size());
    }

    @Test
    public void testStoredProcedureFunctionality_SelfLink() throws DocumentClientException {
        boolean isNameBased = false;
        testStoredProcedureFunctionality(isNameBased);
    }

    @Test
    public void testStoredProcedureFunctionality_NameBased() throws DocumentClientException {
        boolean isNameBased = true;
        testStoredProcedureFunctionality(isNameBased);
    }

    private void testStoredProcedureFunctionality(boolean isNameBased)
            throws DocumentClientException {

        StoredProcedure sproc1 = new StoredProcedure(
                "{" +
                        "  'id': 'storedProcedure1'," +
                        "  'body':" +
                        "    'function () {" +
                        "      for (var i = 0; i < 1000; i++) {" +
                        "        var item = getContext().getResponse().getBody();" +
                        "        if (i > 0 && item != i - 1) throw \"body mismatch\";" +
                        "        getContext().getResponse().setBody(i);" +
                        "      }" +
                        "    }'" +
                        "}");
        StoredProcedure retrievedSproc = client.createStoredProcedure(TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                sproc1,
                null).getResource();
        String result = client.executeStoredProcedure(TestUtils.getStoredProcedureLink(this.databaseForTest, this.collectionForTest, retrievedSproc, isNameBased), null).getResponseAsString();
        Assert.assertEquals("999", result);

        StoredProcedure sproc2 = new StoredProcedure(
                "{" +
                        "  'id': 'storedProcedure2'," +
                        "  'body':" +
                        "    'function () {" +
                        "      for (var i = 0; i < 10; i++) {" +
                        "        getContext().getResponse().appendValue(\"Body\", i);" +
                        "      }" +
                        "    }'" +
                        "}");
        StoredProcedure retrievedSproc2 = client.createStoredProcedure(TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                sproc2,
                null).getResource();
        result = client.executeStoredProcedure(TestUtils.getStoredProcedureLink(this.databaseForTest, this.collectionForTest, retrievedSproc2, isNameBased), null).getResponseAsString();
        Assert.assertEquals("\"0123456789\"", result);

        // String and Integer
        StoredProcedure sproc3 = new StoredProcedure(
                "{" +
                        "  'id': 'storedProcedure3'," +
                        "  'body':" +
                        "    'function (value, num) {" +
                        "      getContext().getResponse().setBody(" +
                        "          \"a\" + value + num * 2);" +
                        "    }'" +
                        "}");
        StoredProcedure retrievedSproc3 = client.createStoredProcedure(TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                sproc3,
                null).getResource();
        result = client.executeStoredProcedure(TestUtils.getStoredProcedureLink(databaseForTest, this.collectionForTest, retrievedSproc3, isNameBased),
                new Object[] {"so", 123}).getResponseAsString();
        Assert.assertEquals("\"aso246\"", result);

        // POJO
        class TempPOJO {
            @SuppressWarnings("unused")
            public String temp = "so2";
        }
        TempPOJO tempPOJO = new TempPOJO();
        StoredProcedure sproc4 = new StoredProcedure(
                "{" +
                        "  'id': 'storedProcedure4'," +
                        "  'body':" +
                        "    'function (value) {" +
                        "      getContext().getResponse().setBody(" +
                        "          \"a\" + value.temp);" +
                        "    }'" +
                        "}");
        StoredProcedure retrievedSproc4 = client.createStoredProcedure(TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                sproc4,
                null).getResource();
        result = client.executeStoredProcedure(TestUtils.getStoredProcedureLink(databaseForTest, this.collectionForTest, retrievedSproc4, isNameBased),
                new Object[] {tempPOJO}).getResponseAsString();
        Assert.assertEquals("\"aso2\"", result);

        // JSONObject
        JSONObject jsonObject = new JSONObject("{'temp': 'so3'}");
        result = client.executeStoredProcedure(TestUtils.getStoredProcedureLink(databaseForTest, this.collectionForTest, retrievedSproc4, isNameBased),
                new Object[] {jsonObject}).getResponseAsString();
        Assert.assertEquals("\"aso3\"", result);

        // Document
        Document document = new Document("{'temp': 'so4'}");
        result = client.executeStoredProcedure(TestUtils.getStoredProcedureLink(databaseForTest, this.collectionForTest, retrievedSproc4, isNameBased),
                new Object[] {document}).getResponseAsString();
        Assert.assertEquals("\"aso4\"", result);
    }

    @Test
    public void testUserDefinedFunctionCrud_SelfLink() throws DocumentClientException {
        boolean isNameBased = false;
        testUserDefinedFunctionCrud(isNameBased);
    }

    @Test
    public void testUserDefinedFunctionCrud_NameBased() throws DocumentClientException {
        boolean isNameBased = true;
        testUserDefinedFunctionCrud(isNameBased);
    }

    private void testUserDefinedFunctionCrud(boolean isNameBased) throws DocumentClientException {

        // create..
        UserDefinedFunction udfDef = new UserDefinedFunction();
        udfDef.setId(TestUtils.getUID());
        udfDef.setBody("function() {var x = 10;}");
        String documentCollectionLink = TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased);
        UserDefinedFunction newUdf = client.createUserDefinedFunction(documentCollectionLink,
                udfDef,
                null).getResource();
        Assert.assertNotNull(newUdf.getBody());
        Assert.assertNotNull(newUdf.getETag());

        // replace...
        String id = TestUtils.getUID();
        newUdf.setId(id);

        newUdf = client.replaceUserDefinedFunction(newUdf, null).getResource();
        Assert.assertEquals(newUdf.getId(), id);

        String userDefinedFunctionLink = TestUtils.getUserDefinedFunctionLink(this.databaseForTest, this.collectionForTest, newUdf, isNameBased);
        newUdf = client.readUserDefinedFunction(userDefinedFunctionLink, null).getResource();
        Assert.assertEquals(newUdf.getId(), id);

        // read udf feed:
        List<UserDefinedFunction> udfs = client.readUserDefinedFunctions(documentCollectionLink,
                null).getQueryIterable().toList();
        Assert.assertEquals(1, udfs.size());

        // query all udf
        udfs = client.queryUserDefinedFunctions(documentCollectionLink, "SELECT * FROM root", null)
            .getQueryIterable().toList();
        Assert.assertEquals(1, udfs.size());

        // query udf by Id
        udfs = client.queryUserDefinedFunctions(
                documentCollectionLink,
                new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
                        new SqlParameterCollection(new SqlParameter("@id", newUdf.getId()))),
                null).getQueryIterable().toList();
        Assert.assertEquals(1, udfs.size());

        client.deleteUserDefinedFunction(userDefinedFunctionLink, null);
    }

    // Upsert test for UserDefinedFunction resource - self link version
    @Test
    public void testUserDefinedFunctionUpsert_SelfLink() throws DocumentClientException {
        boolean isNameBased = false;
        testUserDefinedFunctionUpsert(isNameBased);
    }

    // Upsert test for UserDefinedFunction resource - name based routing version
    @Test
    public void testUserDefinedFunctionUpsert_NameBased() throws DocumentClientException {
        boolean isNameBased = true;
        testUserDefinedFunctionUpsert(isNameBased);
    }

    private void testUserDefinedFunctionUpsert(boolean isNameBased) throws DocumentClientException {

        // Read user defined functions and get initial count
        List<UserDefinedFunction> udfs = client.readUserDefinedFunctions(TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                null).getQueryIterable().toList();

        int initialUserDefinedFunctionCount = udfs.size();

        // Create user defined function definition
        UserDefinedFunction udfDefinition = new UserDefinedFunction();
        udfDefinition.setId(TestUtils.getUID());
        udfDefinition.setBody("function() {var x = 10;}");

        // Upsert should create the udf
        UserDefinedFunction createdUserDefinedFunction = client.upsertUserDefinedFunction(
                TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                udfDefinition, null).getResource();

        // Verify Id property
        Assert.assertEquals(udfDefinition.getId(), createdUserDefinedFunction.getId());

        // Read udfs to check the count and it should increase
        udfs = client.readUserDefinedFunctions(TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                null)
                .getQueryIterable().toList();

        Assert.assertEquals(initialUserDefinedFunctionCount + 1, udfs.size());

        // Update udf
        createdUserDefinedFunction.setBody("function() {var x = 20;}");

        // Upsert should replace the trigger since it already exists
        UserDefinedFunction upsertedUserDefinedFunction = client.upsertUserDefinedFunction(
                TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                createdUserDefinedFunction, null).getResource();

        // Verify Id property
        Assert.assertEquals(createdUserDefinedFunction.getId(), upsertedUserDefinedFunction.getId());

        // Verify updated property
        Assert.assertEquals(createdUserDefinedFunction.getBody(), upsertedUserDefinedFunction.getBody());

        // Read udfs to check the count and it should remain the same
        udfs = client.readUserDefinedFunctions(TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                null)
                .getQueryIterable().toList();

        Assert.assertEquals(initialUserDefinedFunctionCount + 1, udfs.size());

        // Update udf id
        createdUserDefinedFunction.setId(TestUtils.getUID());

        // Upsert should create new udf since id is changed
        UserDefinedFunction newUserDefinedFunction = client.upsertUserDefinedFunction(
                TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                createdUserDefinedFunction, null).getResource();

        // Verify Id property
        Assert.assertEquals(createdUserDefinedFunction.getId(), newUserDefinedFunction.getId());

        // Read udfs to check the count and it should increase
        udfs = client.readUserDefinedFunctions(TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                null)
                .getQueryIterable().toList();

        Assert.assertEquals(initialUserDefinedFunctionCount + 2, udfs.size());

        // Delete udfs
        client.deleteUserDefinedFunction(TestUtils.getUserDefinedFunctionLink(this.databaseForTest, this.collectionForTest, upsertedUserDefinedFunction, isNameBased), null);
        client.deleteUserDefinedFunction(TestUtils.getUserDefinedFunctionLink(this.databaseForTest, this.collectionForTest, newUserDefinedFunction, isNameBased), null);

        // Read udfs to check the count and it should remain same
        udfs = client.readUserDefinedFunctions(TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                null)
                .getQueryIterable().toList();

        Assert.assertEquals(initialUserDefinedFunctionCount, udfs.size());
    }

    @Test
    public void testUserCrud_SelfLink() throws DocumentClientException {
        boolean isNameBased = false;
        testUserCrud(isNameBased);
    }

    @Test
    public void testUserCrud_NameBased() throws DocumentClientException {
        boolean isNameBased = true;
        testUserCrud(isNameBased);
    }

    private void testUserCrud(boolean isNameBased) throws DocumentClientException {

        // List users.
        String databaseLink = TestUtils.getDatabaseLink(this.databaseForTest, isNameBased);
        List<User> users = client.readUsers(databaseLink, null).getQueryIterable().toList();
        int beforeCreateCount = users.size();
        // Create user.
        User user = client.createUser(databaseLink,
                new User("{ 'id': 'new user' }"),
                null).getResource();
        Assert.assertEquals("new user", user.getId());

        // List users after creation.
        users = client.readUsers(databaseLink, null).getQueryIterable().toList();
        Assert.assertEquals(beforeCreateCount + 1, users.size());

        // Query all users
        users = client.queryUsers(databaseLink, "SELECT * FROM root", null)
                .getQueryIterable().toList();
        Assert.assertEquals(1, users.size());

        // Query users by Id.
        users = client.queryUsers(databaseLink,
                new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
                        new SqlParameterCollection(new SqlParameter("@id", "new user"))),
                null).getQueryIterable().toList();
        Assert.assertEquals(1, users.size());

        // Replace user.
        user.setId("replaced user");
        User replacedUser = client.replaceUser(user, null).getResource();
        Assert.assertEquals("replaced user", replacedUser.getId());

        Assert.assertEquals(user.getId(), replacedUser.getId());
        // Read user.
        String userLink = TestUtils.getUserLink(this.databaseForTest, replacedUser, isNameBased);
        user = client.readUser(userLink, null).getResource();
        Assert.assertEquals(replacedUser.getId(), user.getId());
        Assert.assertEquals(replacedUser.getPermissionsLink(), user.getPermissionsLink());
        // Delete user.
        client.deleteUser(TestUtils.getUserLink(this.databaseForTest, user, isNameBased), null);
        // Read user after deletion.
        try {
            client.readUser(userLink, null);
            Assert.fail("Exception didn't happen.");
        } catch (DocumentClientException e) {
            Assert.assertEquals(404, e.getStatusCode());
            Assert.assertEquals("NotFound", e.getError().getCode());
        }
    }

    // Upsert test for User resource - self link version
    @Test
    public void testUserUpsert_SelfLink() throws DocumentClientException {
        boolean isNameBased = false;
        testUserUpsert(isNameBased);
    }

    // Upsert test for User resource - name based routing version
    @Test
    public void testUserUpsert_NameBased() throws DocumentClientException {
        boolean isNameBased = true;
        testUserUpsert(isNameBased);
    }

    private void testUserUpsert(boolean isNameBased) throws DocumentClientException {

        // Read users and get initial count
        List<User> users = client
                .readUsers(TestUtils.getDatabaseLink(this.databaseForTest, isNameBased), null)
                .getQueryIterable().toList();

        int initialUserCount = users.size();

        // Create user definition
        User userDefinition = new User();
        userDefinition.setId(TestUtils.getUID());

        // Upsert should create the user
        User createdUser = client.upsertUser(
                TestUtils.getDatabaseLink(this.databaseForTest, isNameBased),
                userDefinition, null).getResource();

        // Verify Id property
        Assert.assertEquals(userDefinition.getId(), createdUser.getId());

        // Read users to check the count and it should increase
        users = client.readUsers(TestUtils.getDatabaseLink(this.databaseForTest, isNameBased), null)
                .getQueryIterable().toList();

        Assert.assertEquals(initialUserCount + 1, users.size());

        // Update user id
        userDefinition.setId(TestUtils.getUID());

        // Upsert should create new user since id is changed
        User newUser = client.upsertUser(
                TestUtils.getDatabaseLink(this.databaseForTest, isNameBased),
                userDefinition, null).getResource();

        // Verify Id property
        Assert.assertEquals(userDefinition.getId(), newUser.getId());

        // Read users to check the count and it should increase
        users = client.readUsers(TestUtils.getDatabaseLink(this.databaseForTest, isNameBased), null)
                .getQueryIterable().toList();

        Assert.assertEquals(initialUserCount + 2, users.size());

        // Delete users
        client.deleteUser(TestUtils.getUserLink(this.databaseForTest, createdUser, isNameBased), null);
        client.deleteUser(TestUtils.getUserLink(this.databaseForTest, newUser, isNameBased), null);

        // Read users to check the count and it should remain same
        users = client.readUsers(TestUtils.getDatabaseLink(this.databaseForTest, isNameBased), null)
                .getQueryIterable().toList();

        Assert.assertEquals(initialUserCount, users.size());
    }

    @Test
    public void testPermissionCrud_SelfLink() throws DocumentClientException {
        boolean isNameBased = false;
        testPermissionCrud(isNameBased);
    }

    @Test
    public void testPermissionCrud_NameBased() throws DocumentClientException {
        boolean isNameBased = true;
        testPermissionCrud(isNameBased);
    }

    private void testPermissionCrud(boolean isNameBased) throws DocumentClientException {

        // Create user.
        String databaseLink = TestUtils.getDatabaseLink(this.databaseForTest, isNameBased);
        User user = client.createUser(databaseLink,
                new User("{ 'id': 'new user' }"),
                null).getResource();
        // List permissions.
        String userLink = TestUtils.getUserLink(this.databaseForTest, user, isNameBased);
        List<Permission> permissions = client.readPermissions(userLink, null).getQueryIterable().toList();
        int beforeCreateCount = permissions.size();
        Permission permissionDefinition = new Permission();
        permissionDefinition.setId("new permission");
        permissionDefinition.setPermissionMode(PermissionMode.Read);
        permissionDefinition.setResourceLink("dbs/AQAAAA==/colls/AQAAAJ0fgTc=");

        // Create permission.
        Permission permission = client.createPermission(userLink, permissionDefinition, null).getResource();
        Assert.assertEquals("new permission", permission.getId());

        // List permissions after creation.
        permissions = client.readPermissions(userLink, null).getQueryIterable().toList();
        Assert.assertEquals(beforeCreateCount + 1, permissions.size());

        // Query all permissions
        permissions = client.queryPermissions(userLink, "SELECT * FROM root", null)
                .getQueryIterable().toList();
        Assert.assertEquals(1, permissions.size());

        // Query permissions by Id.
        permissions = client.queryPermissions(userLink,
                new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
                        new SqlParameterCollection(new SqlParameter(
                                "@id", permission.getId()))),
                null).getQueryIterable().toList();
        Assert.assertEquals(1, permissions.size());

        // Replace permission.
        permission.setId("replaced permission");
        Permission replacedPermission = client.replacePermission(
                permission, null).getResource();
        Assert.assertEquals("replaced permission", replacedPermission.getId());
        Assert.assertEquals(permission.getId(), replacedPermission.getId());

        // Read permission.
        String permissionLink = TestUtils.getPermissionLink(this.databaseForTest, user, replacedPermission, isNameBased);
        permission = client.readPermission(permissionLink, null).getResource();
        Assert.assertEquals(replacedPermission.getId(), permission.getId());
        // Delete permission.
        client.deletePermission(permissionLink, null);
        // Read permission after deletion.
        try {
            client.readPermission(TestUtils.getPermissionLink(this.databaseForTest, user, permission, isNameBased), null);
            Assert.fail("Exception didn't happen.");
        } catch (DocumentClientException e) {
            Assert.assertEquals(404, e.getStatusCode());
            Assert.assertEquals("NotFound", e.getError().getCode());
        }
    }

    // Upsert test for Permission resource - self link version
    @Test
    public void testPermissionUpsert_SelfLink() throws DocumentClientException {
        boolean isNameBased = false;
        testPermissionUpsert(isNameBased);
    }

    // Upsert test for Permission resource - name based routing version
    @Test
    public void testPermissionUpsert_NameBased() throws DocumentClientException {
        boolean isNameBased = true;
        testPermissionUpsert(isNameBased);
    }

    private void testPermissionUpsert(boolean isNameBased) throws DocumentClientException {

        // Create user definition
        User userDefinition = new User();
        userDefinition.setId(TestUtils.getUID());

        // Create user.
        User user = client.createUser(
                TestUtils.getDatabaseLink(this.databaseForTest, isNameBased),
                userDefinition, null).getResource();

        // Read permissions and get initial count
        List<Permission> permissions = client
                .readPermissions(TestUtils.getUserLink(this.databaseForTest, user, isNameBased), null)
                .getQueryIterable().toList();

        int initialPermissionCount = permissions.size();

        // Create permission definition
        Permission permissionDefinition = new Permission();
        permissionDefinition.setId(TestUtils.getUID());
        permissionDefinition.setPermissionMode(PermissionMode.Read);
        permissionDefinition.setResourceLink("dbs/AQAAAA==/colls/AQAAAJ0fgTc=");

        // Upsert should create the permission
        Permission createdPermission = client.upsertPermission(
                TestUtils.getUserLink(this.databaseForTest, user, isNameBased),
                permissionDefinition, null).getResource();

        // Verify Id property
        Assert.assertEquals(permissionDefinition.getId(), createdPermission.getId());

        // Read permissions to check the count and it should increase
        permissions = client.readPermissions(TestUtils.getUserLink(this.databaseForTest, user, isNameBased), null)
                .getQueryIterable().toList();

        Assert.assertEquals(initialPermissionCount + 1, permissions.size());

        // Update permission
        createdPermission.setPermissionMode(PermissionMode.All);

        // Upsert should replace the permission since it already exists
        Permission upsertedPermission = client.upsertPermission(
                TestUtils.getUserLink(this.databaseForTest, user, isNameBased),
                createdPermission, null).getResource();

        // Verify Id property
        Assert.assertEquals(createdPermission.getId(), upsertedPermission.getId());

        // Verify updated property
        Assert.assertEquals(createdPermission.getPermissionMode(), upsertedPermission.getPermissionMode());

        // Read permissions to check the count and it should remain the same
        permissions = client.readPermissions(TestUtils.getUserLink(this.databaseForTest, user, isNameBased), null)
                .getQueryIterable().toList();

        Assert.assertEquals(initialPermissionCount + 1, permissions.size());

        // Update permission id
        createdPermission.setId(TestUtils.getUID());
        // ResourceLink needs to be changed along with the ID in order to create a new permission
        createdPermission.setResourceLink("dbs/N9EdAA==/colls/N9EdAIugXgA=");

        // Upsert should create new permission since id is changed
        Permission newPermission = client.upsertPermission(
                TestUtils.getUserLink(this.databaseForTest, user, isNameBased),
                createdPermission, null).getResource();

        // Verify Id property
        Assert.assertEquals(createdPermission.getId(), newPermission.getId());

        // Verify ResourceLink property
        Assert.assertEquals(createdPermission.getResourceLink(), newPermission.getResourceLink());

        // Read permissions to check the count and it should increase
        permissions = client.readPermissions(TestUtils.getUserLink(this.databaseForTest, user, isNameBased), null)
                .getQueryIterable().toList();

        Assert.assertEquals(initialPermissionCount + 2, permissions.size());

        // Delete permissions
        client.deletePermission(TestUtils.getPermissionLink(this.databaseForTest, user, upsertedPermission, isNameBased), null);
        client.deletePermission(TestUtils.getPermissionLink(this.databaseForTest, user, newPermission, isNameBased), null);

        // Read permissions to check the count and it should remain same
        permissions = client.readPermissions(TestUtils.getUserLink(this.databaseForTest, user, isNameBased), null)
                .getQueryIterable().toList();

        Assert.assertEquals(initialPermissionCount, permissions.size());
    }

    @Test
    public void testDatabaseAccount() throws DocumentClientException {

        DatabaseAccount dba = client.getDatabaseAccount();
        Assert.assertNotNull("dba Address link works", dba.getAddressesLink());

        if (dba.getConsistencyPolicy().getDefaultConsistencyLevel() == ConsistencyLevel.BoundedStaleness) {
            Assert.assertTrue("StaleInternal should be larger than 5 seconds",
                    dba.getConsistencyPolicy().getMaxStalenessIntervalInSeconds() >= 5);
            Assert.assertTrue("StaleInternal boundness should be larger than 10",
                    dba.getConsistencyPolicy().getMaxStalenessPrefix() >= 10);
        }

    }

    @Test
    public void testAuthorization() throws DocumentClientException {
        // Sets up entities for this test.
        // Create document1
        Document document1 = this.client.createDocument(
                collectionForTest.getSelfLink(),
                new Document("{ 'id': 'coll1doc1', 'spam': 'eggs', 'key': 'value' }"), null, false).getResource();
        // Create document2
        Document document2 = this.client.createDocument(
                collectionForTest.getSelfLink(),
                new Document("{ 'id': 'coll1doc2', 'spam': 'eggs2', 'key': 'value2' }"), null, false).getResource();

        // Create a new collection for test.
        DocumentCollection anotherCollectionForTest = this.client.createCollection(
                databaseForTest.getSelfLink(),
                new DocumentCollection("{ 'id': 'sample collection2' }"),
                null).getResource();
        // Create user1.
        User user1 = this.client.createUser(databaseForTest.getSelfLink(),
                new User("{ 'id': 'user1' }"),
                null).getResource();

        Permission permission1Definition = new Permission(String.format(
                "{" +
                        "  'id': 'permission On Coll1'," +
                        "  'permissionMode': 'Read'," +
                        "  'resource': '%s'" +
                        "}", collectionForTest.getSelfLink()));
        // Create permission for collectionForTest
        Permission permission1 = this.client.createPermission(user1.getSelfLink(),
                permission1Definition,
                null).getResource();

        // Create user 2.
        User user2 = this.client.createUser(databaseForTest.getSelfLink(),
                new User("{ 'id': 'user2' }"),
                null).getResource();

        Permission permission2Definition = new Permission(String.format(
                "{" +
                        "  'id': 'permission On coll2'," +
                        "  'permissionMode': 'All'," +
                        "  'resource': '%s'" +
                        "}", anotherCollectionForTest.getSelfLink()));
        // Create permission on anotherCollectionForTest.
        Permission permission2 = this.client.createPermission(user2.getSelfLink(),
                permission2Definition,
                null).getResource();

        // Now the resources for this test is fully prepared.

        // Client without any authorization will fail.
        DocumentClient badClient = new DocumentClient(HOST,
                "",
                new ConnectionPolicy(),
                ConsistencyLevel.Session);

        try {
            badClient.readDatabases(null).getQueryIterable().toList();
            Assert.fail("Exception didn't happen.");
        } catch (Exception e) {

        }

        // Client with read access on the first collection only.
        List<Permission> resourceTokens1 = new ArrayList<Permission>();
        resourceTokens1.add(permission1);
        DocumentClient clientForCollection1 = new DocumentClient(HOST,
                resourceTokens1,
                new ConnectionPolicy(),
                ConsistencyLevel.Session);

        // 1. Success-- Use Col1 Permission to Read
        DocumentCollection obtainedCollection1 =
                clientForCollection1.readCollection(collectionForTest.getSelfLink(), null).getResource();
        // 2. Failure-- Use Col1 Permission to delete
        try {
            clientForCollection1.deleteCollection(obtainedCollection1.getSelfLink(), null);
            Assert.fail("Exception didn't happen.");
        } catch (DocumentClientException e) {
            Assert.assertEquals(403, e.getStatusCode());
            Assert.assertEquals("Forbidden", e.getError().getCode());
        }

        // 3. Success-- Use Col1 Permission to Read All Docs
        Assert.assertEquals("Expected 2 Documents to be succesfully read",
                2,
                clientForCollection1.readDocuments(obtainedCollection1.getSelfLink(),
                        null).getQueryIterable().toList().size());

        // 4. Success-- Use Col1 Permission to Read Col1Doc1
        Document successDoc = clientForCollection1.readDocument(document1.getSelfLink(), null).getResource();
        Assert.assertEquals("Expected to read children using parent permissions",
                document1.getId(),
                successDoc.getId());

        // Client with full access on the anotherDocumentForTest.
        List<Permission> resourceTokens2 = new ArrayList<Permission>();
        resourceTokens2.add(permission2);
        DocumentClient clientForCollection2 = new DocumentClient(HOST,
                resourceTokens2,
                new ConnectionPolicy(),
                ConsistencyLevel.Session);
        Document newDocument2 = new Document(String.format(
                "{" +
                        "  'CustomProperty1': 'BBBBBB'," +
                        "  'customProperty2': 1000," +
                        "  'id': '%s'" +
                        "}", document2.getId()));  // Same id.
        successDoc = clientForCollection2.createDocument(anotherCollectionForTest.getSelfLink(),
                newDocument2,
                null,
                true).getResource();
        Assert.assertEquals("document should have been created successfully",
                "BBBBBB",
                successDoc.getString("CustomProperty1"));
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

        // Read conflicts.
        String documentCollectionLink = TestUtils.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased);
        List<Conflict> conflicts = client.readConflicts(documentCollectionLink, null).getQueryIterable().toList();
        Assert.assertEquals(0, conflicts.size());

        // Query for all conflicts
        conflicts = client.queryConflicts(documentCollectionLink, "SELECT * FROM root", null)
                .getQueryIterable().toList();
        Assert.assertEquals(0, conflicts.size());

        // Query for conflicts by Id
        client.queryConflicts(
                documentCollectionLink,
                new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
                        new SqlParameterCollection(new SqlParameter("@id", "FakeId"))),
                null).getQueryIterable().toList();
        Assert.assertEquals(0, conflicts.size());
    }

    @Test
    public void testCustomizedUserAgentCrud() throws DocumentClientException {
        ConnectionPolicy policy = new ConnectionPolicy();
        policy.setUserAgentSuffix("My-Custom-User-Agent");
        Assert.assertEquals("User-agent suffix should've been added", "My-Custom-User-Agent", policy.getUserAgentSuffix());
    }

    @Test
    @Ignore
    public void testQuotaHeaders() {

        try {
            Database dbToCreate = new Database();
            dbToCreate.setId("DocumentQuotaDB" + TestUtils.getUID());
            Database db = client.createDatabase(dbToCreate, null).getResource();

            DocumentCollection collectionToCreate = new DocumentCollection();
            collectionToCreate.setId("DocumentQuotaDBDocumentCollectionConsistent");
            IndexingPolicy policy = new IndexingPolicy();
            policy.setIndexingMode(IndexingMode.Consistent);
            collectionToCreate.setIndexingPolicy(policy);
            ;

            DocumentCollection collection = client.createCollection(db.getSelfLink(), collectionToCreate, null).getResource();

            for (int i = 0; i < 10000; i++) {
                String docName = TestUtils.getUID().substring(0, 10);
                Document smallDoc = new Document();
                smallDoc.setId(docName);
                client.createDocument(collection.getSelfLink(), smallDoc, null, false).getResource();
            }

            Thread.sleep(5 * 60 * 1000);

            ResourceResponse<DocumentCollection> response = client.readCollection("dbs/bB1nAA==/colls/bB1nAKpXNQA=/", null);

            Assert.assertTrue(response.getCollectionSizeUsage() > response.getDocumentUsage());
            Assert.assertTrue(response.getCollectionSizeUsage() == 4 * 1024);
            Assert.assertTrue(response.getDocumentUsage() == 2 * 1024);
        } catch (DocumentClientException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testIndexProgressHeaders() throws DocumentClientException {

        Document doc = new Document();
        doc.setId("doc01");
        doc.set("name", "name01");

        // Consistent-indexing collection
        {
            DocumentCollection consistentCollection = new DocumentCollection();
            consistentCollection.setId("Consistent Collection");
            consistentCollection = client.createCollection(this.databaseForTest.getSelfLink(), consistentCollection, null).getResource();
            ResourceResponse<DocumentCollection> collectionResponse = client.readCollection(consistentCollection.getSelfLink(), null);
            Assert.assertEquals(100, collectionResponse.getIndexTransformationProgress());
            Assert.assertEquals(-1, collectionResponse.getLazyIndexingProgress());
        }

        // Lazy-indexing collection
        {
            DocumentCollection lazyCollection = new DocumentCollection();
            lazyCollection.setId("Lazy Collection");
            lazyCollection.getIndexingPolicy().setIndexingMode(IndexingMode.Lazy);
            lazyCollection = client.createCollection(this.databaseForTest.getSelfLink(), lazyCollection, null).getResource();
            ResourceResponse<DocumentCollection> collectionResponse = client.readCollection(lazyCollection.getSelfLink(), null);
            Assert.assertEquals(100, collectionResponse.getIndexTransformationProgress());
            Assert.assertTrue(collectionResponse.getLazyIndexingProgress() >= 0 && collectionResponse.getLazyIndexingProgress() <= 100);
        }

        // None-indexing collection
        {
            DocumentCollection noneCollection = new DocumentCollection();
            noneCollection.setId("None Collection");
            noneCollection.getIndexingPolicy().setIndexingMode(IndexingMode.None);
            noneCollection.getIndexingPolicy().setAutomatic(false);
            noneCollection = client.createCollection(this.databaseForTest.getSelfLink(), noneCollection, null).getResource();
            ResourceResponse<DocumentCollection> collectionResponse = client.readCollection(noneCollection.getSelfLink(), null);
            Assert.assertEquals(100, collectionResponse.getIndexTransformationProgress());
            Assert.assertEquals(-1, collectionResponse.getLazyIndexingProgress());
        }
    }

    @Test
    public void testIdValidation() throws DocumentClientException {

        // Id shouldn't end with a space.
        try {
            client.createDocument(
                    collectionForTest.getSelfLink(),
                    new Document("{ 'id': 'id_with_space ', 'key': 'value' }"), null, false).getResource();
            Assert.fail("An exception is expected.");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("Id ends with a space.", e.getMessage());
        }
        // Id shouldn't contain '/'.
        try {
            client.createDocument(
                    collectionForTest.getSelfLink(),
                    new Document("{ 'id': 'id_with_illegal/_chars/', 'key': 'value' }"), null, false).getResource();
            Assert.fail("An exception is expected.");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("Id contains illegal chars.", e.getMessage());
        }
        // Id shouldn't contain '\'.
        try {
            client.createDocument(
                    collectionForTest.getSelfLink(),
                    new Document("{ 'id': 'id_with_illegal\\\\_chars', 'key': 'value' }"), null, false).getResource();
            Assert.fail("An exception is expected.");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("Id contains illegal chars.", e.getMessage());
        }
        // Id shouldn't contain '?'.
        try {
            client.createDocument(
                    collectionForTest.getSelfLink(),
                    new Document("{ 'id': 'id_with_illegal?_?chars', 'key': 'value' }"), null, false).getResource();
            Assert.fail("An exception is expected.");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("Id contains illegal chars.", e.getMessage());
        }
        // Id shouldn't contain '#'.
        try {
            client.createDocument(
                    collectionForTest.getSelfLink(),
                    new Document("{ 'id': 'id_with_illegal#_chars', 'key': 'value' }"), null, false).getResource();
            Assert.fail("An exception is expected.");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("Id contains illegal chars.", e.getMessage());
        }
        // Id can begin with a space.
        client.createDocument(
                collectionForTest.getSelfLink(),
                new Document("{ 'id': ' id_begin_space', 'key': 'value' }"), null, false).getResource();
        Assert.assertTrue(true);

        // Id can contain space within.
        client.createDocument(
                collectionForTest.getSelfLink(),
                new Document("{ 'id': 'id containing space', 'key': 'value' }"), null, false).getResource();
        Assert.assertTrue(true);
    }

    @Test
    public void testIdCaseValidation() throws DocumentClientException {

        List<DocumentCollection> collections = client.readCollections(TestUtils.getDatabaseLink(this.databaseForTest, true),
                null).getQueryIterable().toList();

        int beforeCreateCollectionsCount = collections.size();

        // pascalCase
        DocumentCollection collectionDefinition1 = new DocumentCollection();
        collectionDefinition1.setId("sampleCollection");

        // CamelCase
        DocumentCollection collectionDefinition2 = new DocumentCollection();
        collectionDefinition2.setId("SampleCollection");

        // Create 2 collections with different casing of IDs
        DocumentCollection createdCollection1 = client.createCollection(TestUtils.getDatabaseLink(this.databaseForTest, true),
                collectionDefinition1,
                null).getResource();

        DocumentCollection createdCollection2 = client.createCollection(TestUtils.getDatabaseLink(this.databaseForTest, true),
                collectionDefinition2,
                null).getResource();

        // Verify if additional 2 collections got created
        collections = client.readCollections(TestUtils.getDatabaseLink(this.databaseForTest, true), null).getQueryIterable().toList();
        Assert.assertEquals(collections.size(), beforeCreateCollectionsCount + 2);

        // Verify that collections are created with specified IDs
        Assert.assertEquals(collectionDefinition1.getId(), createdCollection1.getId());
        Assert.assertEquals(collectionDefinition2.getId(), createdCollection2.getId());
    }

    @Test
    public void testIdUnicodeValidation() throws DocumentClientException {

        // Unicode chars in Hindi for Id which translates to: "Hindi is one of the main  languages of India"
        DocumentCollection collectionDefinition1 = new DocumentCollection();
        collectionDefinition1.setId("à¤¹à¤¿à¤¨à¥�à¤¦à¥€ à¤­à¤¾à¤°à¤¤ à¤•à¥‡ à¤®à¥�à¤–à¥�à¤¯ à¤­à¤¾à¤·à¤¾à¤“à¤‚ à¤®à¥‡à¤‚ à¤¸à¥‡ à¤�à¤• à¤¹à¥ˆ");

        // Special chars for Id
        DocumentCollection collectionDefinition2 = new DocumentCollection();
        collectionDefinition2.setId("!@$%^&*()-~`'_[]{}|;:,.<>");

        // verify that collections are created with specified IDs
        DocumentCollection createdCollection1 = client.createCollection(TestUtils.getDatabaseLink(this.databaseForTest, true),
                collectionDefinition1,
                null).getResource();

        DocumentCollection createdCollection2 = client.createCollection(TestUtils.getDatabaseLink(this.databaseForTest, true),
                collectionDefinition2,
                null).getResource();

        // Verify that collections are created with specified IDs
        Assert.assertEquals(collectionDefinition1.getId(), createdCollection1.getId());
        Assert.assertEquals(collectionDefinition2.getId(), createdCollection2.getId());
    }

    @Test
    public void testSessionContainer() throws DocumentClientException {

        DocumentCollection collectionDefinition = new DocumentCollection();
        collectionDefinition.setId(TestUtils.getUID());
        DocumentCollection collection = client.createCollection(TestUtils.getDatabaseLink(this.databaseForTest, true),
                collectionDefinition,
                null).getResource();
        // create a document
        Document documentDefinition = new Document(
                "{" +
                        "  'name': 'sample document'," +
                        "  'key': '0'" +
                        "}");

        Document document = client.createDocument(TestUtils.getDocumentCollectionLink(this.databaseForTest, collection, false),
                documentDefinition,
                null,
                false).getResource();

        // Replace document.
        for (int i = 0; i < 100; i++) {

            // Update the "key" property in a tight loop
            document.set("key", Integer.toString(i));
            // Replace the document
            Document replacedDocument = client.replaceDocument(document, null).getResource();

            // Read the document.
            Document documentFromRead = client.readDocument(TestUtils.getDocumentLink(this.databaseForTest, collection, replacedDocument, true), null).getResource();
            // Verify that we read our own write(key property)
            Assert.assertEquals(replacedDocument.getString("key"), documentFromRead.getString("key"));
        }
    }

    static class StaticPOJOForTest {
        // Jackson's readValue method supports member class only if it's static.
        public String pojoProp = "456";
    }

    class TestPOJOInner {
        public int intProperty;

        public TestPOJOInner(int i) {
            this.intProperty = i;
        }
    }

    class TestPOJO {

        public String id;
        public int intProperty;
        public String stringProperty;
        public TestPOJOInner objectProperty;
        public List<String> stringList;
        public String[] stringArray;
        private String privateStringProperty;

        public TestPOJO(int i) {
            this.intProperty = i;

            this.stringList = new ArrayList<String>();
            this.stringList.add("ONE");
            this.stringList.add("TWO");
            this.stringList.add("THREE");

            this.stringArray = new String[] {"One", "Two", "Three"};
        }

        public String getPrivateStringProperty() {
            return this.privateStringProperty;
        }

        public void setPrivateStringProperty(String value) {
            this.privateStringProperty = value;
        }
    }
}
