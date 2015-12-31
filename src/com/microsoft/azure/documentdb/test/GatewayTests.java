package com.microsoft.azure.documentdb.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.microsoft.azure.documentdb.AccessCondition;
import com.microsoft.azure.documentdb.AccessConditionType;
import com.microsoft.azure.documentdb.Attachment;
import com.microsoft.azure.documentdb.Conflict;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.DataType;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.DatabaseAccount;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.FeedResponse;
import com.microsoft.azure.documentdb.HashIndex;
import com.microsoft.azure.documentdb.HashPartitionResolver;
import com.microsoft.azure.documentdb.IncludedPath;
import com.microsoft.azure.documentdb.Index;
import com.microsoft.azure.documentdb.IndexKind;
import com.microsoft.azure.documentdb.IndexingMode;
import com.microsoft.azure.documentdb.IndexingPolicy;
import com.microsoft.azure.documentdb.MediaOptions;
import com.microsoft.azure.documentdb.MediaReadMode;
import com.microsoft.azure.documentdb.Offer;
import com.microsoft.azure.documentdb.Permission;
import com.microsoft.azure.documentdb.PermissionMode;
import com.microsoft.azure.documentdb.QueryIterable;
import com.microsoft.azure.documentdb.Range;
import com.microsoft.azure.documentdb.RangeIndex;
import com.microsoft.azure.documentdb.RangePartitionResolver;
import com.microsoft.azure.documentdb.RequestOptions;
import com.microsoft.azure.documentdb.ResourceResponse;
import com.microsoft.azure.documentdb.SpatialIndex;
import com.microsoft.azure.documentdb.SqlParameter;
import com.microsoft.azure.documentdb.SqlParameterCollection;
import com.microsoft.azure.documentdb.SqlQuerySpec;
import com.microsoft.azure.documentdb.StoredProcedure;
import com.microsoft.azure.documentdb.Trigger;
import com.microsoft.azure.documentdb.TriggerOperation;
import com.microsoft.azure.documentdb.TriggerType;
import com.microsoft.azure.documentdb.User;
import com.microsoft.azure.documentdb.UserDefinedFunction;

final class AnotherPOJO {
    public String pojoProp = "789";
}

public final class GatewayTests {
    static final String MASTER_KEY = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
    static final String HOST = "https://localhost:443";

    private Database databaseForTest;
    private DocumentCollection collectionForTest;
    private static final String databaseForTestId = "GatewayTests_database0";
    private static final String databaseForTestAlternativeId1 = "GatewayTests_database1";
    private static final String databaseForTestAlternativeId2 = "GatewayTests_database2";

    static final String DATABASES_PATH_SEGMENT = "dbs";
    static final String USERS_PATH_SEGMENT = "users";
    static final String PERMISSIONS_PATH_SEGMENT = "permissions";
    static final String COLLECTIONS_PATH_SEGMENT = "colls";
    static final String DOCUMENTS_PATH_SEGMENT = "docs";
    static final String ATTACHMENTS_PATH_SEGMENT = "attachments";
    static final String STORED_PROCEDURES_PATH_SEGMENT = "sprocs";
    static final String TRIGGERS_PATH_SEGMENT = "triggers";
    static final String USER_DEFINED_FUNCTIONS_PATH_SEGMENT = "udfs";
    static final String CONFLICTS_PATH_SEGMENT = "conflicts";

    public class Retry implements TestRule {
        private int retryCount;

        public Retry(int retryCount) {
            this.retryCount = retryCount;
        }

        public Statement apply(Statement base, Description description) {
            return statement(base, description);
        }

        private Statement statement(final Statement base, final Description description) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    Throwable caughtThrowable = null;

                    // implement retry logic here
                    for (int i = 0; i < retryCount; i++) {
                        try {
                            base.evaluate();
                            return;
                        } catch (java.lang.IllegalStateException exception) {
                            Throwable cause = exception.getCause();
                            if (cause instanceof javax.net.ssl.SSLPeerUnverifiedException ||
                                    cause.getCause() instanceof javax.net.ssl.SSLPeerUnverifiedException) {
                                // We will retry on SSLPeerUnverifiedException. This is an inconsistent and random
                                // failure that only occasionally happen in non-production environment.
                                // TODO: remove this hack after figuring out the reason of this failure.
                                caughtThrowable = exception;
                                System.err.println(description.getDisplayName() + ": run " + (i+1) + " failed");
                            } else {
                                throw exception;
                            }
                        } catch (Throwable t) {
                            // Everything else is fatal.
                            throw t;
                        }
                    }
                    System.err.println(description.getDisplayName() + ": giving up after " + retryCount + " failures");
                    throw caughtThrowable;
                }
            };
        }
    }

    @Rule
    public Retry retry = new Retry(3);

    void cleanUpGeneratedDatabases() throws DocumentClientException {
        // If one of databaseForTestId, databaseForTestAlternativeId1, or databaseForTestAlternativeId2 already exists,
        // deletes them.
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);

        String[] allDatabaseIds = {
                GatewayTests.databaseForTestId,
                GatewayTests.databaseForTestAlternativeId1,
                GatewayTests.databaseForTestAlternativeId2
        };

        for (String id : allDatabaseIds) {
            try {
                Database database = client.queryDatabases(
                        new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
                                new SqlParameterCollection(new SqlParameter("@id", id))),
                        null).getQueryIterable().iterator().next();
                if (database != null) {
                    client.deleteDatabase(getDatabaseLink(database, true), null);
                }
            }
            catch(IllegalStateException illegalStateException) {
                Throwable causeException = illegalStateException.getCause();
                
                // The above code for querying databases has started throwing IOExceptions and causing the Java tests to fail once in a while
                // Capturing the inner stack trace here so that we have this info when it fails next time and analyze it
                // If that is a retryable error, we will add retries here but need to find out the cause first
                
                if(causeException instanceof IOException) {
                    System.err.println("Detailed message for the exception thrown : " + causeException.toString());
                }
                throw illegalStateException;
            }
        }
    }

    @Before
    public void setUp() throws DocumentClientException {
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);

        // Clean up before setting up in case a previous running fails to tear down.
        this.cleanUpGeneratedDatabases();

        // Create the database for test.
        Database databaseDefinition = new Database();
        databaseDefinition.setId(GatewayTests.databaseForTestId);
        this.databaseForTest = client.createDatabase(databaseDefinition, null).getResource();
        // Create the collection for test.
        DocumentCollection collectionDefinition = new DocumentCollection();
        collectionDefinition.setId(GatewayTests.getUID());

        this.collectionForTest = client.createCollection(getDatabaseLink(this.databaseForTest, true),
                collectionDefinition,
                null).getResource();
    }

    @After
    public void tearDown() throws DocumentClientException {
        this.cleanUpGeneratedDatabases();
    }

    private static String getStringFromInputStream(InputStream is) {
        BufferedReader br = null;
        StringBuilder sb = new StringBuilder();

        String line;
        try {
            br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return sb.toString();
    }

    static class StaticPOJOForTest {
        // Jackson's readValue method supports member class only if it's static.
        public String pojoProp = "456";
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
                                new SqlParameter("@p1", new int[] {1,2,3})))).toString());

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
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);

        // Read databases.
        List<Database> databases = client.readDatabases(null).getQueryIterable().toList();
        // Create a database.
        int beforeCreateDatabasesCount = databases.size();
        Database databaseDefinition = new Database();
        databaseDefinition.setId(GatewayTests.databaseForTestAlternativeId1);

        Database createdDb = client.createDatabase(databaseDefinition, null).getResource();
        Assert.assertEquals(databaseDefinition.getId(), createdDb.getId());
        // Read databases after creation.
        databases = client.readDatabases(null).getQueryIterable().toList();
        // create should increase the number of databases.
        Assert.assertEquals(beforeCreateDatabasesCount + 1, databases.size());
        // query databases.
        databases = client.queryDatabases(new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
                new SqlParameterCollection(new SqlParameter(
                        "@id", databaseDefinition.getId()))),
                null).getQueryIterable().toList();
        // number of results for the query should be > 0.
        Assert.assertTrue(databases.size() > 0);

        // Delete database.
        client.deleteDatabase(getDatabaseLink(createdDb, isNameBased), null);

        // Read database after deletion.
        try {
            client.readDatabase(getDatabaseLink(createdDb, isNameBased), null);
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
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);
        List<DocumentCollection> collections = client.readCollections(this.getDatabaseLink(this.databaseForTest, isNameBased),
                null).getQueryIterable().toList();
        // Create a collection.
        int beforeCreateCollectionsCount = collections.size();

        IndexingPolicy consistentPolicy = new IndexingPolicy("{'indexingMode': 'Consistent'}");
        DocumentCollection collectionDefinition = new DocumentCollection();
        collectionDefinition.setId(GatewayTests.getUID());
        collectionDefinition.setIndexingPolicy(consistentPolicy);
        DocumentCollection createdCollection = client.createCollection(this.getDatabaseLink(this.databaseForTest, isNameBased),
                collectionDefinition,
                null).getResource();
        Assert.assertEquals(collectionDefinition.getId(), createdCollection.getId());
        Assert.assertEquals(IndexingMode.Consistent, createdCollection.getIndexingPolicy().getIndexingMode());

        // Read collections after creation.
        collections = client.readCollections(this.getDatabaseLink(this.databaseForTest, isNameBased), null).getQueryIterable().toList();
        // Create should increase the number of collections.
        Assert.assertEquals(collections.size(), beforeCreateCollectionsCount + 1);
        // Query collections.
        collections = client.queryCollections(this.getDatabaseLink(this.databaseForTest, isNameBased),
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

        // Replacing collection Id should fail.
        try {
            createdCollection.setId(GatewayTests.getUID());
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
        client.deleteCollection(this.getDocumentCollectionLink(this.databaseForTest, createdCollection, isNameBased), null);
        // Read collection after deletion.

        try {
            client.readCollection(this.getDocumentCollectionLink(this.databaseForTest, createdCollection, isNameBased), null);
            Assert.fail("Exception didn't happen.");
        } catch (DocumentClientException e) {
            Assert.assertEquals(404, e.getStatusCode());
            Assert.assertEquals("NotFound", e.getError().getCode());
        }
    }

    @Test
    public void testSpatialIndex() throws DocumentClientException {
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);
        DocumentCollection collectionDefinition = new DocumentCollection();
        collectionDefinition.setId(GatewayTests.getUID());
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
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);
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
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);

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
                        "    'includedPaths': ["+
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
        HashIndex indexToChange = (HashIndex)(
                collectionDefinition.getIndexingPolicy().getIncludedPaths().iterator().next().getIndexes().iterator().next());
        indexToChange.setDataType(DataType.String);
        indexToChange.setPrecision(3);

        client.deleteCollection(consistentCollection.getSelfLink(), null);
        DocumentCollection collectionWithSecondaryIndex = client.createCollection(this.databaseForTest.getSelfLink(),
                collectionDefinition,
                null).getResource();
        // Check the size of included and excluded paths.
        Assert.assertEquals(2, collectionWithSecondaryIndex.getIndexingPolicy().getIncludedPaths().size());
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
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);

        // no indexing policy specified
        DocumentCollection collectionDefinition = new DocumentCollection();
        collectionDefinition.setId("TestCreateDefaultPolicy" + GatewayTests.getUID());
        DocumentCollection collection = client.createCollection(
                this.databaseForTest.getSelfLink(), collectionDefinition, null).getResource();
        GatewayTests.checkDefaultPolicyPaths(collection.getIndexingPolicy());

        // partial policy specified
        collectionDefinition = new DocumentCollection();
        collectionDefinition.setId("TestCreateDefaultPolicy" + GatewayTests.getUID());
        collectionDefinition.getIndexingPolicy().setIndexingMode(IndexingMode.Lazy);
        collectionDefinition.getIndexingPolicy().setAutomatic(true);

        collection = client.createCollection(
                this.databaseForTest.getSelfLink(), collectionDefinition, null).getResource();
        GatewayTests.checkDefaultPolicyPaths(collection.getIndexingPolicy());

        // default policy
        collectionDefinition = new DocumentCollection();
        collectionDefinition.setId("TestCreateDefaultPolicy" + GatewayTests.getUID());
        collectionDefinition.setIndexingPolicy(new IndexingPolicy());

        collection = client.createCollection(
                this.databaseForTest.getSelfLink(), collectionDefinition, null).getResource();
        GatewayTests.checkDefaultPolicyPaths(collection.getIndexingPolicy());

        // missing indexes
        collectionDefinition = new DocumentCollection();
        collectionDefinition.setId("TestCreateDefaultPolicy" + GatewayTests.getUID());
        collectionDefinition.setIndexingPolicy(new IndexingPolicy());
        IncludedPath includedPath = new IncludedPath();
        includedPath.setPath("/*");
        collectionDefinition.getIndexingPolicy().getIncludedPaths().add(includedPath);

        collection = client.createCollection(
                this.databaseForTest.getSelfLink(), collectionDefinition, null).getResource();
        GatewayTests.checkDefaultPolicyPaths(collection.getIndexingPolicy());

        // missing precision
        collectionDefinition = new DocumentCollection();
        collectionDefinition.setId("TestCreateDefaultPolicy" + GatewayTests.getUID());
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

    private static void checkDefaultPolicyPaths(IndexingPolicy indexingPolicy) {
        // no excluded paths            
        Assert.assertEquals(0, indexingPolicy.getExcludedPaths().size());
        // included path should be 2 '_ts' and '/'
        Assert.assertEquals(2, indexingPolicy.getIncludedPaths().size());

        // check default path and ts path
        IncludedPath rootIncludedPath = null;
        IncludedPath tsIncludedPath = null;
        for (IncludedPath path : indexingPolicy.getIncludedPaths()) {
            if (path.getPath().equals("/*")) {
                rootIncludedPath = path;
            } else if (path.getPath().equals("/\"_ts\"/?")) {
                tsIncludedPath = path;
            }
        }
        // ts path should exist.
        Assert.assertNotNull(tsIncludedPath);
        // root path should exist.
        Assert.assertNotNull(rootIncludedPath);
        // RangeIndex for Numbers and HashIndex for Strings.
        Assert.assertEquals(2, rootIncludedPath.getIndexes().size());

        // There exists one HashIndex and one RangeIndex out of these 2 indexes.
        HashIndex hashIndex = null;
        RangeIndex rangeIndex = null;
        for (Index index : rootIncludedPath.getIndexes()) {
            if (index.getKind() == IndexKind.Hash) {
                hashIndex = (HashIndex)index;
            } else if (index.getKind() == IndexKind.Range) {
                rangeIndex = (RangeIndex)index;
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
    public void testIndexingPolicyOverrides() throws DocumentClientException {
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);

        HashIndex hashIndexOverride = Index.Hash(DataType.String, 5);
        RangeIndex rangeIndexOverride = Index.Range(DataType.Number, 2);
        SpatialIndex spatialIndexOverride = Index.Spatial(DataType.Point);

        Index[] indexes = {hashIndexOverride, rangeIndexOverride, spatialIndexOverride};
        IndexingPolicy indexingPolicy = new IndexingPolicy(indexes);

        DocumentCollection collection = new DocumentCollection();
        collection.setId(GatewayTests.getUID());
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
                hashIndex = (HashIndex)index;
            } else if (index.getKind() == IndexKind.Range) {
                rangeIndex = (RangeIndex)index;
            } else if (index.getKind() == IndexKind.Spatial) {
                spatialIndex = (SpatialIndex)index;
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
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);
        // Read documents.
        List<Document> documents = client.readDocuments(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
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
            client.createDocument(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased), documentDefinition, null, true);
            Assert.fail("Exception didn't happen.");
        } catch (DocumentClientException e) {
            Assert.assertEquals(400, e.getStatusCode());
            Assert.assertEquals("BadRequest", e.getError().getCode());
        }

        Document document = client.createDocument(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                documentDefinition,
                null,
                false).getResource();
        Assert.assertEquals(documentDefinition.getString("name"), document.getString("name"));
        Assert.assertEquals(documentDefinition.getString("foo"), document.getString("foo"));
        Assert.assertEquals(documentDefinition.getString("bar"), document.getString("bar"));
        Assert.assertNotNull(document.getId());

        // Read documents after creation.
        documents = client.readDocuments(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased), null).getQueryIterable().toList();
        // Create should increase the number of documents.
        Assert.assertEquals(1, documents.size());

        // Query documents.
        documents = client.queryDocuments(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                new SqlQuerySpec("SELECT * FROM root r WHERE r.name=@id",
                        new SqlParameterCollection(new SqlParameter(
                                "@id", documentDefinition.getString("name")))),
                null).getQueryIterable().toList();
        Assert.assertEquals(1, documents.size());

        // Replace document.
        document.set("name", "replaced document");
        document.set("foo", "not bar");
        Document replacedDocument = client.replaceDocument(document, null).getResource();
        // Document id property should change.
        Assert.assertEquals("replaced document", replacedDocument.getString("name"));
        // Property should have changed.
        Assert.assertEquals("not bar", replacedDocument.getString("foo"));
        // Document id should stay the same.
        Assert.assertEquals(document.getId(), replacedDocument.getId());
        // Read document.
        Document oneDocumentFromRead = client.readDocument(this.getDocumentLink(this.databaseForTest, this.collectionForTest, replacedDocument, isNameBased), null).getResource();
        Assert.assertEquals(replacedDocument.getId(), oneDocumentFromRead.getId());

        AccessCondition accessCondition = new AccessCondition();
        accessCondition.setCondition(oneDocumentFromRead.getETag()) ;
        accessCondition.setType(AccessConditionType.IfNoneMatch);

        RequestOptions options = new RequestOptions();
        options.setAccessCondition(accessCondition);
        ResourceResponse<Document> rr = client.readDocument(this.getDocumentLink(this.databaseForTest, this.collectionForTest, oneDocumentFromRead, isNameBased), options);
        Assert.assertEquals(rr.getStatusCode(), HttpStatus.SC_NOT_MODIFIED);

        // delete document
        client.deleteDocument(this.getDocumentLink(this.databaseForTest, this.collectionForTest, replacedDocument, isNameBased), null);

        // read documents after deletion
        try {
            client.readDocument(this.getDocumentLink(this.databaseForTest, this.collectionForTest, replacedDocument, isNameBased), null);
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
        // Create document client
        DocumentClient client = new DocumentClient(HOST, MASTER_KEY, ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);

        // Read documents and get initial count
        List<Document> documents = client
                .readDocuments(
                        this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased), null)
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
                this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                documentDefinition, null, false).getResource();

        // Read documents to check the count and it should increase
        documents = client
                .readDocuments(
                        this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased), null)
                .getQueryIterable().toList();
        
        Assert.assertEquals(initialDocumentCount + 1, documents.size());

        // Update document.
        createdDocument.set("name", "replaced document");
        createdDocument.set("key", "new value");

        // Upsert should replace the existing document since Id exists
        Document upsertedDocument = client.upsertDocument(
                this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased), createdDocument,
                null, true).getResource();

        // Document id should stay the same.
        Assert.assertEquals(createdDocument.getId(), upsertedDocument.getId());

        // Property should have changed.
        Assert.assertEquals(createdDocument.getString("name"), upsertedDocument.getString("name"));
        Assert.assertEquals(createdDocument.getString("key"), upsertedDocument.getString("key"));

        // Documents count should remain the same
        documents = client
                .readDocuments(
                        this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased), null)
                .getQueryIterable().toList();
        
        Assert.assertEquals(initialDocumentCount + 1, documents.size());

        // Update document id
        createdDocument.setId(GatewayTests.getUID());

        // Upsert should create new document
        Document newDocument = client.upsertDocument(
                this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased), createdDocument,
                null, true).getResource();

        // Verify id property
        Assert.assertEquals(createdDocument.getId(), newDocument.getId());

        // Read documents after upsert to check the count and it should increase
        documents = client
                .readDocuments(
                        this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased), null)
                .getQueryIterable().toList();
        
        Assert.assertEquals(initialDocumentCount + 2, documents.size());

        // Delete documents
        client.deleteDocument(
                this.getDocumentLink(this.databaseForTest, this.collectionForTest, upsertedDocument, isNameBased),
                null);
        client.deleteDocument(
                this.getDocumentLink(this.databaseForTest, this.collectionForTest, newDocument, isNameBased), null);

        // Read documents after delete to check the count and it should be back to original
        documents = client
                .readDocuments(
                        this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased), null)
                .getQueryIterable().toList();
        
        Assert.assertEquals(initialDocumentCount, documents.size());
    }

    class TestPOJOInner {
        public int intProperty;

        public TestPOJOInner(int i) {
            this.intProperty = i;
        }
    }

    class TestPOJO {

        private String privateStringProperty;

        public String id;
        public int intProperty;
        public String  stringProperty;
        public TestPOJOInner objectProperty;
        public List<String>  stringList;
        public String[]  stringArray;

        public TestPOJO(int i) {
            this.intProperty = i;

            this.stringList = new ArrayList<String>();
            this.stringList.add("ONE");
            this.stringList.add("TWO");
            this.stringList.add("THREE");

            this.stringArray = new String[] { "One", "Two", "Three" };
        }

        public String getPrivateStringProperty() {
            return this.privateStringProperty;
        }
        public void setPrivateStringProperty(String value) {
            this.privateStringProperty = value;
        }
    }

    @Test
    public void testPOJODocumentCrud() throws DocumentClientException {


        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);

        TestPOJO testPojo = new TestPOJO(10);
        testPojo.id= "MyPojoObejct" + GatewayTests.getUID();
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

        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);
        // Create document.
        Document documentDefinition = new Document(
                "{" +
                        "  'id': 'sample document'," +
                        "  'foo': 'bar'," +
                        "  'key': 'value'" +
                "}");
        Document document = client.createDocument(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                documentDefinition,
                null,
                false).getResource();
        // List all attachments.
        List<Attachment> attachments = client.readAttachments(this.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased),
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
            client.createAttachment(this.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased), mediaStream, invalidMediaOptions);
            Assert.assertTrue(false);  // This line shouldn't execute.
        } catch (DocumentClientException e) {
            Assert.assertEquals(400, e.getStatusCode());
            Assert.assertEquals("BadRequest", e.getError().getCode());
        }

        // Create attachment with valid content type.
        mediaStream = new ReadableStream("stream content.");
        Attachment validAttachment = client.createAttachment(this.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased),
                mediaStream,
                validMediaOptions).getResource();
        Assert.assertEquals("attachment id", validAttachment.getId());

        mediaStream = new ReadableStream("stream content");
        // Create colliding attachment.
        try {
            client.createAttachment(this.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased), mediaStream, validMediaOptions);
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
        Attachment attachment = client.createAttachment(this.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased),
                attachmentDefinition,
                null).getResource();
        Assert.assertEquals("Book", attachment.getString("MediaType"));
        Assert.assertEquals("My Book Author", attachment.getString("Author"));

        // List all attachments.
        FeedOptions fo = new FeedOptions();
        fo.setPageSize(1);
        attachments = client.readAttachments(this.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased), fo).getQueryIterable().toList();
        Assert.assertEquals(2, attachments.size());
        attachment.set("Author", "new author");

        // Replace the attachment.
        client.replaceAttachment(attachment, null);
        Assert.assertEquals("Book", attachment.getString("MediaType"));
        Assert.assertEquals("new author", attachment.getString("Author"));
        // Read attachment media.
        InputStream mediaResponse = client.readMedia(validAttachment.getMediaLink()).getMedia();
        Assert.assertEquals("stream content.", GatewayTests.getStringFromInputStream(mediaResponse));

        mediaStream = new ReadableStream("updated stream content");
        // Update attachment media.
        client.updateMedia(validAttachment.getMediaLink(), mediaStream, validMediaOptions);

        // Read attachment media after update.
        // read media buffered (default).
        mediaResponse = client.readMedia(validAttachment.getMediaLink()).getMedia();
        Assert.assertEquals("updated stream content", GatewayTests.getStringFromInputStream(mediaResponse));

        // read media streamed, should still work.
        ConnectionPolicy streamPolicy = new ConnectionPolicy();
        streamPolicy.setMediaReadMode(MediaReadMode.Streamed);
        client = new DocumentClient(HOST, MASTER_KEY, streamPolicy, ConsistencyLevel.Session);
        mediaResponse = client.readMedia(validAttachment.getMediaLink()).getMedia();
        Assert.assertEquals("updated stream content", GatewayTests.getStringFromInputStream(mediaResponse));

        // Share attachment with a second document.
        documentDefinition = new Document("{'id': 'document 2'}");
        document = client.createDocument(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
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
        attachment = client.createAttachment(this.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased), secondAttachmentDefinition, null).getResource();
        Assert.assertEquals(validAttachment.getId(), attachment.getId());
        Assert.assertEquals(validAttachment.getMediaLink(), attachment.getMediaLink());
        Assert.assertEquals(validAttachment.getContentType(), attachment.getContentType());
        // Deleting attachment.
        client.deleteAttachment(this.getAttachmentLink(this.databaseForTest, this.collectionForTest, document, attachment, isNameBased), null);
        // read attachments after deletion
        attachments = client.readAttachments(this.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased), null).getQueryIterable().toList();
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

        // Create document client
        DocumentClient client = new DocumentClient(HOST, MASTER_KEY, ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);

        // Create document definition
        Document documentDefinition = new Document(
                "{" +  
                        "  'name': 'document'," + 
                        "  'key': 'value'" + 
                "}");

        // Create document
        Document document = client.createDocument(
                this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                documentDefinition, null, false).getResource();

        // Read attachments and get initial count
        List<Attachment> attachments = client
                .readAttachments(
                        this.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased), null)
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
                this.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased), mediaStream,
                mediaOptions).getResource();
        
        Assert.assertEquals("attachment id", createdMediaStreamAttachment.getId());
        
        // Read attachments to check the count and it should increase
        attachments = client
                .readAttachments(
                        this.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased), fo)
                .getQueryIterable().toList();
        
        Assert.assertEquals(initialAttachmentCount + 1, attachments.size());

        // Update MediaOptions
        mediaOptions.setSlug("new attachment id");
        mediaStream = new ReadableStream("stream content.");

        // Upsert should create new attachment
        Attachment newMediaStreamAttachment = client.upsertAttachment(
                this.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased), mediaStream,
                mediaOptions).getResource();
        
        Assert.assertEquals("new attachment id", newMediaStreamAttachment.getId());

        // Read attachments to check the count and it should increase
        attachments = client
                .readAttachments(
                        this.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased), fo)
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
                this.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased),
                attachmentDefinition, null).getResource();

        // Verify id property
        Assert.assertEquals(attachmentDefinition.getId(), createdDynamicAttachment.getId());

        // Read all attachments and verify the count increase
        attachments = client
                .readAttachments(
                        this.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased), fo)
                .getQueryIterable().toList();
        
        Assert.assertEquals(initialAttachmentCount + 3, attachments.size());

        // Update attachment
        createdDynamicAttachment.set("Author", "new author");

        // Upsert should replace the existing attachment since Id exists
        Attachment upsertedDynamicAttachment = client.upsertAttachment(
                this.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased),
                createdDynamicAttachment, null).getResource();

        // Verify id property
        Assert.assertEquals(createdDynamicAttachment.getId(), upsertedDynamicAttachment.getId());

        // Verify property change
        Assert.assertEquals(createdDynamicAttachment.getString("Author"), upsertedDynamicAttachment.getString("Author"));

        // Read all attachments and verify the count remains the same
        attachments = client
                .readAttachments(
                        this.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased), fo)
                .getQueryIterable().toList();
        
        Assert.assertEquals(initialAttachmentCount + 3, attachments.size());

        // Change id property
        createdDynamicAttachment.setId(GatewayTests.getUID());

        // Upsert should create new attachment
        Attachment newDynamicAttachment = client.upsertAttachment(
                this.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased),
                createdDynamicAttachment, null).getResource();

        // Verify id property
        Assert.assertEquals(createdDynamicAttachment.getId(), newDynamicAttachment.getId());

        // Read all attachments and verify the count increases
        attachments = client
                .readAttachments(
                        this.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased), fo)
                .getQueryIterable().toList();
        
        Assert.assertEquals(initialAttachmentCount + 4, attachments.size());

        // Deleting attachments.
        client.deleteAttachment(this.getAttachmentLink(this.databaseForTest, this.collectionForTest, document,
                createdMediaStreamAttachment, isNameBased), null);
        client.deleteAttachment(this.getAttachmentLink(this.databaseForTest, this.collectionForTest, document,
                newMediaStreamAttachment, isNameBased), null);
        client.deleteAttachment(this.getAttachmentLink(this.databaseForTest, this.collectionForTest, document,
                upsertedDynamicAttachment, isNameBased), null);
        client.deleteAttachment(this.getAttachmentLink(this.databaseForTest, this.collectionForTest, document,
                newDynamicAttachment, isNameBased), null);

        // read attachments after deletion and verify count remains the same
        attachments = client
                .readAttachments(
                        this.getDocumentLink(this.databaseForTest, this.collectionForTest, document, isNameBased), null)
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
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);

        // create..
        Trigger triggerDef = new Trigger();
        triggerDef.setId(GatewayTests.getUID());
        triggerDef.setTriggerType(TriggerType.Pre);
        triggerDef.setTriggerOperation(TriggerOperation.All);
        triggerDef.setBody("function() {var x = 10;}");
        Trigger newTrigger = client.createTrigger(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
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
        client.createDocument(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased), document, options, false);

        // replace...
        String id = GatewayTests.getUID();
        newTrigger.setId(id);

        newTrigger = client.replaceTrigger(newTrigger, null).getResource();
        Assert.assertEquals(newTrigger.getId(), id);

        newTrigger = client.readTrigger(this.getTriggerLink(this.databaseForTest, this.collectionForTest, newTrigger, isNameBased), null).getResource();

        // read triggers:
        List<Trigger> triggers = client.readTriggers(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                null).getQueryIterable().toList();
        if (triggers.size() > 0) {
            //
        } else {
            Assert.fail("Readfeeds fail to find trigger");
        }

        triggers = client.queryTriggers(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
                        new SqlParameterCollection(
                                new SqlParameter("@id", newTrigger.getId()))),
                null).getQueryIterable().toList();
        if (triggers.size() > 0) {
            //
        } else {
            Assert.fail("Query fail to find trigger");
        }

        client.deleteTrigger(this.getTriggerLink(this.databaseForTest, this.collectionForTest, newTrigger, isNameBased), null);
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
        // Create document client
        DocumentClient client = new DocumentClient(HOST, MASTER_KEY, ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);

        // Read triggers and get initial count
        List<Trigger> triggers = client
                .readTriggers(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                        null)
                .getQueryIterable().toList();
        
        int initialTriggerCount = triggers.size();

        // Create trigger definition
        Trigger triggerDefinition = new Trigger();
        triggerDefinition.setId(GatewayTests.getUID());
        triggerDefinition.setTriggerType(TriggerType.Pre);
        triggerDefinition.setTriggerOperation(TriggerOperation.All);
        triggerDefinition.setBody("function() {var x = 10;}");

        // Upsert should create the trigger
        Trigger createdTrigger = client.upsertTrigger(
                this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                triggerDefinition, null).getResource();

        // Verify Id property
        Assert.assertEquals(triggerDefinition.getId(), createdTrigger.getId());
        
        // Read triggers to check the count and it should increase
        triggers = client.readTriggers(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                        null)
                .getQueryIterable().toList();
        
        Assert.assertEquals(initialTriggerCount + 1, triggers.size());

        // Update trigger
        createdTrigger.setTriggerOperation(TriggerOperation.Update);
        createdTrigger.setBody("function() {var x = 20;}");
        
        // Upsert should replace the trigger since it already exists
        Trigger upsertedTrigger = client.upsertTrigger(
                this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                createdTrigger, null).getResource();

        // Verify Id property
        Assert.assertEquals(createdTrigger.getId(), upsertedTrigger.getId());
        
        // Verify updated property
        Assert.assertEquals(createdTrigger.getTriggerOperation(), upsertedTrigger.getTriggerOperation());
        Assert.assertEquals(createdTrigger.getBody(), upsertedTrigger.getBody());
        
        // Read triggers to check the count and it should remain the same
        triggers = client.readTriggers(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                        null)
                .getQueryIterable().toList();
        
        Assert.assertEquals(initialTriggerCount + 1, triggers.size());
        
        // Update trigger id
        createdTrigger.setId(GatewayTests.getUID());
        
        // Upsert should create new trigger since id is changed
        Trigger newTrigger = client.upsertTrigger(
                this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                createdTrigger, null).getResource();

        // Verify Id property
        Assert.assertEquals(createdTrigger.getId(), newTrigger.getId());
        
        // Read triggers to check the count and it should increase
        triggers = client.readTriggers(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                        null)
                .getQueryIterable().toList();
        
        Assert.assertEquals(initialTriggerCount + 2, triggers.size());        

        // Delete triggers
        client.deleteTrigger(this.getTriggerLink(this.databaseForTest, this.collectionForTest, upsertedTrigger, isNameBased),
                null);
        client.deleteTrigger(this.getTriggerLink(this.databaseForTest, this.collectionForTest, newTrigger, isNameBased),
                null);
        
        // Read triggers to check the count and it should remain same
        triggers = client.readTriggers(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
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
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);

        // create..
        StoredProcedure storedProcedureDef = new StoredProcedure();
        storedProcedureDef.setId(GatewayTests.getUID());
        storedProcedureDef.setBody("function() {var x = 10;}");

        StoredProcedure newStoredProcedure = client.createStoredProcedure(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                storedProcedureDef,
                null).getResource();
        Assert.assertNotNull(newStoredProcedure.getBody());
        Assert.assertNotNull(newStoredProcedure.getETag());

        // replace...
        String id = GatewayTests.getUID();
        newStoredProcedure.setId(id);

        newStoredProcedure = client.replaceStoredProcedure(newStoredProcedure, null).getResource();
        Assert.assertEquals(newStoredProcedure.getId(), id);

        newStoredProcedure = client.readStoredProcedure(this.getStoredProcedureLink(this.databaseForTest, this.collectionForTest, newStoredProcedure, isNameBased), null).getResource();

        // read storedProcedures:
        List<StoredProcedure> storedProcedures = client.readStoredProcedures(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                null).getQueryIterable().toList();
        if (storedProcedures.size() > 0) {
            //
        } else {
            Assert.fail("Readfeeds fail to find StoredProcedure");
        }

        storedProcedures = client.queryStoredProcedures(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                        new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
                        new SqlParameterCollection(new SqlParameter(
                                "@id", newStoredProcedure.getId()))),
                 null).getQueryIterable().toList();
        if (storedProcedures.size() > 0) {
            //
        } else {
            Assert.fail("Query fail to find StoredProcedure");
        }

        client.deleteStoredProcedure(this.getStoredProcedureLink(this.databaseForTest, this.collectionForTest, newStoredProcedure, isNameBased), null);
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
        // Create document client
        DocumentClient client = new DocumentClient(HOST, MASTER_KEY, ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);
        
        // Read sprocs and get initial count
        List<StoredProcedure> sprocs = client
                .readStoredProcedures(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                        null)
                .getQueryIterable().toList();
        
        int initialStoredProcedureCount = sprocs.size();
        
        // Create stored procedure definition
        StoredProcedure storedProcedureDefinition = new StoredProcedure();
        storedProcedureDefinition.setId(GatewayTests.getUID());
        storedProcedureDefinition.setBody("function() {var x = 10;}");

        // Upsert should create the sproc
        StoredProcedure createdStoredProcedure = client.upsertStoredProcedure(
                this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                storedProcedureDefinition, null).getResource();
        
        // Verify Id property
        Assert.assertEquals(storedProcedureDefinition.getId(), createdStoredProcedure.getId());

        // Read sprocs to check the count and it should increase
        sprocs = client
                .readStoredProcedures(
                        this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased), null)
                .getQueryIterable().toList();
        
        Assert.assertEquals(initialStoredProcedureCount + 1, sprocs.size());
        
        // Update sproc
        createdStoredProcedure.setBody("function() {var x = 20;}");
        
        // Upsert should replace the sproc
        StoredProcedure upsertedStoredProcedure = client.upsertStoredProcedure(
                this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                createdStoredProcedure, null).getResource();
        
        // Verify Id property
        Assert.assertEquals(createdStoredProcedure.getId(), upsertedStoredProcedure.getId());
        
        // Verify updated property
        Assert.assertEquals(createdStoredProcedure.getBody(), upsertedStoredProcedure.getBody());

        // Read the sprocs and the count should remain the same
        sprocs = client
                .readStoredProcedures(
                        this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased), null)
                .getQueryIterable().toList();
        
        Assert.assertEquals(initialStoredProcedureCount + 1, sprocs.size());
        
        // Update sproc id
        createdStoredProcedure.setId(GatewayTests.getUID());
        
        // Upsert should create the sproc since id is changed
        StoredProcedure newStoredProcedure = client.upsertStoredProcedure(
                this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                createdStoredProcedure, null).getResource();
        
        // Verify Id property
        Assert.assertEquals(createdStoredProcedure.getId(), newStoredProcedure.getId());

        // Read the sprocs and the count should increase
        sprocs = client
                .readStoredProcedures(
                        this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased), null)
                .getQueryIterable().toList();
        
        Assert.assertEquals(initialStoredProcedureCount + 2, sprocs.size());
        
        // Delete sprocs
        client.deleteStoredProcedure(this.getStoredProcedureLink(this.databaseForTest, this.collectionForTest,
                upsertedStoredProcedure, isNameBased), null);
        client.deleteStoredProcedure(this.getStoredProcedureLink(this.databaseForTest, this.collectionForTest,
                newStoredProcedure, isNameBased), null);
        
        // Read the sprocs and the count should remain the same
        sprocs = client
                .readStoredProcedures(
                        this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased), null)
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
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);

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
        StoredProcedure retrievedSproc = client.createStoredProcedure(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                sproc1,
                null).getResource();
        String result = client.executeStoredProcedure(this.getStoredProcedureLink(this.databaseForTest, this.collectionForTest, retrievedSproc, isNameBased), null).getResponseAsString();
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
        StoredProcedure retrievedSproc2 = client.createStoredProcedure(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                sproc2,
                null).getResource();
        result = client.executeStoredProcedure(this.getStoredProcedureLink(this.databaseForTest, this.collectionForTest, retrievedSproc2, isNameBased), null).getResponseAsString();
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
        StoredProcedure retrievedSproc3 = client.createStoredProcedure(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                sproc3,
                null).getResource();
        result = client.executeStoredProcedure(this.getStoredProcedureLink(this.databaseForTest, this.collectionForTest, retrievedSproc3, isNameBased),
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
        StoredProcedure retrievedSproc4 = client.createStoredProcedure(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                sproc4,
                null).getResource();
        result = client.executeStoredProcedure(this.getStoredProcedureLink(this.databaseForTest, this.collectionForTest, retrievedSproc4, isNameBased),
                new Object[] {tempPOJO}).getResponseAsString();
        Assert.assertEquals("\"aso2\"", result);

        // JSONObject
        JSONObject jsonObject = new JSONObject("{'temp': 'so3'}");
        result = client.executeStoredProcedure(this.getStoredProcedureLink(this.databaseForTest, this.collectionForTest, retrievedSproc4, isNameBased),
                new Object[] {jsonObject}).getResponseAsString();
        Assert.assertEquals("\"aso3\"", result);

        // Document
        Document document = new Document("{'temp': 'so4'}");
        result = client.executeStoredProcedure(this.getStoredProcedureLink(this.databaseForTest, this.collectionForTest, retrievedSproc4, isNameBased),
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
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);

        // create..
        UserDefinedFunction udfDef = new UserDefinedFunction();
        udfDef.setId(GatewayTests.getUID());
        udfDef.setBody("function() {var x = 10;}");
        UserDefinedFunction newUdf = client.createUserDefinedFunction(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                udfDef,
                null).getResource();
        Assert.assertNotNull(newUdf.getBody());
        Assert.assertNotNull(newUdf.getETag());

        // replace...
        String id = GatewayTests.getUID();
        newUdf.setId(id);

        newUdf = client.replaceUserDefinedFunction(newUdf, null).getResource();
        Assert.assertEquals(newUdf.getId(), id);

        newUdf = client.readUserDefinedFunction(this.getUserDefinedFunctionLink(this.databaseForTest, this.collectionForTest, newUdf, isNameBased), null).getResource();
        Assert.assertEquals(newUdf.getId(), id);

        // read udf feed:
        {
            List<UserDefinedFunction> udfs = client.readUserDefinedFunctions(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                    null).getQueryIterable().toList();
            if (udfs.size() > 0) {
                //
            } else {
                Assert.fail("Readfeeds fail to find UserDefinedFunction");
            }
        }
        {
            List<UserDefinedFunction> udfs = client.queryUserDefinedFunctions(
                    this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                    new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
                            new SqlParameterCollection(new SqlParameter("@id", newUdf.getId()))),
                    null).getQueryIterable().toList();
            if (udfs.size() > 0) {
                //
            } else {
                Assert.fail("Query fail to find UserDefinedFunction");
            }
        }
        client.deleteUserDefinedFunction(this.getUserDefinedFunctionLink(this.databaseForTest, this.collectionForTest, newUdf, isNameBased), null);
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
        // Create document client
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);
        
        // Read user defined functions and get initial count
        List<UserDefinedFunction> udfs = client.readUserDefinedFunctions(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                null).getQueryIterable().toList();
        
        int initialUserDefinedFunctionCount = udfs.size();

        // Create user defined function definition 
        UserDefinedFunction udfDefinition = new UserDefinedFunction();
        udfDefinition.setId(GatewayTests.getUID());
        udfDefinition.setBody("function() {var x = 10;}");
        
        // Upsert should create the udf
        UserDefinedFunction createdUserDefinedFunction = client.upsertUserDefinedFunction(
                this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                udfDefinition, null).getResource();

        // Verify Id property
        Assert.assertEquals(udfDefinition.getId(), createdUserDefinedFunction.getId());
        
        // Read udfs to check the count and it should increase
        udfs = client.readUserDefinedFunctions(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                        null)
                .getQueryIterable().toList();
        
        Assert.assertEquals(initialUserDefinedFunctionCount + 1, udfs.size());
        
        // Update udf
        createdUserDefinedFunction.setBody("function() {var x = 20;}");
        
        // Upsert should replace the trigger since it already exists
        UserDefinedFunction upsertedUserDefinedFunction = client.upsertUserDefinedFunction(
                this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                createdUserDefinedFunction, null).getResource();

        // Verify Id property
        Assert.assertEquals(createdUserDefinedFunction.getId(), upsertedUserDefinedFunction.getId());
        
        // Verify updated property
        Assert.assertEquals(createdUserDefinedFunction.getBody(), upsertedUserDefinedFunction.getBody());
        
        // Read udfs to check the count and it should remain the same
        udfs = client.readUserDefinedFunctions(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                null)
        .getQueryIterable().toList();

        Assert.assertEquals(initialUserDefinedFunctionCount + 1, udfs.size());
        
        // Update udf id
        createdUserDefinedFunction.setId(GatewayTests.getUID());
        
        // Upsert should create new udf since id is changed
        UserDefinedFunction newUserDefinedFunction = client.upsertUserDefinedFunction(
                this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                createdUserDefinedFunction, null).getResource();

        // Verify Id property
        Assert.assertEquals(createdUserDefinedFunction.getId(), newUserDefinedFunction.getId());
        
        // Read udfs to check the count and it should increase
        udfs = client.readUserDefinedFunctions(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                null)
        .getQueryIterable().toList();

        Assert.assertEquals(initialUserDefinedFunctionCount + 2, udfs.size());        
        
        // Delete udfs
        client.deleteUserDefinedFunction(this.getUserDefinedFunctionLink(this.databaseForTest, this.collectionForTest, upsertedUserDefinedFunction, isNameBased), null);
        client.deleteUserDefinedFunction(this.getUserDefinedFunctionLink(this.databaseForTest, this.collectionForTest, newUserDefinedFunction, isNameBased), null);
        
        // Read udfs to check the count and it should remain same
        udfs = client.readUserDefinedFunctions(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
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
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);

        // List users.
        List<User> users = client.readUsers(this.getDatabaseLink(this.databaseForTest, isNameBased), null).getQueryIterable().toList();
        int beforeCreateCount = users.size();
        // Create user.
        User user = client.createUser(this.getDatabaseLink(this.databaseForTest, isNameBased),
                new User("{ 'id': 'new user' }"),
                null).getResource();
        Assert.assertEquals("new user", user.getId());
        // List users after creation.
        users = client.readUsers(this.getDatabaseLink(this.databaseForTest, isNameBased), null).getQueryIterable().toList();
        Assert.assertEquals(beforeCreateCount + 1, users.size());
        // Query users.
        users = client.queryUsers(this.getDatabaseLink(this.databaseForTest, isNameBased),
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
        user = client.readUser(this.getUserLink(this.databaseForTest, replacedUser, isNameBased), null).getResource();
        Assert.assertEquals(replacedUser.getId(), user.getId());
        // Delete user.
        client.deleteUser(this.getUserLink(this.databaseForTest, user, isNameBased), null);
        // Read user after deletion.
        try {
            client.readUser(this.getUserLink(this.databaseForTest, replacedUser, isNameBased), null);
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
        // Create document client
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);
        
        // Read users and get initial count
        List<User> users = client
                .readUsers(this.getDatabaseLink(this.databaseForTest, isNameBased), null)
                .getQueryIterable().toList();
        
        int initialUserCount = users.size();
        
        // Create user definition
        User userDefinition = new User();
        userDefinition.setId(GatewayTests.getUID());
        
        // Upsert should create the user
        User createdUser = client.upsertUser(
                this.getDatabaseLink(this.databaseForTest, isNameBased),
                userDefinition, null).getResource();

        // Verify Id property
        Assert.assertEquals(userDefinition.getId(), createdUser.getId());
        
        // Read users to check the count and it should increase
        users = client.readUsers(this.getDatabaseLink(this.databaseForTest, isNameBased), null)
                .getQueryIterable().toList();
        
        Assert.assertEquals(initialUserCount + 1, users.size());
        
        // Update user id
        userDefinition.setId(GatewayTests.getUID());
        
        // Upsert should create new user since id is changed
        User newUser = client.upsertUser(
                this.getDatabaseLink(this.databaseForTest, isNameBased),
                userDefinition, null).getResource();

        // Verify Id property
        Assert.assertEquals(userDefinition.getId(), newUser.getId());
        
        // Read users to check the count and it should increase
        users = client.readUsers(this.getDatabaseLink(this.databaseForTest, isNameBased), null)
                .getQueryIterable().toList();
        
        Assert.assertEquals(initialUserCount + 2, users.size());        

        // Delete users
        client.deleteUser(this.getUserLink(this.databaseForTest, createdUser, isNameBased), null);
        client.deleteUser(this.getUserLink(this.databaseForTest, newUser, isNameBased), null);
        
        // Read users to check the count and it should remain same
        users = client.readUsers(this.getDatabaseLink(this.databaseForTest, isNameBased), null)
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
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);

        // Create user.
        User user = client.createUser(this.getDatabaseLink(this.databaseForTest, isNameBased),
                new User("{ 'id': 'new user' }"),
                null).getResource();
        // List permissions.
        List<Permission> permissions = client.readPermissions(this.getUserLink(this.databaseForTest, user, isNameBased), null).getQueryIterable().toList();
        int beforeCreateCount = permissions.size();
        Permission permissionDefinition = new Permission();
        permissionDefinition.setId("new permission");
        permissionDefinition.setPermissionMode(PermissionMode.Read);
        permissionDefinition.setResourceLink("dbs/AQAAAA==/colls/AQAAAJ0fgTc=");

        // Create permission.
        Permission permission = client.createPermission(this.getUserLink(this.databaseForTest, user, isNameBased), permissionDefinition, null).getResource();
        Assert.assertEquals("new permission", permission.getId());

        // List permissions after creation.
        permissions = client.readPermissions(this.getUserLink(this.databaseForTest, user, isNameBased), null).getQueryIterable().toList();
        Assert.assertEquals(beforeCreateCount + 1, permissions.size());

        // Query permissions.
        permissions = client.queryPermissions(this.getUserLink(this.databaseForTest, user, isNameBased),
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
        permission = client.readPermission(this.getPermissionLink(this.databaseForTest, user, replacedPermission, isNameBased), null).getResource();
        Assert.assertEquals(replacedPermission.getId(), permission.getId());
        // Delete permission.
        client.deletePermission(this.getPermissionLink(this.databaseForTest, user, replacedPermission, isNameBased), null);
        // Read permission after deletion.
        try {
            client.readPermission(this.getPermissionLink(this.databaseForTest, user, permission, isNameBased), null);
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
        // Create document client
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);

        // Create user definition
        User userDefinition = new User();
        userDefinition.setId(GatewayTests.getUID());

        // Create user.
        User user = client.createUser(
                this.getDatabaseLink(this.databaseForTest, isNameBased),
                userDefinition, null).getResource();
        
        // Read permissions and get initial count
        List<Permission> permissions = client
                .readPermissions(this.getUserLink(this.databaseForTest, user, isNameBased), null)
                .getQueryIterable().toList();
        
        int initialPermissionCount = permissions.size();

        // Create permission definition
        Permission permissionDefinition = new Permission();
        permissionDefinition.setId(GatewayTests.getUID());
        permissionDefinition.setPermissionMode(PermissionMode.Read);
        permissionDefinition.setResourceLink("dbs/AQAAAA==/colls/AQAAAJ0fgTc=");

        // Upsert should create the permission
        Permission createdPermission = client.upsertPermission(
                this.getUserLink(this.databaseForTest, user, isNameBased),
                permissionDefinition, null).getResource();
        
        // Verify Id property
        Assert.assertEquals(permissionDefinition.getId(), createdPermission.getId());
        
        // Read permissions to check the count and it should increase
        permissions = client.readPermissions(this.getUserLink(this.databaseForTest, user, isNameBased), null)
                .getQueryIterable().toList();
        
        Assert.assertEquals(initialPermissionCount + 1, permissions.size());
        
        // Update permission
        createdPermission.setPermissionMode(PermissionMode.All);
        
        // Upsert should replace the permission since it already exists
        Permission upsertedPermission = client.upsertPermission(
                this.getUserLink(this.databaseForTest, user, isNameBased),
                createdPermission, null).getResource();

        // Verify Id property
        Assert.assertEquals(createdPermission.getId(), upsertedPermission.getId());
        
        // Verify updated property
        Assert.assertEquals(createdPermission.getPermissionMode(), upsertedPermission.getPermissionMode());
        
        // Read permissions to check the count and it should remain the same
        permissions = client.readPermissions(this.getUserLink(this.databaseForTest, user, isNameBased), null)
                .getQueryIterable().toList();
        
        Assert.assertEquals(initialPermissionCount + 1, permissions.size());
        
        // Update permission id
        createdPermission.setId(GatewayTests.getUID());
        // ResourceLink needs to be changed along with the ID in order to create a new permission 
        createdPermission.setResourceLink("dbs/N9EdAA==/colls/N9EdAIugXgA=");
        
        // Upsert should create new permission since id is changed
        Permission newPermission = client.upsertPermission(
                this.getUserLink(this.databaseForTest, user, isNameBased),
                createdPermission, null).getResource();

        // Verify Id property
        Assert.assertEquals(createdPermission.getId(), newPermission.getId());
        
        // Verify ResourceLink property
        Assert.assertEquals(createdPermission.getResourceLink(), newPermission.getResourceLink());
        
        // Read permissions to check the count and it should increase
        permissions = client.readPermissions(this.getUserLink(this.databaseForTest, user, isNameBased), null)
                .getQueryIterable().toList();
        
        Assert.assertEquals(initialPermissionCount + 2, permissions.size());
        
        // Delete permissions
        client.deletePermission(this.getPermissionLink(this.databaseForTest, user, upsertedPermission, isNameBased), null);
        client.deletePermission(this.getPermissionLink(this.databaseForTest, user, newPermission, isNameBased), null);
        
        // Read permissions to check the count and it should remain same
        permissions = client.readPermissions(this.getUserLink(this.databaseForTest, user, isNameBased), null)
                .getQueryIterable().toList();
        
        Assert.assertEquals(initialPermissionCount, permissions.size());
    }

    private void validateOfferResponseBody(Offer offer, String expectedOfferType) {
        Assert.assertNotNull("Id cannot be null", offer.getId());
        Assert.assertNotNull("Resource Id (Rid) cannot be null", offer.getResourceId());
        Assert.assertNotNull("Self link cannot be null", offer.getSelfLink());
        Assert.assertNotNull("Resource Link cannot be null", offer.getResourceLink());
        Assert.assertTrue("Offer id not contained in offer self link", offer.getSelfLink().contains(offer.getId()));

        if (expectedOfferType != null)
        {
            Assert.assertEquals(expectedOfferType, offer.getOfferType());
        }
    }

    @Test
    public void testOfferReadAndQuery() throws DocumentClientException {
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);

        List<Offer> offerList = client.readOffers(null).getQueryIterable().toList();
        int originalOffersCount = offerList.size();
        Offer expectedOffer = null;
        
        String trimmedCollectionLink = StringUtils.removeEnd(StringUtils.removeStart(this.collectionForTest.getSelfLink(), "/"), "/");
        
        for (Offer offer : offerList) {
            String trimmedOfferResourceLink = StringUtils.removeEnd(StringUtils.removeStart(offer.getResourceLink(), "/"), "/");
            if (trimmedOfferResourceLink.equals(trimmedCollectionLink)) {
                expectedOffer = offer;
                break;
            }
        }
        // There is an offer for the test collection we have created
        Assert.assertNotNull(expectedOffer);

        this.validateOfferResponseBody(expectedOffer, null);

        // Read the offer
        Offer readOffer = client.readOffer(expectedOffer.getSelfLink()).getResource();
        this.validateOfferResponseBody(readOffer, expectedOffer.getOfferType());
        // Check if the read resource is what we expect
        Assert.assertEquals(expectedOffer.getId(), readOffer.getId());
        Assert.assertEquals(expectedOffer.getResourceId(), readOffer.getResourceId());
        Assert.assertEquals(expectedOffer.getSelfLink(), readOffer.getSelfLink());
        Assert.assertEquals(expectedOffer.getResourceLink(), readOffer.getResourceLink());

        // Query for the offer.
        List<Offer> queryResultList = client.queryOffers(
                new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
                        new SqlParameterCollection(new SqlParameter("@id", expectedOffer.getId()))),
                null).getQueryIterable().toList();
        
        // We should find only one offer with the given id
        Assert.assertEquals(queryResultList.size(), 1);
        Offer oneQueryResult = queryResultList.get(0);
        
        String trimmedOfferResourceLink = StringUtils.removeEnd(StringUtils.removeStart(oneQueryResult.getResourceLink(), "/"), "/");
        Assert.assertTrue(trimmedCollectionLink.equals(trimmedOfferResourceLink));
        
        this.validateOfferResponseBody(oneQueryResult, expectedOffer.getOfferType());
        // Check if the query result is what we expect
        Assert.assertEquals(expectedOffer.getId(), oneQueryResult.getId());
        Assert.assertEquals(expectedOffer.getResourceId(), oneQueryResult.getResourceId());
        Assert.assertEquals(expectedOffer.getSelfLink(), oneQueryResult.getSelfLink());
        Assert.assertEquals(expectedOffer.getResourceLink(), oneQueryResult.getResourceLink());

        // Modify the SelfLink
        String offerLink = expectedOffer.getSelfLink().substring(
                0, expectedOffer.getSelfLink().length() - 1) + "x";

        // Read the offer
        try {
            readOffer = client.readOffer(offerLink).getResource();
            Assert.fail("Expected an exception when reading offer with bad offer link");
        } catch (DocumentClientException ex) {
            Assert.assertEquals(400, ex.getStatusCode());
        }

        client.deleteCollection(this.collectionForTest.getSelfLink(), null);

        // Now try to get the read the offer after the collection is deleted
        try {
            client.readOffer(expectedOffer.getSelfLink()).getResource();
            Assert.fail("Expected an exception when reading deleted offer");
        } catch (DocumentClientException ex) {
            Assert.assertEquals(404, ex.getStatusCode());
        }

        // Make sure read offers returns one offer less that the original list of offers
        offerList = client.readOffers(null).getQueryIterable().toList();
        Assert.assertEquals(originalOffersCount-1, offerList.size());
    }

    @Test
    public void testOfferReplace() throws DocumentClientException {
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);

        List<Offer> offerList = client.readOffers(null).getQueryIterable().toList();
        Offer expectedOffer = null;
        
        String trimmedCollectionLink = StringUtils.removeEnd(StringUtils.removeStart(this.collectionForTest.getSelfLink(), "/"), "/");
        
        for (Offer offer : offerList) {
            String trimmedOfferResourceLink = StringUtils.removeEnd(StringUtils.removeStart(offer.getResourceLink(), "/"), "/");
            if (trimmedOfferResourceLink.equals(trimmedCollectionLink)) {
                expectedOffer = offer;
                break;
            }
        }
        Assert.assertNotNull(expectedOffer);

        this.validateOfferResponseBody(expectedOffer, null);
        Offer offerToReplace = new Offer(expectedOffer.toString());

        // Modify the offer
        offerToReplace.setOfferType("S2");

        // Replace the offer
        Offer replacedOffer = client.replaceOffer(offerToReplace).getResource();
        this.validateOfferResponseBody(replacedOffer, "S2");

        // Check if the replaced offer is what we expect
        Assert.assertEquals(offerToReplace.getId(), replacedOffer.getId());
        Assert.assertEquals(offerToReplace.getResourceId(), replacedOffer.getResourceId());
        Assert.assertEquals(offerToReplace.getSelfLink(), replacedOffer.getSelfLink());
        Assert.assertEquals(offerToReplace.getResourceLink(), replacedOffer.getResourceLink());

        offerToReplace.setResourceId("NotAllowed");
        try {
            client.replaceOffer(offerToReplace).getResource();
            Assert.fail("Expected an exception when replaceing an offer with bad id");
        } catch (DocumentClientException ex) {
            Assert.assertEquals(400, ex.getStatusCode());
        }

        offerToReplace.setResourceId("InvalidRid");
        try {
            client.replaceOffer(offerToReplace).getResource();
            Assert.fail("Expected an exception when replaceing an offer with bad Rid");
        } catch (DocumentClientException ex) {
            Assert.assertEquals(400, ex.getStatusCode());
        }

        offerToReplace.setId(null);
        offerToReplace.setResourceId(null);
        try {
            client.replaceOffer(offerToReplace).getResource();
            Assert.fail("Expected an exception when replaceing an offer with null id and rid");
        } catch (DocumentClientException ex) {
            Assert.assertEquals(400, ex.getStatusCode());
        }
    }

    @Test
    public void testCreateCollectionWithOfferType() throws DocumentClientException {
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);

        // Create a new collection of offer type S2.
        DocumentCollection collectionDefinition = new DocumentCollection();
        collectionDefinition.setId(GatewayTests.getUID());
        RequestOptions requestOptions = new RequestOptions();
        requestOptions.setOfferType("S2");
        client.createCollection(this.databaseForTest.getSelfLink(), collectionDefinition, requestOptions);

        // We should have an offer of type S2.
        List<Offer> offerList = client.readOffers(null).getQueryIterable().toList();
        boolean hasS2 = false;
        for (Offer offer : offerList) {
            if (offer.getOfferType().equals("S2")) {
                hasS2 = true;
                break;
            }
        }
        Assert.assertTrue("There should be an offer of type S2.", hasS2);
    }

    @Test
    public void testDatabaseAccount() throws DocumentClientException {
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);

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
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);
        // Create document1
        Document document1 = client.createDocument(
                collectionForTest.getSelfLink(),
                new Document("{ 'id': 'coll1doc1', 'spam': 'eggs', 'key': 'value' }"), null, false).getResource();
        // Create document2
        Document document2 = client.createDocument(
                collectionForTest.getSelfLink(),
                new Document("{ 'id': 'coll1doc2', 'spam': 'eggs2', 'key': 'value2' }"), null, false).getResource();

        // Create a new collection for test.
        DocumentCollection anotherCollectionForTest = client.createCollection(
                databaseForTest.getSelfLink(),
                new DocumentCollection("{ 'id': 'sample collection2' }"),
                null).getResource();
        // Create user1.
        User user1 = client.createUser(databaseForTest.getSelfLink(),
                new User("{ 'id': 'user1' }"),
                null).getResource();

        Permission permission1Definition = new Permission(String.format(
                "{" +
                        "  'id': 'permission On Coll1'," +
                        "  'permissionMode': 'Read'," +
                        "  'resource': '%s'" +
                        "}", collectionForTest.getSelfLink()));
        // Create permission for collectionForTest
        Permission permission1 = client.createPermission(user1.getSelfLink(),
                permission1Definition,
                null).getResource();

        // Create user 2.
        User user2 = client.createUser(databaseForTest.getSelfLink(),
                new User("{ 'id': 'user2' }"),
                null).getResource();

        Permission permission2Definition = new Permission(String.format(
                "{" +
                        "  'id': 'permission On coll2'," +
                        "  'permissionMode': 'All'," +
                        "  'resource': '%s'" +
                        "}", anotherCollectionForTest.getSelfLink()));
        // Create permission on anotherCollectionForTest.
        Permission permission2 = client.createPermission(user2.getSelfLink(),
                permission2Definition,
                null).getResource();


        // Now the resources for this test is fully prepared.

        // Client without any authorization will fail.
        DocumentClient badClient = new DocumentClient(HOST,
                "",
                ConnectionPolicy.GetDefault(),
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
                ConnectionPolicy.GetDefault(),
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
                ConnectionPolicy.GetDefault(),
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
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);

        // Read conflicts.
        client.readConflicts(this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased), null).getQueryIterable().toList();

        // Query for conflicts.
        client.queryConflicts(
                this.getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, isNameBased),
                new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
                        new SqlParameterCollection(new SqlParameter("@id", "FakeId"))),
                null).getQueryIterable().toList();
    }

    @Test
    public void testCustomizedUserAgentCrud() throws DocumentClientException {
        ConnectionPolicy policy = ConnectionPolicy.GetDefault();
        policy.setUserAgentSuffix("My-Custom-User-Agent");
        Assert.assertEquals("User-agent suffix should've been added", "My-Custom-User-Agent", policy.getUserAgentSuffix());
    }

    private static String getUID() {
        UUID u = UUID.randomUUID();
        return ("" + u.getMostSignificantBits()) + Math.abs(u.getLeastSignificantBits());
    }

    @Test
    @Ignore
    public void testQuotaHeaders() {
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);

        try {
            cleanUpGeneratedDatabases();
            Database dbToCreate = new Database();
            dbToCreate.setId("DocumentQuotaDB" + getUID());
            Database db = client.createDatabase(dbToCreate, null).getResource();

            DocumentCollection collectionToCreate = new DocumentCollection();
            collectionToCreate.setId("DocumentQuotaDBDocumentCollectionConsistent");
            IndexingPolicy policy = new IndexingPolicy();
            policy.setIndexingMode(IndexingMode.Consistent);
            collectionToCreate.setIndexingPolicy(policy);;

            DocumentCollection collection = client.createCollection(db.getSelfLink(), collectionToCreate, null).getResource();

            for(int i = 0; i< 10000; i++)
            {
                String docName = getUID().substring(0,10);
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
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);

        Document doc = new Document();
        doc.setId("doc01");
        doc.set("name", "name01");

        SqlQuerySpec query = new SqlQuerySpec(
                "SELECT * FROM root r WHERE r.name=@name",
                new SqlParameterCollection(new SqlParameter("@name", "name01")));

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
        // Sets up entities for this test.
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);
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
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);

        List<DocumentCollection> collections = client.readCollections(this.getDatabaseLink(this.databaseForTest, true),
                null).getQueryIterable().toList();

        int beforeCreateCollectionsCount = collections.size();

        // pascalCase
        DocumentCollection collectionDefinition1 = new DocumentCollection();
        collectionDefinition1.setId("sampleCollection");

        // CamelCase
        DocumentCollection collectionDefinition2 = new DocumentCollection();
        collectionDefinition2.setId("SampleCollection");

        // Create 2 collections with different casing of IDs
        DocumentCollection createdCollection1 = client.createCollection(getDatabaseLink(this.databaseForTest, true),
                collectionDefinition1,
                null).getResource();

        DocumentCollection createdCollection2 = client.createCollection(getDatabaseLink(this.databaseForTest, true),
                collectionDefinition2,
                null).getResource();

        // Verify if additional 2 collections got created
        collections = client.readCollections(this.getDatabaseLink(this.databaseForTest, true), null).getQueryIterable().toList();
        Assert.assertEquals(collections.size(), beforeCreateCollectionsCount+2);

        // Verify that collections are created with specified IDs    
        Assert.assertEquals(collectionDefinition1.getId(), createdCollection1.getId());
        Assert.assertEquals(collectionDefinition2.getId(), createdCollection2.getId());
    }

    @Test
    public void testIdUnicodeValidation() throws DocumentClientException {
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);

        // Unicode chars in Hindi for Id which translates to: "Hindi is one of the main  languages of India"
        DocumentCollection collectionDefinition1 = new DocumentCollection();
        collectionDefinition1.setId("à¤¹à¤¿à¤¨à¥�à¤¦à¥€ à¤­à¤¾à¤°à¤¤ à¤•à¥‡ à¤®à¥�à¤–à¥�à¤¯ à¤­à¤¾à¤·à¤¾à¤“à¤‚ à¤®à¥‡à¤‚ à¤¸à¥‡ à¤�à¤• à¤¹à¥ˆ");

        // Special chars for Id
        DocumentCollection collectionDefinition2 = new DocumentCollection();
        collectionDefinition2.setId("!@$%^&*()-~`'_[]{}|;:,.<>");

        // verify that collections are created with specified IDs
        DocumentCollection createdCollection1 = client.createCollection(getDatabaseLink(this.databaseForTest, true),
                collectionDefinition1,
                null).getResource();

        DocumentCollection createdCollection2 = client.createCollection(getDatabaseLink(this.databaseForTest, true),
                collectionDefinition2,
                null).getResource();

        // Verify that collections are created with specified IDs    
        Assert.assertEquals(collectionDefinition1.getId(), createdCollection1.getId());
        Assert.assertEquals(collectionDefinition2.getId(), createdCollection2.getId());
    }

    @Test
    public void testSessionContainer() throws DocumentClientException {
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);

        DocumentCollection collectionDefinition = new DocumentCollection();
        collectionDefinition.setId(GatewayTests.getUID());
        DocumentCollection collection = client.createCollection(this.getDatabaseLink(this.databaseForTest, true),
                collectionDefinition,
                null).getResource();
        // create a document
        Document documentDefinition = new Document(
                "{" +
                        "  'name': 'sample document'," +
                        "  'key': '0'" +
                "}");

        Document document = client.createDocument(this.getDocumentCollectionLink(this.databaseForTest, collection, false),
                documentDefinition,
                null,
                false).getResource();


        // Replace document.
        for(int i=0; i<100; i++) {

            // Update the "key" property in a tight loop
            document.set("key", Integer.toString(i));
            // Replace the document
            Document replacedDocument = client.replaceDocument(document, null).getResource();

            // Read the document.
            Document documentFromRead = client.readDocument(this.getDocumentLink(this.databaseForTest, collection, replacedDocument, true), null).getResource();
            // Verify that we read our own write(key property) 
            Assert.assertEquals(replacedDocument.getString("key"), documentFromRead.getString("key"));
        }		
    }
    
    @Test
    public void testPartitioning() throws DocumentClientException {
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);

        // Create bunch of collections participating in partitioning
        DocumentCollection collectionDefinition = new DocumentCollection();
        
        collectionDefinition.setId("coll_0");
        DocumentCollection collection0 = client.createCollection(this.getDatabaseLink(this.databaseForTest, true),
                collectionDefinition,
                null).getResource();
        
        collectionDefinition.setId("coll_1");
        DocumentCollection collection1 = client.createCollection(this.getDatabaseLink(this.databaseForTest, true),
                collectionDefinition,
                null).getResource();
        
        collectionDefinition.setId("coll_2");
        DocumentCollection collection2 = client.createCollection(this.getDatabaseLink(this.databaseForTest, true),
                collectionDefinition,
                null).getResource();
        
        // Iterator of ID based collection links
        ArrayList<String> collectionLinks = new ArrayList<String>();
        collectionLinks.add(this.getDocumentCollectionLink(this.databaseForTest, collection0, true));
        collectionLinks.add(this.getDocumentCollectionLink(this.databaseForTest, collection1, true));
        collectionLinks.add(this.getDocumentCollectionLink(this.databaseForTest, collection2, true));
        
        // Instantiate PartitionResolver to be used for partitioning 
        TestPartitionResolver partitionResolver = new TestPartitionResolver(collectionLinks);
        
        // Register the test database with partitionResolver created above
        client.registerPartitionResolver(this.getDatabaseLink(this.databaseForTest, true), partitionResolver);
        
        // Create document definition used to create documents
        Document documentDefinition = new Document(
                "{" + 
                        "  'id': '0'," +
                        "  'name': 'sample document'," +
                        "  'key': 'value'" + 
                "}");
        
        documentDefinition.setId("0");
        client.createDocument(this.getDatabaseLink(this.databaseForTest, true),
                documentDefinition,
                null,
                false).getResource();
        
        // Read the documents in collection0 and verify that the count is 1 now
        List<Document> list = client.readDocuments(this.getDocumentCollectionLink(this.databaseForTest, collection0, true), null).getQueryIterable().toList();
        Assert.assertEquals(1, list.size());
        
        // Verify that it contains the document with Id 0
        Assert.assertEquals("0", list.get(0).getId());
        
        documentDefinition.setId("1");
        client.createDocument(this.getDatabaseLink(this.databaseForTest, true),
                documentDefinition,
                null,
                false).getResource();
        
        // Read the documents in collection1 and verify that the count is 1 now
        list = client.readDocuments(this.getDocumentCollectionLink(this.databaseForTest, collection1, true), null).getQueryIterable().toList();
        Assert.assertEquals(1, list.size());
        
        // Verify that it contains the document with Id 1
        Assert.assertEquals("1", list.get(0).getId());
        
        documentDefinition.setId("2");
        client.createDocument(this.getDatabaseLink(this.databaseForTest, true),
                documentDefinition,
                null,
                false).getResource();
         
        // Read the documents in collection2 and verify that the count is 1 now
        list = client.readDocuments(this.getDocumentCollectionLink(this.databaseForTest, collection2, true), null).getQueryIterable().toList();
        Assert.assertEquals(1, list.size());
        
        // Verify that it contains the document with Id 2
        Assert.assertEquals("2", list.get(0).getId());
        
        // Updating the value of "key" property to test UpsertDocument(replace scenario)
        documentDefinition.setId("0");
        documentDefinition.set("key", "new value");
        
        client.upsertDocument(this.getDatabaseLink(this.databaseForTest, true),
                documentDefinition,
                null,
                false).getResource();
         
        // Read the documents in collection0 and verify that the count is still 1
        list = client.readDocuments(this.getDocumentCollectionLink(this.databaseForTest, collection0, true), null).getQueryIterable().toList();
        Assert.assertEquals(1, list.size());
        
        // Verify that it contains the document with new key value
        Assert.assertEquals(documentDefinition.getString("key"), list.get(0).getString("key"));
        
        // Query documents in all collections(since no partition key specified)
        list = client.queryDocuments(this.getDatabaseLink(this.databaseForTest, true),
                "SELECT * FROM root r WHERE r.id='0'",
                null).getQueryIterable().toList();
        Assert.assertEquals(1, list.size());
        
        // Updating the value of id property to test UpsertDocument(create scenario)
        documentDefinition.setId("4");
        
        client.upsertDocument(this.getDatabaseLink(this.databaseForTest, true),
                documentDefinition,
                null,
                false).getResource();
         
        // Read the documents in collection1 and verify that the count is 2 now
        list = client.readDocuments(this.getDocumentCollectionLink(this.databaseForTest, collection1, true), null).getQueryIterable().toList();
        Assert.assertEquals(2, list.size());
        
        // Query documents in all collections(since no partition key specified)
        list = client.queryDocuments(this.getDatabaseLink(this.databaseForTest, true),
                new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
                        new SqlParameterCollection(new SqlParameter(
                                "@id", documentDefinition.getId()))),
                null).getQueryIterable().toList();
        Assert.assertEquals(1, list.size());
        
        // Query documents in collection(with partition key of "4" specified) which resolves to collection1
        list = client.queryDocuments(this.getDatabaseLink(this.databaseForTest, true),
                "SELECT * FROM root r",
                null, documentDefinition.getId()).getQueryIterable().toList();
        Assert.assertEquals(2, list.size());
        
        // Query documents in collection(with partition key "5" specified) which resolves to collection2 but non existent document in that collection
        list = client.queryDocuments(this.getDatabaseLink(this.databaseForTest, true),
                new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
                        new SqlParameterCollection(new SqlParameter(
                                "@id", documentDefinition.getId()))),
                null, "5").getQueryIterable().toList();
        Assert.assertEquals(0, list.size());
    }
    
    @Test
    public void testPartitionPaging() throws DocumentClientException {
        DocumentClient client = new DocumentClient(HOST,
                MASTER_KEY,
                ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);

        // Create bunch of collections participating in partitioning
        DocumentCollection collectionDefinition = new DocumentCollection();
        
        collectionDefinition.setId("coll_0");
        DocumentCollection collection0 = client.createCollection(this.getDatabaseLink(this.databaseForTest, true),
                collectionDefinition,
                null).getResource();
        
        collectionDefinition.setId("coll_1");
        DocumentCollection collection1 = client.createCollection(this.getDatabaseLink(this.databaseForTest, true),
                collectionDefinition,
                null).getResource();
        
        // Iterator of ID based collection links
        ArrayList<String> collectionLinks = new ArrayList<String>();
        collectionLinks.add(this.getDocumentCollectionLink(this.databaseForTest, collection0, true));
        collectionLinks.add(this.getDocumentCollectionLink(this.databaseForTest, collection1, true));
        
        // Instantiate PartitionResolver to be used for partitioning 
        TestPartitionResolver partitionResolver = new TestPartitionResolver(collectionLinks);
        
        // Register the test database with partitionResolver created above
        client.registerPartitionResolver(this.getDatabaseLink(this.databaseForTest, true), partitionResolver);
        
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
                client.createDocument(this.getDatabaseLink(this.databaseForTest, true),
                        documentDefinition,
                        null,
                        false).getResource();    
                Thread.sleep(1500);
            } catch (InterruptedException ie) {
                System.out.println(ie.getMessage());
            }
        }
        
        // Query the documents to ensure that you get the correct count(no paging)
        List<Document> list = client.queryDocuments(this.getDatabaseLink(this.databaseForTest, true),
                "SELECT * FROM root r WHERE r.id < '7'",
                null).getQueryIterable().toList();
        
        Assert.assertEquals(7, list.size());
        
        // Setting PageSize to restrict the max number of documents returned
        FeedOptions feedOptions = new FeedOptions();
        feedOptions.setPageSize(3);
        
        QueryIterable<Document> query = client.queryDocuments(this.getDatabaseLink(this.databaseForTest, true),
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
        query = client.queryDocuments(this.getDatabaseLink(this.databaseForTest, true),
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
        query = client.queryDocuments(this.getDatabaseLink(this.databaseForTest, true),
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
        collectionLinks.add(this.getDocumentCollectionLink(this.databaseForTest, collectionDefinition, true));
        
        collectionDefinition.setId("coll_1");
        collectionLinks.add(this.getDocumentCollectionLink(this.databaseForTest, collectionDefinition, true));
        
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
            collLink = this.getDocumentCollectionLink(databaseDefinition, coll, true);
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
        collLink = this.getDocumentCollectionLink(databaseDefinition, coll, true);
        
        Assert.assertEquals(collLink, list.get(0));
        
        // Querying for a document and verifying that it's in the expected collection
        readCollectionLink = hashPartitionResolver.resolveForRead("999");
        list = new ArrayList<String>();
        for(String collectionLink : readCollectionLink)
            list.add(collectionLink);
        
        Assert.assertEquals(1, list.size());
        
        coll.setId("coll0");
        collLink = this.getDocumentCollectionLink(databaseDefinition, coll, true);
        
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
        
        this.validate("", 0x1B873593, new byte[]{(byte) 0x67, (byte) 0xA2, (byte) 0xA8, (byte) 0xEE}, 1738713326L, method);
        this.validate("1", 0xE82562E4, new byte[]{(byte) 0xED, (byte) 0x24, (byte) 0x92, (byte) 0xD0}, 3978597072L, method);
        this.validate("00", 0xB4C39035, new byte[]{(byte) 0x1B, (byte) 0x64, (byte) 0x09, (byte) 0xFA}, 459540986L, method);
        this.validate("eyetooth", 0x8161BD86, new byte[]{(byte) 0x6F, (byte) 0x1C, (byte) 0x62, (byte) 0x98}, 1864131224L, method);
        this.validate("acid", 0x4DFFEAD7, new byte[]{(byte) 0xB9, (byte) 0xC0, (byte) 0x92, (byte) 0x36}, 3116405302L, method);
        this.validate("elevation", 0x1A9E1828, new byte[]{(byte) 0xDF, (byte) 0x40, (byte) 0xB6, (byte) 0xA9}, 3745560233L, method);
        this.validate("dent", 0xE73C4579, new byte[]{(byte) 0xD3, (byte) 0xE1, (byte) 0x59, (byte) 0xD4}, 3554761172L, method);
        this.validate("homeland", 0xB3DA72CA, new byte[]{(byte) 0xBB, (byte) 0x72, (byte) 0x4D, (byte) 0x06}, 3144830214L, method);
        this.validate("glamor", 0x8078A01B, new byte[]{(byte) 0xA7, (byte) 0xA2, (byte) 0x89, (byte) 0x89}, 2812447113L, method);
        this.validate("flags", 0x4D16CD6C, new byte[]{(byte) 0x02, (byte) 0x66, (byte) 0x87, (byte) 0x52}, 40273746L, method);
        this.validate("democracy", 0x19B4FABD, new byte[]{(byte) 0xB0, (byte) 0xD6, (byte) 0x55, (byte) 0xE4}, 2966836708L, method);
        this.validate("bumble", 0xE653280E, new byte[]{(byte) 0x0C, (byte) 0xC3, (byte) 0xD7, (byte) 0xFE}, 214161406L, method);
        this.validate("catch", 0xB2F1555F, new byte[]{(byte) 0xCD, (byte) 0xB6, (byte) 0x4B, (byte) 0x98}, 3451276184L, method);
        this.validate("omnomnomnivore", 0x7F8F82B0, new byte[]{(byte) 0xFF, (byte) 0xCD, (byte) 0xC4, (byte) 0x38}, 4291675192L, method);
        this.validate("The quick brown fox jumps over the lazy dog", 0x4C2DB001, new byte[]{(byte) 0xC9, (byte) 0x8D, (byte) 0xAB, (byte) 0x6D}, 3381504877L, method);
    }
    
    private void validate(String str, int seed, byte[] expectedHashBytes, long expectedValue, Method method) throws UnsupportedEncodingException {
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
        collectionLinks.add(this.getDocumentCollectionLink(this.databaseForTest, collectionDefinition, true));

        collectionDefinition.setId("coll_1");
        collectionLinks.add(this.getDocumentCollectionLink(this.databaseForTest, collectionDefinition, true));

        collectionDefinition.setId("coll_2");
        collectionLinks.add(this.getDocumentCollectionLink(this.databaseForTest, collectionDefinition, true));
        
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
    
    private String getDatabaseLink(Database database, boolean isNameBased)
    {
        if(isNameBased) {
            return DATABASES_PATH_SEGMENT + "/" + database.getId();
        }
        else {
            return database.getSelfLink();
        }
    }

    private String getUserLink(Database database, User user, boolean isNameBased)
    {
        if(isNameBased) {
            return this.getDatabaseLink(database, true) + "/" + USERS_PATH_SEGMENT + "/"  + user.getId();
        }
        else {
            return user.getSelfLink();
        }
    }

    private String getPermissionLink(Database database, User user, Permission permission, boolean isNameBased)
    {
        if(isNameBased) {
            return this.getUserLink(database, user, true) + "/" + PERMISSIONS_PATH_SEGMENT + "/"  + permission.getId();
        }
        else {
            return permission.getSelfLink();
        }
    }

    private String getDocumentCollectionLink(Database database, DocumentCollection coll, boolean isNameBased)
    {
        if(isNameBased) {
            return this.getDatabaseLink(database, true) + "/" + COLLECTIONS_PATH_SEGMENT + "/"  + coll.getId();
        }
        else {
            return coll.getSelfLink();
        }
    }

    private String getDocumentLink(Database database, DocumentCollection coll, Document doc, boolean isNameBased)
    {
        if(isNameBased) {
            return this.getDocumentCollectionLink(database, coll, true) + "/" + DOCUMENTS_PATH_SEGMENT + "/" + doc.getId();
        }
        else {
            return doc.getSelfLink();
        }
    }

    private String getAttachmentLink(Database database, DocumentCollection coll, Document doc, Attachment attachment, boolean isNameBased)
    {
        if(isNameBased) {
            return this.getDocumentLink(database, coll, doc, true) + "/" + ATTACHMENTS_PATH_SEGMENT + "/" + attachment.getId(); 
        }
        else {
            return attachment.getSelfLink();
        }
    }

    private String getTriggerLink(Database database, DocumentCollection coll, Trigger trigger, boolean isNameBased)
    {
        if(isNameBased) {
            return this.getDocumentCollectionLink(database, coll, true) + "/" + TRIGGERS_PATH_SEGMENT + "/" + trigger.getId(); 
        }
        else {
            return trigger.getSelfLink();
        }
    }

    private String getStoredProcedureLink(Database database, DocumentCollection coll, StoredProcedure storedProcedure, boolean isNameBased)
    {
        if(isNameBased) {
            return this.getDocumentCollectionLink(database, coll, true) + "/" + STORED_PROCEDURES_PATH_SEGMENT + "/" + storedProcedure.getId(); 
        }
        else {
            return storedProcedure.getSelfLink();
        }
    }

    private String getUserDefinedFunctionLink(Database database, DocumentCollection coll, UserDefinedFunction userDefinedFunction, boolean isNameBased)
    {
        if(isNameBased) {
            return this.getDocumentCollectionLink(database, coll, true) + "/" + USER_DEFINED_FUNCTIONS_PATH_SEGMENT + "/" + userDefinedFunction.getId(); 
        }
        else {
            return userDefinedFunction.getSelfLink();
        }
    }

    @SuppressWarnings("unused")
    private String getConflictLink(Database database, DocumentCollection coll, Conflict conflict, boolean isNameBased)
    {
        if(isNameBased) {
            return this.getDocumentCollectionLink(database, coll, true) + "/" + CONFLICTS_PATH_SEGMENT + "/" + conflict.getId(); 
        }
        else {
            return conflict.getSelfLink();
        }
    }
}
