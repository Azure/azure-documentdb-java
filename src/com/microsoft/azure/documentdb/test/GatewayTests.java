package com.microsoft.azure.documentdb.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.http.HttpStatus;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.microsoft.azure.documentdb.AccessCondition;
import com.microsoft.azure.documentdb.AccessConditionType;
import com.microsoft.azure.documentdb.Attachment;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.DatabaseAccount;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.FeedResponse;
import com.microsoft.azure.documentdb.IndexType;
import com.microsoft.azure.documentdb.IndexingMode;
import com.microsoft.azure.documentdb.MediaOptions;
import com.microsoft.azure.documentdb.MediaReadMode;
import com.microsoft.azure.documentdb.Permission;
import com.microsoft.azure.documentdb.PermissionMode;
import com.microsoft.azure.documentdb.QueryIterable;
import com.microsoft.azure.documentdb.RequestOptions;
import com.microsoft.azure.documentdb.ResourceResponse;
import com.microsoft.azure.documentdb.StoredProcedure;
import com.microsoft.azure.documentdb.Trigger;
import com.microsoft.azure.documentdb.TriggerOperation;
import com.microsoft.azure.documentdb.TriggerType;
import com.microsoft.azure.documentdb.User;
import com.microsoft.azure.documentdb.UserDefinedFunction;

public final class GatewayTests {
    static final String HOST = "[YOUR_ENDPOINT_HERE]";
    static final String MASTER_KEY = "[YOUR_KEY_HERE]";

    private Database databaseForTest;
    private DocumentCollection collectionForTest;
    private static final String databaseForTestId = "GatewayTests_database0";
    private static final String databaseForTestAlternativeId1 = "GatewayTests_database1";
    private static final String databaseForTestAlternativeId2 = "GatewayTests_database2";

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
            Database database = client.queryDatabases(String.format("SELECT * FROM root r WHERE r.id='%s'", id),
                                                      null).getQueryIterable().iterator().next();
            if (database != null) {
                client.deleteDatabase(database.getSelfLink(), null);
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
        this.collectionForTest = client.createCollection(this.databaseForTest.getSelfLink(),
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
        // Jackson's readValue method only supports static and non-local POJO.
        public String pojoProp = "456";
    }

    @Test
    public void testJsonSerialization() throws DocumentClientException {
        Document document = new Document();
        document.set("prop0", null);
        document.set("prop1", "abc");
        Document childDocument = new Document("{'child1Prop1': 500}");
        document.set("child1", childDocument);
        document.set("child2", new JSONObject("{'child2Prop1': '800'}"));

        document.set("child3", new StaticPOJOForTest());
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
                "  'collection1': [101, 102]," +
                "  'collection2': [{'foo': 'bar'}]," +
                "  'collection3': [{'pojoProp': '456'}]," +
                "  'collection4': [[['ABCD']]]" +
                "}");
        Assert.assertEquals(expectedDocument.toString(), document.toString());

        Assert.assertEquals("456", document.getObject("child3", StaticPOJOForTest.class).pojoProp);
        Assert.assertEquals("456", document.getCollection("collection3",
                            StaticPOJOForTest.class).iterator().next().pojoProp);

        document = new Document("{'pojoProp': '654'}");
        StaticPOJOForTest pojo = document.toObject(StaticPOJOForTest.class);
        Assert.assertEquals("654", pojo.pojoProp);
        JSONObject jsonObject = document.toObject(JSONObject.class);
        Assert.assertEquals("654", jsonObject.getString("pojoProp"));
    }

    @Test
    public void testDatabaseCrud() throws DocumentClientException {
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
        databases = client.queryDatabases(String.format("SELECT * FROM root r WHERE r.id='%s'",
                                                        databaseDefinition.getId()),
                                          null).getQueryIterable().toList();
        // number of results for the query should be > 0.
        Assert.assertTrue(databases.size() > 0);
        // replace database.
        createdDb.setId(GatewayTests.databaseForTestAlternativeId2);
        Database replacedDb = client.replaceDatabase(createdDb, null).getResource();
        // Db id should change.
        Assert.assertEquals(GatewayTests.databaseForTestAlternativeId2, replacedDb.getId());
        // Read database.
        Database oneDbFromRead = client.readDatabase(replacedDb.getSelfLink(), null).getResource();
        Assert.assertEquals(replacedDb.getId(), oneDbFromRead.getId());
        // Delete database.
        client.deleteDatabase(replacedDb.getSelfLink(), null);

        // Read database after deletion.
        try {
            client.readDatabase(createdDb.getSelfLink(), null);
            Assert.fail("Exception didn't happen.");
        } catch (DocumentClientException e) {
            Assert.assertEquals(404, e.getStatusCode());
            Assert.assertEquals("NotFound", e.getError().getCode());
        }
    }

    @Test
    public void testCollectionCrud() throws DocumentClientException {
        DocumentClient client = new DocumentClient(HOST,
                                                   MASTER_KEY,
                                                   ConnectionPolicy.GetDefault(),
                                                   ConsistencyLevel.Session);
        List<DocumentCollection> collections = client.readCollections(this.databaseForTest.getSelfLink(),
                                                                      null).getQueryIterable().toList();
        // Create a collection.
        int beforeCreateCollectionsCount = collections.size();
        DocumentCollection collectionDefinition = new DocumentCollection();
        collectionDefinition.setId(GatewayTests.getUID());
        DocumentCollection createdCollection = client.createCollection(this.databaseForTest.getSelfLink(),
                                                                       collectionDefinition,
                                                                       null).getResource();
        Assert.assertEquals(createdCollection.getId(), collectionDefinition.getId());
        // Read collections after creation.
        collections = client.readCollections(this.databaseForTest.getSelfLink(), null).getQueryIterable().toList();
        // Create should increase the number of collections.
        Assert.assertEquals(collections.size(), beforeCreateCollectionsCount + 1);
        // Query collections.
        collections = client.queryCollections(this.databaseForTest.getSelfLink(),
                                              String.format("SELECT * FROM root r WHERE r.id='%s'",
                                                            collectionDefinition.getId()),
                                              null).getQueryIterable().toList();
        Assert.assertTrue(collections.size() > 0);
        // Delete collection.
        client.deleteCollection(createdCollection.getSelfLink(), null);
        // Read collection after deletion.

        try {
            client.readCollection(createdCollection.getSelfLink(), null);
            Assert.fail("Exception didn't happen.");
        } catch (DocumentClientException e) {
            Assert.assertEquals(404, e.getStatusCode());
            Assert.assertEquals("NotFound", e.getError().getCode());
        }
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
                "    'indexingMode': 'Lazy'" +
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
                "    'IncludedPaths': ["+
                "      {" +
                "        'IndexType': 'Hash'," +
                "        'Path': '/'" +
                "      }" +
                "    ]," +
                "    'ExcludedPaths': [" +
                "      '/\"systemMetadata\"/*'" +
                "    ]" +
                "  }" +
                "}");
        client.deleteCollection(consistentCollection.getSelfLink(), null);
        DocumentCollection collectionWithSecondaryIndex = client.createCollection(this.databaseForTest.getSelfLink(),
                                                                                  collectionDefinition,
                                                                                  null).getResource();
        // Check the size of included and excluded paths.
        Assert.assertEquals(2, collectionWithSecondaryIndex.getIndexingPolicy().getIncludedPaths().size());
        Assert.assertEquals(
                IndexType.Hash,
                collectionWithSecondaryIndex.getIndexingPolicy().getIncludedPaths().iterator().next().getIndexType());
        Assert.assertEquals(1, collectionWithSecondaryIndex.getIndexingPolicy().getExcludedPaths().size());
    }

    @Test
    public void testDocumentCrud() throws DocumentClientException {
        DocumentClient client = new DocumentClient(HOST,
                                                   MASTER_KEY,
                                                   ConnectionPolicy.GetDefault(),
                                                   ConsistencyLevel.Session);
        // Read documents.
        List<Document> documents = client.readDocuments(this.collectionForTest.getSelfLink(),
                                                        null).getQueryIterable().toList();
        Assert.assertEquals(0, documents.size());
        // create a document
        Document documentDefinition = new Document(
                "{" +
                "  'name': 'sample document'," +
                "  'foo': 'bar'," +
                "  'key': 'value'" +
                "}");

        // Should throw an error because automatic id generation is disabled.
        try {
            client.createDocument(this.collectionForTest.getSelfLink(), documentDefinition, null, true);
            Assert.fail("Exception didn't happen.");
        } catch (DocumentClientException e) {
            Assert.assertEquals(400, e.getStatusCode());
            Assert.assertEquals("BadRequest", e.getError().getCode());
        }

        Document document = client.createDocument(this.collectionForTest.getSelfLink(),
                                                  documentDefinition,
                                                  null,
                                                  false).getResource();
        Assert.assertEquals(documentDefinition.getString("name"), document.getString("name"));
        Assert.assertNotNull(document.getId());

        // Read documents after creation.
        documents = client.readDocuments(this.collectionForTest.getSelfLink(), null).getQueryIterable().toList();
        // Create should increase the number of documents.
        Assert.assertEquals(1, documents.size());

        // Query documents.
        documents = client.queryDocuments(this.collectionForTest.getSelfLink(),
                                          String.format("SELECT * FROM root r WHERE r.name='%s'",
                                                        documentDefinition.getString("name")),
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
        Document oneDocumentFromRead = client.readDocument(replacedDocument.getSelfLink(), null).getResource();
        Assert.assertEquals(replacedDocument.getId(), oneDocumentFromRead.getId());

        AccessCondition accessCondition = new AccessCondition();
        accessCondition.setCondition(oneDocumentFromRead.getETag()) ;
        accessCondition.setType(AccessConditionType.IfNoneMatch);

        RequestOptions options = new RequestOptions();
        options.setAccessCondition(accessCondition);
        ResourceResponse<Document> rr = client.readDocument(oneDocumentFromRead.getSelfLink(), options);
        Assert.assertEquals(rr.getStatusCode(), HttpStatus.SC_NOT_MODIFIED);

        // delete document
        client.deleteDocument(replacedDocument.getSelfLink(), null);

        // read documents after deletion
        try {
            client.readDocument(replacedDocument.getSelfLink(), null);
            Assert.fail("Exception didn't happen.");
        } catch (DocumentClientException e) {
            Assert.assertEquals(404, e.getStatusCode());
            Assert.assertEquals("NotFound", e.getError().getCode());
        }
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
    public void testAttachmentCrud() throws DocumentClientException {
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
        Document document = client.createDocument(this.collectionForTest.getSelfLink(),
                                                  documentDefinition,
                                                  null,
                                                  false).getResource();
        // List all attachments.
        List<Attachment> attachments = client.readAttachments(document.getSelfLink(),
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
            client.createAttachment(document.getSelfLink(), mediaStream, invalidMediaOptions);
            Assert.assertTrue(false);  // This line shouldn't execute.
        } catch (DocumentClientException e) {
            Assert.assertEquals(400, e.getStatusCode());
            Assert.assertEquals("BadRequest", e.getError().getCode());
        }

        // Create attachment with valid content type.
        mediaStream = new ReadableStream("stream content.");
        Attachment validAttachment = client.createAttachment(document.getSelfLink(),
                                                             mediaStream,
                                                             validMediaOptions).getResource();
        Assert.assertEquals("attachment id", validAttachment.getId());

        mediaStream = new ReadableStream("stream content");
        // Create colliding attachment.
        try {
            client.createAttachment(document.getSelfLink(), mediaStream, validMediaOptions);
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
        Attachment attachment = client.createAttachment(document.getSelfLink(),
                                                        attachmentDefinition,
                                                        null).getResource();
        Assert.assertEquals("Book", attachment.getString("MediaType"));
        Assert.assertEquals("My Book Author", attachment.getString("Author"));

        // List all attachments.
        FeedOptions fo = new FeedOptions();
        fo.setPageSize(1);
        attachments = client.readAttachments(document.getSelfLink(), fo).getQueryIterable().toList();
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
        document = client.createDocument(this.collectionForTest.getSelfLink(),
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
        attachment = client.createAttachment(document.getSelfLink(), secondAttachmentDefinition, null).getResource();
        Assert.assertEquals(validAttachment.getId(), attachment.getId());
        Assert.assertEquals(validAttachment.getMediaLink(), attachment.getMediaLink());
        Assert.assertEquals(validAttachment.getContentType(), attachment.getContentType());
        // Deleting attachment.
        client.deleteAttachment(attachment.getSelfLink(), null);
        // read attachments after deletion
        attachments = client.readAttachments(document.getSelfLink(), null).getQueryIterable().toList();
        Assert.assertEquals(0, attachments.size());
    }

    @Test
    public void testTriggerCrud() throws DocumentClientException {
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
        Trigger newTrigger = client.createTrigger(this.collectionForTest.getSelfLink(),
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
        client.createDocument(this.collectionForTest.getSelfLink(), document, options, false);

        // replace...
        String id = GatewayTests.getUID();
        newTrigger.setId(id);

        newTrigger = client.replaceTrigger(newTrigger, null).getResource();
        Assert.assertEquals(newTrigger.getId(), id);

        newTrigger = client.readTrigger(newTrigger.getSelfLink(), null).getResource();

        // read triggers:
        List<Trigger> triggers = client.readTriggers(this.collectionForTest.getSelfLink(),
                                                     null).getQueryIterable().toList();
        if (triggers.size() > 0) {
            //
        } else {
            Assert.fail("Readfeeds fail to find trigger");
        }

        triggers = client.queryTriggers(this.collectionForTest.getSelfLink(),
                                        String.format("SELECT * FROM root r WHERE r.id='%s'",
                                                      newTrigger.getId()), null).getQueryIterable().toList();
        if (triggers.size() > 0) {
            //
        } else {
            Assert.fail("Query fail to find trigger");
        }

        client.deleteTrigger(newTrigger.getSelfLink(), null);
    }

    @Test
    public void testStoredProcedureCrud() throws DocumentClientException {
        DocumentClient client = new DocumentClient(HOST,
                                                   MASTER_KEY,
                                                   ConnectionPolicy.GetDefault(),
                                                   ConsistencyLevel.Session);

        // create..
        StoredProcedure storedProcedureDef = new StoredProcedure();
        storedProcedureDef.setId(GatewayTests.getUID());
        storedProcedureDef.setBody("function() {var x = 10;}");

        StoredProcedure newStoredProcedure = client.createStoredProcedure(this.collectionForTest.getSelfLink(),
                                                                          storedProcedureDef,
                                                                          null).getResource();
        Assert.assertNotNull(newStoredProcedure.getBody());
        Assert.assertNotNull(newStoredProcedure.getETag());

        // replace...
        String id = GatewayTests.getUID();
        newStoredProcedure.setId(id);

        newStoredProcedure = client.replaceStoredProcedure(newStoredProcedure, null).getResource();
        Assert.assertEquals(newStoredProcedure.getId(), id);

        newStoredProcedure = client.readStoredProcedure(newStoredProcedure.getSelfLink(), null).getResource();

        // read storedProcedures:
        List<StoredProcedure> storedProcedures = client.readStoredProcedures(this.collectionForTest.getSelfLink(),
                                                                             null).getQueryIterable().toList();
        if (storedProcedures.size() > 0) {
            //
        } else {
            Assert.fail("Readfeeds fail to find StoredProcedure");
        }

        storedProcedures = client.queryStoredProcedures(this.collectionForTest.getSelfLink(),
                                                        String.format("SELECT * FROM root r WHERE r.id='%s'",
                                                                      newStoredProcedure.getId()),
                                                                      null).getQueryIterable().toList();
        if (storedProcedures.size() > 0) {
            //
        } else {
            Assert.fail("Query fail to find StoredProcedure");
        }

        client.deleteStoredProcedure(newStoredProcedure.getSelfLink(), null);
    }

    @Test
    public void testStoredProcedureFunctionality()
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
        StoredProcedure retrievedSproc = client.createStoredProcedure(collectionForTest.getSelfLink(),
                                                                      sproc1,
                                                                      null).getResource();
        String result = client.executeStoredProcedure(retrievedSproc.getSelfLink(), null).getResponseAsString();
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
        StoredProcedure retrievedSproc2 = client.createStoredProcedure(collectionForTest.getSelfLink(),
                                                                       sproc2,
                                                                       null).getResource();
        result = client.executeStoredProcedure(retrievedSproc2.getSelfLink(), null).getResponseAsString();
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
        StoredProcedure retrievedSproc3 = client.createStoredProcedure(collectionForTest.getSelfLink(),
                                                                       sproc3,
                                                                       null).getResource();
        result = client.executeStoredProcedure(retrievedSproc3.getSelfLink(),
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
        StoredProcedure retrievedSproc4 = client.createStoredProcedure(collectionForTest.getSelfLink(),
                                                                       sproc4,
                                                                       null).getResource();
        result = client.executeStoredProcedure(retrievedSproc4.getSelfLink(),
                                               new Object[] {tempPOJO}).getResponseAsString();
        Assert.assertEquals("\"aso2\"", result);

        // JSONObject
        JSONObject jsonObject = new JSONObject("{'temp': 'so3'}");
        result = client.executeStoredProcedure(retrievedSproc4.getSelfLink(),
                                               new Object[] {jsonObject}).getResponseAsString();
        Assert.assertEquals("\"aso3\"", result);

        // Document
        Document document = new Document("{'temp': 'so4'}");
        result = client.executeStoredProcedure(retrievedSproc4.getSelfLink(),
                                               new Object[] {document}).getResponseAsString();
        Assert.assertEquals("\"aso4\"", result);
    }

    @Test
    public void testUserDefinedFunctionCrud() throws DocumentClientException {
        DocumentClient client = new DocumentClient(HOST,
                                                   MASTER_KEY,
                                                   ConnectionPolicy.GetDefault(),
                                                   ConsistencyLevel.Session);

        // create..
        UserDefinedFunction udfDef = new UserDefinedFunction();
        udfDef.setId(GatewayTests.getUID());
        udfDef.setBody("function() {var x = 10;}");
        UserDefinedFunction newUdf = client.createUserDefinedFunction(this.collectionForTest.getSelfLink(),
                                                                      udfDef,
                                                                      null).getResource();
        Assert.assertNotNull(newUdf.getBody());
        Assert.assertNotNull(newUdf.getETag());

        // replace...
        String id = GatewayTests.getUID();
        newUdf.setId(id);

        newUdf = client.replaceUserDefinedFunction(newUdf, null).getResource();
        Assert.assertEquals(newUdf.getId(), id);

        newUdf = client.readUserDefinedFunction(newUdf.getSelfLink(), null).getResource();
        Assert.assertEquals(newUdf.getId(), id);

        // read udf feed:
        {
            List<UserDefinedFunction> udfs = client.readUserDefinedFunctions(this.collectionForTest.getSelfLink(),
                                                                             null).getQueryIterable().toList();
            if (udfs.size() > 0) {
                //
            } else {
                Assert.fail("Readfeeds fail to find UserDefinedFunction");
            }
        }
        {
            List<UserDefinedFunction> udfs = client.queryUserDefinedFunctions(
                    this.collectionForTest.getSelfLink(),
                    String.format("SELECT * FROM root r WHERE r.id='%s'",
                                  newUdf.getId()), null).getQueryIterable().toList();
            if (udfs.size() > 0) {
                //
            } else {
                Assert.fail("Query fail to find UserDefinedFunction");
            }
        }
        client.deleteUserDefinedFunction(newUdf.getSelfLink(), null);
    }

    @Test
    public void testUserCrud() throws DocumentClientException {
        DocumentClient client = new DocumentClient(HOST,
                                                   MASTER_KEY,
                                                   ConnectionPolicy.GetDefault(),
                                                   ConsistencyLevel.Session);

        // List users.
        List<User> users = client.readUsers(databaseForTest.getSelfLink(), null).getQueryIterable().toList();
        int beforeCreateCount = users.size();
        // Create user.
        User user = client.createUser(databaseForTest.getSelfLink(),
                                      new User("{ 'id': 'new user' }"),
                                      null).getResource();
        Assert.assertEquals("new user", user.getId());
        // List users after creation.
        users = client.readUsers(databaseForTest.getSelfLink(), null).getQueryIterable().toList();
        Assert.assertEquals(beforeCreateCount + 1, users.size());
        // Query users.
        users = client.queryUsers(databaseForTest.getSelfLink(),
                                  "SELECT * FROM root r WHERE r.id='new user'",
                                  null).getQueryIterable().toList();
        Assert.assertEquals(1, users.size());

        // Replace user.
        user.setId("replaced user");
        User replacedUser = client.replaceUser(user, null).getResource();
        Assert.assertEquals("replaced user", replacedUser.getId());

        Assert.assertEquals(user.getId(), replacedUser.getId());
        // Read user.
        user = client.readUser(replacedUser.getSelfLink(), null).getResource();
        Assert.assertEquals(replacedUser.getId(), user.getId());
        // Delete user.
        client.deleteUser(user.getSelfLink(), null);
        // Read user after deletion.
        try {
            client.readUser(user.getSelfLink(), null);
            Assert.fail("Exception didn't happen.");
        } catch (DocumentClientException e) {
            Assert.assertEquals(404, e.getStatusCode());
            Assert.assertEquals("NotFound", e.getError().getCode());
        }
    }

    @Test
    public void testPermissionCrud() throws DocumentClientException {
        DocumentClient client = new DocumentClient(HOST,
                                                   MASTER_KEY,
                                                   ConnectionPolicy.GetDefault(),
                                                   ConsistencyLevel.Session);

        // Create user.
        User user = client.createUser(databaseForTest.getSelfLink(),
                                      new User("{ 'id': 'new user' }"),
                                      null).getResource();
        // List permissions.
        List<Permission> permissions = client.readPermissions(user.getSelfLink(), null).getQueryIterable().toList();
        int beforeCreateCount = permissions.size();
        Permission permissionDefinition = new Permission();
        permissionDefinition.setId("new permission");
        permissionDefinition.setPermissionMode(PermissionMode.Read);
        permissionDefinition.setResourceLink("dbs/AQAAAA==/colls/AQAAAJ0fgTc=");

        // Create permission.
        Permission permission = client.createPermission(user.getSelfLink(), permissionDefinition, null).getResource();
        Assert.assertEquals("new permission", permission.getId());

        // List permissions after creation.
        permissions = client.readPermissions(user.getSelfLink(), null).getQueryIterable().toList();
        Assert.assertEquals(beforeCreateCount + 1, permissions.size());

        // Query permissions.
        permissions = client.queryPermissions(user.getSelfLink(),
                                              String.format("SELECT * FROM root r WHERE r.id='%s'",
                                                            permission.getId()),
                                                            null).getQueryIterable().toList();
        Assert.assertEquals(1, permissions.size());

        // Replace permission.
        permission.setId("replaced permission");
        Permission replacedPermission = client.replacePermission(
            permission, null).getResource();
        Assert.assertEquals("replaced permission", replacedPermission.getId());
        Assert.assertEquals(permission.getId(), replacedPermission.getId());

        // Read permission.
        permission = client.readPermission(replacedPermission.getSelfLink(), null).getResource();
        Assert.assertEquals(replacedPermission.getId(), permission.getId());
        // Delete permission.
        client.deletePermission(replacedPermission.getSelfLink(), null);
        // Read permission after deletion.
        try {
            client.readPermission(permission.getSelfLink(), null);
            Assert.fail("Exception didn't happen.");
        } catch (DocumentClientException e) {
            Assert.assertEquals(404, e.getStatusCode());
            Assert.assertEquals("NotFound", e.getError().getCode());
        }
    }

    @Test
    public void testDatabaseAccount() throws DocumentClientException {
        DocumentClient client = new DocumentClient(HOST,
                                                   MASTER_KEY,
                                                   ConnectionPolicy.GetDefault(),
                                                   ConsistencyLevel.Session);

        DatabaseAccount dba = client.getDatabaseAccount();
        Assert.assertNotNull("dba Address link works", dba.getAddressesLink());
        Assert.assertTrue("provision storage must larger than 10000MB",
                          dba.getProvisionedDocumentStorageInMB() > 10000);

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

    private static String getUID() {
        UUID u = UUID.randomUUID();
        return ("" + u.getMostSignificantBits()) + Math.abs(u.getLeastSignificantBits());
    }
}
