package com.microsoft.azure.documentdb;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * End-to-end test cases for globally distributed databases.
 * 
 * IMPORTANT NOTES:
 * 
 * Most test cases in this project create collections in your DocumentDB
 * account. Collections are billing entities. By running these test cases, you
 * may incur monetary costs on your account.
 * 
 * To Run the test, replace the member fields (MASTER_KEY & HOST & HOST_XXX)
 * with values associated with your DocumentDB account.
 */

public class JavaGlobalDbEndToEndTests extends GatewayTestBase {
    private static final String databaseForTestId = "testdb";
    private static final String collectionForTestId = "testcoll";

    private static final String MASTER_KEY = "[REPLACE WITH YOUR APP MASTER KEY]";
    private static final String HOST = "[REPLACE WITH YOUR APP ENDPOINT, FOR EXAMPLE 'https://myapp.documents.azure.com:443']";
    private static final String HOST_WRITEREGION = "[REPLACE WITH YOUR APP'S WRITE REGION ENDPOINT, FOR EXAMPLE 'https://myapp-westus.documents.azure.com:443']";
    private static final String HOST_READREGION = "[REPLACE WITH YOUR APP'S READ REGION ENDPOINT, FOR EXAMPLE 'https://myapp-eastus.documents.azure.com:443']";
    private static final String HOST_READRGION_NAME = "[REPLACE WITH YOUR APP'S READ REGION Name, FOR EXAMPLE 'East Us']";

    private Database databaseForTest;

    @Before
    @Override
    public void setUp() throws DocumentClientException {
        this.prepareAndCleanupCollection(JavaGlobalDbEndToEndTests.databaseForTestId,
                JavaGlobalDbEndToEndTests.collectionForTestId);
    }

    @After
    @Override
    public void tearDown() throws DocumentClientException {
        // Do nothing.
    }

    /**
     * Test the scenario where write happens in the write region and read
     * happens in the read region. Set EnableEndpointDiscovery to false disables
     * auto discovery and forces read to happen in the read region only.
     */
    @Test
    public void testWriteReadRegionReplication() throws DocumentClientException {
        // Add a document using default client.
        String documentId = GatewayTests.getUID();
        Document documentDefinition = new Document(String
                .format("{" + " 'id': '%s'," + " 'name': 'sample document'," + " 'key': 'value'" + "}", documentId));

        ResourceResponse<Document> createResponse = this.client.createDocument(this.collectionForTest.getSelfLink(),
                documentDefinition, null, false);
        Assert.assertNotNull(createResponse);

        Document createdDocument = createResponse.getResource();
        Assert.assertEquals(documentId, createdDocument.getId());

        // Read the newly created document from the read region
        // Set EnableEndpointDiscovery to false to force read from the read
        // region.
        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        connectionPolicy.setEnableEndpointDiscovery(false);
        DocumentClient readClient = new DocumentClient(HOST_READREGION, MASTER_KEY, connectionPolicy,
                ConsistencyLevel.Session);
        Assert.assertEquals(HOST_READREGION,
                readClient.getGlobalEndpointManager().getReadEndpoint().toString().toLowerCase());

        Document readDocument = readClient.readDocument(createdDocument.getSelfLink(), null).getResource();
        Assert.assertEquals(documentId, readDocument.getId());
    }

    /**
     * Test the scenario where the SDK will automatically find the region to
     * write from if the default endpoint is not writable. Setting
     * EnableEndpointDiscovery to true (default value) enables auto-discover of
     * available write and read regions.
     */
    @Test
    public void testWriteFromReadRegion() throws DocumentClientException {
        // Insert a document should succeed when using a client whose default
        // region is readonly. SDK should automatically find the write region to
        // send the request to.
        DocumentClient readClient = new DocumentClient(HOST_READREGION, MASTER_KEY, new ConnectionPolicy(),
                ConsistencyLevel.Session);
        Assert.assertEquals(HOST_WRITEREGION,
                readClient.getGlobalEndpointManager().getWriteEndpoint().toString().toLowerCase());

        String documentId = GatewayTests.getUID();
        Document documentDefinition = new Document(String
                .format("{" + " 'id': '%s'," + "  'name': 'sample document'," + "  'key': 'value'" + "}", documentId));

        Document createdDocument = readClient
                .createDocument(this.collectionForTest.getSelfLink(), documentDefinition, null, false).getResource();
        Assert.assertEquals(documentId, createdDocument.getId());

        Document readDocument = readClient.readDocument(createdDocument.getSelfLink(), null).getResource();
        Assert.assertEquals(documentId, readDocument.getId());
    }

    /**
     * Test the scenario where EnableEndpointDiscovery is false and a write
     * operation is directed at a read region. The operation should fail with a
     * Forbidden status.
     */
    @Test
    public void testWriteFromReadRegionForbidden() throws DocumentClientException {
        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        connectionPolicy.setEnableEndpointDiscovery(false);
        DocumentClient readClient = new DocumentClient(HOST_READREGION, MASTER_KEY, connectionPolicy,
                ConsistencyLevel.Session);

        Database databaseDefinition = new Database();
        databaseDefinition.setId("TestDb");

        try {
            readClient.createDatabase(databaseDefinition, null);
        } catch (DocumentClientException e) {
            Assert.assertEquals(403, e.getStatusCode());
            Assert.assertEquals((Integer) 3, e.getSubStatusCode());
        }
    }

    @Test
    /**
     * This test the scenario where the DocumentClient is created using the
     * global endpoint and a PreferredReadRegions list is specified. In this
     * case, all writes should go to the write region and all reads should go to
     * the read region.
     */
    public void testGlobalEndpointWithPreferredReadRegion() throws DocumentClientException {
        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        Collection<String> preferredReadRegion = new ArrayList<String>(
                Arrays.asList(new String[] { HOST_READRGION_NAME }));
        connectionPolicy.setPreferredLocations(preferredReadRegion);
        DocumentClient documentClient = new DocumentClient(HOST, MASTER_KEY, connectionPolicy,
                ConsistencyLevel.Session);
        Assert.assertEquals(HOST_WRITEREGION,
                documentClient.getGlobalEndpointManager().getWriteEndpoint().toString().toLowerCase());
        Assert.assertEquals(HOST_READREGION,
                documentClient.getGlobalEndpointManager().getReadEndpoint().toString().toLowerCase());

        String documentId = GatewayTests.getUID();
        Document documentDefinition = new Document(String
                .format("{" + " 'id': '%s'," + "  'name': 'sample document'," + "  'key': 'value'" + "}", documentId));

        Document createdDocument = documentClient
                .createDocument(this.collectionForTest.getSelfLink(), documentDefinition, null, false).getResource();
        Assert.assertEquals(documentId, createdDocument.getId());

        Document readDocument = documentClient.readDocument(createdDocument.getSelfLink(), null).getResource();
        Assert.assertEquals(documentId, readDocument.getId());
    }

    /**
     * Test the scenario where write happens in the write region and read
     * happens in the read region. Set EnableEndpointDiscovery to false disables
     * auto discovery and forces read to happen in the read region only.
     */
    @Test
    public void testReadWithSessionConsistency() throws DocumentClientException {
        String documentId = GatewayTests.getUID();
        Document documentDefinition = new Document(String
                .format("{" + " 'id': '%s'," + " 'name': 'sample document'," + " 'key': 'value'" + "}", documentId));

        ResourceResponse<Document> createResponse = this.client.createDocument(this.collectionForTest.getSelfLink(),
                documentDefinition, null, false);
        Assert.assertNotNull(createResponse);

        Document createdDocument = createResponse.getResource();
        Assert.assertEquals(documentId, createdDocument.getId());

        Document readDocument = this.client.readDocument(createdDocument.getSelfLink(), null).getResource();
        Assert.assertEquals(documentId, readDocument.getId());
    }

    @Test
    public void testReadMediaFromWriteRegion() throws DocumentClientException {
        // Create document.
        String documentId = GatewayTestBase.getUID();
        Document documentDefinition = new Document(String.format(
                "{" +
                        "  'id': '%s'," +
                        "  'foo': 'bar'," +
                        "  'key': 'value'" +
                "}", documentId));
        
        Document document = this.client.createDocument(
                getDocumentCollectionLink(this.databaseForTest, this.collectionForTest, true),
                documentDefinition,
                null,
                false).getResource();

        MediaOptions validMediaOptions = new MediaOptions();
        validMediaOptions.setSlug("attachment id");
        validMediaOptions.setContentType("application/text");
        MediaOptions invalidMediaOptions = new MediaOptions();
        invalidMediaOptions.setSlug("attachment id");
        invalidMediaOptions.setContentType("junt/test");
        
        ReadableStream mediaStream = new ReadableStream("stream content.");
        Attachment validAttachment = client.createAttachment(getDocumentLink(this.databaseForTest, this.collectionForTest, document, true),
                mediaStream,
                validMediaOptions).getResource();
        Assert.assertEquals("attachment id", validAttachment.getId());

        // Read attachment media.
        InputStream mediaResponse = client.readMedia(validAttachment.getMediaLink()).getMedia();
        Assert.assertEquals("stream content.", GatewayTestBase.getStringFromInputStream(mediaResponse));
    }
    
    private void prepareAndCleanupCollection(String databaseId, String collectionId) throws DocumentClientException {
        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        Collection<String> preferredReadRegion = new ArrayList<String>(
                Arrays.asList(new String[] { HOST_READRGION_NAME }));
        connectionPolicy.setPreferredLocations(preferredReadRegion);
        this.client = new DocumentClient(HOST, MASTER_KEY, connectionPolicy, ConsistencyLevel.Session);
        
        // Verify the write and read endpoints are set up correctly.
        Assert.assertEquals(HOST_WRITEREGION, this.client.getWriteEndpoint().toString().toLowerCase());
        Assert.assertEquals(HOST_READREGION, this.client.getReadEndpoint().toString().toLowerCase());
        
        this.databaseForTest = this.client
                .queryDatabases(new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
                        new SqlParameterCollection(new SqlParameter("@id", databaseId))), null)
                .getQueryIterable().iterator().next();

        if (this.databaseForTest == null) {
            Database database = new Database();
            database.setId(databaseId);
            this.databaseForTest = this.client.createDatabase(database, null).getResource();

            // Wait for database to replicate
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }

        this.collectionForTest = this.client
                .queryCollections(this.databaseForTest.getSelfLink(),
                        new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
                                new SqlParameterCollection(new SqlParameter("@id", collectionId))),
                        null)
                .getQueryIterable().iterator().next();

        if (this.collectionForTest == null) {
            RequestOptions options = new RequestOptions();
            options.setOfferThroughput(1000);
            DocumentCollection collection = new DocumentCollection();
            collection.setId(collectionId);
            this.collectionForTest = this.client
                    .createCollection(getDatabaseLink(this.databaseForTest, true), collection, options).getResource();

            // Wait for collection to replicate
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }

        FeedResponse<Document> documents = this.client.queryDocuments(this.collectionForTest.getSelfLink(),
                new SqlQuerySpec("SELECT * FROM root r", new SqlParameterCollection()), null);

        for (Document document : documents.getQueryIterable()) {
            this.client.deleteDocument(document.getSelfLink(), null);
        }
    }
}
