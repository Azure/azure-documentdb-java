package com.microsoft.azure.documentdb;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.microsoft.azure.documentdb.internal.DocumentServiceRequest;
import com.microsoft.azure.documentdb.internal.GatewayProxy;

import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JavaRetryThrottleTests {
    private static final String MASTER_KEY = "somekey";
    private static final String HOST = "https://localhost:443";

    private static DocumentClientException createThrottledException() {
        String errorBody = "{'code':'429'," + " 'message':'Message: {\"Errors\":[\"Request rate is large\"]}'}";

        Error errorResource = new Error(errorBody);
        Map<String, String> responseHeaders = new HashMap<String, String>();
        responseHeaders.put("x-ms-retry-after-ms", "100");

        return new DocumentClientException("resource_link", 429, errorResource, responseHeaders);
    }

    private static DocumentClientException createNotFoundException() {
        String errorBody = "{'code':'NotFound'," + " 'message':'Message: {\"Errors\":[\"Resource not found.\"]}'}";

        Error errorResource = new Error(errorBody);
        Map<String, String> responseHeaders = new HashMap<String, String>();
        responseHeaders.put("x-ms-retry-after-ms", "100");

        return new DocumentClientException("resource_link", 404, errorResource, responseHeaders);
    }

    @Test
    public void testDefaultRetryForCrud() throws DocumentClientException {
        DocumentClientException exception = createThrottledException();
        GatewayProxy proxy = mock(GatewayProxy.class);
        when(proxy.processMessage((DocumentServiceRequest) anyObject())).thenThrow(exception);

        DocumentClient client = new DocumentClient(HOST, MASTER_KEY, new ConnectionPolicy(),
                ConsistencyLevel.Session);
        client.setGatewayProxyOverride(proxy);
        int expectRetryTimes = 10;
        // Test create
        Database databaseDefinition = new Database();
        databaseDefinition.setId(GatewayTestBase.databaseForTestId);

        boolean throttled = false;
        try {
            client.createDatabase(databaseDefinition, null).getResource();
        } catch (DocumentClientException e) {
            Assert.assertEquals(429, e.getStatusCode());
            throttled = true;
        }

        // Default retry count is 3, we should call the doCreate method 4 times.
        verify(proxy, times(expectRetryTimes)).processMessage((DocumentServiceRequest) anyObject());
        Assert.assertTrue(throttled);

        // Test read
        throttled = false;
        try {
            client.readCollection(TestUtils.getDocumentCollectionNameLink("test_db", "test_collection"), null).getResource();
        } catch (DocumentClientException e) {
            Assert.assertEquals(429, e.getStatusCode());
            throttled = true;
        }

        verify(proxy, times(expectRetryTimes * 2)).processMessage((DocumentServiceRequest) anyObject());
        Assert.assertTrue(throttled);

        // Test delete
        throttled = false;
        try {
            client.deleteDocument(TestUtils.getDocumentNameLink("test_db", "test_collection", "test_doc"), null).getResource();
        } catch (IllegalStateException e) {
            Assert.assertEquals(DocumentClientException.class, e.getCause().getCause().getCause().getClass());
            Assert.assertEquals(429, ((DocumentClientException)e.getCause().getCause().getCause()).getStatusCode());
            throttled = true;
        }

        verify(proxy, times(expectRetryTimes * 3)).processMessage((DocumentServiceRequest) anyObject());
        Assert.assertTrue(throttled);

        // Test replace
        throttled = false;
        DocumentCollection collection = new DocumentCollection();
        collection.setId("test_coll");
        collection.setSelfLink(TestUtils.getDocumentCollectionNameLink("test_db", "test_coll"));
        try {
            client.replaceCollection(collection, null).getResource();
        } catch (DocumentClientException e) {
            Assert.assertEquals(429, e.getStatusCode());
            throttled = true;
        }

        verify(proxy, times(expectRetryTimes * 4)).processMessage((DocumentServiceRequest) anyObject());
        Assert.assertTrue(throttled);

        // Test upsert.
        throttled = false;
        try {
            client.upsertAttachment(
                    TestUtils.getDocumentNameLink("test_db", "test_collection", "test_doc"),
                    new Attachment(),
                    new RequestOptions()).getResource();
        } catch (IllegalStateException e) {
            Assert.assertEquals(DocumentClientException.class, e.getCause().getCause().getCause().getClass());
            Assert.assertEquals(429, ((DocumentClientException)e.getCause().getCause().getCause()).getStatusCode());
            throttled = true;
        }

        verify(proxy, times(expectRetryTimes * 5)).processMessage((DocumentServiceRequest) anyObject());
        Assert.assertTrue(throttled);
    }

    @Test
    public void testDefaultRetryForQuery() throws DocumentClientException {
        DocumentClientException exception = createThrottledException();
        GatewayProxy proxy = mock(GatewayProxy.class);
        when(proxy.processMessage((DocumentServiceRequest) anyObject())).thenThrow(exception);
        DocumentClient client = new DocumentClient(HOST, MASTER_KEY, new ConnectionPolicy(),
                ConsistencyLevel.Session);
        client.setGatewayProxyOverride(proxy);

        int expectedRetryTimes = 10;
        boolean throttled = false;
        try {
            client.readDatabases(new FeedOptions()).getQueryIterable().toList();
        } catch (IllegalStateException e) {
            DocumentClientException innerExp = (DocumentClientException) e.getCause();
            Assert.assertEquals(429, innerExp.getStatusCode());
            throttled = true;
        }

        verify(proxy, times(expectedRetryTimes)).processMessage((DocumentServiceRequest) anyObject());
        Assert.assertTrue(throttled);

        throttled = false;
        try {
            client.queryCollections(
                    TestUtils.getDocumentCollectionNameLink("test_db", "test_coll"),
                    "SELECT * fromc", null).getQueryIterable().toList();
        } catch (IllegalStateException e) {
            DocumentClientException innerExp = (DocumentClientException) e.getCause();
            Assert.assertEquals(429, innerExp.getStatusCode());
            throttled = true;
        }

        verify(proxy, times(expectedRetryTimes * 2)).processMessage((DocumentServiceRequest) anyObject());
        Assert.assertTrue(throttled);
    }

    @Test
    public void testNoRetryOnNonThrottledException() throws DocumentClientException {
        DocumentClientException exception = createNotFoundException();
        GatewayProxy proxy = mock(GatewayProxy.class);
        when(proxy.processMessage((DocumentServiceRequest) anyObject())).thenThrow(exception);

        DocumentClient client = new DocumentClient(HOST, MASTER_KEY, new ConnectionPolicy(),
                ConsistencyLevel.Session);
        client.setGatewayProxyOverride(proxy);

        boolean failed = false;
        Database databaseDefinition = new Database();
        databaseDefinition.setId("test_db");
        try {
            client.createDatabase(databaseDefinition, null).getResource();
        } catch (DocumentClientException e) {
            Assert.assertEquals(404, e.getStatusCode());
            failed = true;
        }

        verify(proxy, times(1)).processMessage((DocumentServiceRequest) anyObject());
        Assert.assertTrue(failed);

        failed = false;
        try {
            client.queryCollections(TestUtils.getDatabaseNameLink("test_db"), "SELECT * from c", null).getQueryIterable().toList();
        } catch (IllegalStateException e) {
            DocumentClientException innerExp = (DocumentClientException) e.getCause();
            Assert.assertEquals(404, innerExp.getStatusCode());
            failed = true;
        }

        verify(proxy, times(2)).processMessage((DocumentServiceRequest) anyObject());
        Assert.assertTrue(failed);
    }

    @Test
    public void testRetryPolicyOverrideNoRetry() throws DocumentClientException {
        DocumentClientException exception = createThrottledException();
        GatewayProxy proxy = mock(GatewayProxy.class);
        when(proxy.processMessage((DocumentServiceRequest) anyObject())).thenThrow(exception);

        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        connectionPolicy.getRetryOptions().setMaxRetryAttemptsOnThrottledRequests(0);
        DocumentClient client = new DocumentClient(HOST, MASTER_KEY, connectionPolicy, ConsistencyLevel.Session);
        client.setGatewayProxyOverride(proxy);

        boolean failed = false;
        Database database = new Database();
        database.setId("test_db");
        try {
            client.createDatabase(database, null).getResource();
        } catch (DocumentClientException e) {
            Assert.assertEquals(429, e.getStatusCode());
            failed = true;
        }

        verify(proxy, times(1)).processMessage((DocumentServiceRequest) anyObject());
        Assert.assertTrue(failed);

        failed = false;
        try {
            client.readDatabases(new FeedOptions()).getQueryIterable().toList();
        } catch (IllegalStateException e) {
            DocumentClientException innerExp = (DocumentClientException) e.getCause();
            Assert.assertEquals(429, innerExp.getStatusCode());
            failed = true;
        }

        verify(proxy, times(2)).processMessage((DocumentServiceRequest) anyObject());
        Assert.assertTrue(failed);

        failed = false;
        try {
            client.queryCollections(TestUtils.getDatabaseNameLink("test_db"), "SELECT * fromc", null).getQueryIterable().toList();
        } catch (IllegalStateException e) {
            DocumentClientException innerExp = (DocumentClientException) e.getCause();
            Assert.assertEquals(429, innerExp.getStatusCode());
            failed = true;
        }

        verify(proxy, times(3)).processMessage((DocumentServiceRequest) anyObject());
        Assert.assertTrue(failed);
    }

    @Test
    public void testRetryPolicyOverrideNRetries() throws DocumentClientException {
        DocumentClientException exception = createThrottledException();
        GatewayProxy proxy = mock(GatewayProxy.class);
        when(proxy.processMessage((DocumentServiceRequest) anyObject())).thenThrow(exception);

        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        connectionPolicy.getRetryOptions().setMaxRetryAttemptsOnThrottledRequests(1);
        DocumentClient client = new DocumentClient(HOST, MASTER_KEY, connectionPolicy, ConsistencyLevel.Session);
        client.setGatewayProxyOverride(proxy);

        int expectedRetryCount = 2;
        boolean failed = false;
        Database database = new Database();
        database.setId("test_db");
        try {
            client.createDatabase(database, null).getResource();
        } catch (DocumentClientException e) {
            Assert.assertEquals(429, e.getStatusCode());
            failed = true;
        }

        verify(proxy, times(expectedRetryCount)).processMessage((DocumentServiceRequest) anyObject());
        Assert.assertTrue(failed);

        failed = false;
        try {
            client.queryCollections(TestUtils.getDatabaseNameLink("test_db"), "SELECT * fromc", null).getQueryIterable().toList();
        } catch (IllegalStateException e) {
            DocumentClientException innerExp = (DocumentClientException) e.getCause();
            Assert.assertEquals(429, innerExp.getStatusCode());
            failed = true;
        }

        verify(proxy, times(expectedRetryCount * 2)).processMessage((DocumentServiceRequest) anyObject());
        Assert.assertTrue(failed);
    }

    @Test
    public void testRetryMaxTimeLimit() throws DocumentClientException {
        DocumentClientException exception = createThrottledException();
        GatewayProxy proxy = mock(GatewayProxy.class);
        when(proxy.processMessage((DocumentServiceRequest) anyObject())).thenThrow(exception);

        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        connectionPolicy.getRetryOptions().setMaxRetryAttemptsOnThrottledRequests(100);
        connectionPolicy.getRetryOptions().setMaxRetryWaitTimeInSeconds(1);
        DocumentClient client = new DocumentClient(HOST, MASTER_KEY, connectionPolicy, ConsistencyLevel.Session);
        client.setGatewayProxyOverride(proxy);

        int expectedRetryCount = 10;
        boolean failed = false;
        Database database = new Database();
        database.setId("test_db");
        try {
            client.createDatabase(database, null).getResource();
        } catch (DocumentClientException e) {
            Assert.assertEquals(429, e.getStatusCode());
            failed = true;
        }

        verify(proxy, times(expectedRetryCount)).processMessage((DocumentServiceRequest) anyObject());
        Assert.assertTrue(failed);

        failed = false;
        try {
            client.queryCollections(TestUtils.getDatabaseNameLink("test_db"), "SELECT * fromc", null).getQueryIterable().toList();
        } catch (IllegalStateException e) {
            DocumentClientException innerExp = (DocumentClientException) e.getCause();
            Assert.assertEquals(429, innerExp.getStatusCode());
            failed = true;
        }

        verify(proxy, times(expectedRetryCount * 2)).processMessage((DocumentServiceRequest) anyObject());
        Assert.assertTrue(failed);
    }
}
