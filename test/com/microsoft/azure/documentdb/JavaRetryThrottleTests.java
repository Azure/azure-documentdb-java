package com.microsoft.azure.documentdb;

import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Map;

public class JavaRetryThrottleTests {
    private static final String MASTER_KEY = "somekey";
    private static final String HOST = "https://localhost:443";

    @Test
    public void testDefaultRetryForCrud() throws DocumentClientException {
        DocumentClientException exception = createThrottledException();
        GatewayProxy proxy = mock(GatewayProxy.class);
        when(proxy.doCreate((DocumentServiceRequest) anyObject())).thenThrow(exception);
        when(proxy.doRead((DocumentServiceRequest) anyObject())).thenThrow(exception);
        when(proxy.doDelete((DocumentServiceRequest) anyObject())).thenThrow(exception);
        when(proxy.doReplace((DocumentServiceRequest) anyObject())).thenThrow(exception);
        when(proxy.doUpsert((DocumentServiceRequest) anyObject())).thenThrow(exception);
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
        verify(proxy, times(expectRetryTimes)).doCreate((DocumentServiceRequest) anyObject());
        Assert.assertTrue(throttled);
        
        // Test read
        throttled = false;
        try {
            client.readCollection("collection_link", null).getResource();
        } catch (DocumentClientException e) {
            Assert.assertEquals(429, e.getStatusCode());
            throttled = true;
        }
        
        verify(proxy, times(expectRetryTimes)).doRead((DocumentServiceRequest) anyObject());
        Assert.assertTrue(throttled);
        
        // Test delete
        throttled = false;
        try {
            client.deleteDocument("document_link", null).getResource();
        } catch (DocumentClientException e) {
            Assert.assertEquals(429, e.getStatusCode());
            throttled = true;
        }

        verify(proxy, times(expectRetryTimes)).doDelete((DocumentServiceRequest) anyObject());
        Assert.assertTrue(throttled);
        
        // Test replace
        throttled = false;
        Document document = new Document();
        document.setId("document1");
        try {
            client.replaceDocument("document_link", document, null).getResource();
        } catch (DocumentClientException e) {
            Assert.assertEquals(429, e.getStatusCode());
            throttled = true;
        }

        verify(proxy, times(expectRetryTimes)).doReplace((DocumentServiceRequest) anyObject());
        Assert.assertTrue(throttled);
        
        // Test upsert.
        throttled = false;
        try {
            client.upsertAttachment("document_link", new Attachment(), new RequestOptions()).getResource();
        } catch (DocumentClientException e) {
            Assert.assertEquals(429, e.getStatusCode());
            throttled = true;
        }

        verify(proxy, times(expectRetryTimes)).doUpsert((DocumentServiceRequest) anyObject());
        Assert.assertTrue(throttled);
    }

    @Test
    public void testDefaultRetryForQuery() throws DocumentClientException {
        DocumentClientException exception = createThrottledException();
        GatewayProxy proxy = mock(GatewayProxy.class);
        when(proxy.doReadFeed((DocumentServiceRequest) anyObject())).thenThrow(exception);
        when(proxy.doSQLQuery((DocumentServiceRequest) anyObject())).thenThrow(exception);
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
        
        verify(proxy, times(expectedRetryTimes)).doReadFeed((DocumentServiceRequest) anyObject());
        Assert.assertTrue(throttled);

        throttled = false;
        try {
            client.queryCollections("database_link", "SELECT * fromc", null).getQueryIterable().toList();
        } catch (IllegalStateException e) {
            DocumentClientException innerExp = (DocumentClientException) e.getCause();
            Assert.assertEquals(429, innerExp.getStatusCode());
            throttled = true;
        }
        
        verify(proxy, times(expectedRetryTimes)).doSQLQuery((DocumentServiceRequest) anyObject());
        Assert.assertTrue(throttled);
    }

    @Test
    public void testNoRetryOnNonThrottledException() throws DocumentClientException {
        DocumentClientException exception = createNotFoundException();
        GatewayProxy proxy = mock(GatewayProxy.class);
        when(proxy.doCreate((DocumentServiceRequest) anyObject())).thenThrow(exception);
        when(proxy.doSQLQuery((DocumentServiceRequest) anyObject())).thenThrow(exception);
        DocumentClient client = new DocumentClient(HOST, MASTER_KEY, new ConnectionPolicy(),
                ConsistencyLevel.Session);
        client.setGatewayProxyOverride(proxy);

        boolean failed = false;
        Database databaseDefinition = new Database();
        databaseDefinition.setId(GatewayTestBase.databaseForTestId);
        try {
            client.createDatabase(databaseDefinition, null).getResource();
        } catch (DocumentClientException e) {
            Assert.assertEquals(404, e.getStatusCode());
            failed = true;
        }
        
        verify(proxy, times(1)).doCreate((DocumentServiceRequest) anyObject());
        Assert.assertTrue(failed);

        failed = false;
        try {
            client.queryCollections("database_link", "SELECT * from c", null).getQueryIterable().toList();
        } catch (IllegalStateException e) {
            DocumentClientException innerExp = (DocumentClientException) e.getCause();
            Assert.assertEquals(404, innerExp.getStatusCode());
            failed = true;
        }
        
        verify(proxy, times(1)).doSQLQuery((DocumentServiceRequest) anyObject());
        Assert.assertTrue(failed);
    }

    @Test
    public void testRetryPolicyOverrideNoRetry() throws DocumentClientException {
        DocumentClientException exception = createThrottledException();
        GatewayProxy proxy = mock(GatewayProxy.class);
        when(proxy.doCreate((DocumentServiceRequest) anyObject())).thenThrow(exception);
        when(proxy.doReadFeed((DocumentServiceRequest) anyObject())).thenThrow(exception);
        when(proxy.doSQLQuery((DocumentServiceRequest) anyObject())).thenThrow(exception);

        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        connectionPolicy.getRetryOptions().setMaxRetryAttemptsOnThrottledRequests(0);
        DocumentClient client = new DocumentClient(HOST, MASTER_KEY, connectionPolicy, ConsistencyLevel.Session);
        client.setGatewayProxyOverride(proxy);

        boolean failed = false;
        Database database = new Database();
        database.setId("database1");
        try {
            client.createDatabase(database, null).getResource();
        } catch (DocumentClientException e) {
            Assert.assertEquals(429, e.getStatusCode());
            failed = true;
        }

        verify(proxy, times(1)).doCreate((DocumentServiceRequest) anyObject());
        Assert.assertTrue(failed);
        
        failed = false; 
        try {
            client.readDatabases(new FeedOptions()).getQueryIterable().toList();
        } catch (IllegalStateException e) {
            DocumentClientException innerExp = (DocumentClientException) e.getCause();
            Assert.assertEquals(429, innerExp.getStatusCode());
            failed = true;
        }

        verify(proxy, times(1)).doReadFeed((DocumentServiceRequest) anyObject());
        Assert.assertTrue(failed);

        failed = false;
        try {
            client.queryCollections("database_link", "SELECT * fromc", null).getQueryIterable().toList();
        } catch (IllegalStateException e) {
            DocumentClientException innerExp = (DocumentClientException) e.getCause();
            Assert.assertEquals(429, innerExp.getStatusCode());
            failed = true;
        }
        
        verify(proxy, times(1)).doSQLQuery((DocumentServiceRequest) anyObject());
        Assert.assertTrue(failed);
    }

    @Test
    public void testRetryPolicyOverrideNRetries() throws DocumentClientException {
        DocumentClientException exception = createThrottledException();
        GatewayProxy proxy = mock(GatewayProxy.class);
        when(proxy.doCreate((DocumentServiceRequest) anyObject())).thenThrow(exception);
        when(proxy.doSQLQuery((DocumentServiceRequest) anyObject())).thenThrow(exception);

        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        connectionPolicy.getRetryOptions().setMaxRetryAttemptsOnThrottledRequests(1);
        DocumentClient client = new DocumentClient(HOST, MASTER_KEY, connectionPolicy, ConsistencyLevel.Session);
        client.setGatewayProxyOverride(proxy);

        boolean failed = false;
        Database database = new Database();
        database.setId("database1");
        try {
            client.createDatabase(database, null).getResource();
        } catch (DocumentClientException e) {
            Assert.assertEquals(429, e.getStatusCode());
            failed = true;
        }

        verify(proxy, times(2)).doCreate((DocumentServiceRequest) anyObject());
        Assert.assertTrue(failed);

        failed = false;
        try {
            client.queryCollections("database_link", "SELECT * fromc", null).getQueryIterable().toList();
        } catch (IllegalStateException e) {
            DocumentClientException innerExp = (DocumentClientException) e.getCause();
            Assert.assertEquals(429, innerExp.getStatusCode());
            failed = true;
        }
        
        verify(proxy, times(2)).doSQLQuery((DocumentServiceRequest) anyObject());
        Assert.assertTrue(failed);
    }
    
    @Test
    public void testRetryMaxTimeLimit() throws DocumentClientException {
        DocumentClientException exception = createThrottledException();
        GatewayProxy proxy = mock(GatewayProxy.class);
        when(proxy.doCreate((DocumentServiceRequest) anyObject())).thenThrow(exception);
        when(proxy.doSQLQuery((DocumentServiceRequest) anyObject())).thenThrow(exception);

        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        connectionPolicy.getRetryOptions().setMaxRetryAttemptsOnThrottledRequests(100);
        connectionPolicy.getRetryOptions().setMaxRetryWaitTimeInSeconds(1);
        DocumentClient client = new DocumentClient(HOST, MASTER_KEY, connectionPolicy, ConsistencyLevel.Session);
        client.setGatewayProxyOverride(proxy);

        boolean failed = false;
        Database database = new Database();
        database.setId("database1");
        try {
            client.createDatabase(database, null).getResource();
        } catch (DocumentClientException e) {
            Assert.assertEquals(429, e.getStatusCode());
            failed = true;
        }

        verify(proxy, times(10)).doCreate((DocumentServiceRequest) anyObject());
        Assert.assertTrue(failed);

        failed = false;
        try {
            client.queryCollections("database_link", "SELECT * fromc", null).getQueryIterable().toList();
        } catch (IllegalStateException e) {
            DocumentClientException innerExp = (DocumentClientException) e.getCause();
            Assert.assertEquals(429, innerExp.getStatusCode());
            failed = true;
        }
        
        verify(proxy, times(10)).doSQLQuery((DocumentServiceRequest) anyObject());
        Assert.assertTrue(failed);
    }

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
}
