package com.microsoft.azure.documentdb;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.ConnectException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.conn.HttpHostConnectException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class JavaEndpointSelectionTests {
    private static final String DEFAULT_ENDPOINT = "https://geotest.documents.azure.com:443/";
    private static final String MASTER_KEY = "SomeKeyValue";
    private static final String WRITE_ENDPOINT = "https://geotest-WestUS.documents.azure.com:443/";
    private static final String READ_ENDPOINT1 = "https://geotest-SouthCentralUS.documents.azure.com:443/";
    private static final String READ_ENDPOINT2 = "https://geotest-EastUS.documents.azure.com:443/";
    private static final String WRITE_ENDPOINT_NAME = "West US";
    private static final String READ_ENDPOINT_NAME1 = "South Central US";
    private static final String READ_ENDPOINT_NAME2 = "East US";
    private final String[] preferredRegionalEndpoints = { READ_ENDPOINT_NAME1, READ_ENDPOINT_NAME2 };

    @Test
    public void testSelectWriteAndReadRegions() throws DocumentClientException, URISyntaxException {
        DatabaseAccount databaseAccount = createDatabaseAccountObject();

        // Case 1: when ConnectionPolicy.EndpointDiscovery is set to false, we always use the default endpoint
        // for both read and write.
        Collection<String> preferredRegions = new ArrayList<String>(Arrays.asList(preferredRegionalEndpoints));
        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        connectionPolicy.setEnableEndpointDiscovery(false);
        connectionPolicy.setPreferredLocations(preferredRegions);
        DocumentClient documentClient = new DocumentClient(DEFAULT_ENDPOINT, MASTER_KEY, connectionPolicy,
                ConsistencyLevel.Session);
        DocumentClient spyClient = spy(documentClient);
        doReturn(databaseAccount).when(spyClient).getDatabaseAccountFromEndpoint((URI)anyObject());    
        GlobalEndpointManager endpointManager = new GlobalEndpointManager(spyClient);
        Assert.assertEquals(DEFAULT_ENDPOINT, endpointManager.getWriteEndpoint().toString());
        Assert.assertEquals(DEFAULT_ENDPOINT, endpointManager.getReadEndpoint().toString());

        // Case 2: when ConnectionPolicy.EndpointDiscovery is true, and when no preferred region is specified, 
        // we auto select the write region for reads.
        documentClient = new DocumentClient(DEFAULT_ENDPOINT, MASTER_KEY, new ConnectionPolicy(),
                ConsistencyLevel.Session);
        spyClient = spy(documentClient);
        doReturn(databaseAccount).when(spyClient).getDatabaseAccountFromEndpoint((URI)anyObject());
        endpointManager = new GlobalEndpointManager(spyClient);        
        Assert.assertEquals(WRITE_ENDPOINT, endpointManager.getWriteEndpoint().toString());
        Assert.assertEquals(WRITE_ENDPOINT, endpointManager.getReadEndpoint().toString());

        // Case 3: when ConnectionPolicy.EndpointDiscovery is true, and when preferred region is specified, 
        // we select the first region in the preferred list that is available for reads.
        preferredRegions = new ArrayList<String>(Arrays.asList(preferredRegionalEndpoints));
        connectionPolicy = new ConnectionPolicy();
        connectionPolicy.setPreferredLocations(preferredRegions);
        documentClient = new DocumentClient(DEFAULT_ENDPOINT, MASTER_KEY, connectionPolicy, ConsistencyLevel.Session);
        spyClient = spy(documentClient);
        doReturn(databaseAccount).when(spyClient).getDatabaseAccountFromEndpoint((URI)anyObject());
        
        endpointManager = new GlobalEndpointManager(spyClient);
        Assert.assertEquals(WRITE_ENDPOINT, endpointManager.getWriteEndpoint().toString());
        Assert.assertEquals(READ_ENDPOINT1, endpointManager.getReadEndpoint().toString());

        // Case 3.1: when ConnectionPolicy.EndpointDiscovery is true, and when preferred region is specified, 
        // we select the first region in the preferred list that is available for reads.
        String[] preferred = { WRITE_ENDPOINT };
        preferredRegions = new ArrayList<String>(Arrays.asList(preferred));
        connectionPolicy = new ConnectionPolicy();
        connectionPolicy.setPreferredLocations(preferredRegions);
        documentClient = new DocumentClient(DEFAULT_ENDPOINT, MASTER_KEY, connectionPolicy, ConsistencyLevel.Session);
        spyClient = spy(documentClient);
        doReturn(databaseAccount).when(spyClient).getDatabaseAccountFromEndpoint((URI)anyObject());
        
        endpointManager = new GlobalEndpointManager(spyClient);
        Assert.assertEquals(WRITE_ENDPOINT, endpointManager.getWriteEndpoint().toString());
        Assert.assertEquals(WRITE_ENDPOINT, endpointManager.getReadEndpoint().toString());
        
        // Case 4: when ConnectionPolicy.EndpointDiscovery is true, and when preferred read region is specified, 
        // and the first preferred read region is not available, we select the second preferred region for reads.
        String accountRegionJSONTemplate = "{'name':'%s', 'databaseAccountEndpoint':'%s'}";
        String accountReadRegion2JSON = String.format(accountRegionJSONTemplate, READ_ENDPOINT_NAME2, READ_ENDPOINT2);
        Collection<DatabaseAccountLocation> readableRegions = new ArrayList<DatabaseAccountLocation>();
        readableRegions.add(new DatabaseAccountLocation(accountReadRegion2JSON));
        databaseAccount.setReadableLocations(readableRegions);
        
        preferredRegions = new ArrayList<String>(Arrays.asList(preferredRegionalEndpoints));
        connectionPolicy = new ConnectionPolicy();
        connectionPolicy.setPreferredLocations(preferredRegions);
        documentClient = new DocumentClient(DEFAULT_ENDPOINT, MASTER_KEY, connectionPolicy, ConsistencyLevel.Session);
        spyClient = spy(documentClient);
        doReturn(databaseAccount).when(spyClient).getDatabaseAccountFromEndpoint((URI)anyObject());
        
        endpointManager = new GlobalEndpointManager(spyClient);        
        Assert.assertEquals(WRITE_ENDPOINT, endpointManager.getWriteEndpoint().toString());
        Assert.assertEquals(READ_ENDPOINT2, endpointManager.getReadEndpoint().toString());        
        
        // Case 5: when ConnectionPolicy.EndpointDiscovery is true, and when preferred read region is specified, 
        // and no read region is available, we select the write region for reads.
        readableRegions = new ArrayList<DatabaseAccountLocation>();
        databaseAccount.setReadableLocations(readableRegions);
        
        preferredRegions = new ArrayList<String>(Arrays.asList(preferredRegionalEndpoints));
        connectionPolicy = new ConnectionPolicy();
        documentClient = new DocumentClient(DEFAULT_ENDPOINT, MASTER_KEY, connectionPolicy, ConsistencyLevel.Session);
        spyClient = spy(documentClient);
        doReturn(databaseAccount).when(spyClient).getDatabaseAccountFromEndpoint((URI)anyObject());
        
        endpointManager = new GlobalEndpointManager(spyClient);
        Assert.assertEquals(WRITE_ENDPOINT, endpointManager.getWriteEndpoint().toString());
        Assert.assertEquals(WRITE_ENDPOINT, endpointManager.getReadEndpoint().toString());      
        
        // Case 6: when ConnectionPolicy.EndpointDiscovery is set to true, and the writable regions are not presents
        // for legacy accounts, we select the default endpoint for read and write.
        databaseAccount.setWritableLocations(null);
        documentClient = new DocumentClient(DEFAULT_ENDPOINT, MASTER_KEY, new ConnectionPolicy(),
                ConsistencyLevel.Session);
        spyClient = spy(documentClient);
        doReturn(databaseAccount).when(spyClient).getDatabaseAccountFromEndpoint((URI)anyObject());   
        
        endpointManager = new GlobalEndpointManager(spyClient);
        Assert.assertEquals(DEFAULT_ENDPOINT, endpointManager.getWriteEndpoint().toString());
        Assert.assertEquals(DEFAULT_ENDPOINT, endpointManager.getReadEndpoint().toString());
    }
    
    @Test
    public void testRetryOnWriteRegionForbidden() throws DocumentClientException, URISyntaxException {
        DocumentClientException exception = createWriteForbiddenException();
        GatewayProxy proxy = mock(GatewayProxy.class);
        when(proxy.doCreate((DocumentServiceRequest) anyObject())).thenThrow(exception)
            .thenReturn(createEmptyDocumentServiceResponse());
        
        DocumentClient client = new DocumentClient(DEFAULT_ENDPOINT, MASTER_KEY, new ConnectionPolicy(),
                ConsistencyLevel.Session);
        DatabaseAccount databaseAccount = createDatabaseAccountObject();
        DocumentClient spyClient = spy(client);
        doReturn(databaseAccount).when(spyClient).getDatabaseAccountFromEndpoint((URI)anyObject());
        spyClient.setGatewayProxyOverride(proxy); 
        GlobalEndpointManager endpointManager = new GlobalEndpointManager(spyClient);
        spyClient.setGlobalEndpointManager(endpointManager);
        
        Database databaseDefinition = new Database();
        databaseDefinition.setId("db1");
        spyClient.createDatabase(databaseDefinition, null);
        
        // The first call to getDatabaseAccountFromEndpoint is not captured because we create
        // the spyClient after the first initialize call which happens in the constructor of
        // the DocumentClient class.
        verify(spyClient, times(1)).getDatabaseAccountFromEndpoint((URI)anyObject());
        verify(proxy, times(2)).doCreate((DocumentServiceRequest) anyObject());
    }
    
    @Test
    public void testWriteRegionForbiddenMaxRetry() throws DocumentClientException, URISyntaxException {
        GatewayProxy proxy = mock(GatewayProxy.class);
        when(proxy.doCreate((DocumentServiceRequest) anyObject())).thenThrow(createWriteForbiddenException());
        
        DocumentClient client = new DocumentClient(DEFAULT_ENDPOINT, MASTER_KEY, new ConnectionPolicy(),
                ConsistencyLevel.Session);
        DatabaseAccount databaseAccount = createDatabaseAccountObject();
        DocumentClient spyClient = spy(client);
        doReturn(databaseAccount).when(spyClient).getDatabaseAccountFromEndpoint((URI)anyObject());
        spyClient.setGatewayProxyOverride(proxy);        
        GlobalEndpointManager endpointManager = new GlobalEndpointManager(spyClient);
        spyClient.setGlobalEndpointManager(endpointManager);
        
        Database databaseDefinition = new Database();
        databaseDefinition.setId("db1");
        boolean failed = false;
        try { 
            spyClient.createDatabase(databaseDefinition, null);
        } catch (DocumentClientException e) {
            Assert.assertEquals(403, e.getStatusCode());
            Assert.assertEquals((Integer)3, e.getSubStatusCode());
            failed = true;
        }
        Assert.assertTrue(failed);
        
        // The first call to getDatabaseAccountFromEndpoint is not captured because we create
        // the spyClient after the first initialize call which happens in the constructor of
        // the DocumentClient class.
        verify(spyClient, times(120)).getDatabaseAccountFromEndpoint((URI)anyObject());
        verify(proxy, times(121)).doCreate((DocumentServiceRequest) anyObject());
    }
    
    @Test
    public void testGetDatabaseAccountFail() throws DocumentClientException, URISyntaxException {
        Collection<String>preferredReadRegions = new ArrayList<String>(Arrays.asList(preferredRegionalEndpoints));
        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        connectionPolicy.setPreferredLocations(preferredReadRegions);        
        DocumentClient client = new DocumentClient(DEFAULT_ENDPOINT, MASTER_KEY, connectionPolicy,
                ConsistencyLevel.Session);        
        DocumentClient spyClient = spy(client);        
        GatewayProxy proxy = mock(GatewayProxy.class);
        when(proxy.doCreate((DocumentServiceRequest) anyObject())).thenThrow(createHttpConnectionException(DEFAULT_ENDPOINT));
        spyClient.setGatewayProxyOverride(proxy);

        // Test using global name to find regional endpoints.
        GlobalEndpointManager endpointManager = new GlobalEndpointManager(spyClient);
        spyClient.setGlobalEndpointManager(endpointManager);
        
        URI readRegionUri = endpointManager.getRegionalEndpoint(READ_ENDPOINT_NAME1);
        Assert.assertNotNull(readRegionUri);
        Assert.assertEquals(READ_ENDPOINT1, readRegionUri.toString());

        readRegionUri = endpointManager.getRegionalEndpoint(READ_ENDPOINT_NAME2);
        Assert.assertNotNull(readRegionUri);
        Assert.assertEquals(READ_ENDPOINT2, readRegionUri.toString());
                
        endpointManager.refreshEndpointList();
        verify(spyClient, times(3)).getDatabaseAccountFromEndpoint((URI)anyObject());
    }
    
    @Test
    public void testSessionReadRetry() throws DocumentClientException {
        GatewayProxy proxy = mock(GatewayProxy.class);
        when(proxy.doRead((DocumentServiceRequest) anyObject()))
            .thenThrow(createReadUnavailableException())
            .thenReturn(createEmptyDocumentServiceResponse())
            .thenReturn(createEmptyDocumentServiceResponse());

        when(proxy.doSQLQuery((DocumentServiceRequest) anyObject()))
        .thenThrow(createReadUnavailableException())
        .thenReturn(createEmptyDocumentServiceResponse());
        
        when(proxy.doCreate((DocumentServiceRequest) anyObject()))
        .thenThrow(createReadUnavailableException());

        DatabaseAccount databaseAccount = createDatabaseAccountObject();
        Collection<String>preferredReadRegions = new ArrayList<String>(Arrays.asList(preferredRegionalEndpoints));
        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        connectionPolicy.setPreferredLocations(preferredReadRegions);        
        DocumentClient client = new DocumentClient(DEFAULT_ENDPOINT, MASTER_KEY, connectionPolicy,
                ConsistencyLevel.Session);        
        DocumentClient spyClient = spy(client);        
        doReturn(databaseAccount).when(spyClient).getDatabaseAccountFromEndpoint((URI)anyObject());
        spyClient.setGatewayProxyOverride(proxy);
        GlobalEndpointManager endpointManager = new GlobalEndpointManager(spyClient);
        spyClient.setGlobalEndpointManager(endpointManager);

        ArgumentCaptor<DocumentServiceRequest> argument = ArgumentCaptor.forClass(DocumentServiceRequest.class);
        spyClient.readDocument("document_link", null);
        verify(proxy, times(2)).doRead(argument.capture());        
        Assert.assertEquals(WRITE_ENDPOINT, argument.getValue().getEndpointOverride().toString());
        
        spyClient.queryDocuments("collection_link", "Select * from c", null).getQueryIterable().toList();
        verify(proxy, times(2)).doSQLQuery(argument.capture());        
        Assert.assertEquals(WRITE_ENDPOINT, argument.getValue().getEndpointOverride().toString());

        try {
            spyClient.createDocument("collection_link", new Document(), null, false);
        } catch (Exception e) {}
        verify(proxy, times(1)).doCreate(argument.capture());        
        Assert.assertNull(argument.getValue().getEndpointOverride());
    }
    
    @Test
    public void testNoRetryOnOtherNotFoundExceptions() throws DocumentClientException {
        GatewayProxy proxy = mock(GatewayProxy.class);
        when(proxy.doRead((DocumentServiceRequest) anyObject()))
            .thenThrow(createNotFoundException());

        DatabaseAccount databaseAccount = createDatabaseAccountObject();
        Collection<String>preferredReadRegions = new ArrayList<String>(Arrays.asList(preferredRegionalEndpoints));
        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        connectionPolicy.setPreferredLocations(preferredReadRegions);        
        DocumentClient client = new DocumentClient(DEFAULT_ENDPOINT, MASTER_KEY, connectionPolicy,
                ConsistencyLevel.Session);        
        DocumentClient spyClient = spy(client);        
        doReturn(databaseAccount).when(spyClient).getDatabaseAccountFromEndpoint((URI)anyObject());
        spyClient.setGatewayProxyOverride(proxy);
        GlobalEndpointManager endpointManager = new GlobalEndpointManager(spyClient);
        spyClient.setGlobalEndpointManager(endpointManager);

        boolean notFound = false;
        ArgumentCaptor<DocumentServiceRequest> argument = ArgumentCaptor.forClass(DocumentServiceRequest.class);
        try {
            spyClient.readDocument("document_link", null);
        } catch (DocumentClientException e) {
            Assert.assertEquals(404, e.getStatusCode());
            notFound = true;
        }
        Assert.assertTrue(notFound);
        verify(proxy, times(1)).doRead(argument.capture());        
        Assert.assertNull(argument.getValue().getEndpointOverride());
    }
    
    private static DocumentServiceResponse createEmptyDocumentServiceResponse() {
        HttpResponse mockResponse = mock(HttpResponse.class);
        Header[] headers = new Header[] { };
        StatusLine mockStatusLine = mock(StatusLine.class);
        when(mockStatusLine.getStatusCode()).thenReturn(200);
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockResponse.getAllHeaders()).thenReturn(headers);
        
        return new DocumentServiceResponse(mockResponse);
    }
    
    private static DocumentClientException createWriteForbiddenException() {
        String errorBody = "{'code':'Forbidden'," + " 'message':'Message: {\"Errors\":[\"The requested operation cannot be performed at this region\"]}'}";

        Error errorResource = new Error(errorBody);
        Map<String, String> responseHeaders = new HashMap<String, String>();
        responseHeaders.put(HttpConstants.HttpHeaders.SUB_STATUS, "3");

        return new DocumentClientException("some_link", 403, errorResource, responseHeaders);
    }
    
    private static DocumentClientException createReadUnavailableException() {
        String errorBody = "{'code':'NotFound'," + " 'message':'Message: {\"Errors\":[\"The read session is not available for the input session token.\"]}'}";

        Error errorResource = new Error(errorBody);
        Map<String, String> responseHeaders = new HashMap<String, String>();
        responseHeaders.put(HttpConstants.HttpHeaders.SUB_STATUS, "1002");

        return new DocumentClientException("some_link", 404, errorResource, responseHeaders);
    }
    
    private static IllegalStateException createHttpConnectionException(String hostAddress) {
        String message = String.format("Connection to %s refused", hostAddress);
        ConnectException connectException = new ConnectException(message);
        HttpHostConnectException hostException = new HttpHostConnectException(null, connectException);
        
        return new IllegalStateException("Http client execution failed.", hostException);
    }
    
    private static DocumentClientException createNotFoundException() {
        String errorBody = "{'code':'NotFound'," + " 'message':'Message: {\"Errors\":[\"Resource not found.\"]}'}";
        Error errorResource = new Error(errorBody);

        return new DocumentClientException("some_link", 404, errorResource, null);
    }
    
    private static DatabaseAccount createDatabaseAccountObject() {
        String accountRegionJSONTemplate = "{'name':'%s', 'databaseAccountEndpoint':'%s'}";
        String accountWriteRegionJSON = String.format(accountRegionJSONTemplate, WRITE_ENDPOINT_NAME, WRITE_ENDPOINT);
        DatabaseAccount databaseAccount = new DatabaseAccount();
        Collection<DatabaseAccountLocation> writableRegions = new ArrayList<DatabaseAccountLocation>();
        writableRegions.add(new DatabaseAccountLocation(accountWriteRegionJSON));
        databaseAccount.setWritableLocations(writableRegions);
        
        String accountReadRegion1JSON = String.format(accountRegionJSONTemplate, READ_ENDPOINT_NAME1, READ_ENDPOINT1);
        Collection<DatabaseAccountLocation> readableRegions = new ArrayList<DatabaseAccountLocation>();
        readableRegions.add(new DatabaseAccountLocation(accountReadRegion1JSON));
        
        String accountReadRegion2JSON = String.format(accountRegionJSONTemplate, READ_ENDPOINT_NAME2, READ_ENDPOINT2);
        readableRegions.add(new DatabaseAccountLocation(accountReadRegion2JSON));
        
        databaseAccount.setReadableLocations(readableRegions);
        
        return databaseAccount;
    }
}
