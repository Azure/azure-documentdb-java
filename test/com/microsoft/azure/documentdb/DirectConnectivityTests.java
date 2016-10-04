package com.microsoft.azure.documentdb;


import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

import org.apache.http.HttpStatus;
import org.apache.http.entity.StringEntity;
import org.junit.Assert;
import org.junit.Test;

import com.microsoft.azure.documentdb.directconnectivity.AddressInformation;
import com.microsoft.azure.documentdb.directconnectivity.GatewayAddressCache;
import com.microsoft.azure.documentdb.directconnectivity.HttpTransportClient;
import com.microsoft.azure.documentdb.directconnectivity.StoreResponse;
import com.microsoft.azure.documentdb.internal.BaseAuthorizationTokenProvider;

public class DirectConnectivityTests extends GatewayTestBase {
    private static long nanoSecondsInASecond = 1000000000;
    private static long goneExceptionRetrySeconds = 30;

    @Test
    public void TestEventualConsistency() throws DocumentClientException {
        ConnectionPolicy policy = new ConnectionPolicy();
        policy.setConnectionMode(ConnectionMode.DirectHttps);

        Document doc = this.client.createDocument(this.collectionForTest.getSelfLink(), new Document(), null, false).getResource();

        TransportClientMock transportClient = new TransportClientMock(new HttpTransportClient(policy, null));
        AddressCacheMock addressCache = new AddressCacheMock(new GatewayAddressCache(HOST, policy, null, new BaseAuthorizationTokenProvider(MASTER_KEY)));
        DocumentClient directHttpClient = new DocumentClient(HOST, MASTER_KEY, policy, ConsistencyLevel.Eventual, addressCache, transportClient);
        commonReadTest(directHttpClient, doc, addressCache, transportClient);
    }

    @Test
    public void TestSessionConsistency() throws DocumentClientException {
        ConnectionPolicy policy = new ConnectionPolicy();
        policy.setConnectionMode(ConnectionMode.DirectHttps);

        Document doc = this.client.createDocument(this.collectionForTest.getSelfLink(), new Document(), null, false).getResource();

        TransportClientMock transportClient = new TransportClientMock(new HttpTransportClient(policy, null));
        AddressCacheMock addressCache = new AddressCacheMock(new GatewayAddressCache(HOST, policy, null, new BaseAuthorizationTokenProvider(MASTER_KEY)));
        DocumentClient directHttpClient = new DocumentClient(HOST, MASTER_KEY, policy, ConsistencyLevel.Session, addressCache, transportClient);
        commonReadTest(directHttpClient, doc, addressCache, transportClient);

        String[] links = new String[] {doc.getSelfLink(), this.createDocumentNameBasedLink(doc)};
        for (int linkIndex = 0; linkIndex < links.length; linkIndex++) {
            StoreResponse storeResponse = new StoreResponse();
            storeResponse.setResponseHeaderNames(new String[] {"lsn"});
            storeResponse.setResponseHeaderValues(new String[] {"0"});
            ArrayList<StoreResponse> responses = new ArrayList<StoreResponse>();
            responses.add(storeResponse);
            transportClient.setValuesToReturn(responses);
            Document readResult = null;
            try {
                readResult = directHttpClient.readDocument(links[linkIndex], null).getResource();
                Assert.fail("Request should fail because of no higher LSN.");
            } catch (DocumentClientException e) {
                Assert.assertEquals(503, e.getStatusCode());
            }

            responses = new ArrayList<StoreResponse>();
            responses.add(storeResponse);
            transportClient.setValuesToReturn(responses).times(1);
            readResult = directHttpClient.readDocument(links[linkIndex], null).getResource();
            Assert.assertEquals(doc.getId(), readResult.getId());
        }
    }

    @Test
    public void TestStrongConsistency() throws DocumentClientException {
        ConnectionPolicy policy = new ConnectionPolicy();
        policy.setConnectionMode(ConnectionMode.DirectHttps);

        Document doc = this.client.createDocument(this.collectionForTest.getSelfLink(), new Document(), null, false).getResource();
        TransportClientMock transportClient = new TransportClientMock(new HttpTransportClient(policy, null));
        AddressCacheMock addressCache = new AddressCacheMock(new GatewayAddressCache(HOST, policy, null, new BaseAuthorizationTokenProvider(MASTER_KEY)));
        DocumentClient directHttpClient = new DocumentClient(HOST, MASTER_KEY, policy, ConsistencyLevel.Strong, addressCache, transportClient);
        commonReadTest(directHttpClient, doc, addressCache, transportClient);

        String[] links = new String[] {this.createDocumentNameBasedLink(doc), doc.getSelfLink()};
        for (int linkIndex = 0; linkIndex < links.length; linkIndex++) {
            Document readResult = null;
            ArrayList<StoreResponse> responses = null;
            StoreResponse storeResponse = null;
            ResourceResponse<Document> readResponse = null;

            // Test Quorum NotSelected -> read primary Succeeded
            addressCache.setNumberOfReplicasToReturn(1).times(1);
            storeResponse = new StoreResponse();
            storeResponse.setResponseHeaderNames(new String[] {"lsn", "x-ms-current-replica-set-size", "x-ms-quorum-acked-lsn"});
            storeResponse.setResponseHeaderValues(new String[] {"10", "1", "10"});

            try {
                storeResponse.setResponseBody(new StringEntity(doc.toString()));
            } catch (UnsupportedEncodingException e) {
            }

            responses = new ArrayList<StoreResponse>();
            responses.add(storeResponse);
            transportClient.setValuesToReturn(responses);
            readResult = directHttpClient.readDocument(links[linkIndex], null).getResource();
            Assert.assertEquals(doc.getId(), readResult.getId());

            //Test Quorum NotSelected -> read primary fail because replicaSetSize bigger than quorum size -> retry on secondaries
            addressCache.setNumberOfReplicasToReturn(1).times(1);
            transportClient.reset();
            readResult = directHttpClient.readDocument(links[linkIndex], null).getResource();
            Assert.assertEquals(doc.getId(), readResult.getId());

            // Test Quorum Notselected -> primary doesn't exist
            addressCache.setNumberOfReplicasToReturn(1).setReturnPrimary(false).times(1);
            readResult = directHttpClient.readDocument(links[linkIndex], null).getResource();
            Assert.assertEquals(doc.getId(), readResult.getId());

            // Test Quorum Selected then secondary barrier request success
            storeResponse = new StoreResponse();
            storeResponse.setResponseHeaderNames(new String[] {"lsn"});
            storeResponse.setResponseHeaderValues(new String[] {"0"});
            responses = new ArrayList<StoreResponse>();
            responses.add(storeResponse);
            transportClient.setValuesToReturn(responses).times(1);
            readResponse = directHttpClient.readDocument(links[linkIndex], null);
            Assert.assertEquals(readResponse.getResource().getId(), readResult.getId());
            Assert.assertNotEquals(0, this.getLSN(readResponse.getResponseHeaders().get("x-ms-session-token")));

            // Test Quorum Selected then secondary barrier request fails then primary barrier succeeds
            responses = new ArrayList<StoreResponse>();
            storeResponse = new StoreResponse();
            storeResponse.setResponseHeaderNames(new String[] {"lsn", "x-ms-session-token"});
            storeResponse.setResponseHeaderValues(new String[] {"10", "0:10"});
            try {
                storeResponse.setResponseBody(new StringEntity(doc.toString()));
            } catch (UnsupportedEncodingException e) {
            }

            responses.add(storeResponse);
            // Number of responses is 12 to proceed after the secondaries barrier request retries
            for (int i = 0; i < 12; i++) {
                storeResponse = new StoreResponse();
                storeResponse.setResponseHeaderNames(new String[] {"lsn", "x-ms-session-token"});
                storeResponse.setResponseHeaderValues(new String[] {"1", "0:1"});
                responses.add(storeResponse);
            }

            transportClient.setValuesToReturn(responses).times(responses.size());
            readResponse = directHttpClient.readDocument(links[linkIndex], null);
            Assert.assertEquals(doc.getId(), readResponse.getResource().getId());
            Assert.assertNotEquals(1, this.getLSN(readResponse.getResponseHeaders().get("x-ms-session-token")));
            // Response will have higher lsn than the first read request
            Assert.assertEquals(10, this.getLSN(readResponse.getResponseHeaders().get("x-ms-session-token")));
        }
    }

    public void commonReadTest(DocumentClient directHttpClient, Document doc, AddressCacheMock addressCache, TransportClientMock transportClient) throws DocumentClientException {
        String[] links = new String[] {this.createDocumentNameBasedLink(doc), doc.getSelfLink()};
        for (int i = 0; i < links.length; i++) {
            // Test happy behavior
            Document readResult = directHttpClient.readDocument(links[i], null).getResource();
            Assert.assertEquals(doc.getId(), readResult.getId());
            // Test if resolved addresses is less than the replicas to read
            addressCache.setValueToReturn(new AddressInformation[] {});
            long startTime = System.nanoTime();
            try {
                readResult = directHttpClient.readDocument(links[i], null).getResource();
                Assert.fail("Request should fail because of missing address");
            } catch (DocumentClientException e) {
                Assert.assertEquals(503, e.getStatusCode());
            }

            long totalSeconds = (long) ((System.nanoTime() - startTime) * 1.0 / nanoSecondsInASecond);
            Assert.assertTrue(totalSeconds >= goneExceptionRetrySeconds);

            // Test that the address cache is refreshed after first gone exception and then request succeeds
            addressCache.setValueToReturn(new AddressInformation[] {}).times(1);
            readResult = directHttpClient.readDocument(links[i], null).getResource();
            Assert.assertEquals(doc.getId(), readResult.getId());
            readResult = directHttpClient.readDocument(links[i], null).getResource();
            Assert.assertEquals(doc.getId(), readResult.getId());
            addressCache.setValueToReturn(null);

            // Test if the resolved addresses are returning gone exception
            transportClient.setExceptionToThrow(new DocumentClientException(410));
            startTime = System.nanoTime();
            try {
                readResult = directHttpClient.readDocument(links[i], null).getResource();
                Assert.fail("Request should fail because of backend gone.");
            } catch (DocumentClientException e) {
                Assert.assertEquals(503, e.getStatusCode());
            }

            totalSeconds = (long) ((System.nanoTime() - startTime) * 1.0 / nanoSecondsInASecond);
            Assert.assertTrue(totalSeconds >= goneExceptionRetrySeconds);

            // Test that address cache is refreshed after transport client gets a gone exception when using a wrong address
            transportClient.setExceptionToThrow(new DocumentClientException(410)).times(1);
            readResult = directHttpClient.readDocument(links[i], null).getResource();
            Assert.assertEquals(doc.getId(), readResult.getId());
        }
    }

    private long getLSN(String sessionToken) throws DocumentClientException {
        String[] localTokens = sessionToken.split(",");
        for (String localToken : localTokens) {
            String[] items = localToken.split(":");
            try {
                if (items[0].equals("0")) {
                    return Long.parseLong(items[1]);
                }

            } catch (NumberFormatException exception) {
                throw new DocumentClientException(HttpStatus.SC_BAD_REQUEST, "Invalid session token value.");
            }
        }

        return -1;
    }

    private String createDocumentNameBasedLink(Document doc) {
        return String.format("/dbs/%1s/colls/%2s/docs/%3s", this.databaseForTest.getId(), this.collectionForTest.getId(), doc.getId());
    }
}
