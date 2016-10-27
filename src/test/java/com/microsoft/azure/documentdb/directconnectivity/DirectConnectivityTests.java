package com.microsoft.azure.documentdb.directconnectivity;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;

import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpStatus;
import org.apache.http.entity.StringEntity;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.microsoft.azure.documentdb.ConnectionMode;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.GatewayTestBase;
import com.microsoft.azure.documentdb.ResourceResponse;
import com.microsoft.azure.documentdb.directconnectivity.AddressInformation;
import com.microsoft.azure.documentdb.directconnectivity.GatewayAddressCache;
import com.microsoft.azure.documentdb.directconnectivity.HttpTransportClient;
import com.microsoft.azure.documentdb.directconnectivity.StoreResponse;
import com.microsoft.azure.documentdb.internal.BaseAuthorizationTokenProvider;
import com.microsoft.azure.documentdb.internal.DocumentServiceRequest;

public class DirectConnectivityTests extends GatewayTestBase {
    private static long nanoSecondsInASecond = 1000000000;
    private static long goneExceptionRetrySeconds = 30;
    private ConnectionPolicy policy;
    private Document doc;
    private TransportClientMock transportClient;
    private AddressCacheMock addressCache;
    private ResultCaptor<AddressInformation[]> resolvedAddressedInfoResultCaptor;
    private DocumentClient directHttpClient;
    private ResultCaptor<StoreResponse> storeResponseCaptor;

    public DirectConnectivityTests() {
        super(new DocumentClient(HOST, MASTER_KEY, new ConnectionPolicy(), ConsistencyLevel.Session));
    }

    private AddressCacheMock getAddressCache() {
        return new AddressCacheMock(new GatewayAddressCache(
                HOST,
                policy,
                getCollectionCache(this.client),
                getPartitionKeyRangeCache(this.client),
                null,
                new BaseAuthorizationTokenProvider(MASTER_KEY)));
    }
    
    @Before
    public void setUp() throws DocumentClientException {
        super.setUp();

        policy = new ConnectionPolicy();
        policy.setConnectionMode(ConnectionMode.DirectHttps);

        doc = this.client.createDocument(this.collectionForTest.getSelfLink(), new Document(), null, false).getResource();

        transportClient = spy(new TransportClientMock(new HttpTransportClient(policy, null)));


        resolvedAddressedInfoResultCaptor = new ResultCaptor<AddressInformation[]>();
        storeResponseCaptor = new ResultCaptor<StoreResponse>();

    }

    private void resetMockito() throws DocumentClientException {

        // reset mockito memory
        reset(addressCache);
        reset(transportClient);
        reset(transportClient);
        storeResponseCaptor.reset();
        resolvedAddressedInfoResultCaptor.reset();
        doAnswer(resolvedAddressedInfoResultCaptor).when(addressCache).resolve(any(DocumentServiceRequest.class));
        doAnswer(storeResponseCaptor).when(transportClient).invokeStore(any(URI.class), any((DocumentServiceRequest.class)));
    }

    @SuppressWarnings("rawtypes")
    public class ResultCaptor<T> implements Answer {
        private List<T> results = new ArrayList<>();
        public List<T> getResults() {
            return results;
        }

        @SuppressWarnings("unchecked")
        @Override
        public T answer(InvocationOnMock invocationOnMock) throws Throwable {
            T result = (T) invocationOnMock.callRealMethod();
            results.add(result);
            return result;
        }

        public void reset() {
            results.clear();
        }
    }

    private void setUpEventualConsistency() throws DocumentClientException {
        addressCache = spy(getAddressCache());
        directHttpClient = createDocumentClient(HOST, MASTER_KEY, policy, ConsistencyLevel.Eventual, addressCache, transportClient);
    }

    private void setUpSessionConsistency() throws DocumentClientException {
        addressCache = spy(getAddressCache());
        directHttpClient = createDocumentClient(HOST, MASTER_KEY, policy, ConsistencyLevel.Session, addressCache, transportClient);
    }

    private void setUpBoundedStalenessConsistency() throws DocumentClientException {
        addressCache = spy(getAddressCache());
        directHttpClient = createDocumentClient(HOST, MASTER_KEY, policy, ConsistencyLevel.BoundedStaleness, addressCache, transportClient);
    }

    private void setUpStrongConsistency() throws DocumentClientException {
        addressCache = spy(getAddressCache());
        directHttpClient = createDocumentClient(HOST, MASTER_KEY, policy, ConsistencyLevel.Strong, addressCache, transportClient);
    }

    @Test
    public void TestEventualConsistency() throws DocumentClientException {
        setUpEventualConsistency();
        commonReadTest(directHttpClient, doc, addressCache, transportClient);
    }

    @Test
    public void TestSessionConsistency() throws DocumentClientException {
        setUpSessionConsistency();
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
    public void testBoundedStalenessConsistency_CommonTests() throws DocumentClientException {
        setUpBoundedStalenessConsistency();    	
        commonReadTest(directHttpClient, doc, addressCache, transportClient);
    }

    @Test
    public void testBoundedStalenessConsistency_QuorumNotSelected_ReadPrimary() throws DocumentClientException {
        setUpBoundedStalenessConsistency();

        String[] links = new String[] {this.createDocumentNameBasedLink(doc), doc.getSelfLink()};
        for (int linkIndex = 0; linkIndex < links.length; linkIndex++) {
            resetMockito();

            // Test Quorum NotSelected -> read primary Succeeded
            addressCache.setNumberOfReplicasToReturn(1).times(1);
            StoreResponse storeResponse = new StoreResponse();
            storeResponse.setResponseHeaderNames(new String[] {"lsn", "x-ms-current-replica-set-size", "x-ms-quorum-acked-lsn"});
            storeResponse.setResponseHeaderValues(new String[] {"10", "1", "10"});

            try {
                storeResponse.setResponseBody(new StringEntity(doc.toString()));
            } catch (UnsupportedEncodingException e) {
            }

            ArrayList<StoreResponse> responses = new ArrayList<StoreResponse>();
            responses.add(storeResponse);
            transportClient.setValuesToReturn(responses);
            Document readResult = directHttpClient.readDocument(links[linkIndex], null).getResource();
            Assert.assertEquals(doc.getId(), readResult.getId());

            ArgumentCaptor<URI> uriArg = ArgumentCaptor.forClass(URI.class);
            ArgumentCaptor<DocumentServiceRequest> requestArg = ArgumentCaptor.forClass(DocumentServiceRequest.class);

            // 1) resolve secondaries fail 2) resolve primary address
            verify(addressCache, times(2)).resolve(requestArg.capture());

            DocumentServiceRequest reqToAddressCache = requestArg.getValue();

            // there were not enough secondary replicas so only one request is made to primary
            verify(transportClient, times(1)).invokeResourceOperation(uriArg.capture(), requestArg.capture());

            DocumentServiceRequest reqToPrimary = requestArg.getAllValues().get(0);

            // same request as the most recent to the address cache
            assertThat(reqToPrimary, equalTo(reqToAddressCache));

            assertThat(reqToPrimary.getQuorumSelectedStoreResponse(), nullValue());
            assertThat(reqToPrimary.getSessionLsn(), equalTo(-1L));

            // two times address resolution
            assertThat(resolvedAddressedInfoResultCaptor.getResults(), hasSize(2));

            // address resolution for primary
            AddressInformation[] primaryAddressResolution = resolvedAddressedInfoResultCaptor.getResults().get(0);

            // ensure the request is made to primary
            AddressInformation primaryAddress = findPrimary(primaryAddressResolution);
            assertThat(primaryAddress, notNullValue());
            assertThat(uriArg.getValue().toString(), equalTo(primaryAddress.getPhysicalUri()));
        }
    }

    private AddressInformation findPrimary(AddressInformation[] addresses) {
        for(AddressInformation a: addresses) {
            if (a.isPrimary()) {
                return a;
            }
        }
        return null;
    }

    @Test
    public void testBoundedStalenessConsistency_QuorumNotSelected_ReadPrimaryFailed_RetryOnSecondaries() throws DocumentClientException {
        setUpBoundedStalenessConsistency();

        String[] links = new String[] {this.createDocumentNameBasedLink(doc), doc.getSelfLink()};
        for (int linkIndex = 0; linkIndex < links.length; linkIndex++) {
            resetMockito();

            //Test Quorum NotSelected -> read primary fail because replicaSetSize bigger than quorum size -> retry on secondaries
            addressCache.setNumberOfReplicasToReturn(1).times(1);
            transportClient.reset();
            Document readResult = directHttpClient.readDocument(links[linkIndex], null).getResource();
            Assert.assertEquals(doc.getId(), readResult.getId());

            // checking the arguments
            ArgumentCaptor<URI> uriArg = ArgumentCaptor.forClass(URI.class);
            ArgumentCaptor<DocumentServiceRequest> requestArg = ArgumentCaptor.forClass(DocumentServiceRequest.class);

            // three attempts: each result in 3 address resolution
            verify(addressCache, times(3)).resolve(requestArg.capture());

            DocumentServiceRequest reqToAddressCache = requestArg.getValue();

            // attempt on secondaries
            verify(transportClient, times(3)).invokeResourceOperation(uriArg.capture(), requestArg.capture());

            DocumentServiceRequest reqToPrimary = requestArg.getAllValues().get(0);

            // same request as the most recent to the address cache
            assertThat(reqToPrimary, equalTo(reqToAddressCache));

            assertThat(reqToPrimary.getQuorumSelectedStoreResponse(), nullValue());
            assertThat(reqToPrimary.getSessionLsn(), equalTo(-1L));

            // two times address resolution
            assertThat(resolvedAddressedInfoResultCaptor.getResults(), hasSize(3));
            // Quorum NotSelected attempt
            assertThat(resolvedAddressedInfoResultCaptor.getResults().get(0), arrayWithSize(1));
            // Primary read failed
            assertThat(resolvedAddressedInfoResultCaptor.getResults().get(1), arrayWithSize(3));
            // Retry on secondaries
            assertThat(resolvedAddressedInfoResultCaptor.getResults().get(2), arrayWithSize(3));            
        }
    }

    @Test
    public void testBoundedStalenessConsistency_QuorumNotSelected_PrimaryDoestExist_RetryOnSecondaries() throws DocumentClientException {
        setUpBoundedStalenessConsistency();

        String[] links = new String[] {this.createDocumentNameBasedLink(doc), doc.getSelfLink()};
        for (int linkIndex = 0; linkIndex < links.length; linkIndex++) {
            resetMockito();

            // Test Quorum Notselected -> primary doesn't exist -> Retry on Secondaries 
            addressCache.setNumberOfReplicasToReturn(1).setReturnPrimary(false).times(1);
            Document readResult = directHttpClient.readDocument(links[linkIndex], null).getResource();
            Assert.assertEquals(doc.getId(), readResult.getId());

            ArgumentCaptor<URI> uriArg = ArgumentCaptor.forClass(URI.class);
            ArgumentCaptor<DocumentServiceRequest> requestArg = ArgumentCaptor.forClass(DocumentServiceRequest.class);

            // three attempts: each result in 3 address resolution
            verify(addressCache, times(3)).resolve(requestArg.capture());


            // secondaries attempt:
            verify(transportClient, times(3)).invokeResourceOperation(uriArg.capture(), requestArg.capture());

            // requests made to secondaries
            DocumentServiceRequest requestToSecondary1 = requestArg.getAllValues().get(0);
            DocumentServiceRequest requestToSecondary2 = requestArg.getAllValues().get(1);
            DocumentServiceRequest requestToSecondary3 = requestArg.getAllValues().get(2);

            assertThat(requestToSecondary1, equalTo(requestToSecondary2));
            assertThat(requestToSecondary2, equalTo(requestToSecondary3));

            // responses from secondaries (quorum met, LSN is the same)
            assertThat(storeResponseCaptor.getResults(), hasSize(3));
            assertThat(storeResponseCaptor.getResults().get(0).getLSN(), equalTo(storeResponseCaptor.getResults().get(1).getLSN()));
            assertThat(storeResponseCaptor.getResults().get(1).getLSN(), equalTo(storeResponseCaptor.getResults().get(2).getLSN()));
        }
    }

    @Test
    public void testBoundedStalenessConsistency_QuorumSelected_SecondaryBarrier() throws DocumentClientException {
        setUpBoundedStalenessConsistency();

        String[] links = new String[] {this.createDocumentNameBasedLink(doc), doc.getSelfLink()};
        for (int linkIndex = 0; linkIndex < links.length; linkIndex++) {
            resetMockito();

            // Test Quorum Selected then secondary barrier request success
            StoreResponse storeResponse = new StoreResponse();
            storeResponse.setResponseHeaderNames(new String[] {"lsn"});
            storeResponse.setResponseHeaderValues(new String[] {"0"});
            ArrayList<StoreResponse> responses = new ArrayList<StoreResponse>();
            responses.add(storeResponse);
            transportClient.setValuesToReturn(responses).times(1);
            ResourceResponse<Document> readResponse = directHttpClient.readDocument(links[linkIndex], null);
            Assert.assertEquals(doc.getId(), readResponse.getResource().getId());
            Assert.assertNotEquals(0, this.getLSN(readResponse.getResponseHeaders().get("x-ms-session-token")));

            ArgumentCaptor<URI> uriArg = ArgumentCaptor.forClass(URI.class);
            ArgumentCaptor<DocumentServiceRequest> requestArg = ArgumentCaptor.forClass(DocumentServiceRequest.class);

            // first address resolution gives 3 addresses
            assertThat(resolvedAddressedInfoResultCaptor.getResults().get(0), arrayWithSize(3));
            // second address resolution gives 3 addresses
            assertThat(resolvedAddressedInfoResultCaptor.getResults().get(1), arrayWithSize(3));

            verify(transportClient, times(2+2)).invokeResourceOperation(uriArg.capture(), requestArg.capture());

            // responses from secondaries (quorum met, LSN is the same)
            assertThat(storeResponseCaptor.getResults(), hasSize(2));

            // find lsn 0
            int lsn0Index = -1;
            for(int i =0; i < storeResponseCaptor.getResults().size(); i++) {
                if (storeResponseCaptor.getResults().get(i).getLSN() == 0) {
                    lsn0Index = i;
                    break;
                }
            }
            assertThat(lsn0Index, not(equalTo(-1)));

            // all LSN except the 0 one are equal.
            long quorumLSN = -1;
            for(int i = 0; i < storeResponseCaptor.getResults().size(); i++) {
                if (lsn0Index != i) {
                    if (quorumLSN == -1) {
                        quorumLSN = storeResponseCaptor.getResults().get(1).getLSN();
                    }
                    assertThat(quorumLSN, equalTo(storeResponseCaptor.getResults().get(i).getLSN()));
                }
            }
        }
    }

    @Test
    public void testBoundedStalenessConsistency_QuorumSelected_SecondaryBarrierFailed_SecondaryBarrier() throws DocumentClientException {
        setUpBoundedStalenessConsistency();

        String[] links = new String[] {this.createDocumentNameBasedLink(doc), doc.getSelfLink()};
        for (int linkIndex = 0; linkIndex < links.length; linkIndex++) {

            // Test Quorum Selected then secondary barrier request fails then more waiting till lsn match on secondaries
            ArrayList<StoreResponse> responses = new ArrayList<StoreResponse>();
            StoreResponse storeResponse = new StoreResponse();
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
            ResourceResponse<Document> readResponse = directHttpClient.readDocument(links[linkIndex], null);
            Assert.assertEquals(doc.getId(), readResponse.getResource().getId());
            Assert.assertNotEquals(1, this.getLSN(readResponse.getResponseHeaders().get("x-ms-session-token")));
            // Response will have higher lsn than the first read request
            Assert.assertEquals(10, this.getLSN(readResponse.getResponseHeaders().get("x-ms-session-token")));
        }
    }

    @Test
    public void TestStrongConsistency() throws DocumentClientException {
        setUpStrongConsistency();    	
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
