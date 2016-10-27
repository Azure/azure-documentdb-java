package com.microsoft.azure.documentdb.directconnectivity;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.internal.DocumentServiceRequest;
import com.microsoft.azure.documentdb.internal.HttpConstants;
import com.microsoft.azure.documentdb.internal.OperationType;
import com.microsoft.azure.documentdb.internal.SessionContainer;
import com.microsoft.azure.documentdb.internal.SessionTokenHelper;

class StoreReader {
    private static final SecureRandom random = new SecureRandom();
    private final AddressCache addressCache;
    private final TransportClient transportClient;
    private final SessionContainer sessionContainer;
    private final ExecutorService executor;

    public StoreReader(AddressCache addressCache, TransportClient transportClient, SessionContainer sessionContainer) {
        this.executor = Executors.newCachedThreadPool();
        this.addressCache = addressCache;
        this.transportClient = transportClient;
        this.sessionContainer = sessionContainer;
    }

    private static int generateNextRandom(int maxValue) {
        if (StoreReader.random.nextFloat() < 0.001) {
            // Reseeding the generator for long term randomness
            // SHA1PRNG uses 160 bits of state, 256 bits/32 bytes should be sufficient for most other types
            StoreReader.random.setSeed(StoreReader.random.generateSeed(32));
        }
        return StoreReader.random.nextInt(maxValue);
    }

    public StoreReadResult readEventual(DocumentServiceRequest request) throws DocumentClientException {
        request.getHeaders().remove(HttpConstants.HttpHeaders.SESSION_TOKEN);
        ArrayList<String> resolvedEndpoints = this.getEndpointAddresses(request, true);

        if (resolvedEndpoints.size() == 0) {
            return null;
        }

        StoreReadResult result = readOneReplica(request, resolvedEndpoints);
        request.getRequestChargeTracker().addCharge(result.getRequestCharge());
        return result;
    }

    public StoreReadResult readSession(DocumentServiceRequest request) throws DocumentClientException {
        ArrayList<String> resolvedEndpoints = this.getEndpointAddresses(request, true);

        if (resolvedEndpoints.size() == 0) {
            return null;
        }

        SessionTokenHelper.setPartitionLocalSessionToken(request, this.sessionContainer);
        long minLsnRequired = request.getSessionLsn();

        while (resolvedEndpoints.size() > 0) {
            StoreReadResult result = readOneReplica(request, resolvedEndpoints);
            request.getRequestChargeTracker().addCharge(result.getRequestCharge());
            if (minLsnRequired <= 0 || result.getLSN() >= minLsnRequired) {
                return result;
            }
        }

        return null;
    }

    public StoreReadResult readPrimary(DocumentServiceRequest request, boolean requiresValidLsn) throws DocumentClientException {
        ReadReplicaResult readQuorumResult = this.readPrimaryImpl(request, requiresValidLsn);
        if (readQuorumResult.isRetryWithForceRefresh() && !request.isForceAddressRefresh()) {
            request.setForceAddressRefresh(true);
            readQuorumResult = this.readPrimaryImpl(request, requiresValidLsn);
        }

        if (readQuorumResult.getResponses().size() == 0) {
            throw new DocumentClientException(HttpStatus.SC_GONE, "Didn't receive response from read primary.");
        }

        return readQuorumResult.getResponses().get(0);
    }

    public List<StoreReadResult> readMultipleReplicaImpl(final DocumentServiceRequest request,
                                                         boolean includePrimary, int replicaCountToRead) throws DocumentClientException {
        List<StoreReadResult> storeReadResults = new LinkedList<StoreReadResult>();
        CompletionService<StoreReadResult> completionService = new ExecutorCompletionService<StoreReadResult>(this.executor);
        ArrayList<String> resolvedEndpoints = getEndpointAddresses(request, includePrimary);

        // returning empty array if endpoints will never be able to satisfy the requested number of replicas
        if (resolvedEndpoints.size() < replicaCountToRead) {
            return storeReadResults;
        }

        // session token is not needed in case of strong consistency
        request.getHeaders().remove(HttpConstants.HttpHeaders.SESSION_TOKEN);

        for (int i = 0; i < replicaCountToRead; i++) {
            int uriIndex = StoreReader.generateNextRandom(resolvedEndpoints.size());
            Callable<StoreReadResult> callable = this.getReadReplicaCallable(request, resolvedEndpoints.get(uriIndex));

            resolvedEndpoints.remove(uriIndex);
            completionService.submit(callable);
        }

        int received = 0;
        while (received < replicaCountToRead) {
            try {
                Future<StoreReadResult> resultFuture = completionService.take();
                received++;
                StoreReadResult result = resultFuture.get();
                request.getRequestChargeTracker().addCharge(result.getRequestCharge());
                storeReadResults.add(result);
            } catch (Exception e) {
                throw new DocumentClientException(HttpStatus.SC_INTERNAL_SERVER_ERROR, e);
            }
        }

        return storeReadResults;
    }

    private ReadReplicaResult readPrimaryImpl(DocumentServiceRequest request, boolean requiresValidLsn) throws DocumentClientException {
        URI primaryUri = ReplicatedResourceClient.resolvePrimaryUri(request, this.addressCache);
        request.getHeaders().remove(HttpConstants.HttpHeaders.SESSION_TOKEN);

        StoreReadResult storeReadResult = this.createStoreReadResult(request, primaryUri.toString());
        request.getRequestChargeTracker().addCharge(storeReadResult.getRequestCharge());
        if (storeReadResult.isGoneException()) {
            return null;
        }

        return new ReadReplicaResult(false, Arrays.asList(storeReadResult));
    }

    private StoreReadResult readOneReplica(DocumentServiceRequest request, ArrayList<String> resolvedEndpoints) {
        if (resolvedEndpoints.size() == 0) {
            return null;
        }

        int uriIndex = StoreReader.generateNextRandom(resolvedEndpoints.size());
        String endpoint = resolvedEndpoints.get(uriIndex);
        resolvedEndpoints.remove(uriIndex);
        return createStoreReadResult(request, endpoint);
    }

    private ArrayList<String> getEndpointAddresses(DocumentServiceRequest request, boolean includePrimary) throws DocumentClientException {
        AddressInformation[] addresses = this.addressCache.resolve(request);
        ArrayList<String> resolvedEndpoints = new ArrayList<String>();
        for (int i = 0; i < addresses.length; i++) {
            if (!addresses[i].isPrimary() || includePrimary) {
                resolvedEndpoints.add(addresses[i].getPhysicalUri());
            }
        }

        return resolvedEndpoints;
    }

    private Callable<StoreReadResult> getReadReplicaCallable(final DocumentServiceRequest request, final String endpointAddress) {
        Callable<StoreReadResult> callable = new Callable<StoreReadResult>() {
            @Override
            public StoreReadResult call() {
                return createStoreReadResult(request, endpointAddress);
            }
        };

        return callable;
    }

    private StoreReadResult createStoreReadResult(DocumentServiceRequest request, String endpointAddress) {
        StoreResponse storeResponse = null;
        DocumentClientException storeException = null;

        try {
            storeResponse = readFromStore(request, endpointAddress);
        } catch (DocumentClientException e) {
            storeException = e;
        }

        long quorumAckedLSN = -1;
        int currentReplicaSetSize = -1;
        int currentWriteQuorum = -1;
        double requestCharge = 0;
        long lsn = -1;

        if (storeException != null) {
            Map<String, String> headers = storeException.getResponseHeaders();
            if (headers == null) {
                return new StoreReadResult(storeResponse, storeException, lsn,
                        null, quorumAckedLSN, requestCharge, currentReplicaSetSize,
                        currentWriteQuorum, false);
            }

            String quorumAckedLsnHeaderValue = headers.get(WFConstants.BackendHeaders.QuorumAckedLSN);
            if (!StringUtils.isEmpty(quorumAckedLsnHeaderValue)) {
                quorumAckedLSN = Long.parseLong(quorumAckedLsnHeaderValue);
            }

            String currentReplicaSetSizeHeaderValue = headers.get(WFConstants.BackendHeaders.CurrentReplicaSetSize);
            if (!StringUtils.isEmpty(currentReplicaSetSizeHeaderValue)) {
                currentReplicaSetSize = Integer.parseInt(currentReplicaSetSizeHeaderValue);
            }

            String currentWriteQuorumHeaderValue = headers.get(WFConstants.BackendHeaders.CurrentWriteQuorum);
            if (!StringUtils.isEmpty(currentReplicaSetSizeHeaderValue)) {
                currentWriteQuorum = Integer.parseInt(currentWriteQuorumHeaderValue);
            }

            String currentRequestChargeHeaderValue = headers.get(HttpConstants.HttpHeaders.REQUEST_CHARGE);
            if (!StringUtils.isEmpty(currentRequestChargeHeaderValue)) {
                requestCharge = Double.parseDouble(currentRequestChargeHeaderValue);
            }

            String lsnValue = headers.get(HttpConstants.HttpHeaders.LSN);

            if (!StringUtils.isEmpty(lsnValue)) {
                lsn = Long.parseLong(lsnValue);
            }

            return new StoreReadResult(storeResponse, storeException, lsn,
                    null, quorumAckedLSN, requestCharge, currentReplicaSetSize,
                    currentWriteQuorum, false);
        }

        String quorumAckedLsnHeaderValue = storeResponse.getHeaderValue(WFConstants.BackendHeaders.QuorumAckedLSN);
        if (!StringUtils.isEmpty(quorumAckedLsnHeaderValue)) {
            quorumAckedLSN = Long.parseLong(quorumAckedLsnHeaderValue);
        }

        String currentReplicaSetSizeHeaderValue = storeResponse.getHeaderValue(WFConstants.BackendHeaders.CurrentReplicaSetSize);
        if (!StringUtils.isEmpty(currentReplicaSetSizeHeaderValue)) {
            currentReplicaSetSize = Integer.parseInt(currentReplicaSetSizeHeaderValue);
        }

        String currentWriteQuorumHeaderValue = storeResponse.getHeaderValue(WFConstants.BackendHeaders.CurrentWriteQuorum);
        if (!StringUtils.isEmpty(currentWriteQuorumHeaderValue)) {
            currentWriteQuorum = Integer.parseInt(currentWriteQuorumHeaderValue);
        }

        String currentRequestChargeHeaderValue = storeResponse.getHeaderValue(HttpConstants.HttpHeaders.REQUEST_CHARGE);
        if (!StringUtils.isEmpty(currentRequestChargeHeaderValue)) {
            requestCharge = Double.parseDouble(currentRequestChargeHeaderValue);
        }

        lsn = storeResponse.getLSN();
        boolean isValid = true;

        return new StoreReadResult(storeResponse, storeException, lsn,
                storeResponse.getPartitionKeyRangeId(), quorumAckedLSN, requestCharge, currentReplicaSetSize,
                currentWriteQuorum, isValid);
    }

    private StoreResponse readFromStore(DocumentServiceRequest request, String address) throws DocumentClientException {
        String continuation = null;
        if (request.getOperationType() == OperationType.ReadFeed || request.getOperationType() == OperationType.Query
                || request.getOperationType() == OperationType.SqlQuery) {
            continuation = request.getHeaders().get(HttpConstants.HttpHeaders.CONTINUATION);

            if (continuation != null && continuation.contains(";")) {
                String parts[] = continuation.split(";");
                if (parts.length < 3) {
                    throw new DocumentClientException(HttpStatus.SC_BAD_REQUEST, "Invalid header value");
                }

                continuation = parts[0];
            }

            request.setContinuation(continuation);
        }

        try {
            return this.transportClient.invokeResourceOperation(new URI(address), request);
        } catch (URISyntaxException e) {
            throw new DocumentClientException(HttpStatus.SC_INTERNAL_SERVER_ERROR, e);
        }
    }
}
