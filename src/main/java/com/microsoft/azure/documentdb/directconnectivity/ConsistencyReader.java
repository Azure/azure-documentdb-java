package com.microsoft.azure.documentdb.directconnectivity;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;

import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.internal.AuthorizationTokenProvider;
import com.microsoft.azure.documentdb.internal.DatabaseAccountConfigurationProvider;
import com.microsoft.azure.documentdb.internal.DocumentServiceRequest;
import com.microsoft.azure.documentdb.internal.HttpConstants;
import com.microsoft.azure.documentdb.internal.RequestChargeTracker;
import com.microsoft.azure.documentdb.internal.SessionContainer;

public class ConsistencyReader {
    private StoreReader storeReader;
    private DatabaseAccountConfigurationProvider databaseAccountConfigurationProvider;
    private QuorumReader quorumReader;
    private AuthorizationTokenProvider authorizationTokenProvider;

    public ConsistencyReader(AddressCache addressCache, SessionContainer sessionContainer,
                             TransportClient transportClient, DatabaseAccountConfigurationProvider databaseAccountConfigurationProvider, AuthorizationTokenProvider authorizationTokenProvider) {
        this.databaseAccountConfigurationProvider = databaseAccountConfigurationProvider;
        this.authorizationTokenProvider = authorizationTokenProvider;
        this.storeReader = new StoreReader(addressCache, transportClient, sessionContainer);
        this.quorumReader = new QuorumReader(this.storeReader, this.authorizationTokenProvider);
    }

    public StoreResponse read(DocumentServiceRequest request) throws DocumentClientException {
        if (request.getRequestChargeTracker() == null) {
            request.setRequestChargeTracker(new RequestChargeTracker());
        }
        int maxReplicaCount = this.databaseAccountConfigurationProvider.getMaxReplicaSetSize();
        int readQuorumValue = maxReplicaCount - (maxReplicaCount / 2);
        
        switch (this.getRequiredConsistencyLevel(request)) {
            case Session:
                return this.readSession(request);
            case Eventual:
                return this.readAny(request);
            case BoundedStaleness:
                return this.quorumReader.readBoundedStaleness(request, readQuorumValue);
            case Strong:
                return this.quorumReader.readStrong(request, readQuorumValue);
            default:
                throw new IllegalStateException("Unsupported consistency level.");
        }
    }

    private ConsistencyLevel getRequiredConsistencyLevel(DocumentServiceRequest request) throws DocumentClientException {
        ConsistencyLevel targetConsistency = this.databaseAccountConfigurationProvider.getStoreConsistencyPolicy();
        String requestedConsistencyLevelStr = request.getHeaders().get(HttpConstants.HttpHeaders.CONSISTENCY_LEVEL);
        if (StringUtils.isNotEmpty(requestedConsistencyLevelStr)) {
            ConsistencyLevel requestedConsistency = ConsistencyLevel.valueOf(requestedConsistencyLevelStr);
            if (requestedConsistency == null) {
                throw new IllegalArgumentException("Invalid consistency policy: " + requestedConsistencyLevelStr);
            }

            targetConsistency = requestedConsistency;
        }

        return targetConsistency;
    }

    private StoreResponse readSession(DocumentServiceRequest request) throws DocumentClientException {
        StoreReadResult responses = this.storeReader.readSession(request);
        if (responses == null) {
            throw new DocumentClientException(HttpStatus.SC_GONE, "Didn't receive response from read session.");
        }

        return responses.toStoreResponse(request.getRequestChargeTracker());
    }

    private StoreResponse readAny(DocumentServiceRequest request) throws DocumentClientException {
        StoreReadResult response = this.storeReader.readEventual(request);
        if (response == null) {
            throw new DocumentClientException(HttpStatus.SC_GONE, "Didn't receive response from read eventual.");
        }

        return response.toStoreResponse(request.getRequestChargeTracker());
    }
}
