package com.microsoft.azure.documentdb.directconnectivity;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.internal.AuthorizationTokenProvider;
import com.microsoft.azure.documentdb.internal.DatabaseAccountConfigurationProvider;
import com.microsoft.azure.documentdb.internal.DocumentServiceRequest;
import com.microsoft.azure.documentdb.internal.DocumentServiceResponse;
import com.microsoft.azure.documentdb.internal.RetryRequestDelegate;
import com.microsoft.azure.documentdb.internal.RetryUtility;
import com.microsoft.azure.documentdb.internal.SessionContainer;
import com.microsoft.azure.documentdb.internal.StoreModel;

public class ServerStoreModel implements StoreModel {

    private final SessionContainer sessionContainer;
    private final ReplicatedResourceClient replicatedResourceClient;
    private int defaultReplicaIndex;
    private int lastReadAddress;
    private boolean forceAddressRefresh;


    public ServerStoreModel(
            TransportClient transportClient,
            AddressCache addressResolver,
            SessionContainer sessionContainer,
            int maxRequestSize,
            DatabaseAccountConfigurationProvider databaseAccountConfigurationProvider,
            AuthorizationTokenProvider authorizationTokenProvider) {

        this.sessionContainer = sessionContainer;
        this.replicatedResourceClient = new ReplicatedResourceClient(addressResolver, this.sessionContainer,
                transportClient, databaseAccountConfigurationProvider, authorizationTokenProvider);
    }

    public int getDefaultReplicaIndex() {
        return defaultReplicaIndex;
    }

    public void setDefaultReplicaIndex(int defaultReplicaIndex) {
        this.defaultReplicaIndex = defaultReplicaIndex;
    }

    public int getLastReadAddress() {
        return lastReadAddress;
    }

    public void setLastReadAddress(int lastReadAddress) {
        this.lastReadAddress = lastReadAddress;
    }

    public boolean isForceAddressRefresh() {
        return forceAddressRefresh;
    }

    public void setForceAddressRefresh(boolean forceAddressRefresh) {
        this.forceAddressRefresh = forceAddressRefresh;
    }

    @Override
    public DocumentServiceResponse processMessage(DocumentServiceRequest request) throws DocumentClientException {
        RetryRequestDelegate processDelegate = new RetryRequestDelegate() {

            @Override
            public DocumentServiceResponse apply(DocumentServiceRequest requestInner) throws DocumentClientException {
                StoreResponse storeResponse = replicatedResourceClient.invoke(requestInner);
                return completeResponse(storeResponse);
            }
        };

        DocumentServiceResponse response = RetryUtility.executeStoreClientRequest(processDelegate, request);
        return response;
    }

    private DocumentServiceResponse completeResponse(StoreResponse storeResponse) {
        return new DocumentServiceResponse(storeResponse);
    }
}
