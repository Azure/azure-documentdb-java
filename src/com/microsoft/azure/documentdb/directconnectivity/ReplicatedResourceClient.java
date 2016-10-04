package com.microsoft.azure.documentdb.directconnectivity;

import java.net.URI;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.internal.AuthorizationTokenProvider;
import com.microsoft.azure.documentdb.internal.DatabaseAccountConfigurationProvider;
import com.microsoft.azure.documentdb.internal.DocumentServiceRequest;
import com.microsoft.azure.documentdb.internal.SessionContainer;

class ReplicatedResourceClient {
    private AddressCache addressCache;
    private SessionContainer sesionContainer;
    private TransportClient transportClient;
    private ConsistencyReader consistencyReader;

    public ReplicatedResourceClient(AddressCache addressCache, SessionContainer sesionContainer,
                                    TransportClient transportClient, DatabaseAccountConfigurationProvider databaseAccountConfigurationProvider, AuthorizationTokenProvider authorizationTokenProvider) {
        this.addressCache = addressCache;
        this.sesionContainer = sesionContainer;
        this.transportClient = transportClient;
        this.consistencyReader = new ConsistencyReader(this.addressCache, this.sesionContainer, this.transportClient, databaseAccountConfigurationProvider, authorizationTokenProvider);
    }

    public StoreResponse invoke(DocumentServiceRequest request) throws DocumentClientException {
        switch (request.getOperationType()) {
            case Create:
            case Replace:
            case Delete:
            case ExecuteJavaScript:
            case Upsert:
            case Recreate:
                return this.write(request);

            case Read:
            case ReadFeed:
            case Query:
            case SqlQuery:
            case Head:
            case HeadFeed:
                return this.consistencyReader.read(request);
            default:
                throw new IllegalStateException("Unsupported operation type");
        }
    }

    private StoreResponse write(DocumentServiceRequest request) throws DocumentClientException {
        URI primaryUri = DirectConnectivityUtils.resolvePrimaryUri(request, this.addressCache);
        // TODO: handle session token
        return this.transportClient.invokeResourceOperation(primaryUri, request);
    }
}
