package com.microsoft.azure.documentdb.directconnectivity;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.internal.AuthorizationTokenProvider;
import com.microsoft.azure.documentdb.internal.DatabaseAccountConfigurationProvider;
import com.microsoft.azure.documentdb.internal.DocumentServiceRequest;
import com.microsoft.azure.documentdb.internal.OperationType;
import com.microsoft.azure.documentdb.internal.ResourceType;
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
        URI primaryUri = resolvePrimaryUri(request, this.addressCache);
        // TODO: handle session token
        return this.transportClient.invokeResourceOperation(primaryUri, request);
    }

    static URI resolvePrimaryUri(DocumentServiceRequest request, AddressCache addressCache) throws DocumentClientException {
        AddressInformation[] replicaAddresses = resolveAddresses(request, addressCache);
        // TODO handle default replica index

        for (int i = 0; i < replicaAddresses.length; i++) {
            if (replicaAddresses[i].isPrimary()) {
                try {
                    return new URI(replicaAddresses[i].getPhysicalUri());
                } catch (URISyntaxException e) {
                    throw new IllegalStateException("Invalid replica address");
                }
            }
        }

        throw new DocumentClientException(HttpStatus.SC_GONE, "The requested resource is no longer available at the server.");
    }

    private static AddressInformation[] resolveAddresses(DocumentServiceRequest request, AddressCache addressCache) throws DocumentClientException {
        AddressInformation[] allResolvedAddresses = addressCache.resolve(request);
        ArrayList<AddressInformation> publicResolvedAddresses = new ArrayList<AddressInformation>();
        ArrayList<AddressInformation> internalResolvedAddresses = new ArrayList<AddressInformation>();

        for (int i = 0; i < allResolvedAddresses.length; i++) {
            AddressInformation address = allResolvedAddresses[i];
            if (!StringUtils.isEmpty(address.getPhysicalUri())) {
                if (address.isPublic()) {
                    publicResolvedAddresses.add(address);
                } else {
                    internalResolvedAddresses.add(address);
                }
            }
        }

        if (internalResolvedAddresses.size() > 0) {
            AddressInformation[] result = new AddressInformation[internalResolvedAddresses.size()];
            internalResolvedAddresses.toArray(result);
            return result;
        } else {
            AddressInformation[] result = new AddressInformation[publicResolvedAddresses.size()];
            publicResolvedAddresses.toArray(result);
            return result;
        }
    }
}
