package com.microsoft.azure.documentdb.directconnectivity;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.internal.DocumentServiceRequest;
import com.microsoft.azure.documentdb.internal.HttpConstants;
import com.microsoft.azure.documentdb.internal.OperationType;
import com.microsoft.azure.documentdb.internal.ResourceType;
import com.microsoft.azure.documentdb.internal.SessionContainer;

class DirectConnectivityUtils {
    public static void setPartitionLocalSessionToken(DocumentServiceRequest request, SessionContainer sessionContainer) throws DocumentClientException {
        String originalSessionToken = request.getHeaders().get(HttpConstants.HttpHeaders.SESSION_TOKEN);
        // Add support for partitioned collections
        if (StringUtils.isNotEmpty(originalSessionToken)) {
            long sessionLsn = getLocalSessionToken(originalSessionToken);
            request.setSessionLsn(sessionLsn);
        } else {
            String sessionToken = sessionContainer.resolveSessionToken(request);
            if (StringUtils.isNotEmpty(sessionToken)) {
                long sessionLsn = getLocalSessionToken(sessionToken);
                request.setSessionLsn(sessionLsn);
            }
        }

        if (request.getSessionLsn() == -1) {
            request.getHeaders().remove(HttpConstants.HttpHeaders.SESSION_TOKEN);
        } else {
            request.getHeaders().put(HttpConstants.HttpHeaders.SESSION_TOKEN, String.format("%1s:%2d", "0", request.getSessionLsn()));
        }
    }

    private static long getLocalSessionToken(String sessionToken) throws DocumentClientException {
        String[] localTokens = sessionToken.split(",");
        for (String localToken : localTokens) {
            String[] items = localToken.split(":");
            try {
                // TODO: handle partitioned collection
                if (items[0].equals("0")) {
                    return Long.parseLong(items[1]);
                }

            } catch (NumberFormatException exception) {
                throw new DocumentClientException(HttpStatus.SC_BAD_REQUEST, "Invalid session token value.");
            }
        }

        return -1;
    }

    public static URI resolvePrimaryUri(DocumentServiceRequest request, AddressCache addressCache) throws DocumentClientException {
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

    public static boolean isReadingFromMaster(ResourceType resourceType, OperationType operationType) {
        if (resourceType == ResourceType.Offer ||
                resourceType == ResourceType.Database ||
                resourceType == ResourceType.User ||
                resourceType == ResourceType.Permission ||
                resourceType == ResourceType.Topology ||
                resourceType == ResourceType.DatabaseAccount ||
                resourceType == ResourceType.PartitionKeyRange ||
                (resourceType == ResourceType.DocumentCollection && (operationType == OperationType.ReadFeed || operationType == OperationType.Query || operationType == OperationType.SqlQuery))) {
            return true;
        }

        return false;
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
