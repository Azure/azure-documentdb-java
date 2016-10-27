package com.microsoft.azure.documentdb.directconnectivity;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import com.microsoft.azure.documentdb.internal.AuthorizationTokenProvider;
import com.microsoft.azure.documentdb.internal.DocumentServiceRequest;
import com.microsoft.azure.documentdb.internal.HttpConstants;
import com.microsoft.azure.documentdb.internal.OperationType;
import com.microsoft.azure.documentdb.internal.PathsHelper;
import com.microsoft.azure.documentdb.internal.ResourceId;
import com.microsoft.azure.documentdb.internal.ResourceType;

class ReadBarrierRequestHelper {
    static DocumentServiceRequest create(DocumentServiceRequest request, AuthorizationTokenProvider authorizationTokenProvider) {
        DocumentServiceRequest barrierRequest;
        if (request.getIsNameBased()) {
            String collectionLink = PathsHelper.getCollectionPath(request.getResourceAddress());
            barrierRequest = DocumentServiceRequest.create(OperationType.Head, ResourceType.DocumentCollection, collectionLink,
                    null);
        } else {
            barrierRequest = DocumentServiceRequest.create(
                    OperationType.Head,
                    ResourceId.parse(request.getResourceId()).getDocumentCollectionId().toString(),
                    ResourceType.DocumentCollection,
                    null);
        }

        final Date currentTime = new Date();
        final SimpleDateFormat sdf = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        String xDate = sdf.format(currentTime);
        barrierRequest.getHeaders().put(HttpConstants.HttpHeaders.X_DATE, xDate);
        String token = authorizationTokenProvider.generateKeyAuthorizationSignature("get", barrierRequest.getResourceAddress().toLowerCase(),
                ResourceType.DocumentCollection, barrierRequest.getHeaders());
        try {
            barrierRequest.getHeaders().put(HttpConstants.HttpHeaders.AUTHORIZATION, URLEncoder.encode(token, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Unsupported encoding", e);
        }

        barrierRequest.setForceAddressRefresh(request.isForceAddressRefresh());
        barrierRequest.setRequestChargeTracker(request.getRequestChargeTracker());

        if (request.getPartitionKeyRangeIdentity() != null) {
            barrierRequest.routeTo(request.getPartitionKeyRangeIdentity());
        }
        String partitionKey = request.getHeaders().get(HttpConstants.HttpHeaders.PARTITION_KEY);
        if (partitionKey != null) {
            barrierRequest.getHeaders().put(HttpConstants.HttpHeaders.PARTITION_KEY, partitionKey);
        }

        return barrierRequest;
    }
}
