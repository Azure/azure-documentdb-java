package com.microsoft.azure.documentdb.internal;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;

import com.microsoft.azure.documentdb.DocumentClientException;

public class SessionTokenHelper {
    public static void setPartitionLocalSessionToken(DocumentServiceRequest request, SessionContainer sessionContainer) throws DocumentClientException {
        String originalSessionToken = request.getHeaders().get(HttpConstants.HttpHeaders.SESSION_TOKEN);
        String partitionKeyRangeId = request.getResolvedPartitionKeyRangeId();

        // Add support for partitioned collections
        if (StringUtils.isNotEmpty(originalSessionToken)) {
            long sessionLsn = getLocalSessionToken(originalSessionToken, partitionKeyRangeId);
            request.setSessionLsn(sessionLsn);
        } else {
            String sessionToken = sessionContainer.resolveSessionToken(request);
            if (StringUtils.isNotEmpty(sessionToken)) {
                long sessionLsn = getLocalSessionToken(sessionToken, partitionKeyRangeId);
                request.setSessionLsn(sessionLsn);
            }
        }

        if (request.getSessionLsn() == -1) {
            request.getHeaders().remove(HttpConstants.HttpHeaders.SESSION_TOKEN);
        } else {
            request.getHeaders().put(HttpConstants.HttpHeaders.SESSION_TOKEN, String.format("%1s:%2d", "0", request.getSessionLsn()));
        }
    }

    private static long getLocalSessionToken(
            String sessionToken,
            String partitionKeyRangeId) throws DocumentClientException {

        if (partitionKeyRangeId == null || partitionKeyRangeId.isEmpty()) {
            // AddressCache/address resolution didn't produce partition key range id.
            // In this case it is a bug.
            throw new IllegalStateException("Partition key range Id is absent in the context.");
        }

        String[] localTokens = sessionToken.split(",");
        for (String localToken : localTokens) {
            String[] items = localToken.split(":");
            Long tokenLsn = null;
            if (items.length == 2) {
                try {
                    tokenLsn = Long.parseLong(items[1]);
                } catch (NumberFormatException exception) {
                    // do nothing and throw after this
                }
            }
            if (tokenLsn == null) {
                throw new DocumentClientException(HttpStatus.SC_BAD_REQUEST, "Invalid session token value.");
            }
            if (items[0].equals(partitionKeyRangeId)) {
                return tokenLsn;
            }
        }

        return -1;
    }
}
