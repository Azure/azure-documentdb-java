package com.microsoft.azure.documentdb.internal;

import java.util.Map;

public interface AuthorizationTokenProvider {
    String generateKeyAuthorizationSignature(String verb,
                                             String resourceIdOrFullName,
                                             ResourceType resourceType,
                                             Map<String, String> headers);

    String getAuthorizationTokenUsingResourceTokens(Map<String, String> resourceTokens,
                                                    String path,
                                                    String resourceId);
}
