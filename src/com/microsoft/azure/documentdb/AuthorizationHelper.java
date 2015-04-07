/* 
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 */

package com.microsoft.azure.documentdb;

import java.lang.IllegalArgumentException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Map;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;

/**
 * This class is used by both client (for generating the auth header with master/system key) and by the G/W when
 * verifying the auth header.
 * 
 */
final class AuthorizationHelper {

    /**
     * This API is a helper method to create auth header based on client request using masterkey.
     * 
     * @param verb the verb.
     * @param resourceId the resource id.
     * @param resourceType the resource type.
     * @param headers the request headers.
     * @param masterKey the master key.
     * @return the key authorization signature.
     */
    public static String GenerateKeyAuthorizationSignature(String verb,
                                                           String resourceId,
                                                           ResourceType resourceType,
                                                           Map<String, String> headers,
                                                           String masterKey) {
        if (verb == null || verb.isEmpty()) {
            throw new IllegalArgumentException("verb");
        }

        if (resourceId == null) {
            resourceId = "";
        }

        if (resourceType == null) {
            throw new IllegalArgumentException("resourceType");
        }

        if (headers == null) {
            throw new IllegalArgumentException("headers");
        }

        if (masterKey == null || masterKey.isEmpty()) {
            throw new IllegalArgumentException("masterKey");
        }

        byte[] decodedBytes = Base64.decodeBase64(masterKey.getBytes());
        SecretKey signingKey = new SecretKeySpec(decodedBytes, "HMACSHA256");

        String text = String.format("%s\n%s\n%s\n",
                                    verb,
                                    AuthorizationHelper.getResourceSegement(resourceType),
                                    resourceId.toLowerCase());

        if (headers.containsKey(HttpConstants.HttpHeaders.X_DATE)) {
            text += headers.get(HttpConstants.HttpHeaders.X_DATE);
        }

        text += '\n';

        if (headers.containsKey(HttpConstants.HttpHeaders.HTTP_DATE)) {
            text += headers.get(HttpConstants.HttpHeaders.HTTP_DATE);
        }

        text += '\n';

        String body = text.toLowerCase();

        Mac mac = null;
        try {
            mac = Mac.getInstance("HMACSHA256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Failed to get an instance of HMACSHA256.",
                                            e);
        }

        try {
            mac.init(signingKey);
        } catch (InvalidKeyException e) {
            throw new IllegalStateException("Failed to initialize the Mac.", e);
        }

        byte[] digest = mac.doFinal(body.getBytes());

        String auth = Helper.encodeBase64String(digest);
    
        String authtoken = "type=master&ver=1.0&sig=" + auth;

        return authtoken;
    }

    /**
     * This API is a helper method to create auth header based on client request using resourceTokens.
     * 
     * @param resourceTokens the resource tokens.
     * @param path the path.
     * @param resourceId the resource id.
     * @return the authorization token.
     */
    public static String GetAuthorizationTokenUsingResourceTokens(Map<String, String> resourceTokens,
                                                                  String path,
                                                                  String resourceId) {
        if (resourceTokens == null) {
            throw new IllegalArgumentException("resourceTokens");
        }

        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path");
        }

        if (resourceId == null || resourceId.isEmpty()) {
            throw new IllegalArgumentException("resourceId");
        }

        if (resourceTokens.containsKey(resourceId) && resourceTokens.get(resourceId) != null) {
            return resourceTokens.get(resourceId);
        } else {
            String[] pathParts = path.split("/");
            String[] resourceTypes = { "dbs", "colls", "docs", "sprocs", "udfs", "triggers", "users", "permissions",
                    "attachments", "media", "conflicts" };
            HashSet<String> resourceTypesSet = new HashSet<String>();
            for (String resourceType : resourceTypes) {
                resourceTypesSet.add(resourceType);
            }

            for (int i = pathParts.length - 1; i >= 0; --i) {

                if (!resourceTypesSet.contains(pathParts[i]) && resourceTokens.containsKey(pathParts[i])) {
                    return resourceTokens.get(pathParts[i]);
                }
            }

            return null;
        }
    }
    
    private static String getResourceSegement(ResourceType resourceType) {
        switch (resourceType) {
        case Attachment:
            return Paths.ATTACHMENTS_PATH_SEGMENT;
        case Database:
            return Paths.DATABASES_PATH_SEGMENT;
        case Conflict:
            return Paths.CONFLICTS_PATH_SEGMENT;
        case Document:
            return Paths.DOCUMENTS_PATH_SEGMENT;
        case DocumentCollection:
            return Paths.COLLECTIONS_PATH_SEGMENT;
        case Offer:
            return Paths.OFFERS_PATH_SEGMENT;
        case Permission:
            return Paths.PERMISSIONS_PATH_SEGMENT;
        case StoredProcedure:
            return Paths.STORED_PROCEDURES_PATH_SEGMENT;
        case Trigger:
            return Paths.TRIGGERS_PATH_SEGMENT;
        case UserDefinedFunction:
            return Paths.USER_DEFINED_FUNCTIONS_PATH_SEGMENT;
        case User:
            return Paths.USERS_PATH_SEGMENT;
        case Media:
            return Paths.MEDIA_PATH_SEGMENT;
        case DatabaseAccount:
            return "";            
        default:
            return null;
        }
    }
}
