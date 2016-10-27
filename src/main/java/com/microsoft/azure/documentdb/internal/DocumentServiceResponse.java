/* 
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 */

package com.microsoft.azure.documentdb.internal;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import com.microsoft.azure.documentdb.Attachment;
import com.microsoft.azure.documentdb.Conflict;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.Offer;
import com.microsoft.azure.documentdb.PartitionKeyRange;
import com.microsoft.azure.documentdb.Permission;
import com.microsoft.azure.documentdb.Resource;
import com.microsoft.azure.documentdb.StoredProcedure;
import com.microsoft.azure.documentdb.Trigger;
import com.microsoft.azure.documentdb.User;
import com.microsoft.azure.documentdb.UserDefinedFunction;
import com.microsoft.azure.documentdb.directconnectivity.Address;
import com.microsoft.azure.documentdb.directconnectivity.StoreResponse;

/**
 * This is core Transport/Connection agnostic response for DocumentService. It
 * is marked internal today. If needs arises for client to do no-serialized
 * processing we can open this up to public.
 */
public final class DocumentServiceResponse implements AutoCloseable {
    private int statusCode;
    private Map<String, String> headersMap = new HashMap<String, String>();
    private HttpEntity httpEntity;

    public DocumentServiceResponse(HttpResponse httpResponse) {
        // Gets status code.
        this.statusCode = httpResponse.getStatusLine().getStatusCode();

        // Extracts headers.
        for (Header header : httpResponse.getAllHeaders()) {
            this.headersMap.put(header.getName(), header.getValue());
        }

        // Parses response body.
        this.httpEntity = httpResponse.getEntity();
    }

    public DocumentServiceResponse(StoreResponse response) {
        // Gets status code.
        this.statusCode = response.getStatus();

        // TODO: handle session token

        // Extracts headers.
        for (int i = 0; i < response.getResponseHeaderNames().length; i++) {
            if (!response.getResponseHeaderNames()[i].equals(HttpConstants.HttpHeaders.LSN)) {
                this.headersMap.put(response.getResponseHeaderNames()[i], response.getResponseHeaderValues()[i]);
            }
        }

        // Parses response body.
        this.httpEntity = response.getResponseBody();
    }

    public static <T extends Resource> String getResourceKey(Class<T> c) {
        if (c.equals(Attachment.class)) {
            return InternalConstants.ResourceKeys.ATTACHMENTS;
        } else if (c.equals(Conflict.class)) {
            return InternalConstants.ResourceKeys.CONFLICTS;
        } else if (c.equals(Database.class)) {
            return InternalConstants.ResourceKeys.DATABASES;
        } else if (Document.class.isAssignableFrom(c)) {
            return InternalConstants.ResourceKeys.DOCUMENTS;
        } else if (c.equals(DocumentCollection.class)) {
            return InternalConstants.ResourceKeys.DOCUMENT_COLLECTIONS;
        } else if (c.equals(Offer.class)) {
            return InternalConstants.ResourceKeys.OFFERS;
        } else if (c.equals(PartitionKeyRange.class)) {
            return InternalConstants.ResourceKeys.PARTITION_KEY_RANGES;
        } else if (c.equals(Permission.class)) {
            return InternalConstants.ResourceKeys.PERMISSIONS;
        } else if (c.equals(Trigger.class)) {
            return InternalConstants.ResourceKeys.TRIGGERS;
        } else if (c.equals(StoredProcedure.class)) {
            return InternalConstants.ResourceKeys.STOREDPROCEDURES;
        } else if (c.equals(User.class)) {
            return InternalConstants.ResourceKeys.USERS;
        } else if (c.equals(UserDefinedFunction.class)) {
            return InternalConstants.ResourceKeys.USER_DEFINED_FUNCTIONS;
        } else if (c.equals(Address.class)) {
            return InternalConstants.ResourceKeys.ADDRESSES;
        } else if (c.equals(PartitionKeyRange.class)) {
            return InternalConstants.ResourceKeys.PARTITION_KEY_RANGES;
        }

        throw new IllegalArgumentException("c");
    }

    public int getStatusCode() {
        return this.statusCode;
    }

    public Map<String, String> getResponseHeaders() {
        return this.headersMap;
    }

    public String getReponseBodyAsString() {
        if (this.httpEntity == null) {
            this.close();
            return null;
        }

        try {
            return EntityUtils.toString(this.httpEntity, StandardCharsets.UTF_8);
        } catch (ParseException | IOException e) {
            throw new IllegalStateException("Failed to get UTF-8 string from http entity.", e);
        } finally {
            this.close();
        }
    }

    public <T extends Resource> T getResource(Class<T> c) {
        String responseBody = this.getReponseBodyAsString();
        if (responseBody == null)
            return null;

        try {
            return c.getConstructor(String.class).newInstance(responseBody);
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
                | NoSuchMethodException | SecurityException e) {
            throw new IllegalStateException("Failed to instantiate class object.", e);
        }
    }

    public <T extends Resource> List<T> getQueryResponse(Class<T> c) {
        String responseBody = this.getReponseBodyAsString();
        if (responseBody == null) {
            return new ArrayList<T>();
        }

        JSONObject jobject = new JSONObject(responseBody);
        String resourceKey = DocumentServiceResponse.getResourceKey(c);
        JSONArray jTokenArray = jobject.getJSONArray(resourceKey);

        List<T> queryResults = new ArrayList<T>();

        if (jTokenArray != null) {
            for (int i = 0; i < jTokenArray.length(); ++i) {
                JSONObject jToken = jTokenArray.getJSONObject(i);
                T resource = null;
                try {
                    resource = c.getConstructor(String.class).newInstance(jToken.toString());
                } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
                        | InvocationTargetException | NoSuchMethodException | SecurityException e) {
                    throw new IllegalStateException("Failed to instantiate class object.", e);
                }

                queryResults.add(resource);
            }
        }

        return queryResults;
    }

    public InputStream getContentStream() {
        if (this.httpEntity == null) {
            this.close();
            return null;
        }

        try {
            if (this.httpEntity.isStreaming()) {
                // We shouldn't call close when a stream is returned, it's the
                // user responsibility to close it.
                return this.httpEntity.getContent();
            } else {
                byte[] responseBodyBytes = EntityUtils.toByteArray(this.httpEntity);
                this.close();
                return new ByteArrayInputStream(responseBodyBytes);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to get stream from http entity.", e);
        }
    }

    public void close() {
        if (this.httpEntity != null) {
            try {
                EntityUtils.consume(this.httpEntity);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
