/* 
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 */

package com.microsoft.azure.documentdb;

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

/**
 * This is core Transport/Connection agnostic response for DocumentService. It
 * is marked internal today. If needs arises for client to do no-serialized
 * processing we can open this up to public.
 */
final class DocumentServiceResponse implements AutoCloseable {
    private int statusCode;
    private Map<String, String> headersMap = new HashMap<String, String>();
    private HttpEntity httpEntity;

    DocumentServiceResponse(HttpResponse httpResponse) {
        // Gets status code.
        this.statusCode = httpResponse.getStatusLine().getStatusCode();

        // Extracts headers.
        for (Header header : httpResponse.getAllHeaders()) {
            this.headersMap.put(header.getName(), header.getValue());
        }

        // Parses response body.
        this.httpEntity = httpResponse.getEntity();
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
        if (responseBody == null) return null;

        try {
            return c.getConstructor(String.class).newInstance(responseBody);
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
                | NoSuchMethodException | SecurityException e) {
            throw new IllegalStateException("Failed to instantiate class object.", e);
        }
    }

    public <T extends Resource> List<T> getQueryResponse(Class<T> c) {
        String responseBody = this.getReponseBodyAsString();
        if (responseBody == null) return null;

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
                } catch (InstantiationException | IllegalAccessException | IllegalArgumentException |
                        InvocationTargetException | NoSuchMethodException | SecurityException e) {
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
                // We shouldn't call close when a stream is returned, it's the user responsibility to close it.
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

    public static <T extends Resource> String getResourceKey(Class<T> c) {
        if (c.equals(Attachment.class)) {
            return Constants.ResourceKeys.ATTACHMENTS;
        } else if (c.equals(Conflict.class)) {
            return Constants.ResourceKeys.CONFLICTS;
        } else if (c.equals(Database.class)) {
            return Constants.ResourceKeys.DATABASES;
        } else if (c.equals(Document.class)) {
            return Constants.ResourceKeys.DOCUMENTS;
        } else if (c.equals(DocumentCollection.class)) {
            return Constants.ResourceKeys.DOCUMENT_COLLECTIONS;
        } else if (c.equals(Offer.class)) {
            return Constants.ResourceKeys.OFFERS;
        } else if (c.equals(Permission.class)) {
            return Constants.ResourceKeys.PERMISSIONS;
        } else if (c.equals(Trigger.class)) {
            return Constants.ResourceKeys.TRIGGERS;
        } else if (c.equals(StoredProcedure.class)) {
            return Constants.ResourceKeys.STOREDPROCEDURES;
        } else if (c.equals(User.class)) {
            return Constants.ResourceKeys.USERS;
        } else if (c.equals(UserDefinedFunction.class)) {
            return Constants.ResourceKeys.USER_DEFINED_FUNCTIONS;
        }

        throw new IllegalArgumentException("c");
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
