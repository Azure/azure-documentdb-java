/**
 * The MIT License (MIT)
 * Copyright (c) 2017 Microsoft Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.microsoft.azure.documentdb.examples;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import com.microsoft.azure.documentdb.ConnectionMode;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.FeedResponse;
import com.microsoft.azure.documentdb.internal.GatewayProxy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.FeedOptions;


public class DocumentQuerySamples
{
    private final String databaseId = "CRI";
    private final String collectionId = "AdobeQuery";
    private final String collectionLink = String.format("/dbs/%s/colls/%s", databaseId, collectionId);

    private DocumentClient client;

    @Before
    public void setUp() throws DocumentClientException {
        // create client
        ConnectionPolicy policy = new ConnectionPolicy();
        policy.setConnectionMode(ConnectionMode.Gateway);
        client = new DocumentClient(AccountCredentials.HOST, AccountCredentials.MASTER_KEY, policy, null);

        // removes the database "exampleDB" (if exists)
        //deleteDatabase();
        // create database exampleDB;
        //createDatabase();

        // create collection
        //createMultiPartitionCollection();

        // populate documents
        //populateDocuments();
    }

    @After
    public void shutDown() {
        if (client != null) {
            client.close();
        }
    }

    private void applyCorrelatedActivityIdViaReflection(String correlatedActivityId) {
        Field gatewayProxyField = Arrays.asList(DocumentClient.class.getDeclaredFields())
                                        .stream()
                                        .filter(field -> field.getName().equals("gatewayProxy"))
                                        .findFirst().orElseThrow(() -> new RuntimeException("Field not found"));
        gatewayProxyField.setAccessible(true);
        Object gatewayProxy = null;
        try {
            gatewayProxy = gatewayProxyField.get(client);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        Field defaultHeadersField = Arrays.asList(GatewayProxy.class.getDeclaredFields())
                                          .stream()
                                          .filter(field -> field.getName().equals("defaultHeaders"))
                                          .findFirst().orElseThrow(() -> new RuntimeException("Field not found"));
        defaultHeadersField.setAccessible(true);

        Map<String, String> defaultHeaders = null;
        try {
            defaultHeaders = (Map<String, String>) defaultHeadersField.get(gatewayProxy);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        System.out.println("CORRELATED_ACTIVITY_ID: " + correlatedActivityId);
        defaultHeaders.put("x-ms-cosmos-correlated-activityid", correlatedActivityId);
        defaultHeaders.put("User-Agent", "SampleQuery" + correlatedActivityId);
    }

    private String parseCosmosContinuation(String firstContinuationFragment) {
        if (firstContinuationFragment == "") {
            return null;
        }

        byte[] cosmosContinuationBlob = Base64.getDecoder().decode(firstContinuationFragment);
        return new String(cosmosContinuationBlob, StandardCharsets.UTF_8);
    }

    private Page queryCosmosWithPagination(
        String whereClause,
        String ct, // Format: <CosmosContinuation>|PKRangeIdOfLastDocOnPreviousPage|_rid-OfLastDocOnPreviousPage
        int pageSize) {

        List<Document> docs = new ArrayList<>();
        final AtomicInteger remainingPageSize = new AtomicInteger(pageSize);
        DocumentResourceId skipDocumentsIncludingResourceId = null;
        String skipDocumentsPkRangeId = null;
        FeedOptions options = new FeedOptions();
        options.setEnableCrossPartitionQuery(true);
        options.setResponseContinuationTokenLimitInKb(1);
        options.setPageSize(remainingPageSize.get());
        String nextPageContinuation = null;
        String currentPageContinuation = null;
        if (ct != null) {
            String[] continuationFragments = ct.split(Pattern.quote("|"));
            String cosmosContinuationToken = parseCosmosContinuation(continuationFragments[0]);
            nextPageContinuation = cosmosContinuationToken;
            options.setRequestContinuation(cosmosContinuationToken);

            String skipDocumentsIncludingResourceIdText = continuationFragments.length == 3 ? continuationFragments[1] : null;
            skipDocumentsIncludingResourceId = skipDocumentsIncludingResourceIdText != null
                ? DocumentResourceId.parse(skipDocumentsIncludingResourceIdText)
                : null;
            skipDocumentsPkRangeId = continuationFragments.length == 3 ? continuationFragments[2] : null;
        }
        String sqlQuery = whereClause != null
            ? "SELECT * from c where " + whereClause
            : "SELECT * from c";

        FeedResponse<Document> queryResults = client.queryDocuments(
            collectionLink, sqlQuery, options);
        Iterator<Document> docIterator = queryResults.getQueryIterator();
        String previousActivityId = null;
        String lastResourceId = "";
        String lastResourceIdPkRangeId = "";
        while (remainingPageSize.get() > 0) {
            if (docIterator.hasNext()) {
                String activityId = queryResults.getActivityId();
                // If activityId changed it means we are on the first document of a new page
                // If so, updating the continuations and logging ActivityId and headers
                if (activityId != previousActivityId) {
                    previousActivityId = activityId;
                    System.out.println("New FeedResponse ActivityId " + activityId);
                    currentPageContinuation = nextPageContinuation;
                    nextPageContinuation = queryResults.getResponseContinuation();
                    System.out.println("New FeedResponse Continuation " + currentPageContinuation);
                    System.out.println("New FeedResponse CurrentContinuation " + nextPageContinuation);
                    System.out.println("New FeedResponse Headers " + queryResults.getResponseHeaders());
                    System.out.println("-------------");
                }

                Document doc = docIterator.next();
                String currentPkRangeId = queryResults.getResponseHeaders().get("x-ms-documentdb-partitionkeyrangeid");

                String documentRid = doc.getResourceId();
                DocumentResourceId currentDocumentRid = DocumentResourceId.parse(documentRid);

                // If we are still on the same PKRangeId from which documents were served
                // on the previous page (part of Continuation) then  skip all
                // docs with _rid <= the rid of the last document returned on the previous page
                if (!currentPkRangeId.equals(skipDocumentsPkRangeId)
                    || skipDocumentsIncludingResourceId == null
                    || DocumentResourceId.compareTo(skipDocumentsIncludingResourceId, currentDocumentRid) < 0) {

                    remainingPageSize.decrementAndGet();
                    docs.add(doc);
                    lastResourceId = documentRid;
                    lastResourceIdPkRangeId = queryResults.getResponseHeaders().get("x-ms-documentdb-partitionkeyrangeid");
                } else {
                    System.out.println(
                        "Skipping doc " + doc.getId() + "("
                            + currentPkRangeId + "|" + doc.getResourceId() + "|" + documentRid +
                            ") because it was returned on previous page already");
                }
            } else {
                // Query has been fully drained - just return the docs collected so far
                return new Page(docs, null);
            }
        }

        String base64EncodedCosmosContinuation = currentPageContinuation != null
            ? Base64.getEncoder().encodeToString(currentPageContinuation.getBytes(StandardCharsets.UTF_8))
            : "";
        String returnContinuation = base64EncodedCosmosContinuation
            + "|"
            + lastResourceId
            + "|"
            + lastResourceIdPkRangeId;

        return new Page(docs, returnContinuation);
    }

    @Test
    public void simpleDocumentQuery() {

        applyCorrelatedActivityIdViaReflection(UUID.randomUUID().toString());
        String ct = null;

        // DISCLAIMER: Sample query used for testing purposes only - modify as needed
        String whereClause = "c.expectedProcessTime >= '2019-06-01T00:00' AND c.expectedProcessTime <= '2039-06-01T00:00'";

        int totalCount = 0;
        Set<String> uniqueDocCount = new java.util.HashSet<>();

        do {

            Page page = queryCosmosWithPagination(whereClause, ct, 10);
            System.out.println("CONTINUATION: " + page.getContinuation());
            System.out.println(page.getDocs().size() + " docs");
            totalCount += page.getDocs().size();
            for (Document doc : page.getDocs()) {
                System.out.println("  - " + "(" + doc.getId() + ", " + doc.getResourceId() + ")");;
                uniqueDocCount.add(doc.getId());
            }

            ct = page.getContinuation();
            System.out.println("TOTAL UNIQUE DOC COUNT: " + uniqueDocCount.size());
        } while (ct != null );

        System.out.println("TOTAL DOC COUNT: " + totalCount);

        // DISCLAIMER: uniqueDocCount is only used for testing purposes
        System.out.println("TOTAL UNIQUE DOC COUNT: " + uniqueDocCount.size());
    }

    private static class Page {
        private final List<Document> docs;
        private final String continuation;

        public Page(List<Document> docs, String continuation) {
            this.docs = docs;
            this.continuation = continuation;
        }

        public List<Document> getDocs() { return this.docs; }
        public String getContinuation() { return this.continuation; }
    }

    private static class DocumentResourceId {
        private final int database;
        private final int documentCollection;
        private final long document;

        private DocumentResourceId(int database, int documentCollection, long document) {
            this.database = database;
            this.documentCollection = documentCollection;
            this.document = document;
        }

        private static byte[] verifyAndConvert(String id) {
            byte[] buffer = null;

            try {
                buffer = org.apache.commons.codec.binary.Base64.decodeBase64(id.replace('-', '/'));
            } catch (Exception e) {
                System.out.println("Unable to parse ResourceId from id '" + id + "', Exception: " + e);
                return null;
            }

            if (buffer == null) {
                return null;
            }

            if (buffer.length != 16) {
                System.out.println("Buffer should have size of exactly 16 bytes.");
                return null;
            }

            return buffer;
        }

        // Copy the bytes provided with a for loop, faster when there are only a few
        // bytes to copy
        private static void blockCopy(byte[] src, int srcOffset, byte[] dst, int dstOffset, int count) {
            int stop = srcOffset + count;
            for (int i = srcOffset; i < stop; i++)
                dst[dstOffset++] = src[i];
        }

        public static int compareTo(DocumentResourceId left, DocumentResourceId right) {
            if (left == null) {
                throw new IllegalArgumentException("Argument 'left' must not be null.");
            }

            if (right == null) {
                throw new IllegalArgumentException("Argument 'right' must not be null.");
            }

            if (left.database != right.database) {
                throw new IllegalArgumentException(
                    "Both resource ids must be for the same database to allow comparing them.");
            }

            if (left.documentCollection != right.documentCollection) {
                throw new IllegalArgumentException(
                    "Both resource ids must be for the same collection to allow comparing them.");
            }

            return Long.compare(left.document, right.document);
        }

        public static DocumentResourceId parse(String id) {
            DocumentResourceId rid = null;

            try {
                if (id == null || id.isBlank()) {
                    return null;
                }

                if (id.length() % 4 != 0) {
                    // our ResourceId string is always padded
                    return null;
                }

                byte[] buffer = DocumentResourceId.verifyAndConvert(id);

                if (buffer == null) {
                    return null;
                }

                int database;
                int collection;
                long document;

                database = ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN).getInt();

                byte[] temp = new byte[4];
                DocumentResourceId.blockCopy(buffer, 4, temp, 0, 4);

                boolean isCollection = (temp[0] & (128)) > 0;

                if (!isCollection) {
                    System.out.println("Wrong resourceId - the resource id should be for a collection sub-resource.");
                    return null;
                }

                collection = ByteBuffer.wrap(temp).order(ByteOrder.LITTLE_ENDIAN).getInt();

                byte[] subCollRes = new byte[8];
                DocumentResourceId.blockCopy(buffer, 8, subCollRes, 0, 8);

                long subCollectionResource = ByteBuffer
                    .wrap(buffer, 8, 8)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .getLong();

                // if it is not for a document resource fail
                if ((subCollRes[7] >> 4) != (byte) 0x0) {
                    System.out.println("Wrong resourceId - the resource id should be for a document.");
                    return null;
                }

                document = subCollectionResource;

                return new DocumentResourceId(database, collection, document);
            } catch (Exception e) {
                System.out.println("Unable to parse ResourceId '" + id + "' - Exception: " + e);
                return null;
            }
        }
    }
}
