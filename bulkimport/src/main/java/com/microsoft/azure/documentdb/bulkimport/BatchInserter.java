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
package com.microsoft.azure.documentdb.bulkimport;

import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.AtomicDouble;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.RequestOptions;
import com.microsoft.azure.documentdb.StoredProcedureResponse;
import com.microsoft.azure.documentdb.internal.HttpConstants;

class BatchInserter  {

    private final Logger logger = LoggerFactory.getLogger(BatchInserter.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * The index of the physical partition this batch inserter is responsible for.
     */
    private final String partitionKeyRangeId;

    /**
     * The list of mini-batches this batch inserter is responsible to import.
     */
    private final List<List<String>> batchesToInsert;

    /**
     * The document client to use.
     */
    private final DocumentClient client;

    /**
     * The link to the system bulk import stored procedure.
     */
    private final String bulkImportSprocLink;

    /**
     *  The options passed to the system bulk import stored procedure.
     */
    private final BulkImportStoredProcedureOptions storedProcOptions;

    /**
     *  The count of documents bulk imported by this batch inserter.
     */
    public final AtomicInteger numberOfDocumentsImported;

    /**
     * The total request units consumed by this batch inserter.
     */
    public final AtomicDouble totalRequestUnitsConsumed;

    public BatchInserter(String partitionKeyRangeId, List<List<String>> batchesToInsert, DocumentClient client, String bulkImportSprocLink,
            BulkImportStoredProcedureOptions options) {

        this.partitionKeyRangeId = partitionKeyRangeId;
        this.batchesToInsert = batchesToInsert;
        this.client = client;
        this.bulkImportSprocLink = bulkImportSprocLink;
        this.storedProcOptions = options;
        this.numberOfDocumentsImported = new AtomicInteger();
        this.totalRequestUnitsConsumed = new AtomicDouble();
    }

    public int getNumberOfDocumentsImported() {
        return numberOfDocumentsImported.get();
    }

    public double getTotalRequestUnitsConsumed() {
        return totalRequestUnitsConsumed.get();
    }

    public Iterator<Callable<InsertMetrics>> miniBatchInsertExecutionCallableIterator() {

        Stream<Callable<InsertMetrics>> stream = batchesToInsert.stream().map(miniBatch -> {
            return new Callable<InsertMetrics>() {

                @Override
                public InsertMetrics call() throws Exception {

                    logger.debug("pki {} importing mini batch started", partitionKeyRangeId);
                    Stopwatch stopwatch = Stopwatch.createStarted();
                    double requestUnitsCounsumed = 0;
                    int numberOfThrottles = 0;
                    StoredProcedureResponse response;
                    boolean timedOut = false;

                    int currentDocumentIndex = 0;

                    while (currentDocumentIndex < miniBatch.size()) {
                        logger.debug("pki {} inside for loop, currentDocumentIndex", partitionKeyRangeId, currentDocumentIndex);

                        String[] docBatch = miniBatch.subList(currentDocumentIndex, miniBatch.size()).toArray(new String[0]);

                        // Before c# 6, await not allowed in the catch block, hence the following two local variables.
                        boolean isThrottled = false;
                        Duration retryAfter = Duration.ZERO;

                        try {
                            RequestOptions requestOptions = new RequestOptions();
                            // set partition key range id
                            requestOptions.setPartitionKeyRengeId(partitionKeyRangeId);

                            logger.debug("pki {}, Trying to import minibatch of {} documenents", partitionKeyRangeId, docBatch.length);

                            if (!timedOut) {

                                response = client.executeStoredProcedure(bulkImportSprocLink, requestOptions, new Object[] { docBatch, storedProcOptions,  null });

                            } else {

                                BulkImportStoredProcedureOptions modifiedStoredProcOptions = new BulkImportStoredProcedureOptions(
                                        storedProcOptions.disableAutomaticIdGeneration,
                                        storedProcOptions.softStopOnConflict,
                                        storedProcOptions.systemCollectionId,
                                        storedProcOptions.enableBsonSchema,
                                        true);

                                response = client.executeStoredProcedure(
                                        bulkImportSprocLink, requestOptions,
                                        new Object[] { docBatch, modifiedStoredProcOptions, null });
                            }

                            BulkImportStoredProcedureResponse bulkImportResponse = parseFrom(response);

                            if (bulkImportResponse != null) {
                                if (bulkImportResponse.errorCode != 0) {
                                    logger.warn("pki {} Received response error code {}", partitionKeyRangeId, bulkImportResponse.errorCode);
                                }

                                double requestCharge = response.getRequestCharge();
                                currentDocumentIndex += bulkImportResponse.count;
                                numberOfDocumentsImported.addAndGet(bulkImportResponse.count);
                                requestUnitsCounsumed += requestCharge;
                                totalRequestUnitsConsumed.addAndGet(requestCharge);
                            }
                            else {
                                logger.warn("pki {} Failed to receive response", partitionKeyRangeId);
                            }

                        } catch (DocumentClientException e) {

                            logger.debug("pki {} Importing minibatch failed", partitionKeyRangeId, e);

                            if (isThrottled(e)) {
                                logger.debug("pki {} Throttled on partition range id", partitionKeyRangeId);
                                numberOfThrottles++;
                                isThrottled = true;
                                retryAfter = Duration.ofMillis(e.getRetryAfterInMilliseconds());

                            } else if (isTimedOut(e)) {
                                logger.debug("pki {} Request timed out", partitionKeyRangeId);
                                timedOut = true;

                            } else if (isGone(e)) {
                                if (isSplit(e)) {
                                    String errorMessage = String.format("pki %s is undergoing split, please retry shortly after re-initializing BulkImporter object", partitionKeyRangeId);
                                    logger.error(errorMessage);
                                    throw new RuntimeException(errorMessage);
                                } else {
                                    String errorMessage = String.format("pki %s is gone, please retry shortly after re-initializing BulkImporter object", partitionKeyRangeId);
                                    logger.error(errorMessage);
                                    throw new RuntimeException(errorMessage);
                                }

                            } else {
                                String errorMessage = String.format("pki %s failed to import mini-batch. Exception was %s. Status code was %s",
                                        partitionKeyRangeId,
                                        e.getMessage(),
                                        e.getStatusCode());
                                logger.error(errorMessage, e);
                                throw new RuntimeException(e);
                            }

                        } catch (Exception e) {
                            String errorMessage = String.format("pki %s Failed to import mini-batch. Exception was %s", partitionKeyRangeId,
                                    e.getMessage());
                            logger.error(errorMessage, e);
                            throw new RuntimeException(errorMessage, e);
                        }

                        if (isThrottled) {
                            try {
                                logger.debug("pki {} throttled going to sleep for {} millis ", partitionKeyRangeId, retryAfter.toMillis());
                                Thread.sleep(retryAfter.toMillis());
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }

                    logger.debug("pki {} completed", partitionKeyRangeId);

                    stopwatch.stop();
                    InsertMetrics insertMetrics = new InsertMetrics(currentDocumentIndex, stopwatch.elapsed(), requestUnitsCounsumed, numberOfThrottles);

                    return insertMetrics;
                }
            };
        });

        return stream.iterator();
    }

    private boolean isThrottled(DocumentClientException e) {
        return e.getStatusCode() == HttpConstants.StatusCodes.TOO_MANY_REQUESTS;
    }

    private boolean isTimedOut(DocumentClientException e) {
        return e.getStatusCode() == HttpConstants.StatusCodes.TIMEOUT;
    }

    private boolean isGone(DocumentClientException e) {
        return e.getStatusCode() == HttpConstants.StatusCodes.GONE;
    }

    private boolean isSplit(DocumentClientException e) {
        return e.getStatusCode() == HttpConstants.StatusCodes.GONE
                && HttpConstants.SubStatusCodes.SPLITTING == e.getSubStatusCode();
    }

    private BulkImportStoredProcedureResponse parseFrom(StoredProcedureResponse storedProcResponse) throws JsonParseException, JsonMappingException, IOException {
        String res = storedProcResponse.getResponseAsString();
        logger.debug("MiniBatch Insertion for Partition Key Range Id {}: Stored Proc Response as String {}", partitionKeyRangeId, res);

        if (StringUtils.isEmpty(res))
            return null;

        return objectMapper.readValue(res, BulkImportStoredProcedureResponse.class);
    }
}
