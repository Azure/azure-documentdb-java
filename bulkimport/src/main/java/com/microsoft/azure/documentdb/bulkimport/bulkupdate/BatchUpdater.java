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
package com.microsoft.azure.documentdb.bulkimport.bulkupdate;

import static com.microsoft.azure.documentdb.bulkimport.ExceptionUtils.isGone;
import static com.microsoft.azure.documentdb.bulkimport.ExceptionUtils.isSplit;
import static com.microsoft.azure.documentdb.bulkimport.ExceptionUtils.isThrottled;
import static com.microsoft.azure.documentdb.bulkimport.ExceptionUtils.isTimedOut;

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
import com.microsoft.azure.documentdb.bulkimport.BatchOperator;
import com.microsoft.azure.documentdb.bulkimport.InsertMetrics;

public class BatchUpdater extends BatchOperator {

	/**
	 *  The count of documents bulk updated by this batch updater.
	 */
	public AtomicInteger numberOfDocumentsUpdated;

	/**
	 * The total request units consumed by this batch updater.
	 */
	public AtomicDouble totalRequestUnitsConsumed;

	/**
	 * The list of mini-batches this batch updater is responsible to updates.
	 */
	private final List<List<UpdateItem>> batchesToUpdate;

	/**
	 * The link to the system bulk update stored procedure.
	 */
	private final String bulkUpdateSprocLink;

	/**
	 * The partition key property.
	 */
	private final String partitionKeyProperty;

	/**
	 * The logger instance.
	 */
	private final Logger logger = LoggerFactory.getLogger(BatchUpdater.class);

	public BatchUpdater(String partitionKeyRangeId, List<List<UpdateItem>> batchesToUpdate, DocumentClient client,
			String bulkUpdateSprocLink, String partitionKeyProperty) {

		this.partitionKeyRangeId = partitionKeyRangeId;
		this.batchesToUpdate = batchesToUpdate;
		this.client = client;
		this.bulkUpdateSprocLink = bulkUpdateSprocLink;
		this.partitionKeyProperty = partitionKeyProperty;
		this.numberOfDocumentsUpdated = new AtomicInteger();
		this.totalRequestUnitsConsumed = new AtomicDouble();

		class RequestOptionsInternal extends RequestOptions {
			RequestOptionsInternal(String partitionKeyRangeId) {
				setPartitionKeyRengeId(partitionKeyRangeId);
			}
		}

		this.requestOptions = new RequestOptionsInternal(partitionKeyRangeId);
	}

	public int getNumberOfDocumentsUpdated() {
		return numberOfDocumentsUpdated.get();
	}

	public double getTotalRequestUnitsConsumed() {
		return totalRequestUnitsConsumed.get();
	}

	public Iterator<Callable<InsertMetrics>> miniBatchExecutionCallableIterator() {

		Stream<Callable<InsertMetrics>> stream = batchesToUpdate.stream().map(miniBatch -> {
			return new Callable<InsertMetrics>() {

				@Override
				public InsertMetrics call() throws Exception {

					try {
						logger.debug("pki {} updating mini batch started", partitionKeyRangeId);
						Stopwatch stopwatch = Stopwatch.createStarted();
						double requestUnitsCounsumed = 0;
						int numberOfThrottles = 0;
						StoredProcedureResponse response;
						boolean timedOut = false;

						int currentUpdateItemIndex = 0;

						while (currentUpdateItemIndex < miniBatch.size() && !cancel) {
							logger.debug("pki {} inside for loop, currentUpdateItemIndex", partitionKeyRangeId, currentUpdateItemIndex);

							List<UpdateItem> updateItemBatch = miniBatch.subList(currentUpdateItemIndex, miniBatch.size());

							boolean isThrottled = false;
							Duration retryAfter = Duration.ZERO;

							try {

								logger.debug("pki {}, Trying to update minibatch of {} update items", partitionKeyRangeId, updateItemBatch.size());

								response = client.executeStoredProcedure(bulkUpdateSprocLink, requestOptions, new Object[] { updateItemBatch, partitionKeyProperty,  null });

								BulkUpdateStoredProcedureResponse bulkUpdateResponse = parseFrom(response);

								if (bulkUpdateResponse != null) {
									if (bulkUpdateResponse.errorCode != 0) {
										logger.warn("pki {} Received response error code {}", partitionKeyRangeId, bulkUpdateResponse.errorCode);
										if (bulkUpdateResponse.count == 0) {
											throw new RuntimeException(
													String.format("Stored proc returned failure %s", bulkUpdateResponse.errorCode));
										}
									}

									double requestCharge = response.getRequestCharge();
									currentUpdateItemIndex += bulkUpdateResponse.count;
									numberOfDocumentsUpdated.addAndGet(bulkUpdateResponse.count);
									requestUnitsCounsumed += requestCharge;
									totalRequestUnitsConsumed.addAndGet(requestCharge);
								}
								else {
									logger.warn("pki {} Failed to receive response", partitionKeyRangeId);
								}

							} catch (DocumentClientException e) {

								logger.debug("pki {} Updating minibatch failed", partitionKeyRangeId, e);

								if (isThrottled(e)) {
									logger.debug("pki {} Throttled on partition range id", partitionKeyRangeId);
									numberOfThrottles++;
									isThrottled = true;
									retryAfter = Duration.ofMillis(e.getRetryAfterInMilliseconds());
									// will retry again

								} else if (isTimedOut(e)) {
									logger.debug("pki {} Request timed out", partitionKeyRangeId);
									timedOut = true;
									// will retry again

								} else if (isGone(e)) {
									// there is no value in retrying
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
									// there is no value in retrying
									String errorMessage = String.format("pki %s failed to update mini-batch. Exception was %s. Status code was %s",
											partitionKeyRangeId,
											e.getMessage(),
											e.getStatusCode());
									logger.error(errorMessage, e);
									throw new RuntimeException(e);
								}

							} catch (Exception e) {
								String errorMessage = String.format("pki %s Failed to update mini-batch. Exception was %s", partitionKeyRangeId,
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
						InsertMetrics insertMetrics = new InsertMetrics(currentUpdateItemIndex, stopwatch.elapsed(), requestUnitsCounsumed, numberOfThrottles);

						return insertMetrics;
					} catch (Exception e) {
						cancel = true;
						throw e;
					}
				}
			};
		});

		return stream.iterator();
	}

	private BulkUpdateStoredProcedureResponse parseFrom(StoredProcedureResponse storedProcResponse) throws JsonParseException, JsonMappingException, IOException {
		String res = storedProcResponse.getResponseAsString();
		logger.debug("MiniBatch Update for Partition Key Range Id {}: Stored Proc Response as String {}", partitionKeyRangeId, res);

		if (StringUtils.isEmpty(res))
			return null;

		return objectMapper.readValue(res, BulkUpdateStoredProcedureResponse.class);
	}
}
