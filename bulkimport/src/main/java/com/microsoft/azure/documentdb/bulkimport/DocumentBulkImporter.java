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


import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.AsyncCallable;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Futures.FutureCombiner;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.PartitionKeyRange;
import com.microsoft.azure.documentdb.RetryOptions;
import com.microsoft.azure.documentdb.bulkimport.bulkupdate.BatchUpdater;
import com.microsoft.azure.documentdb.bulkimport.bulkupdate.BulkUpdateResponse;
import com.microsoft.azure.documentdb.bulkimport.bulkupdate.UpdateItem;
import com.microsoft.azure.documentdb.internal.HttpConstants;
import com.microsoft.azure.documentdb.internal.routing.CollectionRoutingMap;
import com.microsoft.azure.documentdb.internal.routing.InMemoryCollectionRoutingMap;
import com.microsoft.azure.documentdb.internal.routing.PartitionKeyInternal;
import com.microsoft.azure.documentdb.internal.routing.Range;

public class DocumentBulkImporter implements AutoCloseable {

	public static class Builder {

		private DocumentClient client;
		private String collectionLink;
		private int maxMiniBatchSize = (int) Math.floor(MAX_BULK_IMPORT_SCRIPT_INPUT_SIZE * FRACTION_OF_MAX_BULK_IMPORT_SCRIPT_INPUT_SIZE_ALLOWED);
		private int maxUpdateMiniBatchCount = 10000;
		private final static int DEFAULT_RETRY_ATTEMPT_ON_THROTTLING_FOR_INIT = 200;
		private final static int DEFAULT_WAIT_TIME_ON_THROTTLING_FOR_INIT_IN_SECONDS = 60;

		private PartitionKeyDefinition partitionKeyDef;
		private int offerThroughput;

		private static RetryOptions DEFAULT_INIT_RETRY_OPTIONS;

		static {
			DEFAULT_INIT_RETRY_OPTIONS = new RetryOptions();
			DEFAULT_INIT_RETRY_OPTIONS.setMaxRetryAttemptsOnThrottledRequests(DEFAULT_RETRY_ATTEMPT_ON_THROTTLING_FOR_INIT);
			DEFAULT_INIT_RETRY_OPTIONS.setMaxRetryWaitTimeInSeconds(DEFAULT_WAIT_TIME_ON_THROTTLING_FOR_INIT_IN_SECONDS);
		}

		private RetryOptions retryOptions = DEFAULT_INIT_RETRY_OPTIONS; 

		/**
		 * Use the instance of {@link DocumentClient} to bulk import to the given instance of {@link DocumentCollection}
		 * @param client an instance of {@link DocumentClient}
		 * @param partitionKeyDef specifies the {@link PartitionKeyDefinition} of the collection
		 * @param databaseName name of the database
		 * @param collectionName name of the collection
		 * @param offerThroughput specifies the collection throughput
		 * @return an instance of {@link Builder}
		 */
		public Builder from(DocumentClient client,
				String databaseName, 
				String collectionName,
				PartitionKeyDefinition partitionKeyDef,
				int offerThroughput) {

			// TODO: validate the retry options for the client
			this.client = client;
			this.collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
			this.partitionKeyDef = partitionKeyDef;
			this.offerThroughput = offerThroughput;
			return this;
		}
		
		/**
		 * use the given size to configure max mini batch size.
		 *
		 * If not specified will use the default.
		 * @param size specifies the size of mini batch.
		 * @return {@link Builder}
		 */
		public Builder withMaxMiniBatchSize(int size) {
			Preconditions.checkArgument(size > 0, "maxMiniBatchSize cannot be negative");
			Preconditions.checkArgument(size <= MAX_BULK_IMPORT_SCRIPT_INPUT_SIZE, "maxMiniBatchSize cannot be negative");

			this.maxMiniBatchSize = size;
			return this;
		}
		
		/**
		 * use the given count to configure max update mini batch count.
		 *
		 * If not specified will use the default.
		 * @param size specifies the count of update mini batch.
		 * @return {@link Builder}
		 */
		public Builder withMaxUpdateMiniBatchCount(int count) {
			Preconditions.checkArgument(count > 0, "maxUpdateMiniBatchCount cannot be negative");

			this.maxUpdateMiniBatchCount = count;
			return this;
		}

		/**
		 * use the given retry option for initialization 
		 * 
		 * @param options an instance of {@link RetryOptions}
		 * @return {@link Builder}
		 */
		public Builder withInitializationRetryOptions(RetryOptions options) {
			this.retryOptions = options;
			return this;
		}

		/**
		 * Instantiates {@link DocumentBulkImporter} given the configured {@link Builder}.
		 *
		 * @return the new builder
		 * @throws Exception if there is any failure
		 */
		public DocumentBulkImporter build() throws Exception {
			DocumentBulkImporter importer = new DocumentBulkImporter(client, collectionLink, partitionKeyDef, offerThroughput);
			try {
				importer.setInitializationRetryOptions(retryOptions);
				importer.setMaxMiniBatchSize(maxMiniBatchSize);
				importer.setMaxUpdateMiniBatchCount(maxUpdateMiniBatchCount);
				
				importer.safeInit();

			} catch (Exception e) {
				importer.close();
				throw e;
			}
			return importer;
		}

		private Builder() {}
	}

	/**
	 * Creates a new {@link DocumentBulkImporter.Builder} instance
	 * @return an instance of {@link DocumentBulkImporter.Builder}
	 */
	public static DocumentBulkImporter.Builder builder() {
		return new DocumentBulkImporter.Builder();
	}

	/**
	 * The name of the system stored procedure for bulk import.
	 */
	private final static String BULK_IMPORT_STORED_PROCECURE_NAME = "__.sys.commonBulkInsert";

	/**
	 * The name of the stored procedure for bulk update.
	 */
	private final static String BULK_UPDATE_STORED_PROCECURE_NAME = "__bulkPatch";

	/**
	 * The maximal sproc payload size sent (as a fraction of 2MB).
	 */
	private final static int MAX_BULK_IMPORT_SCRIPT_INPUT_SIZE = (2202010 * 5) / 10;

	/**
	 * The fraction of maximum sproc payload size up to which documents allowed to be fit in a mini-batch.
	 */
	private final static double FRACTION_OF_MAX_BULK_IMPORT_SCRIPT_INPUT_SIZE_ALLOWED = 0.20;

	/**
	 * Initialization sleep time on 
	 */
	private final static int INITIALIZATION_SLEEP_TIME_ON_THROTTLING = 500;

	/**
	 * Logger
	 */
	private final Logger logger = LoggerFactory.getLogger(DocumentBulkImporter.class);

	/**
	 * Degree of parallelism for each partition which was inferred from previous batch execution.
	 */
	private final Map<String, Integer> partitionKeyRangeIdToInferredDegreeOfParallelism = new ConcurrentHashMap<>();

	/**
	 * Executor Service
	 */
	private final ListeningExecutorService listeningExecutorService;

	/**
	 * The DocumentDB client instance.
	 */
	private final DocumentClient client;

	/**
	 * The document collection to which documents are to be bulk imported.
	 */
	private final String collectionLink;

	/**
	 * Partition Key Definition of the underlying collection.
	 */
	private final PartitionKeyDefinition partitionKeyDefinition;

	/**
	 * Partition Key Range Ids
	 */
	private List<String> partitionKeyRangeIds;

	/**
	 * Collection routing map used to retrieve partition key range Ids of a given collection
	 */
	private CollectionRoutingMap collectionRoutingMap;

	/**
	 * Bulk Import Stored Procedure Link relevant to the given collection
	 */
	private String bulkImportStoredProcLink;

	/**
	 * Bulk Update Stored Procedure Link relevant to the given collection
	 */
	private String bulkUpdateStoredProcLink;

	/**
	 * Collection offer throughput
	 */
	private int collectionThroughput;

	/**
	 * Max Mini Batch Size
	 */
	private int maxMiniBatchSize;
	
	/**
	 * Max Update Mini Batch Count
	 */
	private int maxUpdateMiniBatchCount;

	private RetryOptions retryOptions;

	private void setMaxMiniBatchSize(int size) {
		this.maxMiniBatchSize = size;
	}
	
	private void setMaxUpdateMiniBatchCount(int count) {
		this.maxUpdateMiniBatchCount = count;
	}

	private void setInitializationRetryOptions(RetryOptions options) {
		this.retryOptions = options;
	}

	/**
	 * Initializes a new instance of {@link DocumentBulkImporter}
	 *
	 * @param client {@link DocumentClient} instance to use
	 * @param collectionLink inserts documents to the specified collection
	 * @param maxMiniBatchSize
	 * @throws DocumentClientException if any failure
	 */
	private DocumentBulkImporter(DocumentClient client, 
			String collectionLink,
			PartitionKeyDefinition partitionKeyDefinition,
			int collectionOfferThroughput) {
		Preconditions.checkNotNull(client, "client cannot be null");
		Preconditions.checkNotNull(partitionKeyDefinition, "partitionKeyDefinition cannot be null");
		Preconditions.checkNotNull(collectionLink, "collectionLink cannot be null");
		Preconditions.checkArgument(collectionOfferThroughput > 0, "collection throughput is less than 10,000");

		this.client = client;
		this.collectionLink = collectionLink;
		this.collectionThroughput =  collectionOfferThroughput;
		this.partitionKeyDefinition = partitionKeyDefinition;
		this.listeningExecutorService = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
	}

	private void safeInit() throws Exception {
		int count = 0;
		long startTime = System.currentTimeMillis();
		while(true) {
			try {
				initialize();
				break;
			} catch (Exception e) {
				count++;
				DocumentClientException dce = ExceptionUtils.getThrottelingException(e);
				long now = System.currentTimeMillis();
				if (count < retryOptions.getMaxRetryAttemptsOnThrottledRequests() 
						&& now - startTime < (retryOptions.getMaxRetryWaitTimeInSeconds() * 1000)
						&& dce != null
						&& dce.getStatusCode() == HttpConstants.StatusCodes.TOO_MANY_REQUESTS ) {
					Thread.sleep(count * dce.getRetryAfterInMilliseconds() + INITIALIZATION_SLEEP_TIME_ON_THROTTLING);
					continue;
				} else {
					throw e;
				}
			}
		}
	}

	/**
	 * Releases any internal resources.
	 * It is responsibility of the caller to close {@link DocumentClient}.
	 */
	@Override
	public void close() {
		// disable submission of new tasks
		listeningExecutorService.shutdown();
		try {
			// wait for existing tasks to terminate
			if (!listeningExecutorService.awaitTermination(60, TimeUnit.SECONDS)) {
				// cancel any currently running executing tasks
				listeningExecutorService.shutdownNow();
				// wait for cancelled tasks to terminate
				if (!listeningExecutorService.awaitTermination(60, TimeUnit.SECONDS)) {
					logger.error("some tasks did not terminate");
				}
			}
		} catch (InterruptedException e) {
			listeningExecutorService.shutdownNow();
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * Initializes {@link DocumentBulkImporter}. This happens only once
	 * @throws DocumentClientException
	 */
	private void initialize() throws DocumentClientException {
		logger.debug("Initializing ...");

		this.bulkImportStoredProcLink = String.format("%s/sprocs/%s", collectionLink, BULK_IMPORT_STORED_PROCECURE_NAME);
		this.bulkUpdateStoredProcLink = String.format("%s/sprocs/%s", collectionLink, BULK_UPDATE_STORED_PROCECURE_NAME);

		logger.trace("Fetching partition map of collection");
		Range<String> fullRange = new Range<String>(
				PartitionKeyInternal.MinimumInclusiveEffectivePartitionKey,
				PartitionKeyInternal.MaximumExclusiveEffectivePartitionKey,
				true,
				false);

		this.collectionRoutingMap = getCollectionRoutingMap(client);
		Collection<PartitionKeyRange> partitionKeyRanges = this.collectionRoutingMap.getOverlappingRanges(fullRange);

		this.partitionKeyRangeIds = partitionKeyRanges.stream().map(partitionKeyRange -> partitionKeyRange.getId()).collect(Collectors.toList());

		logger.debug("Initialization completed");
	}

	/**
	 * Executes a bulk import in the Azure Cosmos DB database service.
	 * <blockquote>
	 * <pre>
	 * {@code
	 * ConnectionPolicy connectionPolicy = new ConnectionPolicy();
	 * RetryOptions retryOptions = new RetryOptions();
	 * // set to 0 to let bulk importer handles throttling
	 * retryOptions.setMaxRetryAttemptsOnThrottledRequests(0);
	 * connectionPolicy.setRetryOptions(retryOptions);
	 * connectionPolicy.setMaxPoolSize(200);
	 *
	 * DocumentClient client = new DocumentClient(HOST, MASTER_KEY, connectionPolicy, null);
	 *
	 * String collectionLink = String.format("/dbs/%s/colls/%s", "mydb", "mycol");
	 * DocumentCollection collection = client.readCollection(collectionLink, null).getResource();
	 *
	 * DocumentBulkImporter importer = DocumentBulkImporter.builder().from(client, collection).build();
	 *
	 * for(int i = 0; i < 10; i++) {
	 *   List<String> documents = documentSource.getMoreDocuments();
	 *
	 *   BulkImportResponse bulkImportResponse = importer.importAll(documents, false);
	 *
	 *   //validate that all documents inserted to ensure no failure.
	 *   // bulkImportResponse.getNumberOfDocumentsImported() == documents.size()
	 *   if (bulkImportResponse.getNumberOfDocumentsImported() < documents.size()) {
	 *      for(Exception e: bulkImportResponse.getFailuresIfAny()) {
	 *          // validate why there were some failures
	 *          // in case if you decide to re-import these batch of documents 
	 *          // (as some of them may already have inserted)
	 *          // you should import them with upsert enable option
	 *          e.printStackTrace();
	 *      }
	 *      break;
	 *   }
	 * }
	 *
	 * importer.close();
	 * client.close();
	 * }
	 * </pre>
	 * </blockquote>
	 * 
	 * @param documents to insert
	 * @param isUpsert whether enable upsert (overwrite if it exists)
	 * @return an instance of {@link BulkImportResponse}
	 * @throws DocumentClientException if any failure happens
	 */
	public BulkImportResponse importAll(Collection<String> documents, boolean isUpsert) throws DocumentClientException {
		return executeBulkImportInternal(documents,
				isUpsert);
	}

	/**
	 * Executes a bulk update in the Azure Cosmos DB database service.
	 * 
	 * @param updateItems which comprise of id, partition key and list of update operations
	 * @return an instance of {@link BulkImportResponse}
	 * @throws DocumentClientException if any failure happens
	 */
	public BulkUpdateResponse updateAll(Collection<UpdateItem> updateItems) throws DocumentClientException {
		return executeBulkUpdateInternal(updateItems);
	}

	private BulkImportResponse executeBulkImportInternal(Collection<String> input,
			boolean isUpsert) throws DocumentClientException {
		Preconditions.checkNotNull(input, "document collection cannot be null");
		try {
			return executeBulkImportAsyncImpl(input, isUpsert).get();

		} catch (ExecutionException e) {
			logger.debug("Failed to import documents", e);
			Throwable cause = e.getCause();
			if (cause instanceof Exception) {
				throw toDocumentClientException((Exception) cause);
			} else {
				throw toDocumentClientException(e);
			}
		} catch(Exception e) {
			logger.error("Failed to import documents", e);
			throw toDocumentClientException(e);
		}
	}

	private BulkUpdateResponse executeBulkUpdateInternal(Collection<UpdateItem> updateItems)
			throws DocumentClientException {
		Preconditions.checkNotNull(updateItems, "update items cannot be null");
		try {
			return executeBulkUpdateAsyncImpl(updateItems).get();

		} catch (ExecutionException e) {
			logger.debug("Failed to update documents", e);
			Throwable cause = e.getCause();
			if (cause instanceof Exception) {
				throw toDocumentClientException((Exception) cause);
			} else {
				throw toDocumentClientException(e);
			}
		} catch(Exception e) {
			logger.error("Failed to update documents", e);
			throw toDocumentClientException(e);
		}
	}

	private ListenableFuture<BulkImportResponse> executeBulkImportAsyncImpl(Collection<String> documents,
			boolean isUpsert) throws Exception {
		Stopwatch watch = Stopwatch.createStarted();

		BulkImportStoredProcedureOptions options = new BulkImportStoredProcedureOptions(true, true, null, false, isUpsert);

		logger.debug("Bucketing documents ...");

		ConcurrentHashMap<String, Set<String>> documentsToImportByPartition = new ConcurrentHashMap<String, Set<String>>();
		ConcurrentHashMap<String, List<List<String>>> miniBatchesToImportByPartition = new ConcurrentHashMap<String, List<List<String>>>();

		for (String partitionKeyRangeId: partitionKeyRangeIds) {
			documentsToImportByPartition.put(partitionKeyRangeId,  ConcurrentHashMap.newKeySet(documents.size() / partitionKeyRangeIds.size()));
			miniBatchesToImportByPartition.put(partitionKeyRangeId, new ArrayList<List<String>>(1000));
		}

		documents.parallelStream().forEach(documentAsString -> {
			PartitionKeyInternal partitionKeyValue = DocumentAnalyzer.extractPartitionKeyValue(documentAsString, partitionKeyDefinition);
			String effectivePartitionKey = partitionKeyValue.getEffectivePartitionKeyString(partitionKeyDefinition, true);
			String partitionRangeId = collectionRoutingMap.getRangeByEffectivePartitionKey(effectivePartitionKey).getId();
			documentsToImportByPartition.get(partitionRangeId).add(documentAsString);
		});

		logger.trace("Creating mini batches within each partition bucket");

		documentsToImportByPartition.entrySet().parallelStream().forEach(entry -> {

			String partitionRangeId = entry.getKey();

			Set<String> documentsToImportInPartition =  entry.getValue();

			Iterator<String> it = documentsToImportInPartition.iterator();
			ArrayList<String> currentMiniBatch = new ArrayList<String>(500);
			int currentMiniBatchSize = 0;

			while (it.hasNext()) {
				String currentDocument = it.next();
				int currentDocumentSize = getDocumentSizeOrThrow(currentDocument);

				if ((currentMiniBatchSize + currentDocumentSize <= maxMiniBatchSize)) {
					// add the document to current batch
					currentMiniBatch.add(currentDocument);
					currentMiniBatchSize += currentDocumentSize;
				} else {
					// this batch has reached its max size
					miniBatchesToImportByPartition.get(partitionRangeId).add(currentMiniBatch);
					currentMiniBatch = new ArrayList<String>(500);
					currentMiniBatch.add(currentDocument);
					currentMiniBatchSize = currentDocumentSize;
				}
			}

			if (currentMiniBatch.size() > 0) {
				// add mini batch
				miniBatchesToImportByPartition.get(partitionRangeId).add(currentMiniBatch);
			}
		});

		logger.debug("Beginning bulk import within each partition bucket");
		Map<String, BatchInserter> batchInserters = new HashMap<String, BatchInserter>();
		Map<String, CongestionController> congestionControllers = new HashMap<String, CongestionController>();

		logger.debug("Preprocessing took: " + watch.elapsed().toMillis() + " millis");
		List<ListenableFuture<Void>> futures = new ArrayList<>();

		for (String partitionKeyRangeId: this.partitionKeyRangeIds) {

			BatchInserter batchInserter = new BatchInserter(
					partitionKeyRangeId,
					miniBatchesToImportByPartition.get(partitionKeyRangeId),
					this.client,
					bulkImportStoredProcLink,
					options);
			batchInserters.put(partitionKeyRangeId, batchInserter);

			CongestionController cc = new CongestionController(listeningExecutorService,
					collectionThroughput / partitionKeyRangeIds.size(),
					partitionKeyRangeId,
					batchInserter,
					partitionKeyRangeIdToInferredDegreeOfParallelism.get(partitionKeyRangeId));

			congestionControllers.put(partitionKeyRangeId,cc);

			// starting
			futures.add(cc.executeAllAsync());
		}

		FutureCombiner<Void> futureContainer = Futures.whenAllComplete(futures);
		AsyncCallable<BulkImportResponse> completeAsyncCallback = new AsyncCallable<BulkImportResponse>() {

			@Override
			public ListenableFuture<BulkImportResponse> call() throws Exception {

				List<Exception> failures = new ArrayList<>();

				for(String partitionKeyRangeId: partitionKeyRangeIds) {
					CongestionController cc = congestionControllers.get(partitionKeyRangeId);
					failures.addAll(cc.getFailures());
					partitionKeyRangeIdToInferredDegreeOfParallelism.put(partitionKeyRangeId, cc.getDegreeOfConcurrency());
				}

				int numberOfDocumentsImported = batchInserters.values().stream().mapToInt(b -> b.getNumberOfDocumentsImported()).sum();
				double totalRequestUnitsConsumed = batchInserters.values().stream().mapToDouble(b -> b.getTotalRequestUnitsConsumed()).sum();

				watch.stop();

				BulkImportResponse bulkImportResponse = new
						BulkImportResponse(numberOfDocumentsImported, totalRequestUnitsConsumed, watch.elapsed(), failures);

				return Futures.immediateFuture(bulkImportResponse);
			}
		};

		return futureContainer.callAsync(completeAsyncCallback, listeningExecutorService);
	}

	private ListenableFuture<BulkUpdateResponse> executeBulkUpdateAsyncImpl(Collection<UpdateItem> updateItems) {
		Stopwatch watch = Stopwatch.createStarted();

		logger.debug("Bucketing update items ...");

		ConcurrentHashMap<String, Set<UpdateItem>> updateItemsByPartition = new ConcurrentHashMap<String, Set<UpdateItem>>();
		ConcurrentHashMap<String, List<List<UpdateItem>>> miniBatchesToUpdateByPartition = new ConcurrentHashMap<String, List<List<UpdateItem>>>();

		for (String partitionKeyRangeId: partitionKeyRangeIds) {
			updateItemsByPartition.put(partitionKeyRangeId,  ConcurrentHashMap.newKeySet(updateItems.size() / partitionKeyRangeIds.size()));
			miniBatchesToUpdateByPartition.put(partitionKeyRangeId, new ArrayList<List<UpdateItem>>(1000));
		}

		updateItems.parallelStream().forEach(updateItem -> {
			PartitionKeyInternal partitionKeyValue = DocumentAnalyzer.fromPartitionKeyvalue(updateItem.getPartitionKeyValue());
			String effectivePartitionKey = partitionKeyValue.getEffectivePartitionKeyString(partitionKeyDefinition, true);
			String partitionRangeId = collectionRoutingMap.getRangeByEffectivePartitionKey(effectivePartitionKey).getId();
			updateItemsByPartition.get(partitionRangeId).add(updateItem);
		});

		logger.trace("Creating mini batches within each partition bucket");

		updateItemsByPartition.entrySet().parallelStream().forEach(entry -> {

			String partitionRangeId = entry.getKey();
			Set<UpdateItem> updateItemsInPartition =  entry.getValue();

			Iterator<UpdateItem> it = updateItemsInPartition.iterator();
			ArrayList<UpdateItem> currentMiniBatch = new ArrayList<UpdateItem>(500);
			int currentMiniBatchIndex = 0;

			while (it.hasNext()) {
				UpdateItem currentUpdateItem = it.next();

				if ((currentMiniBatchIndex + 1 <= maxUpdateMiniBatchCount)) {
					// add the update item to current batch
					currentMiniBatch.add(currentUpdateItem);
				} else {
					// this batch has reached its max size
					miniBatchesToUpdateByPartition.get(partitionRangeId).add(currentMiniBatch);
					currentMiniBatch = new ArrayList<UpdateItem>(500);
					currentMiniBatch.add(currentUpdateItem);
					currentMiniBatchIndex = 1;
				}
			}

			if (currentMiniBatch.size() > 0) {
				// add mini batch
				miniBatchesToUpdateByPartition.get(partitionRangeId).add(currentMiniBatch);
			}
		});

		logger.debug("Beginning bulk update within each partition bucket");
		Map<String, BatchUpdater> batchUpdaters = new HashMap<String, BatchUpdater>();
		Map<String, CongestionController> congestionControllers = new HashMap<String, CongestionController>();

		logger.debug("Preprocessing took: " + watch.elapsed().toMillis() + " millis");
		List<ListenableFuture<Void>> futures = new ArrayList<>();

		// Note: we handle only simple partition key path at the moment.
		Collection<String> partitionKeyPath = partitionKeyDefinition.getPaths();
		String partitionKeyProperty = partitionKeyPath.iterator().next().replaceFirst("^/", "");

		for (String partitionKeyRangeId: this.partitionKeyRangeIds) {

			BatchUpdater batchUpdater = new BatchUpdater(
					partitionKeyRangeId,
					miniBatchesToUpdateByPartition.get(partitionKeyRangeId),
					this.client,
					bulkUpdateStoredProcLink,
					partitionKeyProperty);
			batchUpdaters.put(partitionKeyRangeId, batchUpdater);

			CongestionController cc = new CongestionController(listeningExecutorService,
					collectionThroughput / partitionKeyRangeIds.size(),
					partitionKeyRangeId,
					batchUpdater,
					partitionKeyRangeIdToInferredDegreeOfParallelism.get(partitionKeyRangeId));

			congestionControllers.put(partitionKeyRangeId,cc);

			// starting
			futures.add(cc.executeAllAsync());
		}

		FutureCombiner<Void> futureContainer = Futures.whenAllComplete(futures);
		AsyncCallable<BulkUpdateResponse> completeAsyncCallback = new AsyncCallable<BulkUpdateResponse>() {

			@Override
			public ListenableFuture<BulkUpdateResponse> call() throws Exception {

				List<Exception> failures = new ArrayList<>();

				for(String partitionKeyRangeId: partitionKeyRangeIds) {
					CongestionController cc = congestionControllers.get(partitionKeyRangeId);
					failures.addAll(cc.getFailures());
					partitionKeyRangeIdToInferredDegreeOfParallelism.put(partitionKeyRangeId, cc.getDegreeOfConcurrency());
				}

				int numberOfDocumentsImported = batchUpdaters.values().stream().mapToInt(b -> b.getNumberOfDocumentsUpdated()).sum();
				double totalRequestUnitsConsumed = batchUpdaters.values().stream().mapToDouble(b -> b.getTotalRequestUnitsConsumed()).sum();

				watch.stop();

				BulkUpdateResponse bulkUpdateResponse = new
						BulkUpdateResponse(numberOfDocumentsImported, totalRequestUnitsConsumed, watch.elapsed(), failures);

				return Futures.immediateFuture(bulkUpdateResponse);
			}
		};

		return futureContainer.callAsync(completeAsyncCallback, listeningExecutorService);
	}

	private DocumentClientException toDocumentClientException(Exception e) {
		if (e instanceof DocumentClientException) {
			return (DocumentClientException) e;
		} else {
			return new DocumentClientException(500, e);
		}
	}

	private int getDocumentSizeOrThrow(String document) {
		int documentSize = document.getBytes(Charset.forName("UTF-8")).length;
		if (documentSize > maxMiniBatchSize) {
			logger.error("Document size {} larger than script payload limit. {}", documentSize, maxMiniBatchSize);
			throw new UnsupportedOperationException("Cannot import a document whose size is larger than script payload limit.");
		}
		return documentSize;
	}

	private CollectionRoutingMap getCollectionRoutingMap(DocumentClient client) {
		List<ImmutablePair<PartitionKeyRange, Boolean>> ranges = new ArrayList<>();

		for (PartitionKeyRange range : client.readPartitionKeyRanges(this.collectionLink, (FeedOptions) null).getQueryIterable().toList()) {
			ranges.add(new ImmutablePair<>(range, true));
		}

		CollectionRoutingMap routingMap = InMemoryCollectionRoutingMap.tryCreateCompleteRoutingMap(ranges,
				StringUtils.EMPTY);

		if (routingMap == null) {
			throw new IllegalStateException("Cannot create complete routing map");
		}

		return routingMap;
	}
}
