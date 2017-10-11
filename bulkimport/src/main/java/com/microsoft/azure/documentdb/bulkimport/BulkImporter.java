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

import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.FeedResponse;
import com.microsoft.azure.documentdb.Offer;
import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.PartitionKeyRange;
import com.microsoft.azure.documentdb.internal.routing.CollectionRoutingMap;
import com.microsoft.azure.documentdb.internal.routing.PartitionKeyInternal;
import com.microsoft.azure.documentdb.internal.routing.PartitionKeyRangeCache;
import com.microsoft.azure.documentdb.internal.routing.Range;

public class BulkImporter {

    /**
     * The name of the system stored procedure for bulk import.
     */
    private final static String BULK_IMPORT_STORED_PROCECURE_NAME = "__.sys.commonBulkInsert";

    /**
     * The maximal sproc payload size sent (as a fraction of 2MB).
     */
    private final static int MAX_BULK_IMPORT_SCRIPT_INPUT_SIZE = (2202010 * 5) / 10;


    /**
     * The fraction of maximum sproc payload size upto which documents allowed to be fit in a mini-batch.
     */
    private final static double FRACTION_OF_MAX_BULK_IMPORT_SCRIPT_INPUT_SIZE_ALLOWED = 0.5;

    /**
     * Logger
     */
    private final Logger logger = LoggerFactory.getLogger(BulkImporter.class);

    /**
     * Degree of parallelism for each partition
     */
    private final Map<String, Integer> partitionDegreeOfConcurrency = Collections.synchronizedMap(new HashMap<>());

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
    private final DocumentCollection collection;

    /**
     * The list of degrees of concurrency per partition.
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
     * Initialization Future
     */
    private final Future<Void> initializationFuture;

    /**
     * Collection offer throughput
     */
    private int collectionThroughput;

    /**
     * Initializes a new instance of {@link BulkImporter}
     *
     * @param client {@link DocumentClient} instance to use
     * @param collection inserts documents to {@link DocumentCollection}
     * @throws DocumentClientException if any failure
     */
    public BulkImporter(DocumentClient client, DocumentCollection collection) {

        Preconditions.checkNotNull(client, "DocumentClient cannot be null");
        Preconditions.checkNotNull(collection, "collection cannot be null");

        this.client = client;
        this.collection = collection;

        this.partitionKeyDefinition = collection.getPartitionKey();

        this.listeningExecutorService = MoreExecutors.listeningDecorator(Executors.newWorkStealingPool(client.getConnectionPolicy().getMaxPoolSize()));

        this.initializationFuture = this.listeningExecutorService.submit(new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                initialize();
                return null;
            }
        });
    }

    /**
     * Initializes {@link BulkImporter}. This happens only once
     * @throws DocumentClientException
     */
    private void initialize() throws DocumentClientException {
        logger.debug("Initializing ...");
        String databaseId = collection.getSelfLink().split("/")[1];
        Database d = client.readDatabase(String.format("/dbs/%s", databaseId), null).getResource();

        bulkImportStoredProcLink = String.format("/dbs/%s/colls/%s/sprocs/%s", d.getId(), collection.getId(), BULK_IMPORT_STORED_PROCECURE_NAME);

        logger.trace("Fetching partition map of collection");
        Range<String> fullRange = new Range<String>(
                PartitionKeyInternal.MinimumInclusiveEffectivePartitionKey,
                PartitionKeyInternal.MaximumExclusiveEffectivePartitionKey,
                true,
                false);

        this.collectionRoutingMap = getCollectionRoutingMap(client);
        Collection<PartitionKeyRange> partitionKeyRanges = this.collectionRoutingMap.getOverlappingRanges(fullRange);

        this.partitionKeyRangeIds = partitionKeyRanges.stream().map(partitionKeyRange -> partitionKeyRange.getId()).collect(Collectors.toList());

        FeedResponse<Offer> offers = client.queryOffers(String.format("SELECT * FROM c where c.offerResourceId = '%s'", collection.getResourceId()), null);

        Iterator<Offer> offerIterator = offers.getQueryIterator();
        if (!offerIterator.hasNext()) {
            throw new IllegalStateException("Cannot find Collection's corresponding offer");
        }

        Offer offer = offerIterator.next();
        this.collectionThroughput = offer.getContent().getInt("offerThroughput");

        logger.debug("Initialization completed");
    }

    /**
     * Executes a bulk import in the Azure Cosmos DB database service.
     *
     * <code>
     * DocumentClient client = new DocumentClient(HOST, MASTER_KEY, null, null);
     *
     * String collectionLink = String.format("/dbs/%s/colls/%s", "mydb", "perf-col");
     * DocumentCollection collection = client.readCollection(collectionLink, null).getResource();
     *
     * BulkImporter importer = new BulkImporter(client, collection);
     *
     * List<String> docs = new ArrayList<String>();
     * for(int i = 0; i < 200000; i++) {
     *      String id = UUID.randomUUID().toString();
     *      String mypk = "Seattle";
     *      String v = UUID.randomUUID().toString();
     *      String doc = String.format("{" +
     *              "  \"dataField\": \"%s\"," +
     *              "  \"mypk\": \"%s\"," +
     *              "  \"id\": \"%s\"" +
     *              "}", v, mypk, id);
     *
     *      docs.add(doc);
     * }
     *
     * BulkImportResponse bulkImportResponse = importer.bulkImport(docs.iterator(), false);
     *
     * client.close();
     * </code>
     * @param documents to insert
     * @param enableUpsert whether enable upsert (overwrite if it exists)
     * @return an instance of {@link BulkImportResponse}
     * @throws DocumentClientException if any failure happens
     */
    public BulkImportResponse bulkImport(Iterator<String> documents, boolean enableUpsert) throws DocumentClientException {

        Preconditions.checkNotNull(documents, "documents cannot be null");
        try {
            initializationFuture.get();
            return executeBulkImportAsyncImpl(documents, enableUpsert).get();
        } catch(Exception e) {
            logger.error("Failed to import documents", e);
            throw new DocumentClientException(500, e);
        }
    }

    private ListenableFuture<BulkImportResponse> executeBulkImportAsyncImpl(Iterator<String> documents, boolean enableUpsert) {

        BulkImportStoredProcedureOptions options = new BulkImportStoredProcedureOptions(true, true, null, false, enableUpsert);

        ConcurrentHashMap<String, Set<String>> documentsToImportByPartition = new ConcurrentHashMap<String, Set<String>>();
        ConcurrentHashMap<String, List<List<String>>> miniBatchesToImportByPartition = new ConcurrentHashMap<String, List<List<String>>>();

        for (String partitionKeyRangeId: this.partitionKeyRangeIds) {
            documentsToImportByPartition.put(partitionKeyRangeId,  ConcurrentHashMap.newKeySet());
            miniBatchesToImportByPartition.put(partitionKeyRangeId, new ArrayList<List<String>>());
        }

        // Sort documents into partition buckets.
        logger.debug("Sorting documents into partition buckets");

        Stream<String> stream = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(documents, Spliterator.ORDERED),
                false).parallel();

        stream.forEach(document -> {
            PartitionKeyInternal partitionKeyValue = DocumentAnalyzer.extractPartitionKeyValue(document, partitionKeyDefinition);
            String effectivePartitionKey = partitionKeyValue.getEffectivePartitionKeyString(partitionKeyDefinition, true);
            String partitionRangeId = collectionRoutingMap.getRangeByEffectivePartitionKey(effectivePartitionKey).getId();
            documentsToImportByPartition.get(partitionRangeId).add(document);
        });

        logger.trace("Creating mini batches within each partition bucket");
        int maxMiniBatchSize = (int)Math.floor(MAX_BULK_IMPORT_SCRIPT_INPUT_SIZE * FRACTION_OF_MAX_BULK_IMPORT_SCRIPT_INPUT_SIZE_ALLOWED);

        documentsToImportByPartition.entrySet().parallelStream().forEach(entry -> {

            String partitionRangeId = entry.getKey();

            Set<String> documentsToImportInPartition =  entry.getValue();

            Iterator<String> it = documentsToImportInPartition.iterator();

            while (it.hasNext())
            {
                List<String> currentMiniBatch = new ArrayList<String>();
                int currentMiniBatchSize = 0;
                do {
                    String currentDocument = it.next();
                    int currentDocumentSize = getSizeInBytes(currentDocument);
                    if (currentDocumentSize > maxMiniBatchSize)
                    {
                        logger.error("Document size {} larger than script payload limit. {}", currentDocumentSize, maxMiniBatchSize);
                        throw new UnsupportedOperationException ("Cannot try to import a document whose size is larger than script payload limit.");
                    }

                    currentMiniBatch.add(currentDocument);
                    currentMiniBatchSize += currentDocumentSize;
                } while ((currentMiniBatchSize < maxMiniBatchSize) && (it.hasNext()));

                miniBatchesToImportByPartition.get(partitionRangeId).add(currentMiniBatch);
            }
        });

        logger.debug("Beginning bulk import within each partition bucket");
        Map<String, BatchInserter> batchInserters = new HashMap<String, BatchInserter>();
        Map<String, CongestionController> congestionControllers = new HashMap<String, CongestionController>();

        for (String partitionKeyRangeId: this.partitionKeyRangeIds) {

            BatchInserter batchInserter = new BatchInserter(
                    partitionKeyRangeId,
                    miniBatchesToImportByPartition.get(partitionKeyRangeId),
                    this.client,
                    bulkImportStoredProcLink,
                    options);
            batchInserters.put(partitionKeyRangeId, batchInserter);


            congestionControllers.put(partitionKeyRangeId,
                    new CongestionController(listeningExecutorService, collectionThroughput / partitionKeyRangeIds.size(), partitionKeyRangeId, batchInserter, this.partitionDegreeOfConcurrency.get(partitionKeyRangeId)));
        }

        Stopwatch watch = Stopwatch.createStarted();

        List<ListenableFuture<Void>> futures = congestionControllers.values().parallelStream().map(c -> c.ExecuteAll()).collect(Collectors.toList());
        FutureCombiner<Void> futureContainer = Futures.whenAllComplete(futures);
        AsyncCallable<BulkImportResponse> completeAsyncCallback = new AsyncCallable<BulkImportResponse>() {

            @Override
            public ListenableFuture<BulkImportResponse> call() throws Exception {

                // TODO: this can change so aggregation of the result happen at each
                watch.stop();

                for(String partitionKeyRangeId: partitionKeyRangeIds) {
                    partitionDegreeOfConcurrency.put(partitionKeyRangeId, congestionControllers.get(partitionKeyRangeId).getDegreeOfConcurrency());
                }

                int numberOfDocumentsImported = batchInserters.values().stream().mapToInt(b -> b.getNumberOfDocumentsImported()).sum();
                double totalRequestUnitsConsumed = batchInserters.values().stream().mapToDouble(b -> b.getTotalRequestUnitsConsumed()).sum();

                BulkImportResponse bulkImportResponse = new BulkImportResponse(numberOfDocumentsImported, totalRequestUnitsConsumed, watch.elapsed());

                return Futures.immediateFuture(bulkImportResponse);
            }
        };

        return futureContainer.callAsync(completeAsyncCallback,  MoreExecutors.directExecutor());
    }

    private CollectionRoutingMap getCollectionRoutingMap(DocumentClient client) {
        try {
            // NOTE: Java doesn't have internal access modifier
            // This is only invoked once per Bulk Import Initialization. So this is not costly.
            // TODO: explore other options here (if any)
            Field f = client.getClass().getDeclaredField("partitionKeyRangeCache"); //NoSuchFieldException
            f.setAccessible(true);
            PartitionKeyRangeCache cache = (PartitionKeyRangeCache) f.get(client); //IllegalAccessException

            return cache.tryLookUp(collection.getSelfLink(), null);
        }
        catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private int getSizeInBytes(String document) {
        return document.getBytes(Charset.forName("UTF-8")).length;
    }
}
