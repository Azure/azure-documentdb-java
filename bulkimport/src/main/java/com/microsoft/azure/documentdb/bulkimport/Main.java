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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.FeedResponse;
import com.microsoft.azure.documentdb.Offer;
import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.RetryOptions;
import com.microsoft.azure.documentdb.bulkimport.DocumentBulkImporter.Builder;

public class Main {

    public static final Logger LOGGER  = LoggerFactory.getLogger(Main.class);

    private static int getOfferThroughput(DocumentClient client, DocumentCollection collection) {
        FeedResponse<Offer> offers = client.queryOffers(String.format("SELECT * FROM c where c.offerResourceId = '%s'", collection.getResourceId()), null);

        List<Offer> offerAsList = offers.getQueryIterable().toList();
        if (offerAsList.isEmpty()) {
            throw new IllegalStateException("Cannot find Collection's corresponding offer");
        }

        Offer offer = offerAsList.get(0);
        return offer.getContent().getInt("offerThroughput");
    }
    
    public static void main(String[] args) throws Exception {

        CmdLineConfiguration cfg = parseCommandLineArgs(args);

        try(DocumentClient client = documentClientFrom(cfg)) {

            // set retry options high for initialization
            client.getConnectionPolicy().getRetryOptions().setMaxRetryWaitTimeInSeconds(120);
            client.getConnectionPolicy().getRetryOptions().setMaxRetryAttemptsOnThrottledRequests(100);

            String collectionLink = String.format("/dbs/%s/colls/%s", cfg.getDatabaseId(), cfg.getCollectionId());
            // this assumes database and collection already exists
            // also it is a good idea to set your connection pool size to be equal to the number of partitions serving your collection.
            DocumentCollection collection = client.readCollection(collectionLink, null).getResource();

            int offerThroughput = getOfferThroughput(client, collection);
            
            Builder bulkImporterBuilder = DocumentBulkImporter.builder().from(client, 
                    cfg.getDatabaseId(), cfg.getCollectionId(), collection.getPartitionKey(),
                    offerThroughput);

            // instantiates bulk importer
            try(DocumentBulkImporter bulkImporter = bulkImporterBuilder.build()) {
                
                // then set retries to 0 to pass control to bulk importer
                client.getConnectionPolicy().getRetryOptions().setMaxRetryWaitTimeInSeconds(0);
                client.getConnectionPolicy().getRetryOptions().setMaxRetryAttemptsOnThrottledRequests(0);

                Stopwatch fromStartToEnd = Stopwatch.createStarted();

                Stopwatch totalWatch = Stopwatch.createUnstarted();

                double totalRequestCharge = 0;
                long totalTimeInMillis = 0;
                long totalNumberOfDocumentsImported = 0;

                for(int i = 0 ; i < cfg.getNumberOfCheckpoints(); i++) {

                    BulkImportResponse bulkImportResponse;

                    Collection<String> documents = DataMigrationDocumentSource.loadDocuments(cfg.getNumberOfDocumentsForEachCheckpoint(), collection.getPartitionKey());

                    if (documents.size() !=  cfg.getNumberOfDocumentsForEachCheckpoint()) {
                        throw new RuntimeException("not enough documents generated");
                    }

                    // NOTE: only sum the bulk import time,
                    // loading/generating documents is out of the scope of bulk importer and so has to be excluded
                    totalWatch.start();
                    bulkImportResponse = bulkImporter.importAll(documents, false);
                    totalWatch.stop();

                    System.out.println("##########################################################################################");

                    totalNumberOfDocumentsImported += bulkImportResponse.getNumberOfDocumentsImported();
                    totalTimeInMillis += bulkImportResponse.getTotalTimeTaken().toMillis();
                    totalRequestCharge += bulkImportResponse.getTotalRequestUnitsConsumed();

                    // print stats
                    System.out.println("Number of documents inserted in this checkpoint: " + bulkImportResponse.getNumberOfDocumentsImported());
                    System.out.println("Import time for this checkpoint in milli seconds " + bulkImportResponse.getTotalTimeTaken().toMillis());
                    System.out.println("Total request unit consumed in this checkpoint: " + bulkImportResponse.getTotalRequestUnitsConsumed());

                    System.out.println("Average RUs/second in this checkpoint: " + bulkImportResponse.getTotalRequestUnitsConsumed() / (0.001 * bulkImportResponse.getTotalTimeTaken().toMillis()));
                    System.out.println("Average #Inserts/second in this checkpoint: " + bulkImportResponse.getNumberOfDocumentsImported() / (0.001 * bulkImportResponse.getTotalTimeTaken().toMillis()));
                    System.out.println("##########################################################################################");

                    // check the number of imported documents to ensure everything is successfully imported
                    // bulkImportResponse.getNumberOfDocumentsImported() == documents.size()
                    if (bulkImportResponse.getNumberOfDocumentsImported() != cfg.getNumberOfDocumentsForEachCheckpoint()) {
                        System.err.println("Some documents failed to get inserted in this checkpoint. This checkpoint has to get retried with upsert enabled");
                        System.err.println("Number of surfaced failures: " + bulkImportResponse.getErrors().size());
                        for(int j = 0; j < bulkImportResponse.getErrors().size(); j++) {
                            bulkImportResponse.getErrors().get(j).printStackTrace();
                        }
                        break;
                    }
                }

                fromStartToEnd.stop();

                // print average stats
                System.out.println("##########################################################################################");
                System.out.println("Total import time including data generation: " + fromStartToEnd.elapsed().toMillis());
                System.out.println("Total import time in milli seconds measured by stopWatch: " + totalWatch.elapsed().toMillis());
                System.out.println("Total import time in milli seconds measured by api : " + totalTimeInMillis);
                System.out.println("Total Number of documents inserted " + totalNumberOfDocumentsImported);
                System.out.println("Total request unit consumed: " + totalRequestCharge);
                System.out.println("Average RUs/second:" + totalRequestCharge / (totalWatch.elapsed().toMillis() * 0.001));
                System.out.println("Average #Inserts/second: " + totalNumberOfDocumentsImported / (totalWatch.elapsed().toMillis() * 0.001));

            } // close bulk importer
        } // closes client
    }

    static class DataMigrationDocumentSource {

        private static String generateDocument(String partitionKeyName, String partitionKeyValue) {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            sb.append("\"id\":\"").append(UUID.randomUUID().toString()).append("abc\"");

            String data = UUID.randomUUID().toString();
            data = data + data + "0123456789012";

            for(int j = 0; j < 10;j++) {
                sb.append(",").append("\"f").append(j).append("\":\"").append(data).append("\"");
            }

            // partition key
            sb.append(",\"").append(partitionKeyName).append("\":\"").append(partitionKeyValue).append("\"");

            sb.append("}");

            return sb.toString();
        }

        /**
         * Creates a collection of documents.
         *
         * @param numberOfDocuments
         * @param partitionKeyDefinition
         * @return collection of documents.
         */
        public static Collection<String> loadDocuments(int numberOfDocuments, PartitionKeyDefinition partitionKeyDefinition) {

            Preconditions.checkArgument(partitionKeyDefinition != null &&
                    partitionKeyDefinition.getPaths().size() > 0, "there is no partition key definition");

            Collection<String> partitionKeyPath = partitionKeyDefinition.getPaths();
            Preconditions.checkArgument(partitionKeyPath.size() == 1,
                    "the command line benchmark tool only support simple partition key path");

            String partitionKeyName = partitionKeyPath.iterator().next().replaceFirst("^/", "");

            // the size of each document is approximately 1KB

            ArrayList<String> allDocs = new ArrayList<>(numberOfDocuments);

            // return documents to be bulk imported
            // if you are reading documents from disk you can change this to read documents from disk
            return IntStream.range(0, numberOfDocuments).mapToObj(i ->
            {
                String partitionKeyValue = UUID.randomUUID().toString();
                return generateDocument(partitionKeyName, partitionKeyValue);
            }).collect(Collectors.toCollection(() -> allDocs));
        }

        /**
         * Creates a map of documents to partition key value
         *
         * @param numberOfDocuments
         * @param partitionKeyDefinition
         * @return collection of documents
         */
        public static HashMap<String, Object> loadDocumentToPartitionKeyValueMap(int numberOfDocuments, PartitionKeyDefinition partitionKeyDefinition) {

            Preconditions.checkArgument(partitionKeyDefinition != null &&
                    partitionKeyDefinition.getPaths().size() > 0, "there is no partition key definition");

            Collection<String> partitionKeyPath = partitionKeyDefinition.getPaths();
            Preconditions.checkArgument(partitionKeyPath.size() == 1,
                    "the command line benchmark tool only support simple partition key path");

            String partitionKeyName = partitionKeyPath.iterator().next().replaceFirst("^/", "");

            HashMap<String, Object> documentsToPartitionKeyValue = new HashMap<String, Object>(numberOfDocuments);

            // the size of each document is approximately 1KB

            // return collection of <document, partitionKeyValue> to be bulk imported
            // if you are reading documents from disk you can change this to read documents from disk
            IntStream.range(0, numberOfDocuments).mapToObj(i ->
            {
                String partitionKeyValue = UUID.randomUUID().toString();
                String doc = generateDocument(partitionKeyName, partitionKeyValue);
                return new AbstractMap.SimpleEntry<String, Object>(doc, partitionKeyValue);

            }).forEach(entry -> documentsToPartitionKeyValue.put(entry.getKey(), entry.getValue()));
            return documentsToPartitionKeyValue;
        }
    }

    public static DocumentClient documentClientFrom(CmdLineConfiguration cfg) throws DocumentClientException {

        ConnectionPolicy policy = new ConnectionPolicy();
        RetryOptions retryOptions = new RetryOptions();
        retryOptions.setMaxRetryAttemptsOnThrottledRequests(0);
        policy.setRetryOptions(retryOptions);
        policy.setConnectionMode(cfg.getConnectionMode());
        policy.setMaxPoolSize(cfg.getMaxConnectionPoolSize());

        return new DocumentClient(cfg.getServiceEndpoint(), cfg.getMasterKey(),
                policy, cfg.getConsistencyLevel());
    }

    private static CmdLineConfiguration parseCommandLineArgs(String[] args) {
        LOGGER.debug("Parsing the arguments ...");
        CmdLineConfiguration cfg = new CmdLineConfiguration();

        JCommander jcommander = null;
        try {
            jcommander = new JCommander(cfg, args);
        } catch (Exception e) {
            // invalid command line args
            System.err.println(e.getMessage());
            jcommander = new JCommander(cfg);
            jcommander.usage();
            System.exit(-1);
            return null;
        }

        if (cfg.isHelp()) {
            // prints out the usage help
            jcommander.usage();
            System.exit(0);
            return null;
        }
        return cfg;
    }
}
