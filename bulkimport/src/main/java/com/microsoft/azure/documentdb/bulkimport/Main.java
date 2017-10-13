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

import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
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
import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.RetryOptions;

public class Main {

    public static final Logger LOGGER  = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws DocumentClientException, InterruptedException, ExecutionException {

        Configuration cfg = parseCommandLineArgs(args);
        
        DocumentClient client = documentClientFrom(cfg);

        String collectionLink = String.format("/dbs/%s/colls/%s", cfg.getDatabaseId(), cfg.getCollectionId());
        // this assumes database and collection already exists
        // also it is a good idea to set your connection pool size to be equal to the number of partitions serving your collection.
        DocumentCollection collection = client.readCollection(collectionLink, null).getResource();

        // instantiates bulk importer
        BulkImporter bulkImporter = new BulkImporter(client, collection);

        Stopwatch totalWatch = Stopwatch.createStarted();
        
        double totalRequestCharge = 0;
        long totalTimeInMillis = 0;
        long totalNumberOfDocumentsImported = 0;

        for(int i = 0 ; i < cfg.getNumberOfCheckpoints(); i++) {

            Iterator<String> inputDocumentIterator = generatedDocuments(cfg, collection);
            System.out.println("##########################################################################################");

            BulkImportResponse bulkImportResponse = bulkImporter.bulkImport(inputDocumentIterator, false);

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
        }

        totalWatch.stop();
        
        System.out.println("##########################################################################################");
        
        System.out.println("Total summed Import time in milli seconds: " + totalTimeInMillis);
        System.out.println("Total Number of documents inserted " + totalNumberOfDocumentsImported);
        System.out.println("Total Import time measured by stop watch in milli seconds: " + totalWatch.elapsed().toMillis());
        System.out.println("Total request unit consumed: " + totalRequestCharge);
        System.out.println("Average RUs/second:" + totalRequestCharge / (totalWatch.elapsed().toMillis() * 0.001));
        System.out.println("Average #Inserts/second: " + totalNumberOfDocumentsImported / (totalWatch.elapsed().toMillis() * 0.001));

        // close bulk importer to release any existing resources
        bulkImporter.close();
        
        // close document client
        client.close();
    }    

    private static Iterator<String> generatedDocuments(Configuration cfg, DocumentCollection collection) {

        PartitionKeyDefinition partitionKeyDefinition = collection.getPartitionKey();
        Preconditions.checkArgument(partitionKeyDefinition != null &&
                partitionKeyDefinition.getPaths().size() > 0, "there is no partition key definition");
        
        Collection<String> partitionKeyPath = partitionKeyDefinition.getPaths();
        Preconditions.checkArgument(partitionKeyPath.size() == 1, 
                "the command line benchmark tool only support simple partition key path");
        
        String partitionKeyName = partitionKeyPath.iterator().next().replaceFirst("^/", "");
        
        // the size of each document is approximately 1KB
        
        // return documents to be bulk imported
        // if you are reading documents from disk you can change this to read documents from disk
        return IntStream.range(0, cfg.getNumberOfDocumentsForEachCheckpoint()).mapToObj(i ->
        {
            
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            sb.append("\"id\":\"").append(UUID.randomUUID().toString()).append("\"");
            sb.append(",\"").append(partitionKeyName).append("\":\"").append(UUID.randomUUID().toString()).append("abc\"");

            String data = UUID.randomUUID().toString();
            data = data + data + "0123456789012";
            
            for(int j = 0; j < 10;j++) {
                sb.append(",").append("\"f").append(j).append("\":\"").append(data).append("\"");
            }
            
            sb.append("}");

            return sb.toString();
        }).iterator();
    }

    public static DocumentClient documentClientFrom(Configuration cfg) throws DocumentClientException {
        
        ConnectionPolicy policy = cfg.getConnectionPolicy();
        RetryOptions retryOptions = new RetryOptions();
        retryOptions.setMaxRetryAttemptsOnThrottledRequests(0);
        policy.setRetryOptions(retryOptions);
        
        return new DocumentClient(cfg.getServiceEndpoint(), cfg.getMasterKey(),
                policy, cfg.getConsistencyLevel());
    }

    private static Configuration parseCommandLineArgs(String[] args) {
        LOGGER.debug("Parsing the arguments ...");
        Configuration cfg = new Configuration();
        
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
