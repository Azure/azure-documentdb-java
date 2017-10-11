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

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.RetryOptions;

public class Main {

    public static final Logger LOGGER  = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws DocumentClientException, InterruptedException, ExecutionException {

        Configuration cfg = parseCommandLineArgs(args);
        
        DocumentClient client = documentClientFrom(cfg);

        String collectionLink = String.format("/dbs/%s/colls/%s", cfg.getDatabaseId(), cfg.getCollectionId());
        // this assumes database and collection already exists
        DocumentCollection collection = client.readCollection(collectionLink, null).getResource();

        // instantiates bulk importer
        BulkImporter bulkImporter = new BulkImporter(client, collection);

        double totalRequestCharge = 0;
        long totalTimeInMillis = 0;
        long totalNumberOfDocumentsImported = 0;

        for(int i = 0 ; i < cfg.getNumberOfCheckpoints(); i++) {

            Iterator<String> inputDocumentIterator = generatedDocuments(cfg);
            BulkImportResponse bulkImportResponse = bulkImporter.bulkImport(inputDocumentIterator, false);

            totalNumberOfDocumentsImported += bulkImportResponse.getNumberOfDocumentsImported();
            totalTimeInMillis += bulkImportResponse.getTotalTimeTaken().toMillis();
            totalRequestCharge += bulkImportResponse.getTotalRequestUnitsConsumed();

            // print stats
            System.out.println("##########################################################################################");
            System.out.println("Number of documents inserted in this checkpoint: " + bulkImportResponse.getNumberOfDocumentsImported());
            System.out.println("Import time for this checkpoint: " + bulkImportResponse.getTotalTimeTaken());
            System.out.println("Total request unit consumed in this checkpoint: " + bulkImportResponse.getTotalRequestUnitsConsumed());

            System.out.println("Average RUs/second in this checkpoint: " + bulkImportResponse.getTotalRequestUnitsConsumed() / (0.001 * bulkImportResponse.getTotalTimeTaken().toMillis()));
            System.out.println("Average #Inserts/second in this checkpoint: " + bulkImportResponse.getNumberOfDocumentsImported() / (0.001 * bulkImportResponse.getTotalTimeTaken().toMillis()));
            System.out.println("##########################################################################################");
        }

        System.out.println("##########################################################################################");
        System.out.println("##########################################################################################");
        
        System.out.println("Total Number of documents inserted " + totalNumberOfDocumentsImported);
        System.out.println("Total Import time in seconds: " + totalTimeInMillis / 1000);
        System.out.println("Total request unit consumed: " + totalRequestCharge);

        System.out.println("Average RUs/second:" + totalRequestCharge / (totalTimeInMillis * 0.001));
        System.out.println("Average #Inserts/second: " + totalNumberOfDocumentsImported / (totalTimeInMillis * 0.001));

        client.close();
    }    

    private static Iterator<String> generatedDocuments(Configuration cfg) {
        // the size of each document is approximately 1KB
        
        // return documents to be bulk imported
        // if you are reading documents from disk you can change this to read documents from disk
        return IntStream.range(0, cfg.getNumberOfDocumentsForEachCheckpoint()).mapToObj(i ->
        {
            String id = UUID.randomUUID().toString();
            String mypk = UUID.randomUUID().toString();
            String v = UUID.randomUUID().toString();
            String fieldValue = v + "123456789";
            for(int j = 0; j < 24 ; j++) {
                fieldValue += v;
            }
            String doc = String.format("{" +
                    "  \"dataField\": \"%s\"," +
                    "  \"mypk\": \"%s\"," +
                    "  \"id\": \"%s\"" +
                    "}", fieldValue, mypk, id);
            
            //System.out.println("number of bytes in document: " + doc.getBytes(Charset.forName("UTF-8")).length);
            
            return doc;
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
