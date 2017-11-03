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
import java.util.List;

import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.FeedResponse;
import com.microsoft.azure.documentdb.Offer;
import com.microsoft.azure.documentdb.RetryOptions;
import com.microsoft.azure.documentdb.bulkimport.DocumentBulkImporter.Builder;
import com.microsoft.azure.documentdb.bulkimport.Main.DataMigrationDocumentSource;

public class Sample {

    public static final String MASTER_KEY = "[YOUR-MASTERKEY]";
    public static final String HOST = "[YOUR-ENDPOINT]";

    public static void main(String[] args) throws Exception {

        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        RetryOptions retryOptions = new RetryOptions();
        // set to 0 to let bulk importer handles throttling
        retryOptions.setMaxRetryAttemptsOnThrottledRequests(0);
        connectionPolicy.setRetryOptions(retryOptions);        
        connectionPolicy.setMaxPoolSize(200);

        try(DocumentClient client = new DocumentClient(HOST, MASTER_KEY, 
                connectionPolicy, ConsistencyLevel.Session)) {

            String collectionLink = String.format("/dbs/%s/colls/%s", "mydb", "mycol");
            // this assumes database and collection already exists
            DocumentCollection collection = client.readCollection(collectionLink, null).getResource();
            int collectionOfferThroughput = getOfferThroughput(client, collection);
            
            Builder bulkImporterBuilder = DocumentBulkImporter.builder().from(
                    client, 
                    "mydb", "mycol",
                    collection.getPartitionKey(),
                    collectionOfferThroughput);

            try(DocumentBulkImporter importer = bulkImporterBuilder.build()) {

                //NOTE: for getting higher throughput please

                // 1)  Set JVM heap size to a large enough number to avoid any memory issue in handling large number of documents.  
                //     Suggested heap size: max(3GB, 3 * sizeof(all documents passed to bulk import in one batch))
                // 2)  there is a pre-processing and warm up time and due that,
                //     you will get higher throughput for bulks with larger number of documents. 
                //     So if you want to import 10,000,000 documents, 
                //     running bulk import 10 times on 10 bulk of documents each of size 1,000,000 is more preferable
                //     than running bulk import 100 times on 100 bulk of documents each of size 100,000 documents. 

                for(int i = 0; i< 10; i++) {
                    Collection<String> docs = DataMigrationDocumentSource.loadDocuments(1000000, collection.getPartitionKey());
                    BulkImportResponse bulkImportResponse = importer.importAll(docs, false);

                    // returned stats
                    System.out.println("Number of documents inserted: " + bulkImportResponse.getNumberOfDocumentsImported());
                    System.out.println("Import total time: " + bulkImportResponse.getTotalTimeTaken());
                    System.out.println("Total request unit consumed: " + bulkImportResponse.getTotalRequestUnitsConsumed());

                    // validate that all documents in this checkpoint inserted
                    if (bulkImportResponse.getNumberOfDocumentsImported() < docs.size()) {
                        System.err.println("Some documents failed to get inserted in this checkpoint."
                                + " This checkpoint has to get retried with upsert enabled");
                        for(int j = 0; j < bulkImportResponse.getErrors().size(); j++) {
                            bulkImportResponse.getErrors().get(j).printStackTrace();
                        }
                        break;
                    }
                }
            }
        }

    }

    private static int getOfferThroughput(DocumentClient client, DocumentCollection collection) {
        FeedResponse<Offer> offers = client.queryOffers(String.format("SELECT * FROM c where c.offerResourceId = '%s'", collection.getResourceId()), null);

        List<Offer> offerAsList = offers.getQueryIterable().toList();
        if (offerAsList.isEmpty()) {
            throw new IllegalStateException("Cannot find Collection's corresponding offer");
        }

        Offer offer = offerAsList.get(0);
        return offer.getContent().getInt("offerThroughput");
    }
}