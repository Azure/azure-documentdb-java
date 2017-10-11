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
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.google.common.base.Stopwatch;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;

public class Main {

    public static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws DocumentClientException, InterruptedException, ExecutionException {

        LOGGER.debug("Parsing the arguments ...");
        Configuration cfg = new Configuration();
        JCommander jcommander = new JCommander(cfg, args);


        if (cfg.isHelp()) {
            // prints out the usage help
            jcommander.usage();
            return;
        }

        DocumentClient client = fromConfiguration(cfg);


        String collectionLink = String.format("/dbs/%s/colls/%s", cfg.getDatabaseId(), cfg.getCollectionId());
        // this assumes database and collection already exists
        DocumentCollection collection = client.readCollection(collectionLink, null).getResource();

        BulkImporter importer = new BulkImporter(client, collection);
        Iterator<String> inputDocumentIterator = generatedDocuments(cfg).iterator();
        insertSnapshot(importer, inputDocumentIterator);

        client.close();
    }

    private static void insertSnapshot(BulkImporter importer, Iterator<String> inputDocumentIterator) throws DocumentClientException {


        Stopwatch stopWatch = Stopwatch.createStarted();

        BulkImportResponse bulkImportResponse = importer.bulkImport(inputDocumentIterator, false);

        stopWatch.stop();

        System.out.println("total time: from start to end: " + stopWatch.elapsed());

        // print stats
        System.out.println("Number of documents inserted: " + bulkImportResponse.getNumberOfDocumentsImported());
        System.out.println("Import total time: " + bulkImportResponse.getTotalTimeTaken());
        System.out.println("Total request unit consumed: " + bulkImportResponse.getTotalRequestUnitsConsumed());

        System.out.println("Average RUs per second: " + bulkImportResponse.getTotalRequestUnitsConsumed() / bulkImportResponse.getTotalTimeTaken().getSeconds());
        System.out.println("Average # Document Inserts per second: " + bulkImportResponse.getNumberOfDocumentsImported() / bulkImportResponse.getTotalTimeTaken().getSeconds());
    }

    private static Stream<String> generatedDocuments(Configuration cfg) {

        return IntStream.range(0, cfg.getNumberOfDocumentsToInsert()).mapToObj(i ->
        {
            String id = UUID.randomUUID().toString();
            String mypk = UUID.randomUUID().toString();
            String v = UUID.randomUUID().toString();
            String doc = String.format("{" +
                    "  \"dataField\": \"%s\"," +
                    "  \"mypk\": \"%s\"," +
                    "  \"id\": \"%s\"" +
                    "}", v, mypk, id);

            return doc;
        });
    }

    public static DocumentClient fromConfiguration(Configuration cfg) throws DocumentClientException {
        return new DocumentClient(cfg.getServiceEndpoint(), cfg.getMasterKey(),
                cfg.getConnectionPolicy(), cfg.getConsistencyLevel());
    }
}
