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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;

public class Main {

    public static final String MASTER_KEY = "[YOUR-MASTERKEY]";
    public static final String HOST = "[YOUR-ENDPOINT]";

    public static void main(String[] args) throws DocumentClientException, InterruptedException, ExecutionException {
        DocumentClient client = new DocumentClient(HOST, MASTER_KEY, null, null);

        String collectionLink = String.format("/dbs/%s/colls/%s", "mydb", "mycol");
        // this assumes database and collection already exists
        DocumentCollection collection = client.readCollection(collectionLink, null).getResource();

        BulkImporter importer = new BulkImporter(client, collection);

        List<String> docs = new ArrayList<String>();
        for(int i = 0; i < 200000; i++) {
            String id = UUID.randomUUID().toString();
            String mypk = "Seattle";
            String v = UUID.randomUUID().toString();
            String doc = String.format("{" +
                    "  \"dataField\": \"%s\"," +
                    "  \"mypk\": \"%s\"," +
                    "  \"id\": \"%s\"" +
                    "}", v, mypk, id);

            docs.add(doc);
        }

        BulkImportResponse bulkImportResponse = importer.bulkImport(docs, false);

        // returned stats
        System.out.println("Number of documents inserted: " + bulkImportResponse.getNumberOfDocumentsImported());
        System.out.println("Import total time: " + bulkImportResponse.getTotalTimeTaken());
        System.out.println("Total request unit consumed: " + bulkImportResponse.getTotalRequestUnitsConsumed());

        client.close();
    }

}
