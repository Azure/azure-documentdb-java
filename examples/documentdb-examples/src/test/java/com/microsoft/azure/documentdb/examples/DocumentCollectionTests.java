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

package com.microsoft.azure.documentdb.examples;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.RequestOptions;

public class DocumentCollectionTests
{
    private DocumentClient client;
    private String databaseId = "exampleDB";

    @Before
    public void setUp() throws DocumentClientException {
        // create client
        client = new DocumentClient(TestConfigurations.HOST, TestConfigurations.MASTER_KEY, null, null);

        // removes the database "exampleDB" (if exists)
        deleteDatabase();

        // create database exampleDB;
        Database databaseDefinition = new Database();
        databaseDefinition.setId(databaseId);
        client.createDatabase(databaseDefinition, null);
    }

    @After
    public void shutDown() {
        deleteDatabase();
        if (client != null) {
            client.close();
        }
    }

    @Test
    public void createSinglePartitionCollection() throws DocumentClientException {

        String databaseLink = String.format("/dbs/%s", databaseId);
        String collectionId = "testCollection";

        DocumentCollection collectionDefinition = new DocumentCollection();
        collectionDefinition.setId(collectionId);

        // create a collection
        client.createCollection(databaseLink, collectionDefinition, null);
    }

    @Test
    public void deleteCollection() throws DocumentClientException {

        String databaseLink = String.format("/dbs/%s", databaseId);
        String collectionId = "testCollection";

        // create collection
        DocumentCollection collectionDefinition = new DocumentCollection();
        collectionDefinition.setId(collectionId);
        client.createCollection(databaseLink, collectionDefinition, null);

        // delete collection
        String collectionLink = String.format("/dbs/%s/colls/%s", databaseId, collectionId);
        client.deleteCollection(collectionLink, null);
    }

    @Test
    public void createMultiPartitionCollection() throws DocumentClientException {

        String databaseLink = String.format("/dbs/%s", databaseId);
        String collectionId = "testCollection";

        DocumentCollection collectionDefinition = new DocumentCollection();
        collectionDefinition.setId(collectionId);

        // set /city as the partition key path
        PartitionKeyDefinition partitionKeyDefinition = new PartitionKeyDefinition();
        Collection<String> paths = new ArrayList<String>();
        paths.add("/city");
        partitionKeyDefinition.setPaths(paths);
        collectionDefinition.setPartitionKey(partitionKeyDefinition);

        // set the throughput to be 20,000
        RequestOptions options = new RequestOptions();
        options.setOfferThroughput(20000);

        // create a collection
        client.createCollection(databaseLink, collectionDefinition, options);

        // create a document in the collection
        String collectionLink = String.format("/dbs/%s/colls/%s", databaseId, collectionId);
        Document documentDefinition = new Document(
                "{" +
                        "   \"id\": \"test-document\"," +
                        "   \"city\" : \"Seattle\"" +
                "} ") ;

        client.createDocument(collectionLink, documentDefinition, options, false);
    }

    private void deleteDatabase() {
        try {
            client.deleteDatabase(String.format("/dbs/%s", databaseId), null);
        } catch (Exception e) {
        }
    }
}
