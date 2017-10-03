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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.lang3.RandomUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.microsoft.azure.documentdb.DataType;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.IncludedPath;
import com.microsoft.azure.documentdb.Index;
import com.microsoft.azure.documentdb.IndexingPolicy;
import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.RequestOptions;


public class DocumentQuerySamples
{
    private final String databaseId = "exampleDB";
    private final String collectionId = "testCollection";
    private final String partitionKeyFieldName = "city";
    private final String partitionKeyPath = "/" + partitionKeyFieldName;
    private final String collectionLink = String.format("/dbs/%s/colls/%s", databaseId, collectionId);

    private DocumentClient client;

    @Before
    public void setUp() throws DocumentClientException {
        // create client
        client = new DocumentClient(AccountCredentials.HOST, AccountCredentials.MASTER_KEY, null, null);

        // removes the database "exampleDB" (if exists)
        deleteDatabase();
        // create database exampleDB;
        createDatabase();

        // create collection
        createMultiPartitionCollection();

        // populate documents
        populateDocuments();
    }

    @After
    public void shutDown() {
        deleteDatabase();
        if (client != null) {
            client.close();
        }
    }

    private void populateDocuments() throws DocumentClientException {

        String[] cities = new String [] { "Amherst", "Seattle", "Redmond", "北京市", "बंगलौर - विकिपीडिया", "شیراز‎‎" };
        String[] poetNames = new String[] { "Emily Elizabeth Dickinson", "Hafez", "Lao Tzu" };

        for(int i = 0; i < 100; i++) {
            Document documentDefinition = new Document();
            documentDefinition.setId("test-document" + i);
            documentDefinition.set("city", cities[i % cities.length]);
            documentDefinition.set("cnt", i);

            documentDefinition.set("poetName", poetNames[ i % poetNames.length ]);
            documentDefinition.set("popularity", RandomUtils.nextDouble(0, 1));

            client.createDocument(collectionLink, documentDefinition, null, true);

        }
    }

    @Test
    public void simpleDocumentQuery() throws DocumentClientException {

        // as this is a multi collection enable cross partition query
        FeedOptions options = new FeedOptions();
        options.setEnableCrossPartitionQuery(true);

        Iterator<Document> it = client.queryDocuments(collectionLink, "SELECT * from r", options).getQueryIterator();

        int i = 0;
        while(it.hasNext()) {
            Document d = it.next();

            System.out.println("id is " + d.getId());
            System.out.println("citi is " + d.getString("city"));
            System.out.println("cnt is " + d.getInt("cnt"));
            System.out.println("popularity is " + d.getDouble("popularity"));

            i++;
        }

        assertThat(i, equalTo(100));
    }

    @Test
    public void orderByQuery() throws DocumentClientException {

        // as this is a multi collection enable cross partition query
        FeedOptions options = new FeedOptions();
        options.setEnableCrossPartitionQuery(true);

        Iterator<Document> it = client.queryDocuments(collectionLink, "SELECT * from r ORDER BY r.cnt", options).getQueryIterator();

        int i = 0;
        while(it.hasNext()) {
            Document d = it.next();

            System.out.println("id is " + d.getId());
            System.out.println("citi is " + d.getString("city"));
            System.out.println("cnt is " + d.getInt("cnt"));
            System.out.println("popularity is " + d.getDouble("popularity"));

            assertThat(d.getInt("cnt"), equalTo(i));

            i++;
        }
        assertThat(i, equalTo(100));
    }

    private void createDatabase() throws DocumentClientException {
        Database databaseDefinition = new Database();
        databaseDefinition.setId(databaseId);
        client.createDatabase(databaseDefinition, null);
    }

    private void createMultiPartitionCollection() throws DocumentClientException {

        String databaseLink = String.format("/dbs/%s", databaseId);

        DocumentCollection collectionDefinition = new DocumentCollection();
        collectionDefinition.setId(collectionId);

        // set /city as the partition key path
        PartitionKeyDefinition partitionKeyDefinition = new PartitionKeyDefinition();
        Collection<String> paths = new ArrayList<String>();
        paths.add(partitionKeyPath);
        partitionKeyDefinition.setPaths(paths);
        collectionDefinition.setPartitionKey(partitionKeyDefinition);

        // set the throughput to be 10,200
        RequestOptions options = new RequestOptions();
        options.setOfferThroughput(10200);

        // set indexing policy to be range range for string and number
        IndexingPolicy indexingPolicy = new IndexingPolicy();
        Collection<IncludedPath> includedPaths = new ArrayList<IncludedPath>();
        IncludedPath includedPath = new IncludedPath();
        includedPath.setPath("/*");
        Collection<Index> indexes = new ArrayList<Index>();
        Index stringIndex = Index.Range(DataType.String);
        stringIndex.set("precision", -1);
        indexes.add(stringIndex);

        Index numberIndex = Index.Range(DataType.Number);
        numberIndex.set("precision", -1);
        indexes.add(numberIndex);
        includedPath.setIndexes(indexes);
        includedPaths.add(includedPath);
        indexingPolicy.setIncludedPaths(includedPaths);
        collectionDefinition.setIndexingPolicy(indexingPolicy);

        // create a collection
        client.createCollection(databaseLink, collectionDefinition, options);
    }

    private void deleteDatabase() {
        try {
            client.deleteDatabase(String.format("/dbs/%s", databaseId), null);
        } catch (Exception e) {
        }
    }
}
