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
import static org.hamcrest.Matchers.greaterThan;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.microsoft.azure.documentdb.DataType;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.IncludedPath;
import com.microsoft.azure.documentdb.Index;
import com.microsoft.azure.documentdb.IndexingPolicy;
import com.microsoft.azure.documentdb.PartitionKey;
import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.RequestOptions;
import com.microsoft.azure.documentdb.ResourceResponse;


public class DocumentCrudSamples
{
    private final String databaseId = "exampleDB";
    private final String collectionId = "testCollection";
    private final String partitionKeyFieldName = "city";
    private final String partitionKeyPath = "/" + partitionKeyFieldName;

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
    }

    @After
    public void shutDown() {
        deleteDatabase();
        if (client != null) {
            client.close();
        }
    }

    @Test
    public void createDocumentAndReadAndDelete() throws DocumentClientException {

        // create a document in the collection
        String collectionLink = String.format("/dbs/%s/colls/%s", databaseId, collectionId);
        Document documentDefinition = new Document(
                "{" +
                        "   \"id\": \"test-document\"," +
                        "   \"city\" : \"Seattle\"," +
                        "   \"population\" : 704352" +

                "} ") ;

        ResourceResponse<Document> response = client.createDocument(collectionLink, documentDefinition, null, false);

        // access request charge associated with document create
        assertThat(response.getRequestCharge(), greaterThan((double) 0));

        String documentLink = String.format("/dbs/%s/colls/%s/docs/%s", databaseId, collectionId, "test-document");

        // since it is a Point Read and the collection is multi partition collection, partition key has to be provided
        RequestOptions options = new RequestOptions();
        options.setPartitionKey(new PartitionKey("Seattle"));

        // read document using the collectionlink and the provided partition key
        response = client.readDocument(documentLink, options);
        // access request charge associated with document read
        assertThat(response.getRequestCharge(), greaterThan((double) 0));

        // access individual fields
        Document readDocument = response.getResource();

        assertThat(readDocument.getId(), equalTo("test-document"));
        assertThat(readDocument.getInt("population"), equalTo(704352));
        assertThat(readDocument.getString("city"), equalTo("Seattle"));

        // delete document
        response = client.deleteDocument(documentLink, options);
        assertThat(response.getRequestCharge(), greaterThan((double) 0));
    }


    @Test
    public void createDocumentWithProgrammableDocumentDefinition() throws DocumentClientException {

        String collectionLink = String.format("/dbs/%s/colls/%s", databaseId, collectionId);
        Document documentDefinition = new Document();
        documentDefinition.setId("test-document");
        documentDefinition.set("population", 704352);
        documentDefinition.set("city", "Seattle");

        client.createDocument(collectionLink, documentDefinition, null, false);

        String documentLink = String.format("/dbs/%s/colls/%s/docs/%s", databaseId, collectionId, "test-document");

        // since it is a Point Read and the collection is multi partition collection, partition key has to be provided
        RequestOptions options = new RequestOptions();
        options.setPartitionKey(new PartitionKey("Seattle"));
        ResourceResponse<Document> response = client.readDocument(documentLink, options);
        Document readDocument = response.getResource();

        assertThat(readDocument.getId(), equalTo("test-document"));
        assertThat(readDocument.getInt("population"), equalTo(704352));
        assertThat(readDocument.getString("city"), equalTo("Seattle"));
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
