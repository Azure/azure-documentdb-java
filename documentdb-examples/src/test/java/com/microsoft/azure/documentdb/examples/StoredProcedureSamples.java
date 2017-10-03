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
import static org.hamcrest.core.Is.is;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.microsoft.azure.documentdb.DataType;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.IncludedPath;
import com.microsoft.azure.documentdb.Index;
import com.microsoft.azure.documentdb.IndexingPolicy;
import com.microsoft.azure.documentdb.PartitionKey;
import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.RequestOptions;
import com.microsoft.azure.documentdb.StoredProcedure;
import com.microsoft.azure.documentdb.StoredProcedureResponse;
import com.microsoft.azure.documentdb.internal.HttpConstants;

public class StoredProcedureSamples
{
    private final String databaseId = "exampleDB";
    private final String collectionId = "testCollection";
    private final String partitionKeyFieldName = "city";
    private final String partitionKeyPath = "/" + partitionKeyFieldName;

    private DocumentClient client;
    private DocumentCollection collection;

    @Before
    public void setUp() throws DocumentClientException {
        // create client
        client = new DocumentClient(AccountCredentials.HOST, AccountCredentials.MASTER_KEY, null, null);

        //        // removes the database "exampleDB" (if exists)
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
    public void scriptConsoleLogEnabled() throws DocumentClientException {
        // create a stored procedure
        String storeProcedureStr = "{" +
                "  'id':'storedProcedureSample'," +
                "  'body':" +
                "    'function() {" +
                "        var mytext = \"x\";" +
                "        var myval = 1;" +
                "        try {" +
                "            console.log(\"The value of %s is %s.\", mytext, myval);" +
                "            getContext().getResponse().setBody(\"Success!\");" +
                "        }" +
                "        catch(err) {" +
                "            getContext().getResponse().setBody(\"inline err: [\" + err.number + \"] \" + err);" +
                "        }" +
                "    }'" +
                "}";
        StoredProcedure storedProcedure = new StoredProcedure(storeProcedureStr);
        client.createStoredProcedure(collection.getSelfLink(), storedProcedure, null);

        // execute stored procedure
        String storedProcLink = String.format("/dbs/%s/colls/%s/sprocs/%s", databaseId, collectionId, "storedProcedureSample");

        RequestOptions requestOptions = new RequestOptions();
        requestOptions.setScriptLoggingEnabled(true);
        requestOptions.setPartitionKey(new PartitionKey("Seattle"));

        StoredProcedureResponse response = client.executeStoredProcedure(storedProcLink,
                requestOptions, new Object[]{});

        String logResult = "The value of x is 1.";
        assertThat(response.getScriptLog(), is(logResult));
        assertThat(response.getResponseHeaders().get(HttpConstants.HttpHeaders.SCRIPT_LOG_RESULTS), is(logResult));
    }

    @Test
    public void executeStoredProcWithArgs() throws DocumentClientException {
        // create stored procedure
        StoredProcedure storedProcedure = new StoredProcedure(
                "{" +
                        "  'id': 'multiplySample'," +
                        "  'body':" +
                        "    'function (value, num) {" +
                        "      getContext().getResponse().setBody(" +
                        "          \"2*\" + value + \" is \" + num * 2 );" +
                        "    }'" +
                "}");

        client.createStoredProcedure(collection.getSelfLink(), storedProcedure, null).getResource();

        // execute stored procedure
        RequestOptions requestOptions = new RequestOptions();
        requestOptions.setPartitionKey(new PartitionKey("Seattle"));

        String storedProcLink = String.format("/dbs/%s/colls/%s/sprocs/%s", databaseId, collectionId, "multiplySample");

        Object[] storedProcedureArgs = new Object[] {"a", 123};
        String storedProcResultAsString = client.executeStoredProcedure(storedProcLink, requestOptions, storedProcedureArgs).getResponseAsString();
        assertThat(storedProcResultAsString, equalTo("\"2*a is 246\""));
    }

    @Test
    public void executeStoredProcWithPojoArgs() throws DocumentClientException {
        // create stored procedure
        StoredProcedure storedProcedure = new StoredProcedure(
                "{" +
                        "  'id': 'storedProcedurePojoSample'," +
                        "  'body':" +
                        "    'function (value) {" +
                        "      getContext().getResponse().setBody(" +
                        "          \"a is \" + value.temp);" +
                        "    }'" +
                "}");

        client.createStoredProcedure(collection.getSelfLink(), storedProcedure, null).getResource();

        // execute stored procedure
        String storedProcLink = String.format("/dbs/%s/colls/%s/sprocs/%s", databaseId, collectionId, "storedProcedurePojoSample");

        RequestOptions requestOptions = new RequestOptions();
        requestOptions.setPartitionKey(new PartitionKey("Seattle"));

        // POJO
        class SamplePojo {
            public String temp = "my temp value";
        }
        SamplePojo samplePojo = new SamplePojo();

        Object[] storedProcedureArgs = new Object[] { samplePojo };

        String storedProcResultAsString = client.executeStoredProcedure(storedProcLink, requestOptions, storedProcedureArgs).getResponseAsString();
        assertThat(storedProcResultAsString, equalTo("\"a is my temp value\""));
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
        this.collection = client.createCollection(databaseLink, collectionDefinition, options).getResource();
    }

    private void deleteDatabase() {
        try {
            client.deleteDatabase(String.format("/dbs/%s", databaseId), null);
        } catch (Exception e) {
        }
    }
}
