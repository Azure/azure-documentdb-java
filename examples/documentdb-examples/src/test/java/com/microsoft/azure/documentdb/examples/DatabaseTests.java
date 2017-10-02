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

import java.util.Iterator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.FeedResponse;
import com.microsoft.azure.documentdb.RequestOptions;
import com.microsoft.azure.documentdb.ResourceResponse;

public class DatabaseTests
{
    private DocumentClient client;
    private String databaseId = "exampleDB";

    @Before
    public void setUp() {
        client = new DocumentClient(TestConfigurations.HOST, TestConfigurations.MASTER_KEY, null, null);
        deleteDatabase();
    }

    @After
    public void shutDown() {
        deleteDatabase();
        if (client != null) {
            client.close();
        }
    }

    @Test
    public void databaseCreateAndRead() throws DocumentClientException {

        Database databaseDefinition = new Database();
        databaseDefinition.setId(databaseId);

        // this will create a database with resource URI: /dbs/${databaseId}
        RequestOptions options = new RequestOptions();
        client.createDatabase(databaseDefinition, options);

        String databaseLink = String.format("/dbs/%s", databaseId);

        ResourceResponse<Database> response = client.readDatabase(databaseLink, options);
        Database readDatabase = response.getResource();

        assertThat(readDatabase.getId(), equalTo(databaseDefinition.getId()));
    }

    @Test
    public void databaseCreateAndDelete() throws DocumentClientException {

        Database databaseDefinition = new Database();
        databaseDefinition.setId(databaseId);

        // this will create a database with resource URI: /dbs/${databaseId}
        RequestOptions options = new RequestOptions();
        client.createDatabase(databaseDefinition, options);

        String databaseLink = String.format("/dbs/%s", databaseId);

        client.deleteDatabase(databaseLink, options);
    }

    @Test
    public void databaseCreateAndQuery() throws DocumentClientException {

        Database databaseDefinition = new Database();
        databaseDefinition.setId(databaseId);

        RequestOptions options = new RequestOptions();
        client.createDatabase(databaseDefinition, options);

        FeedResponse<Database> queryResults = client.queryDatabases(String.format("SELECT * FROM r where r.id = '%s'", databaseId), null);

        Iterator<Database> it = queryResults.getQueryIterator();

        assertThat(it.hasNext(), equalTo(true));
        Database foundDatabase = it.next();
        assertThat(foundDatabase.getId(), equalTo(databaseDefinition.getId()));
    }

    private void deleteDatabase() {
        try {
            client.deleteDatabase(String.format("/dbs/%s", databaseId), null);
        } catch (Exception e) {
        }
    }
}
