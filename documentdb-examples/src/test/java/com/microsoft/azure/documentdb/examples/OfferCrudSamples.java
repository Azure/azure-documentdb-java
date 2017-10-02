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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.Offer;
import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.RequestOptions;


public class OfferCrudSamples
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

        // create database
        createDatabase();
    }

    @After
    public void shutDown() {
        deleteDatabase();
        if (client != null) {
            client.close();
        }
    }

    @Test
    public void updateOffer() throws DocumentClientException {

        ////////////////////////////////////////////////////////////////////////////
        // create a collection
        ////////////////////////////////////////////////////////////////////////////

        String databaseLink = String.format("/dbs/%s", databaseId);

        DocumentCollection collectionDefinition = new DocumentCollection();
        collectionDefinition.setId(collectionId);

        // set /city as the partition key path
        PartitionKeyDefinition partitionKeyDefinition = new PartitionKeyDefinition();
        Collection<String> paths = new ArrayList<String>();
        paths.add(partitionKeyPath);
        partitionKeyDefinition.setPaths(paths);
        collectionDefinition.setPartitionKey(partitionKeyDefinition);

        int throughput = 10200;
        // set the throughput to be 10,200
        RequestOptions options = new RequestOptions();
        options.setOfferThroughput(throughput);

        // create a collection
        client.createCollection(databaseLink, collectionDefinition, options);

        String collectionLink = String.format("/dbs/%s/colls/%s", databaseId, collectionId);

        ////////////////////////////////////////////////////////////////////////////
        // find the offer associated with the created collection
        ////////////////////////////////////////////////////////////////////////////

        // read the collection
        String collectionResourceId = client.readCollection(collectionLink, options).getResource().getResourceId();

        // find offer associated with this collection
        Iterator<Offer> it = client.queryOffers(
                String.format("SELECT * FROM r where r.offerResourceId = '%s'", collectionResourceId), null).getQueryIterator();
        assertThat(it.hasNext(), equalTo(true));

        Offer offer = it.next();
        assertThat(offer.getString("offerResourceId"), equalTo(collectionResourceId));
        assertThat(offer.getContent().getInt("offerThroughput"), equalTo(throughput));

        // update the offer
        int newThroughput = 10300;
        offer.getContent().put("offerThroughput", newThroughput);
        client.replaceOffer(offer);

        // validate the updated offer
        it = client.queryOffers(String.format("SELECT * FROM r where r.offerResourceId = '%s'", collectionResourceId), null).getQueryIterator();
        assertThat(it.hasNext(), equalTo(true));

        offer = it.next();
        assertThat(offer.getString("offerResourceId"), equalTo(collectionResourceId));
        assertThat(offer.getContent().getInt("offerThroughput"), equalTo(newThroughput));
    }

    private void createDatabase() throws DocumentClientException {
        Database databaseDefinition = new Database();
        databaseDefinition.setId(databaseId);
        client.createDatabase(databaseDefinition, null);
    }

    private void deleteDatabase() {
        try {
            client.deleteDatabase(String.format("/dbs/%s", databaseId), null);
        } catch (Exception e) {
        }
    }
}
