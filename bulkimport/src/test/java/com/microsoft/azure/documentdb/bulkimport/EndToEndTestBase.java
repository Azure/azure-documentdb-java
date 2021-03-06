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

import static com.microsoft.azure.documentdb.bulkimport.TestConfigurations.HOST;
import static com.microsoft.azure.documentdb.bulkimport.TestConfigurations.MASTER_KEY;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.documentdb.ConnectionMode;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.FeedResponse;
import com.microsoft.azure.documentdb.Offer;
import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.RequestOptions;
import com.microsoft.azure.documentdb.internal.directconnectivity.HttpClientFactory;

public class EndToEndTestBase {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    
    protected String databaseId = "bulkImportTestDatabase";
    protected String collectionId;
    protected String collectionLink;
    protected DocumentCollection pCollection;

    protected static Set<String> SYSTEM_FIELD_NAMES;
    
    static {
        SYSTEM_FIELD_NAMES = new HashSet<>();        
        SYSTEM_FIELD_NAMES.addAll(Arrays.asList(new String[] { "_rid", "_attachments", "_self", "_etag" , "_ts"}));
    }
    
    public EndToEndTestBase() {
        HttpClientFactory.DISABLE_HOST_NAME_VERIFICATION = true;
        
        collectionId = UUID.randomUUID().toString();
        collectionLink = "dbs/" + databaseId + "/colls/" + collectionId;
    }

    @Parameters
    public static List<Object[]> configs() {
        ConnectionPolicy directModePolicy = new ConnectionPolicy();
        directModePolicy.setConnectionMode(ConnectionMode.DirectHttps);

        ConnectionPolicy gatewayModePolicy = new ConnectionPolicy();
        gatewayModePolicy.setConnectionMode(ConnectionMode.Gateway);

        return Arrays.asList(new Object[][] {
            { new DocumentClientConfiguration(HOST, MASTER_KEY, gatewayModePolicy, ConsistencyLevel.Eventual) },
            { new DocumentClientConfiguration(HOST, MASTER_KEY, directModePolicy, ConsistencyLevel.Eventual )}
        });
    }

    public void shutdown(DocumentClient client) throws DocumentClientException {
        if (client != null) {
            cleanUpDatabase(client);
            client.close();
        }
    }

    public void cleanUpDatabase(DocumentClient client) throws DocumentClientException {
        try {
            if (client != null) {
                client.deleteDatabase("/dbs/" + databaseId, null);
            }
        } catch (Exception e) {
        }
    }

    public void setup(DocumentClient client) throws Exception {
        logger.debug("setting up ...");
        cleanUpDatabase(client);

        Thread.sleep(5000);
        Database d = new Database();
        d.setId(databaseId);
        Database database = client.createDatabase(d, null).getResource();


        PartitionKeyDefinition partitionKeyDef = new PartitionKeyDefinition();
        ArrayList<String> paths = new ArrayList<String>();
        paths.add("/mypk");
        partitionKeyDef.setPaths(paths);

        pCollection = new DocumentCollection();
        pCollection.setId(collectionId);
        pCollection.setPartitionKey(partitionKeyDef);

        RequestOptions options = new RequestOptions();
        options.setOfferThroughput(10100);
        pCollection = client.createCollection(database.getSelfLink(), pCollection, options).getResource();
        logger.debug("setting up done.");
    }
    
    protected static int getOfferThroughput(DocumentClient client, DocumentCollection collection) {
        FeedResponse<Offer> offers = client.queryOffers(String.format("SELECT * FROM c where c.offerResourceId = '%s'", collection.getResourceId()), null);

        List<Offer> offerAsList = offers.getQueryIterable().toList();
        if (offerAsList.isEmpty()) {
            throw new IllegalStateException("Cannot find Collection's corresponding offer");
        }

        Offer offer = offerAsList.get(0);
        return offer.getContent().getInt("offerThroughput");
    }

}
