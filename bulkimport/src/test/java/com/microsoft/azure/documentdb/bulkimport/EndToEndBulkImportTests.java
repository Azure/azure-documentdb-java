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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.Undefined;
import com.microsoft.azure.documentdb.bulkimport.DocumentBulkImporter.Builder;

@RunWith(Parameterized.class)
public class EndToEndBulkImportTests extends EndToEndTestBase {

    private DocumentClient client;

    public EndToEndBulkImportTests(DocumentClientConfiguration config) {
        this.client = new DocumentClient(config.getEndpoint(), config.getMasterKey(), config.getConnectionPolicy(),
                config.getConsistencyLevel());
    }

    @Before
    public void setup() throws Exception {
        super.setup(this.client);
        Thread.sleep(1000);
    }

    @After
    public void shutdown() throws DocumentClientException {
        super.shutdown(this.client);
    }

    @Test
    public void bulkImport() throws Exception {
        Builder bulkImporterBuilder = DocumentBulkImporter.builder().
                from(client, databaseId, collectionId, this.pCollection.getPartitionKey(),
                        getOfferThroughput(client, pCollection));

        try (DocumentBulkImporter importer = bulkImporterBuilder.build()) {

            List<String> documents = new ArrayList<>();

            Object [] partitionKeyValues = new Object[] { "abc", null, "", Undefined.Value(), 123, 0, -10, 9,223,372,036,854,775,000, 0.5, true, false };

            for(Object partitionKeyValue: partitionKeyValues) {
                documents.add(DocumentDataSource.randomDocument(partitionKeyValue, pCollection.getPartitionKey()));
            }

            BulkImportResponse response = importer.importAll(documents, false);
            validateSuccess(deserialize(documents), response);
        }
    }

    @Test
    public void bulkImportAlreadyExists() throws Exception {
        Builder bulkImporterBuilder = DocumentBulkImporter.builder().
                from(client, databaseId, collectionId, this.pCollection.getPartitionKey(),
                        getOfferThroughput(client, pCollection));

        try (DocumentBulkImporter importer = bulkImporterBuilder.build()) {

            List<String> documents = new ArrayList<>();

            Object [] partitionKeyValues = new Object[] { "abc", null, "", Undefined.Value(), 123, 0, -10, 9,223,372,036,854,775,000, 0.5, true, false };

            for(Object partitionKeyValue: partitionKeyValues) {
                documents.add(DocumentDataSource.randomDocument(partitionKeyValue, pCollection.getPartitionKey()));
            }

            BulkImportResponse response = importer.importAll(documents, false);
            validateSuccess(deserialize(documents), response);

            response = importer.importAll(documents, false);
            assertThat(response.getNumberOfDocumentsImported(), equalTo(0));

            response = importer.importAll(documents, true);
            assertThat(response.getNumberOfDocumentsImported(), equalTo(documents.size()));
        }
    }

    private List<Document> deserialize(List<String> documents) {
        return documents.stream().map(d -> new Document(d)).collect(Collectors.toList());
    }

    private void validateSuccess(Collection<Document> documents, BulkImportResponse response) throws DocumentClientException, InterruptedException {

        assertThat(response.getNumberOfDocumentsImported(), equalTo(documents.size()));
    }
}
