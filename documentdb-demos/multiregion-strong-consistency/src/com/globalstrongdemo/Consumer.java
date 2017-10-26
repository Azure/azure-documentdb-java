package com.globalstrongdemo;

import java.util.ArrayList;
import java.util.List;

import org.fusesource.jansi.AnsiConsole;

import static org.fusesource.jansi.Ansi.*;
import static org.fusesource.jansi.Ansi.Color.*;

import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.RequestOptions;

public class Consumer implements Runnable {

    private static final String METADATA_DOCUMENT_NAME = "metadata";
    private boolean isInitialized;

    private DocumentClient producerDocumentClient;
    private static final String PRODUCER_METADATA_DOCUMENT_PATH = "dbs/producer/colls/producerMetadata/docs/metadata";

    private DocumentClient consumerDocumentClient;
    private DocumentCollection consumerMetadataCollection;
    private Document consumerMetadataDocument;
    private static final String CONSUMER_DB_NAME = "consumer";
    private static final String CONSUMER_METADATA_COLLECTION_NAME = "consumerMetadata";

    private static int oldCount = -1;
    private static long start;
    private static long stop;

    public Consumer(String producerEndpoint, String producerKey, String consumerEndpoint, String consumerKey,
            String currentLocation) {

        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        ArrayList<String> preferredLocations = new ArrayList<String>();
        preferredLocations.add(currentLocation);
        connectionPolicy.setPreferredLocations(preferredLocations);

        producerDocumentClient = new DocumentClient(producerEndpoint, producerKey, connectionPolicy, null);
        consumerDocumentClient = new DocumentClient(consumerEndpoint, consumerKey, connectionPolicy, null);

        isInitialized = false;
    }

    public void initialize() throws DocumentClientException {
        AnsiConsole.systemInstall();

        Database database = new Database();
        database.setId(CONSUMER_DB_NAME);
        database = safeCreateDatabase(consumerDocumentClient, database, null);

        RequestOptions requestOptions = new RequestOptions();
        requestOptions.setOfferThroughput(10000);

        DocumentCollection documentCollection = new DocumentCollection();
        documentCollection.setId(CONSUMER_METADATA_COLLECTION_NAME);
        consumerMetadataCollection = consumerDocumentClient
                .createCollection(database.getSelfLink(), documentCollection, requestOptions).getResource();

        Document doc = new Document();
        doc.setId(METADATA_DOCUMENT_NAME);
        doc.set("count", "0");
        consumerMetadataDocument = consumerDocumentClient
                .createDocument(consumerMetadataCollection.getSelfLink(), doc, null, false).getResource();

        isInitialized = true;
    }

    public Database safeCreateDatabase(DocumentClient documentClient, Database db, RequestOptions options)
            throws DocumentClientException {
        List<Database> res = documentClient
                .queryDatabases(String.format("SELECT * FROM root r where r.id = '%s'", db.getId()), null)
                .getQueryIterable().toList();
        if (res.size() > 0)
            documentClient.deleteDatabase("dbs/" + db.getId(), options);

        return documentClient.createDatabase(db, options).getResource();
    }

    private int yieldTillQueueNotEmpty() throws DocumentClientException, InterruptedException {
        Integer producerCount = null;
        Integer consumerCount = null;

        while (true) {

            start = System.currentTimeMillis();
            consumerCount = consumerDocumentClient.readDocument(consumerMetadataDocument.getSelfLink(), null)
                    .getResource().getInt("count");

            while (true) {
                try {
                    producerCount = producerDocumentClient.readDocument(PRODUCER_METADATA_DOCUMENT_PATH, null)
                            .getResource().getInt("count");
                    break;
                } catch (Exception e) {
                }
            }

            stop = System.currentTimeMillis();
            int currentQueueSize = producerCount - consumerCount;
            if (currentQueueSize <= 0) {
                System.out
                        .println("Consumer: Hit queue empty. Yielding for 1 second, TimeTaken (ms): " + (stop - start));
                Thread.sleep(1000);

            } else {

                if (consumerCount > oldCount) {
                    oldCount = consumerCount;
                    System.out.println("Consumer: Dequeuing..., TimeTaken (ms): " + (stop - start));
                    return consumerCount;
                }
            }
        }
    }

    private void consumePayload(String payload) {
        return;
    }

    @Override
    public void run() {
                
        System.out.println("CONSUMER STARTED!");
        if (!isInitialized)
            try {
                initialize();

                while (true) {
                    int currentCount = yieldTillQueueNotEmpty();
                    currentCount ++;
                    try {
                        consumerMetadataDocument.set("count", currentCount);
                        start = System.currentTimeMillis();
                        consumerDocumentClient.replaceDocument(consumerMetadataDocument, null);
                        stop = System.currentTimeMillis();

                        System.out.println("Consumer: Read next token index " + currentCount + ", TimeTaken (ms): "
                                + (stop - start));

                    } catch (DocumentClientException e) {
                        continue;
                    }

                    try {

                        start = System.currentTimeMillis();
                        String payload = producerDocumentClient
                                .readDocument("dbs/producer/colls/producerPayload/docs/" + currentCount, null)
                                .getResource().getString("payload");
                        stop = System.currentTimeMillis();

                        consumePayload(payload);
                        System.out.println(ansi().bg(GREEN).fg(BLACK)
                                .a("Consumer: Processed token " + currentCount + ", TimeTaken (ms): " + (stop - start))
                                .reset());

                    } catch (DocumentClientException e) {

                        if (e.getStatusCode() == 404)
                            System.out.println(ansi().bg(RED).fg(BLACK).a("Consumer: Read next token index "
                                    + currentCount + ", but did not find token in producer queue.").reset());

                    }

                }
            } catch (DocumentClientException | InterruptedException e) {
                e.printStackTrace();
            }
    }

}
