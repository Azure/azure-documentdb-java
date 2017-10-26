package com.globalstrongdemo;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.RequestOptions;

public class Producer implements Runnable {

    private static final String METADATA_DOCUMENT_NAME = "metadata";
    private boolean isInitialized;

    private DocumentClient producerDocumentClient;
    private DocumentCollection producerPayloadCollection;
    private DocumentCollection producerMetadataCollection;
    private Document producerMetadataDocument;
    private static final String PRODUCER_DB_NAME = "producer";
    private static final String PRODUCER_PAYLOAD_COLLECTION_NAME = "producerPayload";
    private static final String PRODUCER_METADATA_COLLECTION_NAME = "producerMetadata";

    private static int oldCount = -1;
    private static long start;
    private static long stop;

    public Producer(String producerEndpoint, String producerKey, String consumerEndpoint, String consumerKey,
            String currentLocation) {

        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        ArrayList<String> preferredLocations = new ArrayList<String>();
        preferredLocations.add(currentLocation);
        connectionPolicy.setPreferredLocations(preferredLocations);

        producerDocumentClient = new DocumentClient(producerEndpoint, producerKey, connectionPolicy, null);

        isInitialized = false;
    }

    public void initialize() throws DocumentClientException {
        Database database = new Database();
        database.setId(PRODUCER_DB_NAME);
        database = safeCreateDatabase(producerDocumentClient, database, null);

        RequestOptions requestOptions = new RequestOptions();
        requestOptions.setOfferThroughput(10000);

        DocumentCollection documentCollection = new DocumentCollection();
        documentCollection.setId(PRODUCER_PAYLOAD_COLLECTION_NAME);
        producerPayloadCollection = producerDocumentClient
                .createCollection(database.getSelfLink(), documentCollection, requestOptions).getResource();

        documentCollection.setId(PRODUCER_METADATA_COLLECTION_NAME);
        producerMetadataCollection = producerDocumentClient
                .createCollection(database.getSelfLink(), documentCollection, requestOptions).getResource();

        Document doc = new Document();
        doc.setId(METADATA_DOCUMENT_NAME);
        doc.set("count", "0");
        producerMetadataDocument = producerDocumentClient
                .createDocument(producerMetadataCollection.getSelfLink(), doc, null, false).getResource();

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

    private String getPayLoad() throws Error {
        StringBuffer buffer = new StringBuffer();
        Random random = new Random();
        int i;
        for (i = 0; i < 1500 * 1024; i++) {
            char c = (char) (65 + random.nextInt(26));
            buffer.append(c);
        }
        return buffer.toString();
    }

    private int getProducerCount() throws DocumentClientException, InterruptedException {
        Integer producerCount = null;
        
        start = System.currentTimeMillis();
        while (true) {
            producerCount = producerDocumentClient.readDocument(producerMetadataDocument.getSelfLink(), null).getResource()
                .getInt("count");
            if (producerCount > oldCount) {
                oldCount = producerCount;
                break;
            }
        }
        stop = System.currentTimeMillis();
        
        System.out.println("Producer: Read last token number created by prodcuer - " + producerCount
                + ", TimeTaken (ms): " + (stop - start));
        return producerCount;
    }

    @Override
    public void run() {
        System.out.println("PRODUCER STARTED!");
        if (!isInitialized)
            try {
                initialize();
                String payload = "";

                while (true) {
                    if (payload.equals(""))
                        payload = getPayLoad();

                    int currentCount = getProducerCount();
                    ++currentCount;

                    Document newDoc = new Document();
                    newDoc.setId(Integer.toString(currentCount));
                    newDoc.set("payload", payload);

                    try {

                        start = System.currentTimeMillis();
                        producerDocumentClient.createDocument(producerPayloadCollection.getSelfLink(), newDoc, null,
                                false);
                        stop = System.currentTimeMillis();

                        payload = "";
                        System.out.println("Producer: Inserted token " + currentCount + " into queue, TimeTaken (ms): "
                                + (stop - start));

                    } catch (DocumentClientException e) {

                        if (e.getStatusCode() != 409) {
                            System.out
                                    .println("Hit exception " + e.getStatusCode() + " when inserting " + currentCount);
                            continue;
                        }

                    }

                    producerMetadataDocument.set("count", currentCount);
                    
                    start = System.currentTimeMillis();
                    producerDocumentClient.replaceDocument(producerMetadataDocument, null);
                    stop = System.currentTimeMillis();

                    System.out.println(
                            "Producer: Updated metadata value to " + currentCount + ", TimeTaken: " + (stop - start));
                }
            } catch (DocumentClientException | InterruptedException e) {
                e.printStackTrace();
            }
    }

}
