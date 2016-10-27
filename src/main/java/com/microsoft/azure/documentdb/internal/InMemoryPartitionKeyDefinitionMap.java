package com.microsoft.azure.documentdb.internal;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;

import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.PartitionKeyDefinition;

/**
 * This class implements a in-memory partition key definition cache used to extract partition key
 * values from a document when partition key is not specified by the user in create and read document
 * scenarios.
 */
public final class InMemoryPartitionKeyDefinitionMap implements PartitionKeyDefinitionMap {
    private final Map<String, PartitionKeyDefinition> partitionKeyDefMap;
    private final DocumentClient documentClient;
    private final Logger logger;

    /**
     * Create a new instance of the InMemoryPartitionKeyDefinitionMap object.
     *
     * @param documentClient the DocumentClient instance associated with this PartitionKeyDefinitionMap.
     */
    public InMemoryPartitionKeyDefinitionMap(DocumentClient documentClient) {
        if (documentClient == null) {
            throw new IllegalArgumentException("documentClient");
        }

        this.documentClient = documentClient;
        this.partitionKeyDefMap = new ConcurrentHashMap<String, PartitionKeyDefinition>();
        this.logger = Logger.getLogger(this.getClass().getPackage().getName());
    }

    /**
     * Returns a PartitionKeyDefinition instance representing the partition key definition
     * for the collection identified by the collection link.
     *
     * @param collectionLink the selfLink of a collection, or the full path to the collection when using name based routing.
     */
    public PartitionKeyDefinition getPartitionKeyDefinition(String collectionLink) {
        if (StringUtils.isEmpty(collectionLink)) {
            return null;
        }

        String collectionName = Utils.getCollectionName(collectionLink);
        PartitionKeyDefinition partitionKeyDef = this.partitionKeyDefMap.get(collectionName);
        if (partitionKeyDef == null) {
            partitionKeyDef = this.retrievePartitionKeyDefinition(collectionName);
            if (partitionKeyDef != null) {
                this.partitionKeyDefMap.put(collectionName, partitionKeyDef);
            }
        }

        return partitionKeyDef;
    }

    /**
     * Refreshes the PartitionKeyDefinition entry for a given collection.
     *
     * @param collectionLink the selfLink of a collection, or the full path to the collection when using name based routing.
     */
    public void RefreshEntry(String collectionLink) {
        String collectionName = Utils.getCollectionName(collectionLink);
        this.logger.info(String.format("Refresh partition key entry for collection '%s'.", collectionName));
        this.partitionKeyDefMap.remove(collectionName);
        PartitionKeyDefinition partitionKeyDef = this.retrievePartitionKeyDefinition(collectionName);
        if (partitionKeyDef != null) {
            this.partitionKeyDefMap.put(collectionName, partitionKeyDef);
        }
    }

    private PartitionKeyDefinition retrievePartitionKeyDefinition(String collectionName) {
        PartitionKeyDefinition keyDef = null;
        try {
            // Read collection as part of document creation should use the write region.
            DocumentCollection collection = this.documentClient.readCollection(collectionName, null).getResource();
            keyDef = collection.getPartitionKey();
            if (keyDef == null) {
                // Use an empty PartitionKeyDefinition to indicate single partition collection
                keyDef = new PartitionKeyDefinition();
            }
        } catch (DocumentClientException e) {
            // Ignore document retrieval exception and let the server handle partition key missing error.
            this.logger.warning(String.format("InMemoryPartitionKeyDefinitionMap: Failed to read collection '%s'. '%s'", collectionName, e.toString()));
        }

        return keyDef;
    }
}
