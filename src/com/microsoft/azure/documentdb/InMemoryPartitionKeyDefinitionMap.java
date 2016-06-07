package com.microsoft.azure.documentdb;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;

/**
 * This class implements a in-memory partition key definition cache used to extract partition key
 * values from a document when partition key is not specified by the user in create and read document
 * scenarios.
 * 
 * TODO: this cache does not refresh itself when the collection definition changes. You cannot change
 * the partition key definition once a collection is created.  So the only case it will cause problem is
 * when a new collection is created with the same name and a different partition key definition.
 */
final class InMemoryPartitionKeyDefinitionMap implements PartitionKeyDefinitionMap {
	private final Map<String, PartitionKeyDefinition> partitionKeyDefMap;
	private final DocumentClient documentClient;
	
	/**
     * Create a new instance of the InMemoryPartitionKeyDefinitionMap object.
     *
     * @param documentClient
     *            the DocumentClient instance associated with this PartitionKeyDefinitionMap.
     */
	public InMemoryPartitionKeyDefinitionMap(DocumentClient documentClient) {
		if (documentClient == null) {
			throw new IllegalArgumentException("documentClient");
		}
		
		this.documentClient = documentClient;
		this.partitionKeyDefMap = new ConcurrentHashMap<String, PartitionKeyDefinition>();
	}
	
	/**
     * Returns a PartitionKeyDefinition instance representing the partition key definition
     * for the collection identified by the collection link.
     *
     * @param collectionLink
     *            the selfLink of a collection, or the full path to the collection when using name based routing.
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
     * @param collectionLink
     *            the selfLink of a collection, or the full path to the collection when using name based routing.
     */
    public void RefreshEntry(String collectionLink) {
        String collectionName = Utils.getCollectionName(collectionLink);
        this.partitionKeyDefMap.remove(collectionName);
        PartitionKeyDefinition partitionKeyDef = this.retrievePartitionKeyDefinition(collectionName);
        if (partitionKeyDef != null) {
            this.partitionKeyDefMap.put(collectionName, partitionKeyDef);
        }
    }
    
	private PartitionKeyDefinition retrievePartitionKeyDefinition(String collectionName) {
		PartitionKeyDefinition keyDef = null;
		try {
			DocumentCollection collection = this.documentClient.readCollection(collectionName, null).getResource();
			keyDef = collection.getPartitionKey();
		} catch (DocumentClientException e) {
			// Ignore document retrieval exception and let the server handle partition key missing error.
		}
		
		return keyDef;
	}
}
