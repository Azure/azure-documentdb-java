package com.microsoft.azure.documentdb;

/**
 * Defines an interface used to cache partition key definition for a collection.
 */
interface PartitionKeyDefinitionMap {
	public PartitionKeyDefinition getPartitionKeyDefinition(String collectionLink);
}
