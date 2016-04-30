package com.microsoft.azure.documentdb;

/**
 * Defines an interface used to cache partition key definition for a collection.
 */
interface PartitionKeyDefinitionMap {
    /**
     * Returns a PartitionKeyDefinition instance representing the partition key definition
     * for the collection identified by the collection link.
     *
     * @param collectionLink
     *            the selfLink of a collection, or the full path to the collection when using name based routing.
     */
    public PartitionKeyDefinition getPartitionKeyDefinition(String collectionLink);
}
