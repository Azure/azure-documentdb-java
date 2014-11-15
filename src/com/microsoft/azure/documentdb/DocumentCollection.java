package com.microsoft.azure.documentdb;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;


/**
 * Represents a document collection. A collection is a named logical container for documents.
 * <p> 
 * A database may contain zero or more named collections and each collection consists of zero or more JSON documents. 
 * Being schema-free, the documents in a collection do not need to share the same structure or fields. Since collections
 * are application resources, they can be authorized using either the master key or resource keys.
 */
public final class DocumentCollection extends Resource {

    /**
     * Initialize a document collection object.
     */
    public DocumentCollection() {
        super();
    }

    /**
     * Initialize a document collection object from json string.
     * 
     * @param jsonString the json string that represents the document collection.
     */
    public DocumentCollection(String jsonString) {
        super(jsonString);
    }

    /**
     * Initialize a document collection object from json object.
     * 
     * @param jsonObject the json object that represents the document collection.
     */
    public DocumentCollection(JSONObject jsonObject) {
        super(jsonObject);
    }

    private IndexingPolicy indexingPolicy = null;

    /**
     * Sets the indexing policy.
     * 
     * @param indexingPolicy the indexing policy.
     */
    public void setIndexingPolicy(IndexingPolicy indexingPolicy) {
        this.indexingPolicy = indexingPolicy;
    }

    /**
     * Gets the indexing policy.
     * 
     * @return the indexing policy.
     */
    public IndexingPolicy getIndexingPolicy() {
        if (this.indexingPolicy == null) {
            if (super.has(Constants.Properties.INDEXING_POLICY)) {
                this.indexingPolicy = super.getObject(Constants.Properties.INDEXING_POLICY, IndexingPolicy.class);
            } else {
                this.indexingPolicy = new IndexingPolicy();
            }
        }

        return this.indexingPolicy;
    }
    
    /**
     * Gets the self-link for documents in a collection.
     * 
     * @return the document link.
     */
    public String getDocumentsLink() {
        return String.format("%s/%s",
                             StringUtils.stripEnd(super.getSelfLink(), "/"),
                             super.getString(Constants.Properties.DOCUMENTS_LINK));
    }

    /**
     * Gets the self-link for stored procedures in a collection.
     * 
     * @return the stored procedures link.
     */
    public String getStoredProceduresLink() {
        return String.format("%s/%s",
                             StringUtils.stripEnd(super.getSelfLink(), "/"),
                             super.getString(Constants.Properties.STORED_PROCEDURES_LINK));
    }
    
    /**
     * Gets the self-link for triggers in a collection.
     * 
     * @return the trigger link.
     */
    public String getTriggersLink() {
        return StringUtils.removeEnd(this.getSelfLink(), "/") +
                "/" + super.getString(Constants.Properties.TRIGGERS_LINK);
    }

    /**
     * Gets the self-link for user defined functions in a collection.
     * 
     * @return the user defined functions link.
     */
    public String getUserDefinedFunctionsLink() {
        return StringUtils.removeEnd(this.getSelfLink(),"/") +
                "/" + super.getString(Constants.Properties.USER_DEFINED_FUNCTIONS_LINK);
    }

    /**
     * Gets the self-link for conflicts in a collection.
     * 
     * @return the conflicts link.
     */
    public String getConflictsLink() {
        return StringUtils.removeEnd(this.getSelfLink(), "/") +
                "/" + super.getString(Constants.Properties.CONFLICTS_LINK);
    }
    
    @Override
    public void onSave() {
        if (this.indexingPolicy != null) {
            this.indexingPolicy.onSave();
            super.set(Constants.Properties.INDEXING_POLICY, this.indexingPolicy);
        }
    }
}