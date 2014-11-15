package com.microsoft.azure.documentdb;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.lang3.text.WordUtils;
import org.json.JSONObject;


/**
 * Represents the indexing policy configuration for a collection.
 */
public final class IndexingPolicy extends JsonSerializable {

    /**
     * Constructor.
     */
    public IndexingPolicy() {
        this.setAutomatic(true);
        this.setIndexingMode(IndexingMode.Consistent);
    }

    /**
     * Constructor.
     * 
     * @param jsonString the json string that represents the indexing policy.
     */
    public IndexingPolicy(String jsonString) {
        super(jsonString);
    }

    /**
     * Constructor.
     * 
     * @param jsonObject the json object that represents the indexing policy.
     */
    public IndexingPolicy(JSONObject jsonObject) {
        super(jsonObject);
    }

    /**
     * Sets whether automatic indexing is enabled for a collection.
     * <p>
     * In automatic indexing, documents can be explicitly excluded from indexing using RequestOptions. In manual
     * indexing, documents can be explicitly included.
     * 
     * @param automatic the automatic
     */
    public void setAutomatic(boolean automatic) {
        super.set(Constants.Properties.AUTOMATIC, automatic);
    }

    /**
     * Gets whether automatic indexing is enabled for a collection.
     * <p>
     * In automatic indexing, documents can be explicitly excluded from indexing using RequestOptions. In manual
     * indexing, documents can be explicitly included.
     * 
     * @return the automatic
     */
    public Boolean getAutomatic() {
        return super.getBoolean(Constants.Properties.AUTOMATIC);
    }

    /**
     * Sets the indexing mode (consistent or lazy).
     * 
     * @param indexingMode the indexing mode.
     */
    public void setIndexingMode(IndexingMode indexingMode) {
        super.set(Constants.Properties.INDEXING_MODE, indexingMode.name());
    }

    /**
     * Gets the indexing mode (consistent or lazy).
     * 
     * @return the indexing mode.
     */
    public IndexingMode getIndexingMode() {
        IndexingMode result = IndexingMode.Lazy;
        try {
            result = IndexingMode.valueOf(WordUtils.capitalize(super.getString(Constants.Properties.INDEXING_MODE)));
        } catch(IllegalArgumentException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * Gets or sets the path level configurations for indexing.
     */
    private Collection<IndexingPath> included;
    private Collection<String> excluded;


    /**
     * Gets the paths that are chosen to be indexed by the user.
     * 
     * @return the included paths.
     */
    public Collection<IndexingPath> getIncludedPaths() {
        if (this.included == null) {
            this.included = super.getCollection(Constants.Properties.INCLUDED_PATHS, IndexingPath.class);

            if (this.included == null) {
                this.included = new ArrayList<IndexingPath>();
            }
        }

        return this.included;
    }
    
    /**
     * Gets the paths that are not indexed.
     * 
     * @return the excluded paths.
     */
    public Collection<String> getExcludedPaths() {
        if (this.excluded == null) {
            this.excluded = super.getCollection(Constants.Properties.EXCLUDED_PATHS, String.class);

            if (this.excluded == null) {
                this.excluded = new ArrayList<String>();
            }
        }

        return this.excluded;
    }

    @Override
    void onSave() {
        boolean bDefaultPaths = (this.included != null && this.included.size() == 0 && 
                                 this.excluded != null && this.excluded.size() == 0);

        // If we do not have any user-specified paths, do not serialize.
        // If we don't do this, included and excluded will be sent as empty lists [], [] which have a different meaning
        // on server.
        if (bDefaultPaths) return;

        if (this.included != null) {
            super.set(Constants.Properties.INCLUDED_PATHS, this.included);
        }

        if (this.excluded != null) {
            super.set(Constants.Properties.EXCLUDED_PATHS, this.excluded);
        }
    }
}
