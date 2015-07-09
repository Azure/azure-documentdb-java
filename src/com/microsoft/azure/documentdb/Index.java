package com.microsoft.azure.documentdb;

import org.apache.commons.lang3.text.WordUtils;
import org.json.JSONObject;

public abstract class Index extends JsonSerializable {

    /**
     * Constructor.
     */
    protected Index(IndexKind indexKind) {
        super();
        this.setKind(indexKind);
    }

    /**
     * Constructor.
     * 
     * @param jsonString the json string that represents the index.
     */
    protected Index(String jsonString, IndexKind indexKind) {
        super(jsonString);
        this.setKind(indexKind);
    }

    /**
     * Constructor.
     * 
     * @param jsonObject the json object that represents the index.
     */
    protected Index(JSONObject jsonObject, IndexKind indexKind) {
        super(jsonObject);
        this.setKind(indexKind);
    }

    /**
     * Gets index kind.
     * 
     * @return the index kind.
     */
    public IndexKind getKind() {
        IndexKind result = null;
        try {
            result = IndexKind.valueOf(WordUtils.capitalize(super.getString(Constants.Properties.INDEX_KIND)));
        } catch(IllegalArgumentException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * Sets index kind.
     * 
     * @param indexKind the index kind.
     */
    private void setKind(IndexKind indexKind) {
        super.set(Constants.Properties.INDEX_KIND, indexKind.name());
    }
}
