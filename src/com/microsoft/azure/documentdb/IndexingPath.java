package com.microsoft.azure.documentdb;

import org.json.JSONObject;

/**
 * Indexing paths hints to optimize indexing. Indexing paths allow tradeoff between indexing storage and query
 * performance.
 */
public final class IndexingPath extends JsonSerializable {

    /**
     * Constructor.
     */
    public IndexingPath() {
    }

    /**
     * Constructor.
     * 
     * @param jsonString the json string that represents the indexing path.
     */
    public IndexingPath(String jsonString) {
        super(jsonString);
    }

    /**
     * Constructor.
     * 
     * @param jsonObject the json object that represents the indexing path.s
     */
    public IndexingPath(JSONObject jsonObject) {
        super(jsonObject);
    }

    /**
     * Gets path.
     * 
     * @return the path.
     */
    public String getPath() {
        return super.getString(Constants.Properties.PATH);
    }

    /**
     * Sets path.
     * 
     * @param path the path.
     */
    public void setPath(String path) {
        super.set(Constants.Properties.PATH, path);
    }

    /**
     * Gets type of indexing to be applied - either Hash or Range.
     * 
     * @return the index type.
     */
    public IndexType getIndexType() {
        IndexType result = IndexType.Hash;
        String strValue = super.getString(Constants.Properties.INDEX_TYPE);
        if (strValue != null) {
            result = IndexType.valueOf(strValue);
        }
        return result;
    }

    /**
     * Sets type of indexing to be applied - either Hash or Range.
     * 
     * @param indexType the index type.
     */
    public void setIndexType(IndexType indexType) {
        super.set(Constants.Properties.INDEX_TYPE, indexType.name());
    }

    /**
     * Gets precision for this particular Index type for numeric data.
     * 
     * @return the numeric precision.
     */
    public Integer getNumericPrecision() {
        return super.getInt(Constants.Properties.NUMERIC_PRECISION);
    }

    /**
     * Sets precision for this particular Index type for numeric data.
     * 
     * @param numericPrecision the numeric precision.
     */
    public void setNumericPrecision(int numericPrecision) {
        super.set(Constants.Properties.NUMERIC_PRECISION, numericPrecision);
    }

    /**
     * Precision for this particular Index type for string data.
     * 
     * @return the string precision.
     */
    public Integer getStringPrecision() {
        return super.getInt(Constants.Properties.STRING_PRECISION);
    }

    /**
     * Precision for this particular Index type for string data.
     * 
     * @param stringPrecision the string precision.
     */
    public void setStringPrecision(int stringPrecision) {
        super.set(Constants.Properties.STRING_PRECISION, stringPrecision);
    }
}
