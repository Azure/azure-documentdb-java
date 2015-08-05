package com.microsoft.azure.documentdb;

import org.apache.commons.lang3.text.WordUtils;
import org.json.JSONObject;

public final class SpatialIndex extends Index {

    /**
     * Constructor.
     */
    public SpatialIndex(DataType dataType) {
        super(IndexKind.Spatial);
        this.setDataType(dataType);
    }

    /**
     * Constructor.
     * 
     * @param jsonString the json string that represents the index.
     */
    public SpatialIndex(String jsonString) {
        super(jsonString, IndexKind.Spatial);
        if (this.getDataType() == null) {
            throw new IllegalArgumentException("The jsonString doesn't contain a valid 'dataType'.");
        }
    }

    /**
     * Constructor.
     * 
     * @param jsonObject the json object that represents the index.
     */
    public SpatialIndex(JSONObject jsonObject) {
        super(jsonObject, IndexKind.Spatial);
        if (this.getDataType() == null) {
            throw new IllegalArgumentException("The jsonObject doesn't contain a valid 'dataType'.");
        }
    }

    /**
     * Gets data type.
     * 
     * @return the data type.
     */
    public DataType getDataType() {
        DataType result = null;
        try {
            result = DataType.valueOf(WordUtils.capitalize(super.getString(Constants.Properties.DATA_TYPE)));
        } catch(IllegalArgumentException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * Sets data type.
     * 
     * @param dataType the data type.
     */
    public void setDataType(DataType dataType) {
        super.set(Constants.Properties.DATA_TYPE, dataType.name());
    }
}
