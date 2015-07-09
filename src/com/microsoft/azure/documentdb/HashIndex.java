package com.microsoft.azure.documentdb;

import org.apache.commons.lang3.text.WordUtils;
import org.json.JSONObject;

public final class HashIndex extends Index {

    /**
     * Constructor.
     */
    public HashIndex(DataType dataType) {
        super(IndexKind.Hash);
        this.setDataType(dataType);
    }

    /**
     * Constructor.
     * 
     * @param jsonString the json string that represents the index.
     */
    public HashIndex(String jsonString) {
        super(jsonString, IndexKind.Hash);
        if (this.getDataType() == null) {
            throw new IllegalArgumentException("The jsonString doesn't contain a valid 'dataType'.");
        }
    }

    /**
     * Constructor.
     * 
     * @param jsonObject the json object that represents the index.
     */
    public HashIndex(JSONObject jsonObject) {
        super(jsonObject, IndexKind.Hash);
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

    /**
     * Gets precision.
     * 
     * @return the precision.
     */
    public int getPrecision() {
        return super.getInt(Constants.Properties.PRECISION);
    }

    /**
     * Sets precision.
     * 
     * @param precision the precision.
     */
    public void setPrecision(int precision) {
        super.set(Constants.Properties.PRECISION, precision);
    }

    boolean hasPrecision() {
        return super.has(Constants.Properties.PRECISION);
    }
}
