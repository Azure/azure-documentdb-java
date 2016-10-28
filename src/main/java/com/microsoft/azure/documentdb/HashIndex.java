package com.microsoft.azure.documentdb;

import org.apache.commons.lang3.text.WordUtils;
import org.json.JSONObject;

import com.microsoft.azure.documentdb.internal.Constants;

public final class HashIndex extends Index {

    /**
     * Specifies an instance of HashIndex class with specified DataType.
     * <p>
     * Here is an example to instantiate HashIndex class passing in the DataType:
     * <pre>
     * {@code
     *
     * HashIndex hashIndex = new HashIndex(DataType.String);
     *
     * }
     * </pre>
     *
     * @param dataType the data type.
     */
    public HashIndex(DataType dataType) {
        super(IndexKind.Hash);
        this.setDataType(dataType);
    }

    /**
     * Initializes a new instance of the HashIndex class with specified DataType and precision.
     * <p>
     * Here is an example to instantiate HashIndex class passing in the DataType:
     * <pre>
     * {@code
     *
     * HashIndex hashIndex = new HashIndex(DataType.String, 3);
     *
     * }
     * </pre>
     *
     * @param dataType  the data type.
     * @param precision the precision.
     */
    public HashIndex(DataType dataType, int precision) {
        super(IndexKind.Hash);
        this.setDataType(dataType);
        this.setPrecision(precision);
    }

    /**
     * Initializes a new instance of the HashIndex class with json string.
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
     * Initializes a new instance of the HashIndex class with json object.
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
        } catch (IllegalArgumentException e) {
            // Ignore exception and let the caller handle null value.
            this.getLogger().warning(
                    String.format("Invalid index dataType value %s.", super.getString(Constants.Properties.DATA_TYPE)));
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
