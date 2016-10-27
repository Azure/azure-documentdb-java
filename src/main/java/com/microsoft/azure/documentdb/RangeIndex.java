package com.microsoft.azure.documentdb;

import org.apache.commons.lang3.text.WordUtils;
import org.json.JSONObject;

import com.microsoft.azure.documentdb.internal.Constants;

public final class RangeIndex extends Index {

    /**
     * Initializes a new instance of the RangeIndex class with specified DataType.
     * <p>
     * Here is an example to instantiate RangeIndex class passing in the DataType:
     * <pre>
     * {@code
     *
     * RangeIndex rangeIndex = new RangeIndex(DataType.Number);
     *
     * }
     * </pre>
     *
     * @param dataType the data type.
     */
    public RangeIndex(DataType dataType) {
        super(IndexKind.Range);
        this.setDataType(dataType);
    }

    /**
     * Initializes a new instance of the RangeIndex class with specified DataType and precision.
     * <pre>
     * {@code
     *
     * RangeIndex rangeIndex = new RangeIndex(DataType.Number, -1);
     *
     * }
     * </pre>
     * @param dataType   the data type of the RangeIndex
     * @param precision  the precision of the RangeIndex
     */
    public RangeIndex(DataType dataType, int precision) {
        super(IndexKind.Range);
        this.setDataType(dataType);
        this.setPrecision(precision);
    }

    /**
     * Initializes a new instance of the RangeIndex class with json string.
     *
     * @param jsonString the json string that represents the index.
     */
    public RangeIndex(String jsonString) {
        super(jsonString, IndexKind.Range);
        if (this.getDataType() == null) {
            throw new IllegalArgumentException("The jsonString doesn't contain a valid 'dataType'.");
        }
    }

    /**
     * Initializes a new instance of the RangeIndex class with json object.
     *
     * @param jsonObject the json object that represents the index.
     */
    public RangeIndex(JSONObject jsonObject) {
        super(jsonObject, IndexKind.Range);
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
