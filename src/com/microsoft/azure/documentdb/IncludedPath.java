package com.microsoft.azure.documentdb;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.lang3.text.WordUtils;
import org.json.JSONArray;
import org.json.JSONObject;

public class IncludedPath extends JsonSerializable {

    // default number precisions
    private static final short DEFAULT_NUMBER_HASH_PRECISION = 3;
    private static final short DEFAULT_NUMBER_RANGE_PRECISION = -1;

    // default string precision
    private static final short DEFAULT_STRING_HASH_PRECISION = 3;
    private static final short DEFAULT_STRING_RANGE_PRECISION = -1;

    private Collection<Index> indexes;

    /**
     * Constructor.
     */
    public IncludedPath() {
        super();
    }

    /**
     * Constructor.
     * 
     * @param jsonString the json string that represents the included path.
     */
    public IncludedPath(String jsonString) {
        super(jsonString);
    }

    /**
     * Constructor.
     * 
     * @param jsonObject the json object that represents the included path.
     */
    public IncludedPath(JSONObject jsonObject) {
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
     * Gets the paths that are chosen to be indexed by the user.
     * 
     * @return the included paths.
     */
    public Collection<Index> getIndexes() {
        if (this.indexes == null) {
            this.indexes = this.getIndexCollection();

            if (this.indexes == null) {
                this.indexes = new ArrayList<Index>();
            }
        }

        return this.indexes;
    }
    
    private Collection<Index> getIndexCollection() {
        if (this.propertyBag != null && this.propertyBag.has(Constants.Properties.INDEXES)) {
            JSONArray jsonArray = this.propertyBag.getJSONArray(Constants.Properties.INDEXES);
            Collection<Index> result = new ArrayList<Index>();

            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                
                IndexKind indexKind = IndexKind.valueOf(WordUtils.capitalize(
                        jsonObject.getString(Constants.Properties.INDEX_KIND)));
                switch (indexKind) {
                case Hash:
                    result.add(new HashIndex(jsonObject.toString()));
                    break;
                case Range:
                    result.add(new RangeIndex(jsonObject.toString()));
                    break;
                case Spatial:
                    result.add(new SpatialIndex(jsonObject.toString()));
                    break;
                }
            }

            return result;
        }

        return null;
    }

    public void setIndexes(Collection<Index> indexes) {
        this.indexes = indexes;
    }

    @Override
    void onSave() {
        if (this.getIndexes().size() == 0) {
            HashIndex hashIndex = new HashIndex(DataType.String);
            hashIndex.setPrecision(IncludedPath.DEFAULT_STRING_HASH_PRECISION);
            this.indexes.add(hashIndex);

            RangeIndex rangeIndex = new RangeIndex(DataType.Number);
            rangeIndex.setPrecision(IncludedPath.DEFAULT_NUMBER_RANGE_PRECISION);
            this.indexes.add(rangeIndex);
        }

        for (Index index : this.indexes) {
            if (index.getKind() == IndexKind.Hash) {
                HashIndex hashIndex = (HashIndex)index;
                if (!hashIndex.hasPrecision()) {
                    if(hashIndex.getDataType() == DataType.Number) {
                        hashIndex.setPrecision(IncludedPath.DEFAULT_NUMBER_HASH_PRECISION);
                    } else if(hashIndex.getDataType() == DataType.String) {
                        hashIndex.setPrecision(IncludedPath.DEFAULT_STRING_HASH_PRECISION);
                    }
                }
            } else if(index.getKind() == IndexKind.Range) {
                RangeIndex rangeIndex = (RangeIndex)index;
                if (!rangeIndex.hasPrecision()) {
                    if (rangeIndex.getDataType() == DataType.Number) {
                        rangeIndex.setPrecision(IncludedPath.DEFAULT_NUMBER_RANGE_PRECISION);
                    } else if (rangeIndex.getDataType() == DataType.String) {
                        rangeIndex.setPrecision(IncludedPath.DEFAULT_STRING_RANGE_PRECISION);
                    }
                }
            }
        }

        super.set(Constants.Properties.INDEXES, this.indexes);
    }
}
