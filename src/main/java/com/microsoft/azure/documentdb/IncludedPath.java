package com.microsoft.azure.documentdb;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.lang3.text.WordUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import com.microsoft.azure.documentdb.internal.Constants;

public class IncludedPath extends JsonSerializable {

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

    public void setIndexes(Collection<Index> indexes) {
        this.indexes = indexes;
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

    @Override
    void onSave() {
        if (this.indexes != null) {
            for (Index index : this.indexes) {
                index.onSave();
            }

            super.set(Constants.Properties.INDEXES, this.indexes);
        }
    }
}
