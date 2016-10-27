package com.microsoft.azure.documentdb;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.lang3.text.WordUtils;
import org.json.JSONObject;

import com.microsoft.azure.documentdb.internal.Constants;

/**
 * Represents a partition key definition. A partition key definition specifies which
 * document property is used as the partition key in a collection that has multiple partitions.
 */
public final class PartitionKeyDefinition extends JsonSerializable {

    /**
     * Constructor. Creates a new instance of the PartitionKeyDefinition object.
     */
    public PartitionKeyDefinition() {
        this.setKind(PartitionKind.Hash);
    }

    /**
     * Constructor. Creates a new instance of the PartitionKeyDefinition object from a
     * JSON string.
     *
     * @param jsonString the JSON string that represents the partition key definition.
     */
    public PartitionKeyDefinition(String jsonString) {
        super(jsonString);
    }

    /**
     * Constructor. Creates a new instance of the PartitionKeyDefinition object from a
     * JSON object.
     *
     * @param jsonObject the JSON object that represents the partition key definition.
     */
    public PartitionKeyDefinition(JSONObject jsonObject) {
        super(jsonObject);
    }

    /**
     * Sets the partition algorithm used to calculate the partition id given a partition key.
     *
     * @return the partition algorithm.
     */
    public PartitionKind getKind() {
        return PartitionKind.valueOf(WordUtils.capitalize(super.getString(Constants.Properties.PARTITION_KIND)));
    }

    /**
     * Sets the partition algorithm used to calculate the partition id given a partition key.
     *
     * @param kind the partition algorithm.
     */
    public void setKind(PartitionKind kind) {
        super.set(Constants.Properties.PARTITION_KIND, kind.name());
    }

    /**
     * Gets the document property paths for the partition key.
     *
     * @return the paths to the document properties that form the partition key.
     */
    public Collection<String> getPaths() {
        Collection<String> paths = super.getCollection(Constants.Properties.PARTITION_KEY_PATHS, String.class);
        if (paths == null) {
            paths = new ArrayList<>();
            super.set(Constants.Properties.PARTITION_KEY_PATHS, paths);
        }
        return paths;
    }

    /**
     * Sets the document property paths for the partition key.
     *
     * @param paths the paths to document properties that form the partition key.
     */
    public void setPaths(Collection<String> paths) {
        if (paths == null || paths.size() == 0) {
            throw new IllegalArgumentException("paths must not be null or empty.");
        }

        super.set(Constants.Properties.PARTITION_KEY_PATHS, paths);
    }
}
