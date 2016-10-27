package com.microsoft.azure.documentdb.internal.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONObject;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.microsoft.azure.documentdb.JsonSerializable;
import com.microsoft.azure.documentdb.internal.Constants;
import com.microsoft.azure.documentdb.internal.Utils;
import com.microsoft.azure.documentdb.internal.routing.PartitionKeyInternal;
import com.microsoft.azure.documentdb.internal.routing.Range;

public final class PartitionedQueryExecutionInfoInternal extends JsonSerializable {
    static final String QUERY_INFO_PROPERTY = "queryInfo";
    static final String QUERY_RANGES_PROPERTY = "queryRanges";
    static final String PARTITIONED_QUERY_EXECUTION_INFO_VERSION_PROPERTY = "partitionedQueryExecutionInfoVersion";

    @SuppressWarnings("unchecked")
    private static final Class<Range<PartitionKeyInternal>> QUERY_RANGE_CLASS = (Class<Range<PartitionKeyInternal>>) Range
            .getEmptyRange((PartitionKeyInternal) null).getClass();

    private QueryInfo queryInfo;
    private List<Range<PartitionKeyInternal>> queryRanges;

    public PartitionedQueryExecutionInfoInternal() {
        super.set(PARTITIONED_QUERY_EXECUTION_INFO_VERSION_PROPERTY, Constants.PartitionedQueryExecutionInfo.VERSION_1);
    }

    public PartitionedQueryExecutionInfoInternal(String jsonString) {
        super(jsonString);
    }

    public PartitionedQueryExecutionInfoInternal(JSONObject jsonObject) {
        super(jsonObject);
    }

    public int getVersion() {
        return super.getInt(PARTITIONED_QUERY_EXECUTION_INFO_VERSION_PROPERTY);
    }

    public QueryInfo getQueryInfo() {
        return this.queryInfo != null ? this.queryInfo
                : (this.queryInfo = super.getObject(QUERY_INFO_PROPERTY, QueryInfo.class));
    }

    public void setQueryInfo(QueryInfo queryInfo) {
        this.queryInfo = queryInfo;
    }

    public List<Range<PartitionKeyInternal>> getQueryRanges() {
        return this.queryRanges != null ? this.queryRanges
                : (this.queryRanges = (List<Range<PartitionKeyInternal>>) super.getCollection(QUERY_RANGES_PROPERTY, QUERY_RANGE_CLASS));
    }

    public void setQueryRanges(List<Range<PartitionKeyInternal>> queryRanges) {
        this.queryRanges = queryRanges;
    }

    @Override
    public String toString() {
        try {
            return Utils.getSimpleObjectMapper().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Unable to serialize partition query execution info internal.");
        }
    }
}
