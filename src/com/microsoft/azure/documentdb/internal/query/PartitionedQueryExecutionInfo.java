package com.microsoft.azure.documentdb.internal.query;

import java.util.*;

import org.json.JSONObject;

import com.microsoft.azure.documentdb.JsonSerializable;
import com.microsoft.azure.documentdb.internal.routing.Range;

final class PartitionedQueryExecutionInfo extends JsonSerializable {
    @SuppressWarnings("unchecked")
    private static final Class<Range<String>> QUERY_RANGES_CLASS = (Class<Range<String>>) Range
            .getEmptyRange((String) null).getClass();

    private QueryInfo queryInfo;
    private List<Range<String>> queryRanges;

    public PartitionedQueryExecutionInfo(String jsonString) {
        super(jsonString);
    }

    public PartitionedQueryExecutionInfo(JSONObject jsonObject) {
        super(jsonObject);
    }

    public int getVersion() {
        return super.getInt("partitionedQueryExecutionInfoVersion");
    }

    public QueryInfo getQueryInfo() {
        return this.queryInfo != null ? this.queryInfo
                : (this.queryInfo = super.getObject("queryInfo", QueryInfo.class));
    }

    public List<Range<String>> getQueryRanges() {
        return this.queryRanges != null ? this.queryRanges
                : (this.queryRanges = (List<Range<String>>) super.getCollection("queryRanges", QUERY_RANGES_CLASS));
    }
}
