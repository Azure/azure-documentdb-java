package com.microsoft.azure.documentdb.internal.query;

import java.util.List;

import org.json.JSONObject;

import com.microsoft.azure.documentdb.JsonSerializable;
import com.microsoft.azure.documentdb.internal.Constants;
import com.microsoft.azure.documentdb.internal.routing.Range;

public final class PartitionedQueryExecutionInfo extends JsonSerializable {
    @SuppressWarnings("unchecked")
    private static final Class<Range<String>> QUERY_RANGES_CLASS = (Class<Range<String>>) Range
            .getEmptyRange((String) null).getClass();

    private QueryInfo queryInfo;
    private List<Range<String>> queryRanges;

    PartitionedQueryExecutionInfo(QueryInfo queryInfo, List<Range<String>> queryRanges) {
        this.queryInfo = queryInfo;
        this.queryRanges = queryRanges;

        super.set(
                PartitionedQueryExecutionInfoInternal.PARTITIONED_QUERY_EXECUTION_INFO_VERSION_PROPERTY,
                Constants.PartitionedQueryExecutionInfo.VERSION_1);
    }

    public PartitionedQueryExecutionInfo(String jsonString) {
        super(jsonString);
    }

    public PartitionedQueryExecutionInfo(JSONObject jsonObject) {
        super(jsonObject);
    }

    public int getVersion() {
        return super.getInt(PartitionedQueryExecutionInfoInternal.PARTITIONED_QUERY_EXECUTION_INFO_VERSION_PROPERTY);
    }

    public QueryInfo getQueryInfo() {
        return this.queryInfo != null ? this.queryInfo
                : (this.queryInfo = super.getObject(
                        PartitionedQueryExecutionInfoInternal.QUERY_INFO_PROPERTY, QueryInfo.class));
    }

    public List<Range<String>> getQueryRanges() {
        return this.queryRanges != null ? this.queryRanges
                : (this.queryRanges = (List<Range<String>>) super.getCollection(
                        PartitionedQueryExecutionInfoInternal.QUERY_RANGES_PROPERTY, QUERY_RANGES_CLASS));
    }
}
