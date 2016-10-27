package com.microsoft.azure.documentdb.internal.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.SqlQuerySpec;
import com.microsoft.azure.documentdb.internal.ServiceJNIWrapper;
import com.microsoft.azure.documentdb.internal.Utils;
import com.microsoft.azure.documentdb.internal.routing.PartitionKeyInternal;
import com.microsoft.azure.documentdb.internal.routing.Range;

public class QueryPartitionProvider {

    private static Map<String, Object> getQueryEngineConfiguration() {
        HashMap<String, Object> defaultConfigMap = new HashMap<String, Object>() {{
            put("maxSqlQueryInputLength", 30720);
            put("maxJoinsPerSqlQuery", 5);
            put("maxLogicalAndPerSqlQuery", 200);
            put("maxLogicalOrPerSqlQuery", 200);
            put("maxUdfRefPerSqlQuery", 2);
            put("maxInExpressionItemsCount", 8000);
            put("sqlAllowTop", true);
            put("sqlAllowSubQuery", false);
            put("allowNewKeywords", true);
            put("enableSpatialIndexing", true);
            put("maxSpatialQueryCells", 12);
            put("spatialMaxGeometryPointCount", 256);
        }};
        return defaultConfigMap;
    }

    private static final List<Range<String>> singleFullRange = new ArrayList<Range<String>>() {{
        add(new Range<>(
                PartitionKeyInternal.MinimumInclusiveEffectivePartitionKey,
                PartitionKeyInternal.MaximumExclusiveEffectivePartitionKey,
                true,
                false));
    }};
    private final String queryEngineConfiguration;
    private long serviceProvider;

    public QueryPartitionProvider() {
        Map<String, Object> queryEngineConfiguration = getQueryEngineConfiguration();
        if (queryEngineConfiguration == null) {
            throw new IllegalArgumentException("queryEngineConfiguration");
        }

        if (queryEngineConfiguration.size() == 0) {
            throw new IllegalArgumentException("queryEngineConfiguration cannot be empty.");
        }

        try {
            this.queryEngineConfiguration = Utils.getSimpleObjectMapper().writeValueAsString(queryEngineConfiguration);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
        this.serviceProvider = 0;
    }

    public PartitionedQueryExecutionInfo getPartitionQueryExcecutionInfo(
            SqlQuerySpec querySpec, PartitionKeyDefinition partitionKeyDefinition) {
        if (querySpec == null
                || partitionKeyDefinition == null
                || partitionKeyDefinition.getPaths().size() == 0) {
            return new PartitionedQueryExecutionInfo(
                    new QueryInfo(),
                    QueryPartitionProvider.singleFullRange
            );
        }
        this.initializeServiceProvider();

        PartitionedQueryExecutionInfoInternal partitionedQueryExecutionInfoInternal = ServiceJNIWrapper.getPartitionKeyRangesFromQuery(
                this.serviceProvider,
                querySpec,
                partitionKeyDefinition
        );

        final List<Range<String>> effectiveRanges =
                new ArrayList<Range<String>>(partitionedQueryExecutionInfoInternal.getQueryRanges().size());
        for (Range<PartitionKeyInternal> internalRange : partitionedQueryExecutionInfoInternal.getQueryRanges()) {
            effectiveRanges.add(new Range<String>(
                    internalRange.getMin().getEffectivePartitionKeyString(partitionKeyDefinition, false),
                    internalRange.getMax().getEffectivePartitionKeyString(partitionKeyDefinition, false),
                    internalRange.isMinInclusive(),
                    internalRange.isMaxInclusive()
            ));
        }
        Collections.sort(effectiveRanges, new Range.MinComparator<String>());

        return new PartitionedQueryExecutionInfo(
                partitionedQueryExecutionInfoInternal.getQueryInfo(),
                effectiveRanges
        );
    }

    private void initializeServiceProvider() {
        if (this.serviceProvider == 0) {
            synchronized (this) {
                if (this.serviceProvider == 0) {
                    this.serviceProvider = ServiceJNIWrapper.createServiceProvider(queryEngineConfiguration);
                }
            }
        }
    }
}
