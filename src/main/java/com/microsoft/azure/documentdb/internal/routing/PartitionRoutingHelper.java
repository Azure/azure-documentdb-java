package com.microsoft.azure.documentdb.internal.routing;

import java.io.IOException;
import java.security.Provider;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.http.protocol.HTTP;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.VersionUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.PartitionKeyRange;
import com.microsoft.azure.documentdb.SqlQuerySpec;
import com.microsoft.azure.documentdb.internal.Constants;
import com.microsoft.azure.documentdb.internal.HttpConstants;
import com.microsoft.azure.documentdb.internal.ServiceJNIWrapper;
import com.microsoft.azure.documentdb.internal.Utils;
import com.microsoft.azure.documentdb.internal.VersionUtility;
import com.microsoft.azure.documentdb.internal.query.PartitionedQueryExecutionInfo;
import com.microsoft.azure.documentdb.internal.query.QueryPartitionProvider;

public class PartitionRoutingHelper {
    public static PartitionKeyRange tryGetTargetRangeFromContinuationTokenRange(
            List<Range<String>> providedPartitionKeyRanges,
            RoutingMapProvider routingMapProvider,
            String collectionLink,
            Range<String> rangeFromContinuationToken) {

        // For queries such as "SELECT * FROM root WHERE false",
        // we will have empty ranges and just forward the request to the first partition
        if (providedPartitionKeyRanges.size() == 0) {
            return routingMapProvider.tryGetRangeByEffectivePartitionKey(collectionLink,
                    PartitionKeyInternal.MinimumInclusiveEffectivePartitionKey);
        }

        // Initially currentRange will be empty
        if (rangeFromContinuationToken.isEmpty()) {
            Range<String> minimumRange = PartitionRoutingHelper.min(providedPartitionKeyRanges, new Range.MinComparator<String>());

            return routingMapProvider.tryGetRangeByEffectivePartitionKey(collectionLink, minimumRange.getMin());
        }

        PartitionKeyRange targetPartitionKeyRange = routingMapProvider.tryGetRangeByEffectivePartitionKey(
                collectionLink, rangeFromContinuationToken.getMin()
        );

        if (targetPartitionKeyRange == null
                || !rangeFromContinuationToken.equals(targetPartitionKeyRange.toRange())) {
            // Cannot find target range. Either collection was resolved incorrectly or the range was split.
            // We cannot distinguish here. Returning null and refresh the cache and retry.
            return null;
        }

        return targetPartitionKeyRange;
    }

    private static <T> T min(List<T> values, Comparator<T> comparer) {
        if (values.size() == 0) {
            throw new IllegalArgumentException("values");
        }

        T min = values.get(0);
        for (int i = 1; i < values.size(); ++i) {
            if (comparer.compare(values.get(i), min) < 0) {
                min = values.get(i);
            }
        }

        return min;
    }

    public static Range<String> extractPartitionKeyRangeFromContinuationToken(Map<String, String> headers) {
        Range<String> range = Range.getEmptyRange(PartitionKeyInternal.MinimumInclusiveEffectivePartitionKey);

        String continuationToken = headers.get(HttpConstants.HttpHeaders.CONTINUATION);

        if (continuationToken == null || continuationToken.isEmpty()) {
            return range;
        }

        CompositeContinuationToken compositeContinuationToken;
        try {
            compositeContinuationToken = Utils.getSimpleObjectMapper().readValue(continuationToken,
                    CompositeContinuationToken.class);
            if (compositeContinuationToken.getRange() != null) {
                range = compositeContinuationToken.getRange();
            }
            headers.put(HttpConstants.HttpHeaders.CONTINUATION,
                    compositeContinuationToken.getToken() != null && !compositeContinuationToken.getToken().isEmpty()
                            ? compositeContinuationToken.getToken()
                            : "");
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        return range;
    }

    public static boolean tryAddPartitionKeyRangeToContinuationToken(
            Map<String, String> headers,
            List<Range<String>> providedPartitionKeyRanges,
            RoutingMapProvider routingMapProvider,
            String collectionLink,
            PartitionKeyRange currentRange) {
        PartitionKeyRange rangeToUse = currentRange;

        // We only need to get the next range if we have to
        if (headers.get(HttpConstants.HttpHeaders.CONTINUATION) == null
                || headers.get(HttpConstants.HttpHeaders.CONTINUATION).isEmpty()) {
            Range<String> nextProvidedRange = PartitionRoutingHelper.minAfter(
                    providedPartitionKeyRanges,
                    currentRange.toRange(),
                    new Range.MaxComparator<String>()
            );

            if (nextProvidedRange == null) {
                return true;
            }

            String max = nextProvidedRange.getMin().compareTo(currentRange.getMaxExclusive()) > 0
                    ? nextProvidedRange.getMin()
                    : currentRange.getMaxExclusive();

            if (max.compareTo(PartitionKeyInternal.MaximumExclusiveEffectivePartitionKey) == 0) {
                return true;
            }

            PartitionKeyRange nextRange = routingMapProvider.tryGetRangeByEffectivePartitionKey(collectionLink, max);
            if (nextRange == null) {
                return false;
            }

            rangeToUse = nextRange;
        }

        if (rangeToUse != null) {
            headers.put(HttpConstants.HttpHeaders.CONTINUATION, PartitionRoutingHelper.addPartitionKeyRangeToContinuationToken(
                    headers.get(HttpConstants.HttpHeaders.CONTINUATION),
                    rangeToUse
            ));
        }

        return true;
    }

    private static <T> T minAfter(List<T> values, T minValue, Comparator<T> comparer) {
        if (values.size() == 0) {
            throw new IllegalArgumentException("values");
        }

        T min = null;
        for (T value : values) {
            if (comparer.compare(value, minValue) > 0 && (min == null || comparer.compare(value, min) < 0)) {
                min = value;
            }
        }

        return min;
    }

    private static String addPartitionKeyRangeToContinuationToken(String continuationToken, PartitionKeyRange partitionKeyRange) {
        CompositeContinuationToken compositeContinuationToken = new CompositeContinuationToken();
        compositeContinuationToken.setToken(continuationToken);
        compositeContinuationToken.setRange(partitionKeyRange.toRange());
        try {
            String serializedCompositeToken = Utils.getSimpleObjectMapper().writeValueAsString(compositeContinuationToken);
            return serializedCompositeToken;
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    public static List<Range<String>> getProvidedPartitionKeyRanges(
            SqlQuerySpec querySpec,
            boolean enableCrossPartitionQuery,
            boolean parallelizeCrossPartitionQuery,
            PartitionKeyDefinition partitionKeyDefinition,
            QueryPartitionProvider queryPartitionProvider,
            String clientApiVersion) throws DocumentClientException {
        if (querySpec == null) {
            throw new IllegalArgumentException("querySpec");
        }

        if (queryPartitionProvider == null) {
            throw new IllegalArgumentException("queryPartitionProvider");
        }

        if (partitionKeyDefinition != null && partitionKeyDefinition.getPaths().size() > 0) {
            PartitionedQueryExecutionInfo queryExecutionInfo = null;

            queryExecutionInfo = queryPartitionProvider.getPartitionQueryExcecutionInfo(querySpec, partitionKeyDefinition);

            if (queryExecutionInfo == null
                    || queryExecutionInfo.getQueryRanges() == null
                    || queryExecutionInfo.getQueryInfo() == null) {
                Logger.getGlobal().log(Level.INFO, "QueryPartitionProvider");
            }

            boolean isSinglePartitionQuery = queryExecutionInfo.getQueryRanges().size() == 1
                    && queryExecutionInfo.getQueryRanges().iterator().next().isSingleValue();
            if (!isSinglePartitionQuery) {
                if (!enableCrossPartitionQuery) {
                    throw new DocumentClientException(
                            HttpConstants.StatusCodes.BADREQUEST,
                            "Cross partition query is required but disabled. Please set x-ms-documentdb-query-enablecrosspartition " +
                                    "to true, specify x-ms-documentdb-partitionkey, or revise your query to avoid this exception.");
                } else {
                    if (parallelizeCrossPartitionQuery
                            || (queryExecutionInfo.getQueryInfo() != null
                            && (queryExecutionInfo.getQueryInfo().hasTop() || queryExecutionInfo.getQueryInfo().hasOrderBy()))) {
                        if (isSupportedPartitionQueryExecutionInfo(queryExecutionInfo, clientApiVersion)) {
                            throw new DocumentClientException(HttpConstants.StatusCodes.BADREQUEST,
                                    "Cross partition query with TOP and/or ORDER BY is not supported. " + queryExecutionInfo.toString()
                            );
                        } else {
                            throw new DocumentClientException(HttpConstants.StatusCodes.BADREQUEST,
                                    "Cross partition query with TOP and/or ORDER BY is not supported.");
                        }
                    }
                }
            }

            return queryExecutionInfo.getQueryRanges();
        } else {
            return new ArrayList<Range<String>>() {{
                add(Range.getPointRange(PartitionKeyInternal.MinimumInclusiveEffectivePartitionKey));
            }};
        }
    }

    private static boolean isSupportedPartitionQueryExecutionInfo(PartitionedQueryExecutionInfo queryExecutionInfo,
                                                                  String clientApiVersion) throws DocumentClientException {
        if (VersionUtility.isLaterThan(clientApiVersion, HttpConstants.Versions.V2016_07_11)) {
            return queryExecutionInfo.getVersion() <= Constants.PartitionedQueryExecutionInfo.VERSION_1;
        }

        return false;
    }
}

final class CompositeContinuationToken {
    private String token;
    private Range<String> range;

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public Range<String> getRange() {
        return range;
    }

    public void setRange(Range<String> range) {
        this.range = range;
    }
}