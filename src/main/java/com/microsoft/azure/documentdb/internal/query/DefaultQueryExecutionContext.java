package com.microsoft.azure.documentdb.internal.query;

import java.util.*;

import com.microsoft.azure.documentdb.*;
import com.microsoft.azure.documentdb.internal.routing.Range;
import com.microsoft.azure.documentdb.internal.*;
import com.microsoft.azure.documentdb.internal.routing.*;

final class DefaultQueryExecutionContext<T extends Resource> extends AbstractQueryExecutionContext<T> {
    private final Queue<T> buffer;
    private final DocumentServiceRequest request;
    private PartitionedQueryExecutionInfo queryExecutionInfo;
    private List<Range<String>> providedRanges;
    private DocumentCollection collection;

    public DefaultQueryExecutionContext(
            DocumentQueryClient client, ResourceType resourceType, Class<T> classT,
            SqlQuerySpec querySpec, PartitionedQueryExecutionInfo queryExecutionInfo,
            FeedOptions options, String resourceLink) {
        super(client, resourceType, classT, querySpec, options, resourceLink);
        this.buffer = new ArrayDeque<T>();
        this.request = super.createRequest(querySpec, this.getPartitionKeyInternal());
        this.queryExecutionInfo = queryExecutionInfo;
    }

    @Override
    public List<T> fetchNextBlock() throws DocumentClientException {
        if (!this.hasNext()) {
            return null;
        }

        this.fillBuffer();
        List<T> result = new ArrayList<T>(this.buffer);
        this.buffer.clear();
        return result;
    }

    @Override
    public boolean hasNext() {
        return super.hasNextInternal() || !this.buffer.isEmpty();
    }

    @Override
    public T next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException("next");
        }

        try {
            this.fillBuffer();
        } catch (DocumentClientException e) {
            throw new IllegalStateException(e);
        }

        return this.buffer.poll();
    }

    @Override
    public void onNotifyStop() {
        // Nothing to do
    }

    private void fillBuffer() throws DocumentClientException {
        while (super.hasNextInternal() && this.buffer.isEmpty()) {
            DocumentServiceResponse response = executeOnce();

            super.responseHeaders = response.getResponseHeaders();
            this.buffer.addAll(response.getQueryResponse(this.classT));
            this.request.getHeaders().put(HttpConstants.HttpHeaders.CONTINUATION, super.getContinuationToken());
        }
    }

    private DocumentServiceResponse executeOnce() throws DocumentClientException {
        String partitionKey = this.request.getHeaders().get(HttpConstants.HttpHeaders.PARTITION_KEY);
        if (partitionKey != null && !partitionKey.isEmpty()
                || !this.resourceType.isPartitioned()) {
            return this.executeRequest(this.request);
        }

        if (this.collection == null) {
            this.collection = this.client.getCollectionCache().resolveCollection(this.request);
        }

        if (!Utils.isCollectionPartitioned(this.collection)) {
            this.request.routeTo(new PartitionKeyRangeIdentity(this.collection.getResourceId(), "0"));
            return this.executeRequest(this.request);
        }

        if (!ServiceJNIWrapper.isServiceJNIAvailable()
                && this.shouldExecuteQuery()) {
            return this.executeRequest(this.request);
        }

        Range<String> rangeFromContinuationToken = PartitionRoutingHelper
                .extractPartitionKeyRangeFromContinuationToken(this.request.getHeaders());

        RoutingMapProvider routingMapProvider = this.client.getPartitionKeyRangeCache();
        CollectionCache collectionCache = this.client.getCollectionCache();

        this.populateProvidedRanges();

        PartitionKeyRange targetPartitionKeyRange = this.tryGetTargetPartitionKeyRange(rangeFromContinuationToken);

        if (this.request.getIsNameBased() && targetPartitionKeyRange == null) {
            this.request.setForceNameCacheRefresh(true);
            this.collection = collectionCache.resolveCollection(this.request);
            targetPartitionKeyRange = this.tryGetTargetPartitionKeyRange(rangeFromContinuationToken);
        }

        if (targetPartitionKeyRange == null) {
            throw new DocumentClientException(HttpConstants.StatusCodes.NOTFOUND, "Target range information not found.");
        }

        this.request.routeTo(new PartitionKeyRangeIdentity(this.collection.getResourceId(), targetPartitionKeyRange.getId()));

        DocumentServiceResponse response = this.executeRequest(this.request);

        if (!PartitionRoutingHelper.tryAddPartitionKeyRangeToContinuationToken(
                response.getResponseHeaders(),
                this.providedRanges,
                routingMapProvider,
                this.collection.getSelfLink(),
                targetPartitionKeyRange)) {
            throw new DocumentClientException(HttpConstants.StatusCodes.NOTFOUND, "Collection not found");
        }

        return response;
    }

    private PartitionKeyRange tryGetTargetPartitionKeyRange(
            Range<String> rangeFromContinuationToken) throws DocumentClientException {

        PartitionKeyRange targetPartitionKeyRange = PartitionRoutingHelper.tryGetTargetRangeFromContinuationTokenRange(
                this.providedRanges,
                this.client.getPartitionKeyRangeCache(),
                this.collection.getSelfLink(),
                rangeFromContinuationToken);

        return targetPartitionKeyRange;
    }

    /**
     * Derive the ranges for the query of this execution context.
     * @return                          a boolean value indicating whether the population has been taken place or not
     * @throws DocumentClientException
     */
    private boolean populateProvidedRanges() throws DocumentClientException {

        if (this.providedRanges != null) {
            return false;
        }

        if (this.providedRanges == null && this.queryExecutionInfo != null) {
            this.providedRanges = this.queryExecutionInfo.getQueryRanges();
            return true;
        }

        String version = this.request.getHeaders().get(HttpConstants.HttpHeaders.VERSION);
        version = version == null || version.isEmpty() ? HttpConstants.Versions.CURRENT_VERSION : version;

        String enableCrossPartitionQueryHeader = this.request.getHeaders().get(HttpConstants.HttpHeaders.ENABLE_CROSS_PARTITION_QUERY);
        boolean enableCrossPartitionQuery = Boolean.parseBoolean(enableCrossPartitionQueryHeader);

        if (this.shouldExecuteQuery()) {
            this.providedRanges = PartitionRoutingHelper.getProvidedPartitionKeyRanges(
                    this.querySpec,
                    enableCrossPartitionQuery,
                    false,
                    this.collection.getPartitionKey(),
                    this.client.getQueryPartitionProvider(),
                    version);
        } else {
            this.providedRanges = new ArrayList<Range<String>>() {{
                add(new Range<>(
                        PartitionKeyInternal.MinimumInclusiveEffectivePartitionKey,
                        PartitionKeyInternal.MaximumExclusiveEffectivePartitionKey,
                        true,
                        false
                ));
            }};
        }

        return true;
    }
}
