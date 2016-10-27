package com.microsoft.azure.documentdb.directconnectivity;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.message.BasicNameValuePair;

import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.PartitionKeyRange;
import com.microsoft.azure.documentdb.internal.*;
import com.microsoft.azure.documentdb.internal.routing.CollectionCache;
import com.microsoft.azure.documentdb.internal.routing.InMemoryCollectionRoutingMap;
import com.microsoft.azure.documentdb.internal.routing.PartitionKeyInternal;
import com.microsoft.azure.documentdb.internal.routing.PartitionKeyRangeCache;
import com.microsoft.azure.documentdb.internal.routing.PartitionKeyRangeIdentity;
import com.microsoft.azure.documentdb.internal.routing.RoutingMapProvider;

public class GatewayAddressCache extends AddressCache {
    private final static String PROTOCOL_HTTPS = "https";
    private final static String PROTOCOL_FILTER_FORMAT = "%1$s eq %2$s";
    private final Logger logger;
    private ConcurrentHashMap<PartitionKeyRangeIdentity, AddressInformation[]> serverPartitionAddressCache;
    private String protocolFilter;
    private HttpClient httpClient;
    private List<NameValuePair> defaultHeaders;
    private String addressEndpoint;
    private AuthorizationTokenProvider authorizationTokenProvider;
    private final CollectionCache collectionCache;
    private RoutingMapProvider partitionKeyRangeCache;
    private ImmutablePair<String, AddressInformation[]> masterPartitionAddressCache;

    public GatewayAddressCache(
            String serviceEndpoint,
            ConnectionPolicy connectionPolicy,
            CollectionCache collectionCache,
            RoutingMapProvider partitionKeyRangeCache,
            UserAgentContainer userAgent,
            AuthorizationTokenProvider authorizationTokenProvider) {
        this.logger = Logger.getLogger(this.getClass().getPackage().getName());
        this.serverPartitionAddressCache = new ConcurrentHashMap<>();
        this.authorizationTokenProvider = authorizationTokenProvider;
        this.collectionCache = collectionCache;
        this.partitionKeyRangeCache = partitionKeyRangeCache;

        this.httpClient = HttpClientFactory.createHttpClient(
                connectionPolicy.getMaxPoolSize(),
                connectionPolicy.getIdleConnectionTimeout(),
                connectionPolicy.getRequestTimeout());

        if (userAgent == null) {
            userAgent = new UserAgentContainer();
        }

        this.protocolFilter = String.format(PROTOCOL_FILTER_FORMAT, Constants.Properties.PROTOCOL, PROTOCOL_HTTPS);
        this.defaultHeaders = new LinkedList<NameValuePair>();
        this.defaultHeaders
                .add(new BasicNameValuePair(HttpConstants.HttpHeaders.VERSION, HttpConstants.Versions.CURRENT_VERSION));
        this.defaultHeaders
                .add(new BasicNameValuePair(HttpConstants.HttpHeaders.USER_AGENT, userAgent.getUserAgent()));
        this.addressEndpoint = serviceEndpoint.substring(0, serviceEndpoint.lastIndexOf(":")) + "//"
                + Paths.ADDRESS_PATH_SEGMENT;
    }

    public AddressInformation[] resolve(DocumentServiceRequest request) {
        boolean isMasterResource = GatewayAddressCache.isReadingFromMaster(
                request.getResourceType(),
                request.getOperationType());
        return isMasterResource ? this.resolveMaster(request) : this.resolveServer(request);
    }

    private AddressInformation[] resolveMaster(DocumentServiceRequest request) {
        if (request.isForceNameCacheRefresh() || this.masterPartitionAddressCache == null) {
            List<Address> response = this.resolveAddressesViaGatewayAsync(
                    request,
                    null,
                    request.getResourceType() == ResourceType.DocumentCollection);

            this.masterPartitionAddressCache = this.toPartitionAddressAndRange(response);
        }

        request.setResolvedCollectionRid(this.masterPartitionAddressCache.getLeft());
        return this.masterPartitionAddressCache.getRight();
    }

    private AddressInformation[] resolveServer(DocumentServiceRequest request) {

        DocumentCollection collection = this.collectionCache.resolveCollection(request);
        String partitionKeyRangeId = null;

        if (request.getHeaders().get(HttpConstants.HttpHeaders.PARTITION_KEY) != null) {
            partitionKeyRangeId = this.tryResolveServerPartitionByPartitionKey(
                    request.getHeaders().get(HttpConstants.HttpHeaders.PARTITION_KEY),
                    collection
            );
        } else if (request.getPartitionKeyRangeIdentity() != null) {
            partitionKeyRangeId = request.getPartitionKeyRangeIdentity().getPartitionKeyRangeId();
        }

        if (partitionKeyRangeId == null) {
            this.logger.log(Level.INFO, "Request contains neither partition key nor partition range Id.");
            throw new IllegalStateException();
        }
        request.setResolvedPartitionKeyRangeId(partitionKeyRangeId);

        return resolveAddressesForRangeId(request, collection, partitionKeyRangeId);
    }

    private AddressInformation[] resolveAddressesForRangeId(
            DocumentServiceRequest request,
            DocumentCollection collection,
            String partitionKeyRangeId) {

        PartitionKeyRangeIdentity partitionKeyRangeIdentity = new PartitionKeyRangeIdentity(
                collection.getResourceId(), partitionKeyRangeId);
        AddressInformation[] addresses = null;
        if (request.isForceAddressRefresh()
                || !this.serverPartitionAddressCache.containsKey(partitionKeyRangeIdentity)) {

            addresses = this.getAddressesForRangeId(
                    request,
                    collection,
                    partitionKeyRangeId
            );
            this.serverPartitionAddressCache.put(partitionKeyRangeIdentity, addresses);
        }

        return this.serverPartitionAddressCache.get(partitionKeyRangeIdentity);
    }

    private AddressInformation[] getAddressesForRangeId(
            DocumentServiceRequest request,
            DocumentCollection collection,
            String partitionKeyRangeId) {

        List<Address> addresses = this.resolveAddressesViaGatewayAsync(
                request,
                new String[] { partitionKeyRangeId },
                false
        );

        HashMap<String, List<Address>> addressesByPartitionRangeId = new HashMap<>();
        for (Address address : addresses) {
            String partitionRangeId = address.getParitionKeyRangeId();
            if (!addressesByPartitionRangeId.containsKey(partitionRangeId)) {
                addressesByPartitionRangeId.put(partitionRangeId, new ArrayList<Address>());
            }
            addressesByPartitionRangeId.get(partitionRangeId).add(address);
        }

        ArrayList<ImmutablePair<String, AddressInformation[]>> addressInfos = new ArrayList<>();
        for (Map.Entry<String, List<Address>> entry : addressesByPartitionRangeId.entrySet()) {
            addressInfos.add(this.toPartitionAddressAndRange(entry.getValue()));
        }

        ImmutablePair<String, AddressInformation[]> result = null;
        for (ImmutablePair<String, AddressInformation[]> addressInfo : addressInfos) {
            if (addressInfo.getLeft().equals(partitionKeyRangeId)) {
                result = addressInfo;
                break;
            }
        }

        if (result == null) {
            throw new IllegalStateException(String.format("PartitionKeyRange with id %s in collection %s doesn't exist.",
                    partitionKeyRangeId,
                    collection.getSelfLink()));
        }

        return result.getRight();
    }

    private String tryResolveServerPartitionByPartitionKey(
            String partitionKeyString,
            DocumentCollection collection) {
        PartitionKeyInternal partitionKey = null;
        try {
            partitionKey = Utils.getSimpleObjectMapper().readValue(
                    partitionKeyString,
                    PartitionKeyInternal.class);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to deserialize PartitionKeyInternal due to I/O error");
        }

        if (partitionKey.getComponents().size() == collection.getPartitionKey().getPaths().size()) {

            String effectivePartitionKey = partitionKey.getEffectivePartitionKeyString(collection.getPartitionKey(), true);

            // There should be exactly one range which contains a partition key. Always.
            Collection<PartitionKeyRange> ranges = this.partitionKeyRangeCache.getOverlappingRanges(collection.getSelfLink(),
                    com.microsoft.azure.documentdb.internal.routing.Range.getPointRange(effectivePartitionKey));

            return ranges.size() > 0 ? ranges.iterator().next().getId() : null;
        }

        return null;
    }

    private ImmutablePair<String, AddressInformation[]> toPartitionAddressAndRange(List<Address> addresses) {
        // The addressList should always contains at least one address
        List<AddressInformation> addressInfos = new ArrayList<>();
        for (Address address : addresses) {
            addressInfos.add(new AddressInformation(true, address.IsPrimary(), address.getPhyicalUri()));
        }

        AddressInformation[] addressInfoArr = new AddressInformation[addressInfos.size()];
        return new ImmutablePair<>(addresses.get(0).getParitionKeyRangeId(), addressInfos.toArray(addressInfoArr));
    }

    private List<Address> resolveAddressesViaGatewayAsync(
            DocumentServiceRequest request,
            String[] partitionKeyRangeIds,
            boolean useMasterCollectionResolver) {
        String entryUrl = PathsHelper.generatePath(
                request.getResourceType(),
                request,
                Utils.isFeedRequest(request.getOperationType()));

        ResourceType resourceType = request.getResourceType();
        boolean forceRefresh = request.isForceAddressRefresh();
        List<NameValuePair> addressQuery = new LinkedList<NameValuePair>();

        try {
            addressQuery.add(new BasicNameValuePair(
                    HttpConstants.QueryStrings.URL,
                    URLEncoder.encode(entryUrl, "UTF-8")));
            addressQuery.add(new BasicNameValuePair(
                    HttpConstants.QueryStrings.FILTER,
                    new URI(null, null, null, this.protocolFilter, null).toString().substring(1)));

            if (partitionKeyRangeIds != null && partitionKeyRangeIds.length > 0) {
                addressQuery.add(new BasicNameValuePair(
                        HttpConstants.QueryStrings.PARTITION_KEY_RANGE_IDS,
                        StringUtils.join(partitionKeyRangeIds, ",")
                ));
            }
        } catch (UnsupportedEncodingException e1) {
            this.logger.log(Level.WARNING, e1.toString(), e1);
        } catch (URISyntaxException e) {
            this.logger.log(Level.WARNING, e.toString(), e);
        }

        URL targetEndpoint = Utils.setQuery(this.addressEndpoint, Utils.createQuery(addressQuery));

        HttpGet httpGet = new HttpGet(targetEndpoint.toString());
        Map<String, String> headers = new HashMap<String, String>();

        for (NameValuePair nameValuePair : this.defaultHeaders) {
            httpGet.addHeader(nameValuePair.getName(), nameValuePair.getValue());
            headers.put(nameValuePair.getName(), nameValuePair.getValue());
        }

        if (forceRefresh) {
            httpGet.addHeader(HttpConstants.HttpHeaders.FORCE_REFRESH, Boolean.TRUE.toString());
            headers.put(HttpConstants.HttpHeaders.FORCE_REFRESH, Boolean.TRUE.toString());
        }

        final Date currentTime = new Date();
        final SimpleDateFormat sdf = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        String xDate = sdf.format(currentTime);
        httpGet.addHeader(HttpConstants.HttpHeaders.X_DATE, xDate);
        headers.put(HttpConstants.HttpHeaders.X_DATE, xDate);

        String token = null;
        if (!request.getIsNameBased()) {
            token = this.authorizationTokenProvider.generateKeyAuthorizationSignature("get", request.getResourceAddress().toLowerCase(),
                    resourceType, headers);
        } else {
            token = this.authorizationTokenProvider.generateKeyAuthorizationSignature("get", request.getResourceFullName(),
                    resourceType, headers);
        }

        try {
            httpGet.addHeader(HttpConstants.HttpHeaders.AUTHORIZATION, URLEncoder.encode(token, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Unsupported encoding", e);
        }

        HttpResponse httpResponse = null;
        try {
            httpResponse = this.httpClient.execute(httpGet);
            ErrorUtils.maybeThrowException(targetEndpoint.getPath(), httpResponse, true, this.logger);
        } catch (IOException e) {
            httpGet.releaseConnection();
            throw new IllegalStateException("Http client execution failed.", e);
        } catch (DocumentClientException e) {
            httpGet.releaseConnection();
            throw new IllegalStateException("Http client execution failed.", e);
        }

        DocumentServiceResponse response = new DocumentServiceResponse(httpResponse);

        List<Address> addresses = response.getQueryResponse(Address.class);
        response.close();

        return addresses;
    }

    public static boolean isReadingFromMaster(ResourceType resourceType, OperationType operationType) {
        if (resourceType == ResourceType.Offer ||
                resourceType == ResourceType.Database ||
                resourceType == ResourceType.User ||
                resourceType == ResourceType.Permission ||
                resourceType == ResourceType.Topology ||
                resourceType == ResourceType.DatabaseAccount ||
                resourceType == ResourceType.PartitionKeyRange ||
                (resourceType == ResourceType.DocumentCollection
                        && (operationType == OperationType.ReadFeed
                        || operationType == OperationType.Query
                        || operationType == OperationType.SqlQuery))) {
            return true;
        }

        return false;
    }
}
