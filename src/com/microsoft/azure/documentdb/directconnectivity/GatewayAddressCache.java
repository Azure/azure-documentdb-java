package com.microsoft.azure.documentdb.directconnectivity;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
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

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.message.BasicNameValuePair;

import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.internal.AuthorizationTokenProvider;
import com.microsoft.azure.documentdb.internal.Constants;
import com.microsoft.azure.documentdb.internal.DocumentServiceRequest;
import com.microsoft.azure.documentdb.internal.DocumentServiceResponse;
import com.microsoft.azure.documentdb.internal.ErrorUtils;
import com.microsoft.azure.documentdb.internal.HttpConstants;
import com.microsoft.azure.documentdb.internal.Paths;
import com.microsoft.azure.documentdb.internal.PathsHelper;
import com.microsoft.azure.documentdb.internal.ResourceId;
import com.microsoft.azure.documentdb.internal.ResourceType;
import com.microsoft.azure.documentdb.internal.UserAgentContainer;
import com.microsoft.azure.documentdb.internal.Utils;

public class GatewayAddressCache extends AddressCache {
    private final static String PROTOCOL_HTTPS = "https";
    private final static String PROTOCOL_FILTER_FORMAT = "%1$s eq %2$s";
    private final Logger logger;
    private ConcurrentHashMap<String, AddressInformation[]> serverPartitionAddressCache;
    private String protocolFilter;
    private HttpClient httpClient;
    private List<NameValuePair> defaultHeaders;
    private String addressEndpoint;
    private AuthorizationTokenProvider authorizationTokenProvider;

    public GatewayAddressCache(String serviceEndpoint, ConnectionPolicy connectionPolicy,
                               UserAgentContainer userAgent, AuthorizationTokenProvider authorizationTokenProvider) {
        this.logger = Logger.getLogger(this.getClass().getPackage().getName());
        this.serverPartitionAddressCache = new ConcurrentHashMap<>();
        this.authorizationTokenProvider = authorizationTokenProvider;

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
        boolean isMasterResource = false;
        return isMasterResource ? this.resolveMaster(request) : this.resolveServer(request);
    }

    private AddressInformation[] resolveMaster(DocumentServiceRequest request) {
        return null;
    }

    private AddressInformation[] resolveServer(DocumentServiceRequest request) {
        if (request.isForceAddressRefresh() || this.serverPartitionAddressCache.get(this.getLookupKey(request)) == null) {
            String entryUrl = PathsHelper.generatePath(request.getResourceType(), request,
                    Utils.isFeedRequest(request.getOperationType()));
            List<Address> response = this.resolveAddressesViaGatewayAsync(request, entryUrl, false);

            AddressInformation[] addresses = new AddressInformation[response.size()];
            int index = 0;
            for (Address address : response) {
                addresses[index++] = new AddressInformation(true, address.IsPrimary(), address.getPhyicalUri());
            }

            this.serverPartitionAddressCache.put(this.getLookupKey(request), addresses);
        }

        return this.serverPartitionAddressCache.get(this.getLookupKey(request));
    }

    private List<Address> resolveAddressesViaGatewayAsync(DocumentServiceRequest request,
                                                          String entryUrl, boolean useMasterCollectionResolver) {

        ResourceType resourceType = request.getResourceType();
        boolean forceRefresh = request.isForceAddressRefresh();
        List<NameValuePair> addressQuery = new LinkedList<NameValuePair>();

        try {
            addressQuery
                    .add(new BasicNameValuePair(HttpConstants.QueryStrings.URL, URLEncoder.encode(entryUrl, "UTF-8")));
            addressQuery.add(new BasicNameValuePair(HttpConstants.QueryStrings.FILTER,
                    new URI(null, null, null, this.protocolFilter, null).toString().substring(1)));
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

        // TODO NameCacheRefresh

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
            ErrorUtils.maybeThrowException(httpResponse, true, this.logger);
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

    private String getLookupKey(DocumentServiceRequest request) {
        if (request.getIsNameBased()) {
            if (DirectConnectivityUtils.isReadingFromMaster(request.getResourceType(), request.getOperationType())) {
                return PathsHelper.getDatabasePath(request.getResourceAddress());
            } else {
                return PathsHelper.getCollectionPath(request.getResourceAddress());
            }
        } else {
            ResourceId resourceId = ResourceId.parse(request.getResourceId());
            if (request.getResourceType() == ResourceType.Database) {
                return resourceId.getDatabaseId().toString();
            } else if (request.getResourceType() == ResourceType.User) {
                return resourceId.getUserId().toString();
            } else if (request.getResourceType() == ResourceType.Permission) {
                return resourceId.getPermissionId().toString();
            } else {
                return resourceId.getDocumentCollectionId().toString();
            }
        }
    }
}
