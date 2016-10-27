package com.microsoft.azure.documentdb;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;

import com.microsoft.azure.documentdb.internal.EndpointManager;
import com.microsoft.azure.documentdb.internal.OperationType;
import com.microsoft.azure.documentdb.internal.Utils;

/**
 * This class implements the logic for endpoint management for geo-replicated
 * database accounts.
 * <p>
 * When ConnectionPolicy.getEnableEndpointDiscovery is true,
 * the GlobalEndpointManager will choose the correct endpoint to use for write
 * and read operations based on database account information retrieved from the
 * service in conjunction with user's preference as specified in
 * ConnectionPolicy().getPreferredLocations.
 */
class GlobalEndpointManager implements EndpointManager {
    private final DocumentClient client;
    private final Collection<String> preferredLocations;
    private final boolean enableEndpointDiscovery;
    private final URI defaultEndpoint;
    private Map<String, URI> readableLocations;
    private Map<String, URI> writableLocations;
    private URI currentWriteLocation;
    private URI currentReadLocation;
    private boolean initialized;
    private boolean refreshing;
    private Logger logger;

    public GlobalEndpointManager(DocumentClient client) {
        this.client = client;
        this.preferredLocations = client.getConnectionPolicy().getPreferredLocations();
        this.enableEndpointDiscovery = client.getConnectionPolicy().getEnableEndpointDiscovery();
        this.logger = Logger.getLogger(this.getClass().getPackage().getName());
        this.defaultEndpoint = this.client.getServiceEndpoint();
        this.initialized = false;
        this.refreshing = false;
    }

    public URI getWriteEndpoint() {
        if (!initialized) {
            this.initialize();
        }

        return this.currentWriteLocation;
    }

    public URI getReadEndpoint() {
        if (!initialized) {
            this.initialize();
        }

        return this.currentReadLocation;
    }

    public URI resolveServiceEndpoint(OperationType operationType) {
        URI endpoint = null;

        if (Utils.isWriteOperation(operationType)) {
            endpoint = this.getWriteEndpoint();
        } else {
            endpoint = this.getReadEndpoint();
        }

        if (endpoint == null) {
            // Unable to resolve service endpoint through querying database account info.
            // use the value passed in by the user.
            endpoint = this.defaultEndpoint;
        }

        return endpoint;
    }

    public synchronized void refreshEndpointList() {
        if (this.refreshing) {
            return;
        }

        this.refreshing = true;
        try {
            this.refreshEndpointListInternal();
        } finally {
            this.refreshing = false;
        }
    }

    public DatabaseAccount getDatabaseAccountFromAnyEndpoint() {
        DatabaseAccount databaseAccount = null;
        try {
            databaseAccount = this.client.getDatabaseAccountFromEndpoint(this.defaultEndpoint);

            // The global endpoint was not working. Try other endpoints in the preferred read region list.
            if (databaseAccount == null && this.preferredLocations != null && this.preferredLocations.size() > 0) {
                for (String regionName : this.preferredLocations) {
                    URI regionalUri = this.getRegionalEndpoint(regionName);
                    if (regionalUri != null) {
                        databaseAccount = this.client.getDatabaseAccountFromEndpoint(regionalUri);
                        if (databaseAccount != null) {
                            break;
                        }
                    }
                }
            }
        } catch (DocumentClientException e) {
            this.logger.warning(String.format("Failed to retrieve database account information. %s", e.toString()));
        }

        return databaseAccount;
    }

    private synchronized void initialize() {
        if (initialized) {
            return;
        }

        this.initialized = true;
        this.refreshEndpointListInternal();
    }

    private void refreshEndpointListInternal() {
        Map<String, URI> writableLocations = new HashMap<String, URI>();
        Map<String, URI> readableLocations = new HashMap<String, URI>();
        if (this.enableEndpointDiscovery) {
            DatabaseAccount databaseAccount = this.getDatabaseAccountFromAnyEndpoint();
            if (databaseAccount != null) {
                if (databaseAccount.getWritableLocations() != null) {
                    for (DatabaseAccountLocation location : databaseAccount.getWritableLocations()) {
                        if (StringUtils.isNotEmpty(location.getName())) {
                            URI regionUri = null;
                            try {
                                regionUri = new URI(location.getEndpoint());
                            } catch (URISyntaxException e) {
                            }

                            if (regionUri != null) {
                                writableLocations.put(location.getName(), regionUri);
                            }
                        }
                    }
                }

                if (databaseAccount.getReadableLocations() != null) {
                    for (DatabaseAccountLocation location : databaseAccount.getReadableLocations()) {
                        if (StringUtils.isNotEmpty(location.getName())) {
                            URI regionUri = null;
                            try {
                                regionUri = new URI(location.getEndpoint());
                            } catch (URISyntaxException e) {
                            }

                            if (regionUri != null) {
                                readableLocations.put(location.getName(), regionUri);
                            }
                        }
                    }
                }
            }
        }

        this.updateEndpointsCache(writableLocations, readableLocations);
    }

    private synchronized void updateEndpointsCache(Map<String, URI> writableLocations,
                                                   Map<String, URI> readableLocations) {
        this.writableLocations = writableLocations;
        this.readableLocations = readableLocations;

        // If enableEndpointDiscovery is false, we will always use the default
        // value the user has set when creating the DocumentClient object.
        if (!this.enableEndpointDiscovery) {
            this.currentReadLocation = this.defaultEndpoint;
            this.currentWriteLocation = this.defaultEndpoint;
            return;
        }

        // If enableEndpointDiscovery is true, we will choose the first
        // writable region as the current write region, unless there is 
        // no writable region, in which case we'll use the default value.
        if (this.writableLocations.size() == 0) {
            this.currentWriteLocation = this.defaultEndpoint;
        } else {
            Iterator<Entry<String, URI>> iterator = this.writableLocations.entrySet().iterator();
            this.currentWriteLocation = iterator.next().getValue();
        }

        // If there is no readable region, or if there is no preferred
        // regions, we use the same region to read and write.
        URI newReadRegion = null;
        if (this.readableLocations.size() == 0) {
            // If there is no readable region available, we use the write region
            // for reads.
            newReadRegion = this.currentWriteLocation;
        } else {
            // If no preferred read regions are specified, set the read region the same
            // as the write region.
            if (this.preferredLocations == null || this.preferredLocations.size() == 0) {
                newReadRegion = this.currentWriteLocation;
            } else {
                // Choose the first region from the preferred list that is available.
                for (String regionName : this.preferredLocations) {
                    if (StringUtils.isNotEmpty(regionName)) {
                        newReadRegion = this.readableLocations.get(regionName);
                        if (newReadRegion != null) {
                            break;
                        }

                        newReadRegion = this.writableLocations.get(regionName);
                        if (newReadRegion != null) {
                            break;
                        }
                    }
                }
            }
        }

        if (newReadRegion != null) {
            this.currentReadLocation = newReadRegion;
        } else {
            this.currentReadLocation = this.currentWriteLocation;
        }
    }

    URI getRegionalEndpoint(String regionName) {
        if (StringUtils.isNotEmpty(regionName)) {
            String databaseAccountName = this.defaultEndpoint.getHost();
            int indexOfDot = this.defaultEndpoint.getHost().indexOf('.');
            if (indexOfDot >= 0) {
                databaseAccountName = databaseAccountName.substring(0, indexOfDot);
            }

            // Add region name suffix to the account name.
            String reginalAccountName = databaseAccountName + "-" + regionName.replace(" ", "");
            String regionalUrl = this.defaultEndpoint.toString().replace(databaseAccountName, reginalAccountName);

            try {
                return new URI(regionalUrl);
            } catch (URISyntaxException e) {
                return null;
            }
        }

        return null;
    }
}
