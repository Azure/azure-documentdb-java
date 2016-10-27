/*
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 */

package com.microsoft.azure.documentdb;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.documentdb.directconnectivity.AddressCache;
import com.microsoft.azure.documentdb.directconnectivity.GatewayAddressCache;
import com.microsoft.azure.documentdb.directconnectivity.HttpTransportClient;
import com.microsoft.azure.documentdb.directconnectivity.ServerStoreModel;
import com.microsoft.azure.documentdb.directconnectivity.TransportClient;
import com.microsoft.azure.documentdb.internal.*;
import com.microsoft.azure.documentdb.internal.routing.ClientCollectionCache;
import com.microsoft.azure.documentdb.internal.routing.CollectionCache;
import com.microsoft.azure.documentdb.internal.routing.PartitionKeyInternal;
import com.microsoft.azure.documentdb.internal.routing.PartitionKeyRangeCache;
import com.microsoft.azure.documentdb.internal.routing.RoutingMapProvider;

/**
 * Provides a client-side logical representation of the Azure DocumentDB service. This client is used to configure and
 * execute requests against the service.
 * <p>
 * The service client encapsulates the endpoint and credentials used to access the DocumentDB service.
 */
public class DocumentClient {

    @Deprecated
    protected static final String PartitionResolverErrorMessage = "Couldn't find any partition resolvers for the database link provided. Ensure that the link you used when registering the partition resolvers matches the link provided or you need to register both types of database link(self link as well as ID based link).";
    private static final int DEFAULT_REQUEST_SIZE = 4194304;

    private URI serviceEndpoint;
    private String masterKey;
    private Map<String, String> resourceTokens;
    private ConnectionPolicy connectionPolicy;
    private GatewayProxy gatewayProxy;
    private SessionContainer sessionContainer;
    private ConsistencyLevel desiredConsistencyLevel;
    private EndpointManager globalEndpointManager;
    private Logger logger;
    @SuppressWarnings("deprecation")
    private ConcurrentHashMap<String, PartitionResolver> partitionResolvers;
    private StoreModel storeModel;
    private AddressCache addressCache;
    private TransportClient transportClient;
    private AuthorizationTokenProvider authorizationTokenProvider;
    private DatabaseAccountConfigurationProvider databaseAccountConfigurationProvider;
    private ClientCollectionCache collectionCache;
    private PartitionKeyRangeCache partitionKeyRangeCache;

    /**
     * Compatibility mode:
     * Allows to specify compatibility mode used by client when making query requests. Should be removed when
     * application/sql is no longer supported.
     */
    private QueryCompatibilityMode queryCompatibilityMode = QueryCompatibilityMode.Default;

    /**
     * Initializes a new instance of the DocumentClient class using the specified DocumentDB service endpoint and keys.
     * 
     * @param serviceEndpoint         the URI of the service end point.
     * @param masterKey               the master key.
     * @param connectionPolicy        the connection policy.
     * @param desiredConsistencyLevel the desired consistency level.
     */
    public DocumentClient(String serviceEndpoint,
                          String masterKey,
                          ConnectionPolicy connectionPolicy,
                          ConsistencyLevel desiredConsistencyLevel) {
        this(serviceEndpoint, masterKey, connectionPolicy, desiredConsistencyLevel, null, null);
        }

    /**
     * Initializes a new instance of the Microsoft.Azure.Documents.Client.DocumentClient class using the specified
     * DocumentDB service endpoint and permissions.
     * 
     * @param serviceEndpoint         the URI of the service end point.
     * @param permissionFeed          the permission feed.
     * @param connectionPolicy        the connection policy.
     * @param desiredConsistencyLevel the desired consistency level.
     */
    public DocumentClient(String serviceEndpoint,
                          List<Permission> permissionFeed,
                          ConnectionPolicy connectionPolicy,
                          ConsistencyLevel desiredConsistencyLevel) {
        URI uri = null;
        try {
            uri = new URI(serviceEndpoint);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid serviceEndPoint.", e);
        }

        this.resourceTokens = new HashMap<String, String>();

        for (Permission permission : permissionFeed) {
            String[] segments = permission.getResourceLink().split("/");

            if (segments.length <= 0) {
                throw new IllegalArgumentException("link");
            }

            String resourceId = segments[segments.length - 1];
            this.resourceTokens.put(resourceId, permission.getToken());
        }

        this.initialize(uri, connectionPolicy, desiredConsistencyLevel);
    }

    DocumentClient(String serviceEndpoint, String masterKey, ConnectionPolicy connectionPolicy,
            ConsistencyLevel desiredConsistencyLevel, AddressCache addressCache, TransportClient transportClient) {
        URI uri = null;
        try {
            uri = new URI(serviceEndpoint);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid serviceEndPoint.", e);
        }

        this.masterKey = masterKey;
        this.addressCache = addressCache;
        this.transportClient = transportClient;
        this.initialize(uri, connectionPolicy, desiredConsistencyLevel);
    }
    
    private static String serializeProcedureParams(Object[] objectArray) {
        ObjectMapper mapper = null;
        String[] stringArray = new String[objectArray.length];

        for (int i = 0; i < objectArray.length; ++i) {
            Object object = objectArray[i];
            if (object instanceof JsonSerializable || object instanceof JSONObject) {
                stringArray[i] = object.toString();
            } else {
                if (mapper == null) {
                    mapper = new ObjectMapper();
                }

                // POJO, number, String or Boolean
                try {
                    stringArray[i] = mapper.writeValueAsString(object);
                } catch (IOException e) {
                    throw new IllegalArgumentException("Can't serialize the object into the json string", e);
                }
            }
        }

        return String.format("[%s]", StringUtils.join(stringArray, ","));
    }

    private static void validateResource(Resource resource) {
        if (!StringUtils.isEmpty(resource.getId())) {
            if (resource.getId().indexOf('/') != -1 || resource.getId().indexOf('\\') != -1 ||
                    resource.getId().indexOf('?') != -1 || resource.getId().indexOf('#') != -1) {
                throw new IllegalArgumentException("Id contains illegal chars.");
            }

            if (resource.getId().endsWith(" ")) {
                throw new IllegalArgumentException("Id ends with a space.");
            }
        }
    }

    @SuppressWarnings("deprecation") // use of PartitionResolver is deprecated.
    private void initialize(URI serviceEndpoint,
                            ConnectionPolicy connectionPolicy,
                            ConsistencyLevel desiredConsistencyLevel) {

        this.serviceEndpoint = serviceEndpoint;
        this.logger = Logger.getLogger(this.getClass().getPackage().getName());
        
        if (connectionPolicy != null) {
            this.connectionPolicy = connectionPolicy;
        } else {
            this.connectionPolicy = new ConnectionPolicy();
        }

        this.sessionContainer = new SessionContainer(this.serviceEndpoint.getHost());
        this.desiredConsistencyLevel = desiredConsistencyLevel;

        UserAgentContainer userAgentContainer = new UserAgentContainer();
        String userAgentSuffix = this.connectionPolicy.getUserAgentSuffix();
        if (userAgentSuffix != null && userAgentSuffix.length() > 0) {
            userAgentContainer.setSuffix(userAgentSuffix);
        }
        
        this.globalEndpointManager = new GlobalEndpointManager(this);
        this.gatewayProxy = new GatewayProxy(this.connectionPolicy,
                                             desiredConsistencyLevel,
                                             this.queryCompatibilityMode,
                                             this.masterKey,
                                             this.resourceTokens,
                                             userAgentContainer,
                                             this.globalEndpointManager);
        
        // use of PartitionResolver is deprecated.
        this.partitionResolvers = new ConcurrentHashMap<String, PartitionResolver>();
        this.authorizationTokenProvider = new BaseAuthorizationTokenProvider(this.masterKey);
        this.collectionCache = new ClientCollectionCache(this);
        this.partitionKeyRangeCache = new PartitionKeyRangeCache(new DocumentQueryClient(this));

        if (this.connectionPolicy.getConnectionMode() == ConnectionMode.DirectHttps) {
            if (this.addressCache == null) {
                this.addressCache = new GatewayAddressCache(
                        this.serviceEndpoint.toString(),
                        this.connectionPolicy,
                        this.collectionCache,
                        this.partitionKeyRangeCache,
                        userAgentContainer,
                        this.authorizationTokenProvider);
            }

            if (this.transportClient == null) {
                this.transportClient = new HttpTransportClient(this.connectionPolicy, userAgentContainer);
            }
            
            this.databaseAccountConfigurationProvider = new BaseDatabaseAccountConfigurationProvider(this.globalEndpointManager.getDatabaseAccountFromAnyEndpoint(), this.desiredConsistencyLevel);
            this.storeModel = new ServerStoreModel(this.transportClient, this.addressCache, this.sessionContainer, DEFAULT_REQUEST_SIZE, this.databaseAccountConfigurationProvider, this.authorizationTokenProvider);
        } else {
            this.storeModel = this.gatewayProxy;
        }
    }

    @Deprecated
    public void registerPartitionResolver(String databaseLink, PartitionResolver partitionResolver) 
            throws DocumentClientException {
        if (StringUtils.isEmpty(databaseLink)) {
            throw new IllegalArgumentException("databaseLink");
        }
        if (partitionResolver == null) {
            throw new IllegalArgumentException("partitionResolver");
        }
        
        this.partitionResolvers.put(Utils.trimBeginingAndEndingSlashes(databaseLink), partitionResolver);
    }
    
    @Deprecated
    protected PartitionResolver getPartitionResolver(String databaseLink) {
        if (StringUtils.isEmpty(databaseLink)) {
            throw new IllegalArgumentException("databaseLink");
        }
        return this.partitionResolvers.get(Utils.trimBeginingAndEndingSlashes(databaseLink));
    }
    
    QueryCompatibilityMode getQueryCompatiblityMode() {
        return this.queryCompatibilityMode;
    }

    /**
     * Gets the default service endpoint as passed in by the user during construction.
     *
     * @return the service endpoint URI
     */
    public URI getServiceEndpoint() {
        return this.serviceEndpoint;
    }
    
    /**
     * Gets the current write endpoint chosen based on availability and preference.
     *
     * @return the write endpoint URI
     */
    public URI getWriteEndpoint() {
        return this.globalEndpointManager.getWriteEndpoint();
    }

    /**
     * Gets the current read endpoint chosen based on availability and preference.
     *
     * @return the read endpoint URI
     */
    public URI getReadEndpoint() {
        return this.globalEndpointManager.getReadEndpoint();
    }

    public ConnectionPolicy getConnectionPolicy() {
        return this.connectionPolicy;
    }
    
    EndpointManager getEndpointManager() {
        return this.globalEndpointManager;
    }
    
    void setEndpointManager(EndpointManager endpointManager) {
        this.globalEndpointManager = endpointManager;
    }

    RoutingMapProvider getPartitionKeyRangeCache() {
        return this.partitionKeyRangeCache;
    }

    CollectionCache getCollectionCache() {
        return this.collectionCache;
    }

    // used by unit test to mock gateway proxy.
    void setGatewayProxyOverride(GatewayProxy proxyOverride) {
        this.gatewayProxy = proxyOverride;
        if (this.connectionPolicy.getConnectionMode() != ConnectionMode.DirectHttps) {
            this.storeModel = proxyOverride;
        }
    }

    // used by unit test to mock gateway proxy.
    void setCollectionCache(ClientCollectionCache clientCollectionCache) {
        this.collectionCache = clientCollectionCache;
    }

    /**
     * Creates a database.
     * 
     * @param database the database.
     * @param options  the request options.
     * @return the resource response with the created database.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Database> createDatabase(Database database, RequestOptions options)
            throws DocumentClientException {
        if (database == null) {
            throw new IllegalArgumentException("Database");          
        }

        DocumentClient.validateResource(database);

        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Create,
                                                                       ResourceType.Database,
                                                                       Paths.DATABASES_ROOT,
                                                                       database,
                                                                       requestHeaders);
        return new ResourceResponse<Database>(this.doCreate(request), Database.class);
    }

    /**
     * Deletes a database.
     * 
     * @param databaseLink the database link.
     * @param options      the request options.
     * @return the resource response.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Database> deleteDatabase(String databaseLink, RequestOptions options)
            throws DocumentClientException {
        if (StringUtils.isEmpty(databaseLink)) {
            throw new IllegalArgumentException("databaseLink");
        }
        
        String path = Utils.joinPath(databaseLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Delete,
                                                                       ResourceType.Database,
                                                                       path,
                                                                       requestHeaders);
        return new ResourceResponse<Database>(this.doDelete(request), Database.class);
    }

    /**
     * Reads a database.
     * 
     * @param databaseLink the database link.
     * @param options      the request options.
     * @return the resource response with the read database.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Database> readDatabase(String databaseLink, RequestOptions options)
            throws DocumentClientException {
        if (StringUtils.isEmpty(databaseLink)) {
            throw new IllegalArgumentException("databaseLink");
        }
        
        String path = Utils.joinPath(databaseLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Read,
                                                                       ResourceType.Database,
                                                                       path,
                                                                       requestHeaders);
        return new ResourceResponse<Database>(this.doRead(request), Database.class);
    }

    /**
     * Reads all databases.
     * 
     * @param options the feed options.
     * @return the feed response with the read databases.
     */
    public FeedResponse<Database> readDatabases(FeedOptions options) {
        return new FeedResponse<Database>(new QueryIterable<Database>(this, 
                                                                       ResourceType.Database,
                                                                      Database.class, 
                                                                       Paths.DATABASES_ROOT,
                                                                      options));
    }

    /**
     * Query for databases.
     * 
     * @param query   the query.
     * @param options the feed options.
     * @return the feed response with the obtained databases.
     */
    public FeedResponse<Database> queryDatabases(String query, FeedOptions options) {
        if (StringUtils.isEmpty(query)) {
            throw new IllegalArgumentException("query");
        }

        return queryDatabases(new SqlQuerySpec(query, null), options);
    }
    
    /**
     * Query for databases.
     * 
     * @param querySpec the SQL query specification.
     * @param options   the feed options.
     * @return the feed response with the obtained databases.
     */
    public FeedResponse<Database> queryDatabases(SqlQuerySpec querySpec, FeedOptions options) {
        if (querySpec == null) {
            throw new IllegalArgumentException("querySpec");
        }

        return new FeedResponse<Database>(new QueryIterable<Database>(this, 
                                                                      ResourceType.Database,
                                                                      Database.class, 
                                                                      Paths.DATABASES_ROOT,
                                                                      querySpec,
                                                                      options));
    }

    /**
     * Creates a document collection.
     * 
     * @param databaseLink the database link.
     * @param collection   the collection.
     * @param options      the request options.
     * @return the resource response with the created collection.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<DocumentCollection> createCollection(String databaseLink,
                                                                 DocumentCollection collection,
                                                                 RequestOptions options)
            throws DocumentClientException {

        if (StringUtils.isEmpty(databaseLink)) {
            throw new IllegalArgumentException("databaseLink");
        }
        if (collection == null) {
            throw new IllegalArgumentException("collection");          
        }

        DocumentClient.validateResource(collection);

        String path = Utils.joinPath(databaseLink, Paths.COLLECTIONS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Create,
                                                                       ResourceType.DocumentCollection,
                                                                       path,
                                                                       collection,
                                                                       requestHeaders);
        
        return new ResourceResponse<DocumentCollection>(this.doCreate(request), DocumentCollection.class);
    }

    /**
     * Replaces a document collection.
     * 
     * @param collection the document collection to use.
     * @param options    the request options.
     * @return the resource response with the replaced document collection.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<DocumentCollection> replaceCollection(DocumentCollection collection,
                                                                  RequestOptions options)
            throws DocumentClientException {

        if (collection == null) {
            throw new IllegalArgumentException("collection");          
        }

        DocumentClient.validateResource(collection);

        String path = Utils.joinPath(collection.getSelfLink(), null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);

        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Replace, 
                                                                       ResourceType.DocumentCollection,
                                                                       path,
                                                                       collection,
                                                                       requestHeaders);
        return new ResourceResponse<DocumentCollection>(this.doReplace(request), DocumentCollection.class);
    }

    /**
     * Deletes a document collection by the collection link.
     * 
     * @param collectionLink the collection link.
     * @param options        the request options.
     * @return the resource response.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<DocumentCollection> deleteCollection(String collectionLink, RequestOptions options)
            throws DocumentClientException {

        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }

        String path = Utils.joinPath(collectionLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Delete, 
                                                                       ResourceType.DocumentCollection,
                                                                       path,
                                                                       requestHeaders);
        return new ResourceResponse<DocumentCollection>(this.doDelete(request), DocumentCollection.class);
    }

    /**
     * Reads a document collection by the collection link.
     * 
     * @param collectionLink the collection link.
     * @param options        the request options.
     * @return the resource response with the read collection.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<DocumentCollection> readCollection(String collectionLink, RequestOptions options)
            throws DocumentClientException {

        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }

        String path = Utils.joinPath(collectionLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Read, 
                                                                       ResourceType.DocumentCollection,
                                                                       path,
                                                                       requestHeaders);

        return new ResourceResponse<DocumentCollection>(this.doRead(request), DocumentCollection.class);
    }

    /**
     * Reads all document collections in a database.
     * 
     * @param databaseLink the database link.
     * @param options      the fee options.
     * @return the feed response with the read collections.
     */
    public FeedResponse<DocumentCollection> readCollections(String databaseLink, FeedOptions options) {

        if (StringUtils.isEmpty(databaseLink)) {
            throw new IllegalArgumentException("databaseLink");
        }

        String path = Utils.joinPath(databaseLink, Paths.COLLECTIONS_PATH_SEGMENT);
        return new FeedResponse<DocumentCollection>(new QueryIterable<DocumentCollection>(this, 
                                                                       ResourceType.DocumentCollection,
                                                                                          DocumentCollection.class, 
                                                                       path,
                                                                                          options));
    }

    /**
     * Query for document collections in a database.
     * 
     * @param databaseLink the database link.
     * @param query        the query.
     * @param options      the feed options.
     * @return the feed response with the obtained collections.
     */
    public FeedResponse<DocumentCollection> queryCollections(String databaseLink, String query, FeedOptions options) {

        if (StringUtils.isEmpty(databaseLink)) {
            throw new IllegalArgumentException("databaseLink");
        }

        if (StringUtils.isEmpty(query)) {
            throw new IllegalArgumentException("query");
        }

        return queryCollections(databaseLink, new SqlQuerySpec(query, null), options);
    }

    /**
     * Query for document collections in a database.
     * 
     * @param databaseLink the database link.
     * @param querySpec    the SQL query specification.
     * @param options      the feed options.
     * @return the feed response with the obtained collections.
     */
    public FeedResponse<DocumentCollection> queryCollections(String databaseLink, SqlQuerySpec querySpec,
                                                             FeedOptions options) {

        if (StringUtils.isEmpty(databaseLink)) {
            throw new IllegalArgumentException("databaseLink");
        }

        if (querySpec == null) {
            throw new IllegalArgumentException("querySpec");
        }

        String path = Utils.joinPath(databaseLink, Paths.COLLECTIONS_PATH_SEGMENT);
        return new FeedResponse<DocumentCollection>(new QueryIterable<DocumentCollection>(this, 
                                                                       ResourceType.DocumentCollection,
                                                                                          DocumentCollection.class, 
                                                                       path,
                                                                       querySpec,
                                                                                          options));
    }

    /**
     * Creates a document.
     * 
     * @param collectionLink               the link to the parent document collection.
     * @param document                     the document represented as a POJO or Document object.
     * @param options                      the request options.
     * @param disableAutomaticIdGeneration the flag for disabling automatic id generation.
     * @return the resource response with the created document.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Document> createDocument(String collectionLink,
                                                     Object document,
                                                     RequestOptions options,
                                                     boolean disableAutomaticIdGeneration)
            throws DocumentClientException {
        
        final String documentCollectionLink = this.getTargetDocumentCollectionLink(collectionLink, document);
        final Object documentLocal = document;
        final RequestOptions optionsLocal = options;
        final boolean disableAutomaticIdGenerationLocal = disableAutomaticIdGeneration;
        final boolean shouldRetry = options == null || options.getPartitionKey() == null;
        
        RetryCreateDocumentDelegate createDelegate = new RetryCreateDocumentDelegate() {

            @Override
            public ResourceResponse<Document> apply() throws DocumentClientException {
                DocumentServiceRequest request = getCreateDocumentRequest(documentCollectionLink, documentLocal, optionsLocal,
                        disableAutomaticIdGenerationLocal, OperationType.Create);
                return new ResourceResponse<Document>(doCreate(request), Document.class);   
            }
        };

        return shouldRetry 
            ? RetryUtility.executeCreateDocument(createDelegate, this.collectionCache, documentCollectionLink)
            : createDelegate.apply();
    }

     /**
     * Upserts a document.
     * 
     * @param collectionLink               the link to the parent document collection.
     * @param document                     the document represented as a POJO or Document object to upsert.
     * @param options                      the request options.
     * @param disableAutomaticIdGeneration the flag for disabling automatic id generation.
     * @return the resource response with the upserted document.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Document> upsertDocument(String collectionLink,
                                                     Object document,
                                                     RequestOptions options,
                                                     boolean disableAutomaticIdGeneration)
            throws DocumentClientException {
        final String documentCollectionLink = this.getTargetDocumentCollectionLink(collectionLink, document);
        final Object documentLocal = document;
        final RequestOptions optionsLocal = options;
        final boolean disableAutomaticIdGenerationLocal = disableAutomaticIdGeneration;
        final boolean shouldRetry = options == null || options.getPartitionKey() == null; 
        
        RetryCreateDocumentDelegate upsertDelegate = new RetryCreateDocumentDelegate() {

            @Override
            public ResourceResponse<Document> apply() throws DocumentClientException {
                DocumentServiceRequest request = getCreateDocumentRequest(documentCollectionLink, documentLocal, optionsLocal,
                        disableAutomaticIdGenerationLocal, OperationType.Upsert);
                return new ResourceResponse<Document>(doUpsert(request), Document.class);
    }
        };
    
        return shouldRetry
            ? RetryUtility.executeCreateDocument(upsertDelegate, this.collectionCache, documentCollectionLink)
            : upsertDelegate.apply();
    }
    
    @SuppressWarnings("deprecation")
    private String getTargetDocumentCollectionLink(String collectionLink, Object document) {
        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }
        if (document == null) {
            throw new IllegalArgumentException("document");
        }
        
        String documentCollectionLink = collectionLink;
        if (Utils.isDatabaseLink(collectionLink)) {
            // Gets the partition resolver(if it exists) for the specified database link
            PartitionResolver partitionResolver = this.getPartitionResolver(collectionLink);
            
            // If the partition resolver exists, get the collection to which the Create/Upsert should be directed using the partition key
            if (partitionResolver != null) {
                documentCollectionLink = partitionResolver.resolveForCreate(document);
            } else {
                throw new IllegalArgumentException(PartitionResolverErrorMessage);
            }
        }
        
        return documentCollectionLink;
    }
    
    private DocumentServiceRequest getCreateDocumentRequest(String documentCollectionLink, Object document, RequestOptions options,
            boolean disableAutomaticIdGeneration, OperationType operationType) {
        if (StringUtils.isEmpty(documentCollectionLink)) {
            throw new IllegalArgumentException("documentCollectionLink");
        }
        if (document == null) {
            throw new IllegalArgumentException("document");
        }
        
        Document typedDocument = Document.FromObject(document);

        DocumentClient.validateResource(typedDocument);

        if (typedDocument.getId() == null && !disableAutomaticIdGeneration) {
            // We are supposed to use GUID. Basically UUID is the same as GUID
            // when represented as a string.
            typedDocument.setId(UUID.randomUUID().toString());
        }
        String path = Utils.joinPath(documentCollectionLink, Paths.DOCUMENTS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        
        DocumentServiceRequest request = DocumentServiceRequest.create(operationType, 
                                                                       ResourceType.Document,
                                                                       path,
                                                                       typedDocument,
                                                                       requestHeaders);
        this.addPartitionKeyInformation(request, typedDocument, options);
        return request;
    }
    
    /**
     * Replaces a document using a POJO object.
     * 
     * @param documentLink the document link.
     * @param document     the document represented as a POJO or Document object.
     * @param options      the request options.
     * @return the resource response with the replaced document.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Document> replaceDocument(String documentLink, Object document, RequestOptions options)
            throws DocumentClientException {

        if (StringUtils.isEmpty(documentLink)) {
            throw new IllegalArgumentException("documentLink");
        }
        
        if (document == null) {
            throw new IllegalArgumentException("document");          
        }

        Document typedDocument = Document.FromObject(document);
        return this.replaceDocumentInternal(documentLink, typedDocument, options);
    }

    /**
     * Replaces a document with the passed in document.
     * 
     * @param document the document to replace (containing the document id).
     * @param options  the request options.
     * @return the resource response with the replaced document.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Document> replaceDocument(Document document, RequestOptions options)
            throws DocumentClientException {

        if (document == null) {
            throw new IllegalArgumentException("document");          
        }

        return this.replaceDocumentInternal(document.getSelfLink(), document, options);
    }
    
    private ResourceResponse<Document> replaceDocumentInternal(String documentLink, Document document, RequestOptions options)
            throws DocumentClientException {

        if (document == null) {
            throw new IllegalArgumentException("document");          
        }

        final String documentCollectionName = Utils.getCollectionName(documentLink);
        final String documentCollectionLink = this.getTargetDocumentCollectionLink(documentCollectionName, document);
        final String path = Utils.joinPath(documentLink, null);
        final Map<String, String> requestHeaders = getRequestHeaders(options);
        final DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Replace,
                                                                       ResourceType.Document,
                                                                       path,
                                                                       document,
                                                                       requestHeaders);
        this.addPartitionKeyInformation(request, document, options);
        final boolean shouldRetry = options == null || options.getPartitionKey() == null;
        
        DocumentClient.validateResource(document);

      	RetryCreateDocumentDelegate replaceDelegate = new RetryCreateDocumentDelegate() {

            @Override
            public ResourceResponse<Document> apply() throws DocumentClientException {
                return new ResourceResponse<Document>(doReplace(request), Document.class);
            }
        };
        
        return shouldRetry 
            ? RetryUtility.executeCreateDocument(replaceDelegate, this.collectionCache, documentCollectionLink)
            : replaceDelegate.apply();
    }

    /**
     * Deletes a document by the document link. 
     * 
     * @param documentLink the document link.
     * @param options      the request options.
     * @return the resource response.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Document> deleteDocument(String documentLink, RequestOptions options)
            throws DocumentClientException {

        if (StringUtils.isEmpty(documentLink)) {
            throw new IllegalArgumentException("documentLink");
        }

        String path = Utils.joinPath(documentLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Delete, ResourceType.Document, path, requestHeaders);
        this.addPartitionKeyInformation(request, null, options);
        return new ResourceResponse<Document>(this.doDelete(request), Document.class);
    }

    /**
     * Reads a document by the document link.
     * 
     * @param documentLink the document link.
     * @param options      the request options.
     * @return the resource response with the read document.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Document> readDocument(String documentLink, RequestOptions options)
            throws DocumentClientException {

        if (StringUtils.isEmpty(documentLink)) {
            throw new IllegalArgumentException("documentLink");
        }

        String path = Utils.joinPath(documentLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Read, ResourceType.Document, path, requestHeaders);
        this.addPartitionKeyInformation(request, null, options);
        return new ResourceResponse<Document>(this.doRead(request), Document.class);
    }

    /**
     * Reads all documents in a document collection.
     * 
     * @param collectionLink the collection link.
     * @param options        the feed options.
     * @return the feed response with read documents.
     */
    public FeedResponse<Document> readDocuments(String collectionLink, FeedOptions options) {

        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }

        String path = Utils.joinPath(collectionLink, Paths.DOCUMENTS_PATH_SEGMENT);
        return new FeedResponse<Document>(new QueryIterable<Document>(this, 
                                                                      ResourceType.Document, 
                                                                      Document.class, 
                                                                      path, 
                                                                      options));
    }

    /**
     * Query for documents in a document collection.
     * 
     * @param collectionLink the link to the parent document collection.
     * @param query          the query.
     * @param options        the feed options.
     * @return the feed response with the obtained documents.
     */
    public FeedResponse<Document> queryDocuments(String collectionLink, String query, FeedOptions options) {
        return this.queryDocuments(collectionLink, query, options, null);
    }
    
    /**
     * Query for documents in a document collection with a partitionKey
     * 
     * @param collectionLink the link to the parent document collection.
     * @param query          the query.
     * @param options        the feed options.
     * @param partitionKey   the partitionKey.
     * @return the feed response with the obtained documents.
     */
    public FeedResponse<Document> queryDocuments(String collectionLink, String query, FeedOptions options,
            Object partitionKey) {
        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }
        if (StringUtils.isEmpty(query)) {
            throw new IllegalArgumentException("query");
        }
        
        return queryDocuments(collectionLink, new SqlQuerySpec(query, null), options, partitionKey);
    }
    
    /**
     * Query for documents in a document collection.
     * 
     * @param collectionLink the link to the parent document collection.
     * @param querySpec      the SQL query specification.
     * @param options        the feed options.
     * @return the feed response with the obtained documents.
     */
    public FeedResponse<Document> queryDocuments(String collectionLink, SqlQuerySpec querySpec, FeedOptions options) {
        return this.queryDocuments(collectionLink, querySpec, options, null);
    }

    /**
     * Query for documents in a document collection.
     * 
     * @param collectionLink the link to the parent document collection.
     * @param querySpec      the SQL query specification.
     * @param options        the feed options.
     * @param partitionKey   the partitionKey.
     * @return the feed response with the obtained documents.
     */
    public FeedResponse<Document> queryDocuments(String collectionLink, SqlQuerySpec querySpec, FeedOptions options,
            Object partitionKey) {
        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }
        if (querySpec == null) {
            throw new IllegalArgumentException("querySpec");
        }  
        
        String path;
        if (Utils.isDatabaseLink(collectionLink)) {
            path = collectionLink;
        } else {
            path = Utils.joinPath(collectionLink, Paths.DOCUMENTS_PATH_SEGMENT);
        }

        return new FeedResponse<Document>(new QueryIterable<Document>(this, 
                                                                           ResourceType.Document,
                                                                      Document.class, 
                                                                           path,
                                                                           querySpec,
                                                                      options, 
                                                                      partitionKey));
        }

    /**
     * Creates a stored procedure.
     * 
     * @param collectionLink  the collection link.
     * @param storedProcedure the stored procedure to create.
     * @param options         the request options.
     * @return the resource response with the created stored procedure.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<StoredProcedure> createStoredProcedure(String collectionLink,
                                                                   StoredProcedure storedProcedure,
                                                                   RequestOptions options)
            throws DocumentClientException {

        DocumentServiceRequest request = getStoredProcedureRequest(collectionLink, storedProcedure, options, OperationType.Create);
        return new ResourceResponse<StoredProcedure>(this.doCreate(request), StoredProcedure.class);
    }    
    
    /**
     * Upserts a stored procedure.
     * 
     * @param collectionLink  the collection link.
     * @param storedProcedure the stored procedure to upsert.
     * @param options         the request options.
     * @return the resource response with the upserted stored procedure.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<StoredProcedure> upsertStoredProcedure(String collectionLink,
                                                                   StoredProcedure storedProcedure,
                                                                   RequestOptions options)
            throws DocumentClientException {

        DocumentServiceRequest request = getStoredProcedureRequest(collectionLink, storedProcedure, options, OperationType.Upsert);
        return new ResourceResponse<StoredProcedure>(this.doUpsert(request), StoredProcedure.class);
    }
    
    private DocumentServiceRequest getStoredProcedureRequest(String collectionLink, StoredProcedure storedProcedure,
            RequestOptions options, OperationType operationType) {
        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }
        if (storedProcedure == null) {
            throw new IllegalArgumentException("storedProcedure");          
        }

        DocumentClient.validateResource(storedProcedure);

        String path = Utils.joinPath(collectionLink, Paths.STORED_PROCEDURES_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(operationType, 
                                                                       ResourceType.StoredProcedure,
                                                                       path,
                                                                       storedProcedure,
                                                                       requestHeaders);
        return request;
    }

    /**
     * Replaces a stored procedure.
     * 
     * @param storedProcedure the stored procedure to use.
     * @param options         the request options.
     * @return the resource response with the replaced stored procedure.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<StoredProcedure> replaceStoredProcedure(StoredProcedure storedProcedure,
                                                                    RequestOptions options)
            throws DocumentClientException {

        if (storedProcedure == null) {
            throw new IllegalArgumentException("storedProcedure");          
        }

        DocumentClient.validateResource(storedProcedure);

        String path = Utils.joinPath(storedProcedure.getSelfLink(), null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Replace, 
                                                                       ResourceType.StoredProcedure,
                                                                       path,
                                                                       storedProcedure,
                                                                       requestHeaders);
        return new ResourceResponse<StoredProcedure>(this.doReplace(request), StoredProcedure.class);
    }

    /**
     * Deletes a stored procedure by the stored procedure link.
     * 
     * @param storedProcedureLink the stored procedure link.
     * @param options             the request options.
     * @return the resource response.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<StoredProcedure> deleteStoredProcedure(String storedProcedureLink, RequestOptions options)
            throws DocumentClientException {

        if (StringUtils.isEmpty(storedProcedureLink)) {
            throw new IllegalArgumentException("storedProcedureLink");
        }

        String path = Utils.joinPath(storedProcedureLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Delete, 
                                                                       ResourceType.StoredProcedure,
                                                                       path,
                                                                       requestHeaders);
        return new ResourceResponse<StoredProcedure>(this.doDelete(request), StoredProcedure.class);
    }

    /**
     * Read a stored procedure by the stored procedure link.
     * 
     * @param storedProcedureLink the stored procedure link.
     * @param options             the request options.
     * @return the resource response with the read stored procedure.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<StoredProcedure> readStoredProcedure(String storedProcedureLink, RequestOptions options)
            throws DocumentClientException {

        if (StringUtils.isEmpty(storedProcedureLink)) {
            throw new IllegalArgumentException("storedProcedureLink");
        }

        String path = Utils.joinPath(storedProcedureLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Read, 
                                                                       ResourceType.StoredProcedure,
                                                                       path,
                                                                       requestHeaders);
        return new ResourceResponse<StoredProcedure>(this.doRead(request), StoredProcedure.class);
    }

    /**
     * Reads all stored procedures in a document collection link.
     * 
     * @param collectionLink the collection link.
     * @param options        the feed options.
     * @return the feed response with the read stored procedure.
     */
    public FeedResponse<StoredProcedure> readStoredProcedures(String collectionLink, FeedOptions options) {

        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }

        String path = Utils.joinPath(collectionLink, Paths.STORED_PROCEDURES_PATH_SEGMENT);
        return new FeedResponse<StoredProcedure>(new QueryIterable<StoredProcedure>(this, 
                                                                       ResourceType.StoredProcedure,
                                                                                    StoredProcedure.class, 
                                                                       path,
                                                                                    options));
    }

    /**
     * Query for stored procedures in a document collection.
     * 
     * @param collectionLink the collection link.
     * @param query          the query.
     * @param options        the feed options.
     * @return the feed response with the obtained stored procedures.
     */
    public FeedResponse<StoredProcedure> queryStoredProcedures(String collectionLink, String query,
                                                               FeedOptions options) {

        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }
        if (StringUtils.isEmpty(query)) {
            throw new IllegalArgumentException("query");
        }

        return queryStoredProcedures(collectionLink, new SqlQuerySpec(query, null), options);
    }

    /**
     * Query for stored procedures in a document collection.
     * 
     * @param collectionLink the collection link.
     * @param querySpec      the SQL query specification.
     * @param options        the feed options.
     * @return the feed response with the obtained stored procedures.
     */
    public FeedResponse<StoredProcedure> queryStoredProcedures(String collectionLink, SqlQuerySpec querySpec,
                                                               FeedOptions options) {

        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }
        if (querySpec == null) {
            throw new IllegalArgumentException("querySpec");
        }

        String path = Utils.joinPath(collectionLink, Paths.STORED_PROCEDURES_PATH_SEGMENT);
        return new FeedResponse<StoredProcedure>(new QueryIterable<StoredProcedure>(this, 
                                                                       ResourceType.StoredProcedure,
                                                                                    StoredProcedure.class, 
                                                                       path,
                                                                       querySpec,
                                                                                    options));
    }

    /**
     * Executes a stored procedure by the stored procedure link.
     * 
     * @param storedProcedureLink the stored procedure link.
     * @param procedureParams     the array of procedure parameter values.
     * @return the stored procedure response.
     * @throws DocumentClientException the document client exception.
     */
    public StoredProcedureResponse executeStoredProcedure(String storedProcedureLink, Object[] procedureParams)
            throws DocumentClientException {
        return this.executeStoredProcedure(storedProcedureLink, null, procedureParams);
    }
    
    /**
     * Executes a stored procedure by the stored procedure link.
     * 
     * @param storedProcedureLink the stored procedure link.
     * @param options             the request options.
     * @param procedureParams     the array of procedure parameter values.
     * @return the stored procedure response.
     * @throws DocumentClientException the document client exception.
     */
    public StoredProcedureResponse executeStoredProcedure(String storedProcedureLink, RequestOptions options, Object[] procedureParams)
            throws DocumentClientException {
        String path = Utils.joinPath(storedProcedureLink, null);
        
        Map<String, String> requestHeaders = new HashMap<String, String>();
        requestHeaders.put(HttpConstants.HttpHeaders.ACCEPT, RuntimeConstants.MediaTypes.JSON);
        if (options != null) {
            if (options.getPartitionKey() != null) {
                requestHeaders.put(HttpConstants.HttpHeaders.PARTITION_KEY, options.getPartitionKey().toString());
            }
        }
        
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.ExecuteJavaScript,
                ResourceType.StoredProcedure,
                path,
                procedureParams != null ? DocumentClient.serializeProcedureParams(procedureParams) : "",
                requestHeaders);
        this.addPartitionKeyInformation(request, null, options);
        return new StoredProcedureResponse(this.doCreate(request));
    }

    /**
     * Creates a trigger.
     * 
     * @param collectionLink the collection link.
     * @param trigger        the trigger.
     * @param options        the request options.
     * @return the resource response with the created trigger.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Trigger> createTrigger(String collectionLink, Trigger trigger, RequestOptions options)
             throws DocumentClientException {

        DocumentServiceRequest request = getTriggerRequest(collectionLink, trigger, options, OperationType.Create);
        return new ResourceResponse<Trigger>(this.doCreate(request), Trigger.class);
    }    
    
    /**
     * Upserts a trigger.
     * 
     * @param collectionLink the collection link.
     * @param trigger        the trigger to upsert.
     * @param options        the request options.
     * @return the resource response with the upserted trigger.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Trigger> upsertTrigger(String collectionLink, Trigger trigger, RequestOptions options)
             throws DocumentClientException {

        DocumentServiceRequest request = getTriggerRequest(collectionLink, trigger, options, OperationType.Upsert);
        return new ResourceResponse<Trigger>(this.doUpsert(request), Trigger.class);
    }
    
    private DocumentServiceRequest getTriggerRequest(String collectionLink, Trigger trigger, RequestOptions options, OperationType operationType) {
        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }
        if (trigger == null) {
            throw new IllegalArgumentException("trigger");          
        }

        DocumentClient.validateResource(trigger);

        String path = Utils.joinPath(collectionLink, Paths.TRIGGERS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(operationType, 
                                                                       ResourceType.Trigger,
                                                                       path,
                                                                       trigger,
                                                                       requestHeaders);
        return request;
    }

    /**
     * Replaces a trigger.
     * 
     * @param trigger the trigger to use.
     * @param options the request options.
     * @return the resource response with the replaced trigger.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Trigger> replaceTrigger(Trigger trigger, RequestOptions options)
            throws DocumentClientException {

        if (trigger == null) {
            throw new IllegalArgumentException("trigger");          
        }

        DocumentClient.validateResource(trigger);

        String path = Utils.joinPath(trigger.getSelfLink(), null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Replace, ResourceType.Trigger,
                                                                       path,
                                                                       trigger,
                                                                       requestHeaders);
        return new ResourceResponse<Trigger>(this.doReplace(request), Trigger.class);
    }

    /**
     * Deletes a trigger.
     * 
     * @param triggerLink the trigger link.
     * @param options     the request options.
     * @return the resource response.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Trigger> deleteTrigger(String triggerLink, RequestOptions options)
            throws DocumentClientException {

        if (StringUtils.isEmpty(triggerLink)) {
            throw new IllegalArgumentException("triggerLink");
        }

        String path = Utils.joinPath(triggerLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Delete, ResourceType.Trigger, path, requestHeaders);
        return new ResourceResponse<Trigger>(this.doDelete(request), Trigger.class);
    }

    /**
     * Reads a trigger by the trigger link.
     * 
     * @param triggerLink the trigger link.
     * @param options     the request options.
     * @return the resource response with the read trigger.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Trigger> readTrigger(String triggerLink, RequestOptions options)
            throws DocumentClientException {

        if (StringUtils.isEmpty(triggerLink)) {
            throw new IllegalArgumentException("triggerLink");
        }

        String path = Utils.joinPath(triggerLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Read, ResourceType.Trigger, path, requestHeaders);
        return new ResourceResponse<Trigger>(this.doRead(request), Trigger.class);
    }

    /**
     * Reads all triggers in a document collection.
     * 
     * @param collectionLink the collection link.
     * @param options        the feed options.
     * @return the feed response with the read triggers.
     */
    public FeedResponse<Trigger> readTriggers(String collectionLink, FeedOptions options) {

        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }

        String path = Utils.joinPath(collectionLink, Paths.TRIGGERS_PATH_SEGMENT);
        return new FeedResponse<Trigger>(
                new QueryIterable<Trigger>(this, 
                                           ResourceType.Trigger, 
                                           Trigger.class, 
                                           path, 
                                           options));
    }

    /**
     * Query for triggers.
     * 
     * @param collectionLink the collection link.
     * @param query          the query.
     * @param options        the feed options.
     * @return the feed response with the obtained triggers.
     */
    public FeedResponse<Trigger> queryTriggers(String collectionLink,
                                               String query,
                                               FeedOptions options) {

        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }

        if (StringUtils.isEmpty(query)) {
            throw new IllegalArgumentException("query");
        }

        return queryTriggers(collectionLink, new SqlQuerySpec(query, null), options);
    }

    /**
     * Query for triggers.
     * 
     * @param collectionLink the collection link.
     * @param querySpec      the SQL query specification.
     * @param options        the feed options.
     * @return the feed response with the obtained triggers.
     */
    public FeedResponse<Trigger> queryTriggers(String collectionLink,
                                               SqlQuerySpec querySpec,
                                               FeedOptions options) {

        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }

        if (querySpec == null) {
            throw new IllegalArgumentException("querySpec");
        }

        String path = Utils.joinPath(collectionLink, Paths.TRIGGERS_PATH_SEGMENT);
        return new FeedResponse<Trigger>(new QueryIterable<Trigger>(this, 
                                                                    ResourceType.Trigger, 
                                                                    Trigger.class, 
                                                                       path,
                                                                       querySpec,
                                                                    options));
    }

    /**
     * Reads all partition key ranges in a document collection.
     * 
     * @param collectionLink the collection link.
     * @param options the feed options.
     * @return the feed response with the read partition key ranges.
     */
    FeedResponse<PartitionKeyRange> readPartitionKeyRanges(String collectionLink, FeedOptions options) {
        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }

        String path = Utils.joinPath(collectionLink, Paths.PARTITION_KEY_RANGE_PATH_SEGMENT);
        return new FeedResponse<PartitionKeyRange>(new QueryIterable<PartitionKeyRange>(this,
                                                                                        ResourceType.PartitionKeyRange, 
                                                                                        PartitionKeyRange.class, 
                                                                                        path, 
                                                                                        options));
    }

    /**
     * Creates a user defined function.
     * 
     * @param collectionLink the collection link.
     * @param udf            the user defined function.
     * @param options        the request options.
     * @return the resource response with the created user defined function.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<UserDefinedFunction> createUserDefinedFunction(
             String collectionLink,
             UserDefinedFunction udf,
             RequestOptions options)
             throws DocumentClientException {
        
        DocumentServiceRequest request = getUserDefinedFunctionRequest(collectionLink, udf, options, OperationType.Create);
        return new ResourceResponse<UserDefinedFunction>(this.doCreate(request), UserDefinedFunction.class);
    }    
    
    /**
     * Upserts a user defined function.
     * 
     * @param collectionLink the collection link.
     * @param udf            the user defined function to upsert.
     * @param options        the request options.
     * @return the resource response with the upserted user defined function.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<UserDefinedFunction> upsertUserDefinedFunction(
             String collectionLink,
             UserDefinedFunction udf,
             RequestOptions options)
             throws DocumentClientException {
        
        DocumentServiceRequest request = getUserDefinedFunctionRequest(collectionLink, udf, options, OperationType.Upsert);
        return new ResourceResponse<UserDefinedFunction>(this.doUpsert(request), UserDefinedFunction.class);
    }
    
    private DocumentServiceRequest getUserDefinedFunctionRequest(String collectionLink, UserDefinedFunction udf,
            RequestOptions options, OperationType operationType) {
        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }
        if (udf == null) {
            throw new IllegalArgumentException("udf");          
        }

        DocumentClient.validateResource(udf);

        String path = Utils.joinPath(collectionLink, Paths.USER_DEFINED_FUNCTIONS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(operationType, ResourceType.UserDefinedFunction,
                                                                       path,
                                                                       udf,
                                                                       requestHeaders);
        return request;
    }

    /**
     * Replaces a user defined function.
     * 
     * @param udf     the user defined function.
     * @param options the request options.
     * @return the resource response with the replaced user defined function.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<UserDefinedFunction> replaceUserDefinedFunction(UserDefinedFunction udf,
                                                                            RequestOptions options)
            throws DocumentClientException {
        if (udf == null) {
            throw new IllegalArgumentException("udf");          
        }

        DocumentClient.validateResource(udf);

        String path = Utils.joinPath(udf.getSelfLink(), null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Replace, ResourceType.UserDefinedFunction,
                                                                       path,
                                                                       udf,
                                                                       requestHeaders);
        return new ResourceResponse<UserDefinedFunction>(this.doReplace(request), UserDefinedFunction.class);
    }

    /**
     * Deletes a user defined function.
     * 
     * @param udfLink the user defined function link.
     * @param options the request options.
     * @return the resource response.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<UserDefinedFunction> deleteUserDefinedFunction(String udfLink, RequestOptions options)
            throws DocumentClientException {
        if (StringUtils.isEmpty(udfLink)) {
            throw new IllegalArgumentException("udfLink");
        }
        
        String path = Utils.joinPath(udfLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Delete, ResourceType.UserDefinedFunction,
                                                                       path,
                                                                       requestHeaders);
        return new ResourceResponse<UserDefinedFunction>(this.doDelete(request), UserDefinedFunction.class);
    }

    /**
     * Read a user defined function.
     * 
     * @param udfLink the user defined function link.
     * @param options the request options.
     * @return the resource response with the read user defined function.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<UserDefinedFunction> readUserDefinedFunction(String udfLink, RequestOptions options)
            throws DocumentClientException {
        if (StringUtils.isEmpty(udfLink)) {
            throw new IllegalArgumentException("udfLink");
        }
        
        String path = Utils.joinPath(udfLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Read, ResourceType.UserDefinedFunction,
                                                                       path,
                                                                       requestHeaders);
        return new ResourceResponse<UserDefinedFunction>(this.doRead(request), UserDefinedFunction.class);
    }

    /**
     * Reads all user defined functions in a document collection.
     * 
     * @param collectionLink the collection link.
     * @param options        the feed options.
     * @return the feed response with the read user defined functions.
     */
    public FeedResponse<UserDefinedFunction> readUserDefinedFunctions(String collectionLink, FeedOptions options) {

        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }

        String path = Utils.joinPath(collectionLink, Paths.USER_DEFINED_FUNCTIONS_PATH_SEGMENT);
        return new FeedResponse<UserDefinedFunction>(new QueryIterable<UserDefinedFunction>(this,
                                                                                            ResourceType.UserDefinedFunction, 
                                                                                            UserDefinedFunction.class, 
                                                                       path,
                                                                                            options));
    }

    /**
     * Query for user defined functions.
     * 
     * @param collectionLink the collection link.
     * @param query          the query.
     * @param options        the feed options.
     * @return the feed response with the obtained user defined functions.
     */
    public FeedResponse<UserDefinedFunction> queryUserDefinedFunctions(String collectionLink,
                                                                       String query,
                                                                       FeedOptions options) {

        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }

        if (StringUtils.isEmpty(query)) {
            throw new IllegalArgumentException("query");
        }

        return queryUserDefinedFunctions(collectionLink, new SqlQuerySpec(query, null), options);
    }

    /**
     * Query for user defined functions.
     * 
     * @param collectionLink the collection link.
     * @param querySpec      the SQL query specification.
     * @param options        the feed options.
     * @return the feed response with the obtained user defined functions.
     */
    public FeedResponse<UserDefinedFunction> queryUserDefinedFunctions(String collectionLink,
                                                                       SqlQuerySpec querySpec,
                                                                       FeedOptions options) {

        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }

        if (querySpec == null) {
            throw new IllegalArgumentException("querySpec");
        }

        String path = Utils.joinPath(collectionLink, Paths.USER_DEFINED_FUNCTIONS_PATH_SEGMENT);
        return new FeedResponse<UserDefinedFunction>(new QueryIterable<UserDefinedFunction>(this,
                                                                                            ResourceType.UserDefinedFunction, 
                                                                                            UserDefinedFunction.class, 
                                                                       path,
                                                                       querySpec,
                                                                                            options));
    }

    /**
     * Creates an attachment.
     * 
     * @param documentLink the document link.
     * @param attachment   the attachment to create.
     * @param options      the request options.
     * @return the resource response with the created attachment.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Attachment> createAttachment(String documentLink,
                                                         Attachment attachment,
                                                         RequestOptions options)
            throws DocumentClientException {
        DocumentServiceRequest request = getAttachmentRequest(documentLink, attachment, options, OperationType.Create);
        return new ResourceResponse<Attachment>(this.doCreate(request), Attachment.class);
    }
    
    /**
     * Upserts an attachment.
     * 
     * @param documentLink the document link.
     * @param attachment   the attachment to upsert.
     * @param options      the request options.
     * @return the resource response with the upserted attachment.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Attachment> upsertAttachment(String documentLink,
                                                         Attachment attachment,
                                                         RequestOptions options)
            throws DocumentClientException {
        DocumentServiceRequest request = getAttachmentRequest(documentLink, attachment, options, OperationType.Upsert);
        return new ResourceResponse<Attachment>(this.doUpsert(request), Attachment.class);
    }
    
    private DocumentServiceRequest getAttachmentRequest(String documentLink, Attachment attachment,
            RequestOptions options, OperationType operationType) {
        if (StringUtils.isEmpty(documentLink)) {
            throw new IllegalArgumentException("documentLink");
        }
        if (attachment == null) {
            throw new IllegalArgumentException("attachment");          
        }

        DocumentClient.validateResource(attachment);

        String path = Utils.joinPath(documentLink, Paths.ATTACHMENTS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(operationType, ResourceType.Attachment,
                                                                       path,
                                                                       attachment,
                                                                       requestHeaders);
        this.addPartitionKeyInformation(request, null, options);
        return request;
    }
    
    /**
     * Replaces an attachment.
     * 
     * @param attachment the attachment to use.
     * @param options    the request options.
     * @return the resource response with the replaced attachment.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Attachment> replaceAttachment(Attachment attachment, RequestOptions options)
            throws DocumentClientException {
        if (attachment == null) {
            throw new IllegalArgumentException("attachment");          
        }

        DocumentClient.validateResource(attachment);

        String path = Utils.joinPath(attachment.getSelfLink(), null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Replace, ResourceType.Attachment,
                                                                       path,
                                                                       attachment,
                                                                       requestHeaders);
        this.addPartitionKeyInformation(request, null, options);
        return new ResourceResponse<Attachment>(this.doReplace(request), Attachment.class);
    }

    /**
     * Deletes an attachment.
     * 
     * @param attachmentLink the attachment link.
     * @param options        the request options.
     * @return the resource response.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Attachment> deleteAttachment(String attachmentLink, RequestOptions options)
            throws DocumentClientException {
        if (StringUtils.isEmpty(attachmentLink)) {
            throw new IllegalArgumentException("attachmentLink");
        }
        
        String path = Utils.joinPath(attachmentLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Delete, ResourceType.Attachment,
                                                                       path,
                                                                       requestHeaders);
        this.addPartitionKeyInformation(request, null, options);
        return new ResourceResponse<Attachment>(this.doDelete(request), Attachment.class);
    }

    /**
     * Reads an attachment.
     * 
     * @param attachmentLink the attachment link.
     * @param options        the request options.
     * @return the resource response with the read attachment.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Attachment> readAttachment(String attachmentLink, RequestOptions options)
            throws DocumentClientException {
        if (StringUtils.isEmpty(attachmentLink)) {
            throw new IllegalArgumentException("attachmentLink");
        }
        
        String path = Utils.joinPath(attachmentLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Read, ResourceType.Attachment, path, requestHeaders);
        this.addPartitionKeyInformation(request, null, options);
        return new ResourceResponse<Attachment>(this.doRead(request), Attachment.class);
    }

    /**
     * Reads all attachments in a document.
     * 
     * @param documentLink the document link.
     * @param options      the feed options.
     * @return the feed response with the read attachments.
     */
    public FeedResponse<Attachment> readAttachments(String documentLink, FeedOptions options) {
        if (StringUtils.isEmpty(documentLink)) {
            throw new IllegalArgumentException("documentLink");
        }
        
        String path = Utils.joinPath(documentLink, Paths.ATTACHMENTS_PATH_SEGMENT);
        return new FeedResponse<Attachment>(new QueryIterable<Attachment>(this, 
                                                                          ResourceType.Attachment, 
                                                                          Attachment.class, 
                                                                       path,
                                                                          options));
    }

    /**
     * Query for attachments.
     * 
     * @param documentLink the document link.
     * @param query        the query.
     * @param options      the feed options.
     * @return the feed response with the obtained attachments.
     */
    public FeedResponse<Attachment> queryAttachments(String documentLink, String query, FeedOptions options) {
        if (StringUtils.isEmpty(documentLink)) {
            throw new IllegalArgumentException("documentLink");
        }

        if (StringUtils.isEmpty(query)) {
            throw new IllegalArgumentException("query");
        }
        
        return queryAttachments(documentLink, new SqlQuerySpec(query, null), options); 
    }

    /**
     * Query for attachments.
     * 
     * @param documentLink the document link.
     * @param querySpec    the SQL query specification.
     * @param options      the feed options.
     * @return the feed response with the obtained attachments.
     */
    public FeedResponse<Attachment> queryAttachments(String documentLink, SqlQuerySpec querySpec, FeedOptions options) {
        if (StringUtils.isEmpty(documentLink)) {
            throw new IllegalArgumentException("documentLink");
        }

        if (querySpec == null) {
            throw new IllegalArgumentException("querySpec");
        }

        String path = Utils.joinPath(documentLink, Paths.ATTACHMENTS_PATH_SEGMENT);
        return new FeedResponse<Attachment>(new QueryIterable<Attachment>(this, 
                                                                          ResourceType.Attachment, 
                                                                          Attachment.class, 
                                                                       path,
                                                                       querySpec,
                                                                          options));
    }

    /**
     * Creates an attachment.
     * 
     * @param documentLink the document link.
     * @param mediaStream  the media stream for creating the attachment.
     * @param options      the media options.
     * @return the resource response with the created attachment.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Attachment> createAttachment(String documentLink,
                                                         InputStream mediaStream,
                                                         MediaOptions options)
            throws DocumentClientException {

        DocumentServiceRequest request = getAttachmentRequest(documentLink, mediaStream, options, OperationType.Create);
        return new ResourceResponse<Attachment>(this.doCreate(request), Attachment.class);
    }
        
    /**
     * Upserts an attachment to the media stream
     * 
     * @param documentLink the document link.
     * @param mediaStream  the media stream for upserting the attachment.
     * @param options      the media options.
     * @return the resource response with the upserted attachment.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Attachment> upsertAttachment(String documentLink,
                                                         InputStream mediaStream,
                                                         MediaOptions options)
            throws DocumentClientException {

        DocumentServiceRequest request = getAttachmentRequest(documentLink, mediaStream, options, OperationType.Upsert);
        return new ResourceResponse<Attachment>(this.doUpsert(request), Attachment.class);
    }
    
    private DocumentServiceRequest getAttachmentRequest(String documentLink, InputStream mediaStream,
            MediaOptions options, OperationType operationType) {
        if (StringUtils.isEmpty(documentLink)) {
            throw new IllegalArgumentException("documentLink");
        }
        if (mediaStream == null) {
            throw new IllegalArgumentException("mediaStream");          
        }

        String path = Utils.joinPath(documentLink, Paths.ATTACHMENTS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getMediaHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(operationType, ResourceType.Attachment,
                                                                       path,
                                                                       mediaStream,
                                                                       requestHeaders);
        request.setIsMedia(true);
        this.addPartitionKeyInformation(request, null, null);
        return request;
    }

    /**
     * Reads a media by the media link.
     * 
     * @param mediaLink the media link.
     * @return the media response.
     * @throws DocumentClientException the document client exception.
     */
    public MediaResponse readMedia(String mediaLink) throws DocumentClientException {

        if (StringUtils.isEmpty(mediaLink)) {
            throw new IllegalArgumentException("mediaLink");
        }
        
        String path = Utils.joinPath(mediaLink, null);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Read, ResourceType.Media, path, null);
        request.setIsMedia(true);
        return new MediaResponse(this.doRead(request),
                                 this.connectionPolicy.getMediaReadMode() == MediaReadMode.Buffered);
    }

    /**
     * Updates a media by the media link.
     * 
     * @param mediaLink   the media link.
     * @param mediaStream the media stream to upload.
     * @param options     the media options.
     * @return the media response.
     * @throws DocumentClientException the document client exception.
     */
    public MediaResponse updateMedia(String mediaLink, InputStream mediaStream, MediaOptions options)
            throws DocumentClientException {
        
        if (StringUtils.isEmpty(mediaLink)) {
            throw new IllegalArgumentException("mediaLink");
        }
        if (mediaStream == null) {
            throw new IllegalArgumentException("mediaStream");          
        }
        
        String path = Utils.joinPath(mediaLink, null);
        Map<String, String> requestHeaders = this.getMediaHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Replace, ResourceType.Media,
                                                                       path,
                                                                       mediaStream,
                                                                       requestHeaders);
        request.setIsMedia(true);
        return new MediaResponse(this.doReplace(request),
                                 this.connectionPolicy.getMediaReadMode() == MediaReadMode.Buffered);
    }

    /**
     * Reads a conflict.
     * 
     * @param conflictLink the conflict link.
     * @param options      the request options.
     * @return the resource response with the read conflict.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Conflict> readConflict(String conflictLink, RequestOptions options)
            throws DocumentClientException {
        if (StringUtils.isEmpty(conflictLink)) {
            throw new IllegalArgumentException("conflictLink");
        }

        String path = Utils.joinPath(conflictLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Read, ResourceType.Conflict, path, requestHeaders);
        this.addPartitionKeyInformation(request, null, options);
        return new ResourceResponse<Conflict>(this.doRead(request), Conflict.class);
    }

    /**
     * Reads all conflicts in a document collection.
     * 
     * @param collectionLink the collection link.
     * @param options        the feed options.
     * @return the feed response with the read conflicts.
     */
    public FeedResponse<Conflict> readConflicts(String collectionLink, FeedOptions options) {
        
        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }
        
        String path = Utils.joinPath(collectionLink, Paths.CONFLICTS_PATH_SEGMENT);
        return new FeedResponse<Conflict>(new QueryIterable<Conflict>(this, 
            ResourceType.Conflict,
                                                                      Conflict.class, 
            path,
                                                                      options));
    }

    /**
     * Query for conflicts.
     * 
     * @param collectionLink the collection link.
     * @param query          the query.
     * @param options        the feed options.
     * @return the feed response of the obtained conflicts.
     */
    public FeedResponse<Conflict> queryConflicts(String collectionLink, String query, FeedOptions options) {

        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }

        if (StringUtils.isEmpty(query)) {
            throw new IllegalArgumentException("query");
        }

        return queryConflicts(collectionLink, new SqlQuerySpec(query, null), options);
    }

    /**
     * Query for conflicts.
     * 
     * @param collectionLink the collection link.
     * @param querySpec      the SQL query specification.
     * @param options        the feed options.
     * @return the feed response of the obtained conflicts.
     */
    public FeedResponse<Conflict> queryConflicts(String collectionLink, SqlQuerySpec querySpec, FeedOptions options) {

        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }

        if (querySpec == null) {
            throw new IllegalArgumentException("querySpec");
        }

        String path = Utils.joinPath(collectionLink, Paths.CONFLICTS_PATH_SEGMENT);
        return new FeedResponse<Conflict>(new QueryIterable<Conflict>(this, 
                                                                      ResourceType.Conflict, 
                                                                      Conflict.class, 
                                                                       path,
                                                                       querySpec,
                                                                      options));
    }

    /**
     * Deletes a conflict.
     * 
     * @param conflictLink the conflict link.
     * @param options      the request options.
     * @return the resource response.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Conflict> deleteConflict(String conflictLink, RequestOptions options)
            throws DocumentClientException {

        if (StringUtils.isEmpty(conflictLink)) {
            throw new IllegalArgumentException("conflictLink");
        }

        String path = Utils.joinPath(conflictLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Delete, ResourceType.Conflict, path, requestHeaders);
        this.addPartitionKeyInformation(request, null, options);
        return new ResourceResponse<Conflict>(this.doDelete(request), Conflict.class);
    }

    /**
     * Creates a user.
     * 
     * @param databaseLink the database link.
     * @param user         the user to create.
     * @param options      the request options.
     * @return the resource response with the created user.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<User> createUser(String databaseLink, User user, RequestOptions options)
            throws DocumentClientException {

        DocumentServiceRequest request = getUserRequest(databaseLink, user, options, OperationType.Create);
        return new ResourceResponse<User>(this.doCreate(request), User.class);
    }
        
    /**
     * Upserts a user.
     * 
     * @param databaseLink the database link.
     * @param user         the user to upsert.
     * @param options      the request options.
     * @return the resource response with the upserted user.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<User> upsertUser(String databaseLink, User user, RequestOptions options)
            throws DocumentClientException {

        DocumentServiceRequest request = getUserRequest(databaseLink, user, options, OperationType.Upsert);
        return new ResourceResponse<User>(this.doUpsert(request), User.class);
    }
    
    private DocumentServiceRequest getUserRequest(String databaseLink, User user, RequestOptions options, OperationType operationType) {
        if (StringUtils.isEmpty(databaseLink)) {
            throw new IllegalArgumentException("databaseLink");
        }
        if (user == null) {
            throw new IllegalArgumentException("user");
        }

        DocumentClient.validateResource(user);

        String path = Utils.joinPath(databaseLink, Paths.USERS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(operationType, ResourceType.User, path, user, requestHeaders);
        return request;
    }

    /**
     * Replaces a user.
     * 
     * @param user    the user to use.
     * @param options the request options.
     * @return the resource response with the replaced user.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<User> replaceUser(User user, RequestOptions options) throws DocumentClientException {

        if (user == null) {
            throw new IllegalArgumentException("user");          
        }

        DocumentClient.validateResource(user);

        String path = Utils.joinPath(user.getSelfLink(), null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Replace, ResourceType.User, path, user, requestHeaders);
        return new ResourceResponse<User>(this.doReplace(request), User.class);
    }

    /**
     * Deletes a user.
     * 
     * @param userLink the user link.
     * @param options  the request options.
     * @return the resource response.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<User> deleteUser(String userLink, RequestOptions options) throws DocumentClientException {

        if (StringUtils.isEmpty(userLink)) {
            throw new IllegalArgumentException("userLink");
        }

        String path = Utils.joinPath(userLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Delete, ResourceType.User, path, requestHeaders);
        return new ResourceResponse<User>(this.doDelete(request), User.class);
    }

    /**
     * Reads a user.
     * 
     * @param userLink the user link.
     * @param options  the request options.
     * @return the resource response with the read user.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<User> readUser(String userLink, RequestOptions options) throws DocumentClientException {

        if (StringUtils.isEmpty(userLink)) {
            throw new IllegalArgumentException("userLink");
        }

        String path = Utils.joinPath(userLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Read, ResourceType.User, path, requestHeaders);
        return new ResourceResponse<User>(this.doRead(request), User.class);
    }

    /**
     * Reads all users in a database.
     * 
     * @param databaseLink the database link.
     * @param options      the feed options.
     * @return the feed response with the read users.
     */
    public FeedResponse<User> readUsers(String databaseLink, FeedOptions options) {

        if (StringUtils.isEmpty(databaseLink)) {
            throw new IllegalArgumentException("databaseLink");
        }

        String path = Utils.joinPath(databaseLink, Paths.USERS_PATH_SEGMENT);
        return new FeedResponse<User>(new QueryIterable<User>(this, ResourceType.User, User.class, path, options));
    }

    /**
     * Query for users.
     * 
     * @param databaseLink the database link.
     * @param query        the query.
     * @param options      the feed options.
     * @return the feed response of the obtained users.
     */
    public FeedResponse<User> queryUsers(String databaseLink, String query, FeedOptions options) {

        if (StringUtils.isEmpty(databaseLink)) {
            throw new IllegalArgumentException("databaseLink");
        }

        if (StringUtils.isEmpty(query)) {
            throw new IllegalArgumentException("query");
        }
        
        return queryUsers(databaseLink, new SqlQuerySpec(query, null), options);
    }

    /**
     * Query for users.
     * 
     * @param databaseLink the database link.
     * @param querySpec    the SQL query specification.
     * @param options      the feed options.
     * @return the feed response of the obtained users.
     */
    public FeedResponse<User> queryUsers(String databaseLink, SqlQuerySpec querySpec, FeedOptions options) {

        if (StringUtils.isEmpty(databaseLink)) {
            throw new IllegalArgumentException("databaseLink");
        }

        if (querySpec == null) {
            throw new IllegalArgumentException("querySpec");
        }

        String path = Utils.joinPath(databaseLink, Paths.USERS_PATH_SEGMENT);
        return new FeedResponse<User>(new QueryIterable<User>(this, 
                                                              ResourceType.User, 
                                                              User.class, 
                                                                       path,
                                                                       querySpec,
                                                              options));
    }

    /**
     * Creates a permission.
     * 
     * @param userLink   the user link.
     * @param permission the permission to create.
     * @param options    the request options.
     * @return the resource response with the created permission.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Permission> createPermission(String userLink, Permission permission, RequestOptions options)
            throws DocumentClientException {

        DocumentServiceRequest request = getPermissionRequest(userLink, permission, options, OperationType.Create);
        return new ResourceResponse<Permission>(this.doCreate(request), Permission.class);
    }
        
    /**
     * Upserts a permission.
     * 
     * @param userLink   the user link.
     * @param permission the permission to upsert.
     * @param options    the request options.
     * @return the resource response with the upserted permission.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Permission> upsertPermission(String userLink, Permission permission, RequestOptions options)
            throws DocumentClientException {

        DocumentServiceRequest request = getPermissionRequest(userLink, permission, options, OperationType.Upsert);
        return new ResourceResponse<Permission>(this.doUpsert(request), Permission.class);
    }
    
    private DocumentServiceRequest getPermissionRequest(String userLink, Permission permission,
            RequestOptions options, OperationType operationType) {
        if (StringUtils.isEmpty(userLink)) {
            throw new IllegalArgumentException("userLink");
        }
        if (permission == null) {
            throw new IllegalArgumentException("permission");          
        }

        DocumentClient.validateResource(permission);

        String path = Utils.joinPath(userLink, Paths.PERMISSIONS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(operationType, ResourceType.Permission,
                                                                       path,
                                                                       permission,
                                                                       requestHeaders);
        return request;
    }

    /**
     * Replaces a permission.
     * 
     * @param permission the permission to use.
     * @param options    the request options.
     * @return the resource response with the replaced permission.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Permission> replacePermission(Permission permission, RequestOptions options)
            throws DocumentClientException {

        if (permission == null) {
            throw new IllegalArgumentException("permission");          
        }

        DocumentClient.validateResource(permission);

        String path = Utils.joinPath(permission.getSelfLink(), null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Replace, ResourceType.Permission,
                                                                       path,
                                                                       permission,
                                                                       requestHeaders);
        return new ResourceResponse<Permission>(this.doReplace(request), Permission.class);
    }

    /**
     * Deletes a permission.
     * 
     * @param permissionLink the permission link.
     * @param options        the request options.
     * @return the resource response.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Permission> deletePermission(String permissionLink, RequestOptions options)
            throws DocumentClientException {
        
        if (StringUtils.isEmpty(permissionLink)) {
            throw new IllegalArgumentException("permissionLink");
        }
        
        String path = Utils.joinPath(permissionLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Delete, ResourceType.Permission, path, requestHeaders);
        return new ResourceResponse<Permission>(this.doDelete(request), Permission.class);
    }

    /**
     * Reads a permission.
     * 
     * @param permissionLink the permission link.
     * @param options        the request options.
     * @return the resource response with the read permission.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Permission> readPermission(String permissionLink, RequestOptions options)
            throws DocumentClientException {

        if (StringUtils.isEmpty(permissionLink)) {
            throw new IllegalArgumentException("permissionLink");
        }
        
        String path = Utils.joinPath(permissionLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Read, ResourceType.Permission, path, requestHeaders);
        return new ResourceResponse<Permission>(this.doRead(request), Permission.class);
    }

    /**
     * Reads all permissions.
     * 
     * @param permissionLink the permission link.
     * @param options        the feed options.
     * @return the feed response with the read permissions.
     */
    public FeedResponse<Permission> readPermissions(String permissionLink, FeedOptions options) {
        if (StringUtils.isEmpty(permissionLink)) {
            throw new IllegalArgumentException("permissionLink");
        }

        String path = Utils.joinPath(permissionLink, Paths.PERMISSIONS_PATH_SEGMENT);
        return new FeedResponse<Permission>(new QueryIterable<Permission>(this,
                                                                          ResourceType.Permission, 
                                                                          Permission.class, 
                                                                          path, 
                                                                          options));
    }

    /**
     * Query for permissions.
     * 
     * @param permissionLink the permission link.
     * @param query          the query.
     * @param options        the feed options.
     * @return the feed response with the obtained permissions.
     */
    public FeedResponse<Permission> queryPermissions(String permissionLink, String query, FeedOptions options) {

        if (StringUtils.isEmpty(permissionLink)) {
            throw new IllegalArgumentException("permissionLink");
        }

        if (StringUtils.isEmpty(query)) {
            throw new IllegalArgumentException("query");
        }

        return queryPermissions(permissionLink, new SqlQuerySpec(query, null), options);
    }

    /**
     * Query for permissions.
     * 
     * @param permissionLink the permission link.
     * @param querySpec      the SQL query specification.
     * @param options        the feed options.
     * @return the feed response with the obtained permissions.
     */
    public FeedResponse<Permission> queryPermissions(String permissionLink, SqlQuerySpec querySpec,
                                                     FeedOptions options) {

        if (StringUtils.isEmpty(permissionLink)) {
            throw new IllegalArgumentException("permissionLink");
        }

        if (querySpec == null) {
            throw new IllegalArgumentException("querySpec");
        }

        String path = Utils.joinPath(permissionLink, Paths.PERMISSIONS_PATH_SEGMENT);
        return new FeedResponse<Permission>(new QueryIterable<Permission>(this, 
                                                                          ResourceType.Permission, 
                                                                          Permission.class, 
                                                                       path,
                                                                       querySpec,
                                                                          options));
    }

    /**
     * Replaces an offer.
     * 
     * @param offer the offer to use.
     * @return the resource response with the replaced offer.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Offer> replaceOffer(Offer offer) throws DocumentClientException {

        if (offer == null) {
            throw new IllegalArgumentException("offer");          
        }

        DocumentClient.validateResource(offer);

        String path = Utils.joinPath(offer.getSelfLink(), null);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Replace, ResourceType.Offer,
                                                                       path,
                                                                       offer,
                                                                       null);
        return new ResourceResponse<Offer>(this.doReplace(request), Offer.class);
    }

    /**
     * Reads an offer.
     * 
     * @param offerLink the offer link.
     * @return the resource response with the read offer.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Offer> readOffer(String offerLink) throws DocumentClientException {

        if (StringUtils.isEmpty(offerLink)) {
            throw new IllegalArgumentException("offerLink");
        }
        
        String path = Utils.joinPath(offerLink, null);
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Read, ResourceType.Offer, path, null);
        return new ResourceResponse<Offer>(this.doRead(request), Offer.class);
    }

    /**
     * Reads offers.
     * 
     * @param options the feed options.
     * @return the feed response with the read offers.
     */
    public FeedResponse<Offer> readOffers(FeedOptions options) {
        String path = Utils.joinPath(Paths.OFFERS_PATH_SEGMENT, null);
        return new FeedResponse<Offer>(new QueryIterable<Offer>(this, 
                                                                ResourceType.Offer, 
                                                                Offer.class, 
                                                                path, 
                                                                options));
    }

    /**
     * Query for offers in a database.
     * 
     * @param query   the query.
     * @param options the feed options.
     * @return the feed response with the obtained offers.
     */
    public FeedResponse<Offer> queryOffers(String query, FeedOptions options) {
        if (StringUtils.isEmpty(query)) {
            throw new IllegalArgumentException("query");
        }

        return queryOffers(new SqlQuerySpec(query, null), options);
    }

    /**
     * Query for offers in a database.
     * 
     * @param querySpec the query specification.
     * @param options   the feed options.
     * @return the feed response with the obtained offers.
     */
    public FeedResponse<Offer> queryOffers(SqlQuerySpec querySpec, FeedOptions options) {
        if (querySpec == null) {
            throw new IllegalArgumentException("querySpec");
        }

        String path = Utils.joinPath(Paths.OFFERS_PATH_SEGMENT, null);
        return new FeedResponse<Offer>(new QueryIterable<Offer>(this, 
                                                                ResourceType.Offer, 
                                                                Offer.class, 
                                                                       path,
                                                                       querySpec,
                                                                options));
    }

    /**
     * Gets database account information.
     * 
     * @return the database account.
     * @throws DocumentClientException the document client exception.
     */
    public DatabaseAccount getDatabaseAccount() throws DocumentClientException {
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Read, ResourceType.DatabaseAccount,
                                                                       "",  // path
                                                                       null);
        DocumentServiceResponse response = this.doRead(request);
        DatabaseAccount account = response.getResource(DatabaseAccount.class);

        // read the headers and set to the account
        Map<String, String> responseHeader = response.getResponseHeaders();

        account.setMaxMediaStorageUsageInMB(Long.valueOf(responseHeader.get(
                HttpConstants.HttpHeaders.MAX_MEDIA_STORAGE_USAGE_IN_MB)));
        account.setMediaStorageUsageInMB(Long.valueOf(responseHeader.get(
                HttpConstants.HttpHeaders.CURRENT_MEDIA_STORAGE_USAGE_IN_MB)));

        return account;
    }
    
    private DocumentServiceResponse doCreate(DocumentServiceRequest request) throws DocumentClientException {
        this.putMoreContentIntoDocumentServiceRequest(request, HttpConstants.HttpMethods.POST);
        RetryRequestDelegate createDelegate = new RetryRequestDelegate() {

            @Override
            public DocumentServiceResponse apply(DocumentServiceRequest requestInner) throws DocumentClientException {
                
                StoreModel proxy = getStoreProxy(requestInner);
                return proxy.processMessage(requestInner);
            }
        };
        
        this.applySessionToken(request);
        DocumentServiceResponse response = RetryUtility.executeDocumentClientRequest(createDelegate, this, this.globalEndpointManager, request);
        this.captureSessionToken(request, response);
        return response;
    }
    
    private DocumentServiceResponse doUpsert(DocumentServiceRequest request) throws DocumentClientException {
        
        this.putMoreContentIntoDocumentServiceRequest(request, HttpConstants.HttpMethods.POST);
        RetryRequestDelegate upsertDelegate = new RetryRequestDelegate() {

            @Override
            public DocumentServiceResponse apply(DocumentServiceRequest requestInner) throws DocumentClientException {
                return getStoreProxy(requestInner).processMessage(requestInner);
            }
        };
        
        this.applySessionToken(request);        
        Map<String, String> headers = request.getHeaders();
        // headers can never be null, since it will be initialized even when no request options are specified,
        // hence using assertion here instead of exception, being in the private method
        assert (headers != null);
        
        if (headers != null) {
            headers.put(HttpConstants.HttpHeaders.IS_UPSERT, "true");
        }

        DocumentServiceResponse response = RetryUtility.executeDocumentClientRequest(upsertDelegate, this, this.globalEndpointManager, request);
        this.captureSessionToken(request, response);
        return response;
    }
    
    private DocumentServiceResponse doReplace(DocumentServiceRequest request) throws DocumentClientException {
        this.putMoreContentIntoDocumentServiceRequest(request, HttpConstants.HttpMethods.PUT);
        RetryRequestDelegate replaceDelegate = new RetryRequestDelegate() {

            @Override
            public DocumentServiceResponse apply(DocumentServiceRequest requestInner) throws DocumentClientException {
                return getStoreProxy(requestInner).processMessage(requestInner);
            }
        };
        
        this.applySessionToken(request);
        DocumentServiceResponse response = RetryUtility.executeDocumentClientRequest(replaceDelegate, this, this.globalEndpointManager, request);
        this.captureSessionToken(request, response);
        return response;
    }
    
    private DocumentServiceResponse doDelete(DocumentServiceRequest request) throws DocumentClientException {
        RetryRequestDelegate deleteDelegate = new RetryRequestDelegate() {

            @Override
            public DocumentServiceResponse apply(DocumentServiceRequest requestInner) throws DocumentClientException {
                return getStoreProxy(requestInner).processMessage(requestInner);
            }
        };
        
        this.putMoreContentIntoDocumentServiceRequest(request, HttpConstants.HttpMethods.DELETE);
        this.applySessionToken(request);
        DocumentServiceResponse response = RetryUtility.executeDocumentClientRequest(deleteDelegate, this, this.globalEndpointManager, request);

        if (request.getResourceType() != ResourceType.DocumentCollection) {
            this.captureSessionToken(request, response);
        } else {
            this.clearToken(request, response);
        }
        return response;
    }
    
    private DocumentServiceResponse doRead(DocumentServiceRequest request) throws DocumentClientException {
        
       this.putMoreContentIntoDocumentServiceRequest(request, HttpConstants.HttpMethods.GET);
       RetryRequestDelegate readDelegate = new RetryRequestDelegate() {

            @Override
            public DocumentServiceResponse apply(DocumentServiceRequest requestInner) throws DocumentClientException {
                return getStoreProxy(requestInner).processMessage(requestInner);
            }
        };
        
        this.applySessionToken(request);

        DocumentServiceResponse response = RetryUtility.executeDocumentClientRequest(readDelegate, this, this.globalEndpointManager, request);
        this.captureSessionToken(request, response);
        return response;
    }
    
    DocumentServiceResponse doReadFeed(DocumentServiceRequest request) throws DocumentClientException {
        RetryRequestDelegate readDelegate = new RetryRequestDelegate() {

            @Override
            public DocumentServiceResponse apply(DocumentServiceRequest requestInner) throws DocumentClientException {
                return getStoreProxy(requestInner).processMessage(requestInner);
            }
        };
        
        this.putMoreContentIntoDocumentServiceRequest(request, HttpConstants.HttpMethods.GET);
        this.applySessionToken(request);

        DocumentServiceResponse response = RetryUtility.executeDocumentClientRequest(readDelegate, this, this.globalEndpointManager, request);
        this.captureSessionToken(request, response);
        return response;
    }
    
    DocumentServiceResponse doQuery(DocumentServiceRequest request) throws DocumentClientException {
        RetryRequestDelegate readDelegate = new RetryRequestDelegate() {

            @Override
            public DocumentServiceResponse apply(DocumentServiceRequest requestInner) throws DocumentClientException {
                return getStoreProxy(requestInner).processMessage(requestInner);
            }
        };
        
        this.putMoreContentIntoDocumentServiceRequest(request, HttpConstants.HttpMethods.POST);
        this.applySessionToken(request);
        DocumentServiceResponse response = RetryUtility.executeDocumentClientRequest(readDelegate, this, this.globalEndpointManager, request);
        this.captureSessionToken(request, response);
        return response;
    }
    
    DatabaseAccount getDatabaseAccountFromEndpoint(URI endpoint) throws DocumentClientException {
        DocumentServiceRequest request = DocumentServiceRequest.create(OperationType.Read, ResourceType.DatabaseAccount, "", null);
        this.putMoreContentIntoDocumentServiceRequest(request, HttpConstants.HttpMethods.GET);
       
        DocumentServiceResponse response = null;
        try {
            request.setEndpointOverride(endpoint);
            response = this.gatewayProxy.doRead(request);
        } catch (IllegalStateException e) {
            // Ignore all errors. Discover is an optimization.
            String message = "Failed to retrieve database account information. %s";
            Throwable cause = e.getCause();
            if (cause != null) {
                message = String.format(message, cause.toString());
            } else {
                message = String.format(message, e.toString());
            }
            
            this.logger.warning(message);
        }
        
        if (response != null) {
            return response.getResource(DatabaseAccount.class);
        } else {
            return null;
        }
    }
    
    private void applySessionToken(DocumentServiceRequest request) throws DocumentClientException {
        Map<String, String> headers = request.getHeaders();
        if (headers != null && !StringUtils.isEmpty(headers.get(HttpConstants.HttpHeaders.SESSION_TOKEN))) {
            return;  // User is explicitly controlling the session.
        }

        if (this.desiredConsistencyLevel != ConsistencyLevel.Session) {
            return;  // Only apply the session token in case of session consistency
        }

        // Apply the ambient session.
        if (!StringUtils.isEmpty(request.getResourceId())) {
            String sessionToken = this.sessionContainer.resolveSessionToken(request);

            if (!StringUtils.isEmpty(sessionToken)) {
                headers.put(HttpConstants.HttpHeaders.SESSION_TOKEN, sessionToken);
            }
        }
    }

    private void captureSessionToken(DocumentServiceRequest request, DocumentServiceResponse response)
            throws DocumentClientException {
            this.sessionContainer.setSessionToken(request, response);
        }

    private void clearToken(DocumentServiceRequest request, DocumentServiceResponse response) {
        this.sessionContainer.clearToken(request, response);
    }

    private Map<String, String> getRequestHeaders(RequestOptions options) {
        if (options == null)
            return null;

        Map<String, String> headers = new HashMap<String, String>();

        if (options.getAccessCondition() != null) {
            if (options.getAccessCondition().getType() == AccessConditionType.IfMatch) {
                headers.put(HttpConstants.HttpHeaders.IF_MATCH, options.getAccessCondition().getCondition());
            } else {
                headers.put(HttpConstants.HttpHeaders.IF_NONE_MATCH, options.getAccessCondition().getCondition());
            }
        }

        if (options.getConsistencyLevel() != null) {
            headers.put(HttpConstants.HttpHeaders.CONSISTENCY_LEVEL, options.getConsistencyLevel().name());
        }

        if (options.getIndexingDirective() != null) {
            headers.put(HttpConstants.HttpHeaders.INDEXING_DIRECTIVE, options.getIndexingDirective().name());
        }

        if (options.getPostTriggerInclude() != null && options.getPostTriggerInclude().size() > 0) {
            String postTriggerInclude = StringUtils.join(options.getPostTriggerInclude(), ",");
            headers.put(HttpConstants.HttpHeaders.POST_TRIGGER_INCLUDE, postTriggerInclude);
        }

        if (options.getPreTriggerInclude() != null && options.getPreTriggerInclude().size() > 0) {
            String preTriggerInclude = StringUtils.join(options.getPreTriggerInclude(), ",");
            headers.put(HttpConstants.HttpHeaders.PRE_TRIGGER_INCLUDE, preTriggerInclude);
        }

        if (options.getSessionToken() != null && !options.getSessionToken().isEmpty()) {
            headers.put(HttpConstants.HttpHeaders.SESSION_TOKEN, options.getSessionToken());
        }

        if (options.getResourceTokenExpirySeconds() != null) {
            headers.put(HttpConstants.HttpHeaders.RESOURCE_TOKEN_EXPIRY,
                    String.valueOf(options.getResourceTokenExpirySeconds()));
        }

        if (options.getOfferThroughput() != null && options.getOfferThroughput() >= 0) {
            headers.put(HttpConstants.HttpHeaders.OFFER_THROUGHPUT, options.getOfferThroughput().toString());
        } else if (options.getOfferType() != null) {
            headers.put(HttpConstants.HttpHeaders.OFFER_TYPE, options.getOfferType());
        }

        if (options.getPartitionKey() != null) {
            headers.put(HttpConstants.HttpHeaders.PARTITION_KEY, options.getPartitionKey().toString());
        }

        return headers;
    }

    private Map<String, String> getMediaHeaders(MediaOptions options) {
        Map<String, String> requestHeaders = new HashMap<String, String>();

        if (options == null || options.getContentType().isEmpty()) {
            requestHeaders.put(HttpConstants.HttpHeaders.CONTENT_TYPE, RuntimeConstants.MediaTypes.OCTET_STREAM);
        }

        if (options != null) {
            if (!options.getContentType().isEmpty()) {
                requestHeaders.put(HttpConstants.HttpHeaders.CONTENT_TYPE, options.getContentType());
            }

            if (!options.getSlug().isEmpty()) {
                requestHeaders.put(HttpConstants.HttpHeaders.SLUG, options.getSlug());
            }
        }
        return requestHeaders;
    }

    private void addPartitionKeyInformation(DocumentServiceRequest request,
                                            Document document,
                                            RequestOptions options) {
        DocumentCollection collection = this.collectionCache.resolveCollection(request);
        PartitionKeyDefinition partitionKeyDefinition = collection.getPartitionKey();

        PartitionKeyInternal partitionKeyInternal = null;
        if (options != null && options.getPartitionKey() != null) {
            partitionKeyInternal = options.getPartitionKey().getInternalPartitionKey();
        } else if (partitionKeyDefinition == null || partitionKeyDefinition.getPaths().size() == 0) {
            // For backward compatibility, if collection doesn't have partition key defined, we assume all documents
            // have empty value for it and user doesn't need to specify it explicitly.
            partitionKeyInternal = PartitionKeyInternal.getEmpty();
        } else if (document != null) {
            partitionKeyInternal = this.extractPartitionKeyValueFromDocument(document, partitionKeyDefinition)
                    .getInternalPartitionKey();
        } else {
            throw new UnsupportedOperationException("PartitionKey value must be supplied for this operation.");
        }

        request.getHeaders().put(HttpConstants.HttpHeaders.PARTITION_KEY, partitionKeyInternal.toString());
    }

    private PartitionKey extractPartitionKeyValueFromDocument(Document document, PartitionKeyDefinition partitionKeyDefinition) {
         if (partitionKeyDefinition != null) {
             String path = partitionKeyDefinition.getPaths().iterator().next();
             Collection<String> parts = PathParser.getPathParts(path);
             if (parts.size() >= 1) {
                 Object value = document.getObjectByPath(parts);
                 if (value == null || value.getClass() == JSONObject.class) {
                     value = Undefined.Value();
                 }
                 
                 return new PartitionKey(value);
             }
         }
         
         return null;
    }

    private StoreModel getStoreProxy(DocumentServiceRequest request) {
        ResourceType resourceType = request.getResourceType();
        OperationType operationType = request.getOperationType();

        if (resourceType == ResourceType.PartitionKeyRange
                || resourceType == ResourceType.DatabaseAccount
                || request.getIsMedia()) {
            return this.gatewayProxy;
        }

        if (Utils.isCollectionChild(request.getResourceType())) {
            DocumentCollection collection = this.collectionCache.resolveCollection(request);
            if (collection != null
                    && Utils.isCollectionPartitioned(collection)
                    && operationType == OperationType.Query
                    && !ServiceJNIWrapper.isServiceJNIAvailable()
                    && request.getPartitionKeyRangeIdentity() == null) {
                // Fallback to gateway to get query execution information for partitioned collection queries
                // when ServiceJNI is not available
                return this.gatewayProxy;
            }
        }

        if (resourceType == ResourceType.Offer
                || resourceType.isScript() && operationType != OperationType.ExecuteJavaScript) {
            return this.gatewayProxy;
        }

        if (operationType == OperationType.Create || operationType == OperationType.Upsert) {
            if (resourceType == ResourceType.Database ||
                    resourceType == ResourceType.User ||
                    resourceType == ResourceType.DocumentCollection ||
                    resourceType == ResourceType.Permission) {
                return this.gatewayProxy;
            } else {
                return this.storeModel;
            }
        } else if (operationType == OperationType.Delete) {
            if (resourceType == ResourceType.Database ||
                    resourceType == ResourceType.User ||
                    resourceType == ResourceType.DocumentCollection) {
                return this.gatewayProxy;
            } else {
                return this.storeModel;
            }
        } else if (operationType == OperationType.Replace) {
            if (resourceType == ResourceType.DocumentCollection) {
                return this.gatewayProxy;
            } else {
                return this.storeModel;
            }
        } else if (operationType == OperationType.Read) {
            if (resourceType == ResourceType.DocumentCollection) {
                return this.gatewayProxy;
            } else {
                return this.storeModel;
            }
        } else {
            return this.storeModel;
        }
    }
    
    private void putMoreContentIntoDocumentServiceRequest(DocumentServiceRequest request, String httpMethod) {
        if (this.masterKey != null) {
            final Date currentTime = new Date();
            final SimpleDateFormat sdf = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);
            sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
            String xDate = sdf.format(currentTime);

            request.getHeaders().put(HttpConstants.HttpHeaders.X_DATE, xDate);
        }

        if (this.masterKey != null || this.resourceTokens != null) {
            String resourceName = request.getResourceFullName();
            String authorization = this.getAuthorizationToken(resourceName, request.getPath(),
                    request.getResourceType(), httpMethod, request.getHeaders(), this.masterKey, this.resourceTokens);
            try {
                authorization = URLEncoder.encode(authorization, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new IllegalStateException("Failed to encode authtoken.", e);
            }
            request.getHeaders().put(HttpConstants.HttpHeaders.AUTHORIZATION, authorization);
        }

        if ((httpMethod == HttpConstants.HttpMethods.POST || httpMethod == HttpConstants.HttpMethods.PUT)
                && !request.getHeaders().containsKey(HttpConstants.HttpHeaders.CONTENT_TYPE)) {
            request.getHeaders().put(HttpConstants.HttpHeaders.CONTENT_TYPE, RuntimeConstants.MediaTypes.JSON);
        }

        if (!request.getHeaders().containsKey(HttpConstants.HttpHeaders.ACCEPT)) {
            request.getHeaders().put(HttpConstants.HttpHeaders.ACCEPT, RuntimeConstants.MediaTypes.JSON);
        }
    }
    
    private String getAuthorizationToken(String resourceOrOwnerId, String path, ResourceType resourceType,
            String requestVerb, Map<String, String> headers, String masterKey, Map<String, String> resourceTokens) {
        if (masterKey != null) {
            return this.authorizationTokenProvider.generateKeyAuthorizationSignature(requestVerb, resourceOrOwnerId, resourceType,
                    headers);
        } else if (resourceTokens != null) {
            return this.authorizationTokenProvider.getAuthorizationTokenUsingResourceTokens(resourceTokens, path,
                    resourceOrOwnerId);
        }

        return null;
    }
}
