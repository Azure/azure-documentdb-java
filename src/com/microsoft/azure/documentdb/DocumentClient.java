/*
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 */

package com.microsoft.azure.documentdb;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;

/**
 * Provides a client-side logical representation of the Azure DocumentDB service. This client is used to configure and
 * execute requests against the service.
 * <p>
 * The service client encapsulates the endpoint and credentials used to access the DocumentDB service.
 */
public final class DocumentClient {

    private URI serviceEndpoint;
    private String masterKey;
    private Map<String, String> resourceTokens;
    private ConnectionPolicy connectionPolicy;
    private GatewayProxy gatewayProxy;
    private SessionContainer sessionContainer;
    private ConsistencyLevel desiredConsistencyLevel;
    private RetryPolicy retryPolicy;

    /**
     * A client query compatibility mode when making query request. Can be used to force a specific query request
     * format.
     */
    enum QueryCompatibilityMode {
        Default,
        Query,
        SqlQuery
    }

    /**
     * Compatibility mode:
     * Allows to specify compatibility mode used by client when making query requests. Should be removed when
     * application/sql is no longer supported.
     */
    QueryCompatibilityMode queryCompatibilityMode = QueryCompatibilityMode.Default;

    /**
     * Initializes a new instance of the DocumentClient class using the specified DocumentDB service endpoint and keys.
     * 
     * @param serviceEndpoint the URI of the service end point.
     * @param masterKey the master key.
     * @param connectionPolicy the connection policy.
     * @param desiredConsistencyLevel the desired consistency level.
     */
    public DocumentClient(String serviceEndpoint,
                          String masterKey,
                          ConnectionPolicy connectionPolicy,
                          ConsistencyLevel desiredConsistencyLevel) {
        URI uri = null;
        try {
            uri = new URI(serviceEndpoint);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid serviceEndPoint.", e);
        }

        this.masterKey = masterKey;

        this.initialize(uri, connectionPolicy, desiredConsistencyLevel);
    }

    /**
     * Initializes a new instance of the Microsoft.Azure.Documents.Client.DocumentClient class using the specified
     * DocumentDB service endpoint and permissions.
     * 
     * @param serviceEndpoint the URI of the service end point.
     * @param permissionFeed the permission feed.
     * @param connectionPolicy the connection policy.
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

    private void initialize(URI serviceEndpoint,
                            ConnectionPolicy connectionPolicy,
                            ConsistencyLevel desiredConsistencyLevel) {

        this.serviceEndpoint = serviceEndpoint;

        if (connectionPolicy != null) {
            this.connectionPolicy = connectionPolicy;
        } else {
            this.connectionPolicy = new ConnectionPolicy();
        }

        this.retryPolicy = RetryPolicy.getDefault();

        this.sessionContainer = new SessionContainer(this.serviceEndpoint.getHost());
        this.desiredConsistencyLevel = desiredConsistencyLevel;

        UserAgentContainer userAgentContainer = new UserAgentContainer();
        String userAgentSuffix = connectionPolicy.getUserAgentSuffix();
        if(userAgentSuffix != null && userAgentSuffix.length() > 0) {
            userAgentContainer.setSuffix(userAgentSuffix);
        }

        this.gatewayProxy = new GatewayProxy(this.serviceEndpoint,
                                             this.connectionPolicy,
                                             desiredConsistencyLevel,
                                             this.queryCompatibilityMode,
                                             this.masterKey,
                                             this.resourceTokens,
                                             userAgentContainer);
    }

    RetryPolicy getRetryPolicy() {
        return this.retryPolicy;
    }

    /**
     * Creates a database.
     * 
     * @param database the database.
     * @param options the request options.
     * @return the resource response with the created database.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Database> createDatabase(Database database, RequestOptions options)
            throws DocumentClientException {
        if (database == null) {
            throw new IllegalArgumentException("Database");          
        }

        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Database,
                                                                       Paths.DATABASES_ROOT,
                                                                       database,
                                                                       requestHeaders);
        return new ResourceResponse<Database>(this.doCreate(request), Database.class);
    }

    /**
     * Replaces a database.
     * 
     * @param database the database.
     * @param options the request options.
     * @return the resource response with the replaced database.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Database> replaceDatabase(Database database,
                                                      RequestOptions options)
            throws DocumentClientException {
        if (database == null) {
            throw new IllegalArgumentException("Database");          
        }
        
        String path = DocumentClient.joinPath(database.getSelfLink(), null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Database,
                                                                       path,
                                                                       database,
                                                                       requestHeaders);
        return new ResourceResponse<Database>(this.doReplace(request), Database.class);
    }

    /**
     * Deletes a database.
     * 
     * @param databaseLink the database link.
     * @param options the request options.
     * @return the resource response.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Database> deleteDatabase(String databaseLink, RequestOptions options)
            throws DocumentClientException {
        if (StringUtils.isEmpty(databaseLink)) {
            throw new IllegalArgumentException("databaseLink");
        }
        
        String path = DocumentClient.joinPath(databaseLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Database,
                                                                       path,
                                                                       requestHeaders);
        return new ResourceResponse<Database>(this.doDelete(request), Database.class);
    }

    /**
     * Reads a database.
     * 
     * @param databaseLink the database link.
     * @param options the request options.
     * @return the resource response with the read database.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Database> readDatabase(String databaseLink, RequestOptions options)
            throws DocumentClientException {
        if (StringUtils.isEmpty(databaseLink)) {
            throw new IllegalArgumentException("databaseLink");
        }
        
        String path = DocumentClient.joinPath(databaseLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Database,
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
        Map<String, String> requestHeaders = this.getFeedHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Database,
                                                                       Paths.DATABASES_ROOT,
                                                                       requestHeaders);
        return new FeedResponse<Database>(new QueryIterable<Database>(this, request, ReadType.Feed, Database.class));
    }

    /**
     * Query for databases.
     * 
     * @param query the query.
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
     * @param options the feed options.
     * @return the feed response with the obtained databases.
     */
    public FeedResponse<Database> queryDatabases(SqlQuerySpec querySpec, FeedOptions options) {
        if (querySpec == null) {
            throw new IllegalArgumentException("querySpec");
        }

        Map<String, String> requestHeaders = this.getFeedHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Database,
                                                                       Paths.DATABASES_ROOT,
                                                                       querySpec,
                                                                       this.queryCompatibilityMode,
                                                                       requestHeaders);
        return new FeedResponse<Database>(new QueryIterable<Database>(this, request, ReadType.Query, Database.class));
    }

    /**
     * Creates a document collection.
     * 
     * @param databaseLink the database link.
     * @param collection the collection.
     * @param options the request options.
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

        String path = DocumentClient.joinPath(databaseLink, Paths.COLLECTIONS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.DocumentCollection,
                                                                       path,
                                                                       collection,
                                                                       requestHeaders);
        return new ResourceResponse<DocumentCollection>(this.doCreate(request), DocumentCollection.class);
    }

    /**
     * Deletes a document collection by the collection link.
     * 
     * @param collectionLink the collection link.
     * @param options the request options.
     * @return the resource response.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<DocumentCollection> deleteCollection(String collectionLink, RequestOptions options)
            throws DocumentClientException {

        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }

        String path = DocumentClient.joinPath(collectionLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.DocumentCollection,
                                                                       path,
                                                                       requestHeaders);
        return new ResourceResponse<DocumentCollection>(this.doDelete(request), DocumentCollection.class);
    }

    /**
     * Reads a document collection by the collection link.
     * 
     * @param collectionLink the collection link.
     * @param options the request options.
     * @return the resource response with the read collection.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<DocumentCollection> readCollection(String collectionLink, RequestOptions options)
            throws DocumentClientException {

        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }

        String path = DocumentClient.joinPath(collectionLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.DocumentCollection,
                                                                       path,
                                                                       requestHeaders);
        return new ResourceResponse<DocumentCollection>(this.doRead(request), DocumentCollection.class);
    }

    /**
     * Reads all document collections in a database.
     * 
     * @param databaseLink the database link.
     * @param options the fee options.
     * @return the feed response with the read collections.
     */
    public FeedResponse<DocumentCollection> readCollections(String databaseLink, FeedOptions options) {

        if (StringUtils.isEmpty(databaseLink)) {
            throw new IllegalArgumentException("databaseLink");
        }

        String path = DocumentClient.joinPath(databaseLink,
                                              Paths.COLLECTIONS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getFeedHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.DocumentCollection,
                                                                       path,
                                                                       requestHeaders);
        return new FeedResponse<DocumentCollection>(new QueryIterable<DocumentCollection>(this,
                                                                                          request,
                                                                                          ReadType.Feed,
                                                                                          DocumentCollection.class));
    }

    /**
     * Query for document collections in a database.
     * 
     * @param databaseLink the database link.
     * @param query the query.
     * @param options the feed options.
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
     * @param querySpec the SQL query specification.
     * @param options the feed options.
     * @return the feed response with the obtained collections.
     */
    public FeedResponse<DocumentCollection> queryCollections(String databaseLink,
                                                             SqlQuerySpec querySpec,
                                                             FeedOptions options) {

        if (StringUtils.isEmpty(databaseLink)) {
            throw new IllegalArgumentException("databaseLink");
        }

        if (querySpec == null) {
            throw new IllegalArgumentException("querySpec");
        }

        String path = DocumentClient.joinPath(databaseLink, Paths.COLLECTIONS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getFeedHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.DocumentCollection,
                                                                       path,
                                                                       querySpec,
                                                                       this.queryCompatibilityMode,
                                                                       requestHeaders);
        return new FeedResponse<DocumentCollection>(new QueryIterable<DocumentCollection>(this,
                                                                                          request,
                                                                                          ReadType.Query,
                                                                                          DocumentCollection.class));
    }

    /**
     * Creates a document.
     * 
     * @param collectionLink the collection link.
     * @param document the document represented as a POJO or Document object.
     * @param options the request options.
     * @param disableAutomaticIdGeneration the flag for disabling automatic id generation.
     * @return the resource response with the created document.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Document> createDocument(String collectionLink,
                                                     Object document,
                                                     RequestOptions options,
                                                     boolean disableAutomaticIdGeneration)
            throws DocumentClientException {
        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }
        if (document == null) {
            throw new IllegalArgumentException("document");
        }

        Document typedDocument = Document.FromObject(document);

        if (typedDocument.getId() == null && !disableAutomaticIdGeneration) {
            // We are supposed to use GUID. Basically UUID is the same as GUID
            // when represented as a string.
            typedDocument.setId(UUID.randomUUID().toString());
        }
        String path = DocumentClient.joinPath(collectionLink, Paths.DOCUMENTS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Document,
                                                                       path,
                                                                       typedDocument,
                                                                       requestHeaders);
        return new ResourceResponse<Document>(this.doCreate(request), Document.class);
    }

    /**
     * Replaces a document using a POJO object.
     * 
     * @param documentLink the document link.
     * @param document the document represented as a POJO or Document object.
     * @param options the request options.
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

        String path = DocumentClient.joinPath(documentLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        Document typedDocument = Document.FromObject(document);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Document,
                                                                       path,
                                                                       typedDocument,
                                                                       requestHeaders);
        return new ResourceResponse<Document>(this.doReplace(request), Document.class);
    }

    /**
     * Replaces a document with the passed in document.
     * 
     * @param document the document to replace (containing the document id).
     * @param options the request options.
     * @return the resource response with the replaced document.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Document> replaceDocument(Document document, RequestOptions options)
            throws DocumentClientException {

        if (document == null) {
            throw new IllegalArgumentException("document");          
        }

        String path = DocumentClient.joinPath(document.getSelfLink(), null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Document,
                                                                       path,
                                                                       document,
                                                                       requestHeaders);
        return new ResourceResponse<Document>(this.doReplace(request), Document.class);
    }

    /**
     * Deletes a document by the document link. 
     * 
     * @param documentLink the document link.
     * @param options the request options.
     * @return the resource response.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Document> deleteDocument(String documentLink, RequestOptions options)
            throws DocumentClientException {

        if (StringUtils.isEmpty(documentLink)) {
            throw new IllegalArgumentException("documentLink");
        }

        String path = DocumentClient.joinPath(documentLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Document, path, requestHeaders);
        return new ResourceResponse<Document>(this.doDelete(request), Document.class);
    }

    /**
     * Reads a document by the document link.
     * 
     * @param documentLink the document link.
     * @param options the request options.
     * @return the resource response with the read document.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Document> readDocument(String documentLink, RequestOptions options)
            throws DocumentClientException {

        if (StringUtils.isEmpty(documentLink)) {
            throw new IllegalArgumentException("documentLink");
        }

        String path = DocumentClient.joinPath(documentLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Document, path, requestHeaders);
        return new ResourceResponse<Document>(this.doRead(request), Document.class);
    }

    /**
     * Reads all documents in a document collection.
     * 
     * @param collectionLink the collection link.
     * @param options the feed options.
     * @return the feed response with read documents.
     */
    public FeedResponse<Document> readDocuments(String collectionLink, FeedOptions options) {

        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }

        String path = DocumentClient.joinPath(collectionLink, Paths.DOCUMENTS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getFeedHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Document, path, requestHeaders);
        return new FeedResponse<Document>(new QueryIterable<Document>(this, request, ReadType.Feed, Document.class));
    }

    /**
     * Query for documents in a document collection.
     * 
     * @param collectionLink the collection link.
     * @param query the query.
     * @param options the feed options.
     * @return the feed response with the obtained documents.
     */
    public FeedResponse<Document> queryDocuments(String collectionLink, String query, FeedOptions options) {

        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }
        if (StringUtils.isEmpty(query)) {
            throw new IllegalArgumentException("query");
        }  

        return queryDocuments(collectionLink, new SqlQuerySpec(query, null), options);
    }

    /**
     * Query for documents in a document collection.
     * 
     * @param collectionLink the collection link.
     * @param querySpec the SQL query specification.
     * @param options the feed options.
     * @return the feed response with the obtained documents.
     */
    public FeedResponse<Document> queryDocuments(String collectionLink, SqlQuerySpec querySpec, FeedOptions options) {

        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }
        if (querySpec == null) {
            throw new IllegalArgumentException("querySpec");
        }  

        String path = DocumentClient.joinPath(collectionLink, Paths.DOCUMENTS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getFeedHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Document,
                                                                       path,
                                                                       querySpec,
                                                                       this.queryCompatibilityMode,
                                                                       requestHeaders);
        return new FeedResponse<Document>(new QueryIterable<Document>(this,
                                                                      request,
                                                                      ReadType.Query,
                                                                      Document.class));
    }

    /**
     * Creates a stored procedure.
     * 
     * @param collectionLink the collection link.
     * @param storedProcedure the stored procedure to create.
     * @param options the request options.
     * @return the resource response with the created stored procedure.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<StoredProcedure> createStoredProcedure(String collectionLink,
                                                                   StoredProcedure storedProcedure,
                                                                   RequestOptions options)
            throws DocumentClientException {

        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }
        if (storedProcedure == null) {
            throw new IllegalArgumentException("storedProcedure");          
        }

        String path = DocumentClient.joinPath(collectionLink, Paths.STORED_PROCEDURES_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.StoredProcedure,
                                                                       path,
                                                                       storedProcedure,
                                                                       requestHeaders);
        return new ResourceResponse<StoredProcedure>(this.doCreate(request), StoredProcedure.class);
    }

    /**
     * Replaces a stored procedure.
     * 
     * @param storedProcedure the stored procedure to use.
     * @param options the request options.
     * @return the resource response with the replaced stored procedure.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<StoredProcedure> replaceStoredProcedure(StoredProcedure storedProcedure,
                                                                    RequestOptions options)
            throws DocumentClientException {

        if (storedProcedure == null) {
            throw new IllegalArgumentException("storedProcedure");          
        }

        String path = DocumentClient.joinPath(storedProcedure.getSelfLink(), null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.StoredProcedure,
                                                                       path,
                                                                       storedProcedure,
                                                                       requestHeaders);
        return new ResourceResponse<StoredProcedure>(this.doReplace(request), StoredProcedure.class);
    }

    /**
     * Deletes a stored procedure by the stored procedure link.
     * 
     * @param storedProcedureLink the stored procedure link.
     * @param options the request options.
     * @return the resource response.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<StoredProcedure> deleteStoredProcedure(String storedProcedureLink, RequestOptions options)
            throws DocumentClientException {

        if (StringUtils.isEmpty(storedProcedureLink)) {
            throw new IllegalArgumentException("storedProcedureLink");
        }

        String path = DocumentClient.joinPath(storedProcedureLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.StoredProcedure,
                                                                       path,
                                                                       requestHeaders);
        return new ResourceResponse<StoredProcedure>(this.doDelete(request), StoredProcedure.class);
    }

    /**
     * Read a stored procedure by the stored procedure link.
     * 
     * @param storedProcedureLink the stored procedure link.
     * @param options the request options.
     * @return the resource response with the read stored procedure.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<StoredProcedure> readStoredProcedure(String storedProcedureLink, RequestOptions options)
            throws DocumentClientException {

        if (StringUtils.isEmpty(storedProcedureLink)) {
            throw new IllegalArgumentException("storedProcedureLink");
        }

        String path = DocumentClient.joinPath(storedProcedureLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.StoredProcedure,
                                                                       path,
                                                                       requestHeaders);
        return new ResourceResponse<StoredProcedure>(this.doRead(request), StoredProcedure.class);
    }

    /**
     * Reads all stored procedures in a document collection link.
     * 
     * @param collectionLink the collection link.
     * @param options the feed options.
     * @return the feed response with the read stored procedure.
     */
    public FeedResponse<StoredProcedure> readStoredProcedures(String collectionLink, FeedOptions options) {

        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }

        String path = DocumentClient.joinPath(collectionLink, Paths.STORED_PROCEDURES_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getFeedHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.StoredProcedure,
                                                                       path,
                                                                       requestHeaders);
        return new FeedResponse<StoredProcedure>(new QueryIterable<StoredProcedure>(this,
                                                                                    request,
                                                                                    ReadType.Feed,
                                                                                    StoredProcedure.class));
    }

    /**
     * Query for stored procedures in a document collection.
     * 
     * @param collectionLink the collection link.
     * @param query the query.
     * @param options the feed options.
     * @return the feed response with the obtained stored procedures.
     */
    public FeedResponse<StoredProcedure> queryStoredProcedures(String collectionLink,
                                                               String query,
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
     * @param querySpec the SQL query specification.
     * @param options the feed options.
     * @return the feed response with the obtained stored procedures.
     */
    public FeedResponse<StoredProcedure> queryStoredProcedures(String collectionLink,
                                                               SqlQuerySpec querySpec,
                                                               FeedOptions options) {

        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }
        if (querySpec == null) {
            throw new IllegalArgumentException("querySpec");
        }

        String path = DocumentClient.joinPath(collectionLink, Paths.STORED_PROCEDURES_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getFeedHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.StoredProcedure,
                                                                       path,
                                                                       querySpec,
                                                                       this.queryCompatibilityMode,
                                                                       requestHeaders);
        return new FeedResponse<StoredProcedure>(new QueryIterable<StoredProcedure>(this,
                                                                                    request,
                                                                                    ReadType.Query,
                                                                                    StoredProcedure.class));
    }

    /**
     * Executes a stored procedure by the stored procedure link.
     * 
     * @param storedProcedureLink the stored procedure link.
     * @param procedureParams the array of procedure parameter values.
     * @return the stored procedure response.
     * @throws DocumentClientException the document client exception.
     */
    public StoredProcedureResponse executeStoredProcedure(String storedProcedureLink, Object[] procedureParams)
            throws DocumentClientException {
        String path = DocumentClient.joinPath(storedProcedureLink, null);
        Map<String, String> requestHeaders = new HashMap<String, String>();
        requestHeaders.put(HttpConstants.HttpHeaders.ACCEPT, RuntimeConstants.MediaTypes.JSON);
        DocumentServiceRequest request = DocumentServiceRequest.create(
                ResourceType.StoredProcedure,
                path,
                procedureParams != null ? DocumentClient.serializeProcedureParams(procedureParams) : "",
                null);
        return new StoredProcedureResponse(this.doCreate(request));
    }

    /**
     * Creates a trigger.
     * 
     * @param collectionLink the collection link.
     * @param trigger the trigger.
     * @param options the request options.
     * @return the resource response with the created trigger.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Trigger> createTrigger(String collectionLink, Trigger trigger, RequestOptions options)
             throws DocumentClientException {

        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }
        if (trigger == null) {
            throw new IllegalArgumentException("trigger");          
        }

        String path = DocumentClient.joinPath(collectionLink, Paths.TRIGGERS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Trigger,
                                                                       path,
                                                                       trigger,
                                                                       requestHeaders);
        return new ResourceResponse<Trigger>(this.doCreate(request), Trigger.class);
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

        String path = DocumentClient.joinPath(trigger.getSelfLink(), null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Trigger,
                                                                       path,
                                                                       trigger,
                                                                       requestHeaders);
        return new ResourceResponse<Trigger>(this.doReplace(request), Trigger.class);
    }

    /**
     * Deletes a trigger.
     * 
     * @param triggerLink the trigger link.
     * @param options the request options.
     * @return the resource response.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Trigger> deleteTrigger(String triggerLink, RequestOptions options)
            throws DocumentClientException {

        if (StringUtils.isEmpty(triggerLink)) {
            throw new IllegalArgumentException("triggerLink");
        }

        String path = DocumentClient.joinPath(triggerLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Trigger, path, requestHeaders);
        return new ResourceResponse<Trigger>(this.doDelete(request), Trigger.class);
    }

    /**
     * Reads a trigger by the trigger link.
     * 
     * @param triggerLink the trigger link.
     * @param options the request options.
     * @return the resource response with the read trigger.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Trigger> readTrigger(String triggerLink, RequestOptions options)
            throws DocumentClientException {

        if (StringUtils.isEmpty(triggerLink)) {
            throw new IllegalArgumentException("triggerLink");
        }

        String path = DocumentClient.joinPath(triggerLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Trigger, path, requestHeaders);
        return new ResourceResponse<Trigger>(this.gatewayProxy.doRead(request), Trigger.class);
    }

    /**
     * Reads all triggers in a document collection.
     * 
     * @param collectionLink the collection link.
     * @param options the feed options.
     * @return the feed response with the read triggers.
     */
    public FeedResponse<Trigger> readTriggers(String collectionLink, FeedOptions options) {

        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }

        String path = DocumentClient.joinPath(collectionLink, Paths.TRIGGERS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getFeedHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Trigger, path, requestHeaders);
        return new FeedResponse<Trigger>(new QueryIterable<Trigger>(this, request, ReadType.Feed, Trigger.class));
    }

    /**
     * Query for triggers.
     * 
     * @param collectionLink the collection link.
     * @param query the query.
     * @param options the feed options.
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
     * @param querySpec the SQL query specification.
     * @param options the feed options.
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

        String path = DocumentClient.joinPath(collectionLink, Paths.TRIGGERS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getFeedHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Trigger,
                                                                       path,
                                                                       querySpec,
                                                                       this.queryCompatibilityMode,
                                                                       requestHeaders);
        return new FeedResponse<Trigger>(new QueryIterable<Trigger>(this, request, ReadType.Query, Trigger.class));
    }

    /**
     * Creates a user defined function.
     * 
     * @param collectionLink the collection link.
     * @param udf the user defined function.
     * @param options the request options.
     * @return the resource response with the created user defined function.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<UserDefinedFunction> createUserDefinedFunction(
             String collectionLink,
             UserDefinedFunction udf,
             RequestOptions options)
             throws DocumentClientException {
        
        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }
        if (udf == null) {
            throw new IllegalArgumentException("udf");          
        }

        String path = DocumentClient.joinPath(collectionLink, Paths.USER_DEFINED_FUNCTIONS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.UserDefinedFunction,
                                                                       path,
                                                                       udf,
                                                                       requestHeaders);
        return new ResourceResponse<UserDefinedFunction>(this.doCreate(request), UserDefinedFunction.class);
    }

    /**
     * Replaces a user defined function.
     * 
     * @param udf the user defined function.
     * @param options the request options.
     * @return the resource response with the replaced user defined function.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<UserDefinedFunction> replaceUserDefinedFunction(UserDefinedFunction udf,
                                                                            RequestOptions options)
            throws DocumentClientException   {
        if (udf == null) {
            throw new IllegalArgumentException("udf");          
        }

        String path = DocumentClient.joinPath(udf.getSelfLink(), null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.UserDefinedFunction,
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
        
        String path = DocumentClient.joinPath(udfLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.UserDefinedFunction,
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
        
        String path = DocumentClient.joinPath(udfLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.UserDefinedFunction,
                                                                       path,
                                                                       requestHeaders);
        return new ResourceResponse<UserDefinedFunction>(this.doRead(request), UserDefinedFunction.class);
    }

    /**
     * Reads all user defined functions in a document collection.
     * 
     * @param collectionLink the collection link.
     * @param options the feed options.
     * @return the feed response with the read user defined functions.
     */
    public FeedResponse<UserDefinedFunction> readUserDefinedFunctions(String collectionLink, FeedOptions options) {

        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }

        String path = DocumentClient.joinPath(collectionLink, Paths.USER_DEFINED_FUNCTIONS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getFeedHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.UserDefinedFunction,
                                                                       path,
                                                                       requestHeaders);
        return new FeedResponse<UserDefinedFunction>(new QueryIterable<UserDefinedFunction>(this,
                                                                                            request,
                                                                                            ReadType.Feed,
                                                                                            UserDefinedFunction.class));
    }

    /**
     * Query for user defined functions.
     * 
     * @param collectionLink the collection link.
     * @param query the query.
     * @param options the feed options.
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
     * @param querySpec the SQL query specification.
     * @param options the feed options.
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

        String path = DocumentClient.joinPath(collectionLink, Paths.USER_DEFINED_FUNCTIONS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getFeedHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.UserDefinedFunction,
                                                                       path,
                                                                       querySpec,
                                                                       this.queryCompatibilityMode,
                                                                       requestHeaders);
        return new FeedResponse<UserDefinedFunction>(new QueryIterable<UserDefinedFunction>(this,
                                                                                            request,
                                                                                            ReadType.Query,
                                                                                            UserDefinedFunction.class));
    }

    /**
     * Creates an attachment.
     * 
     * @param documentLink the document link.
     * @param attachment the attachment to create.
     * @param options the request options.
     * @return the resource response with the created attachment.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Attachment> createAttachment(String documentLink,
                                                         Attachment attachment,
                                                         RequestOptions options)
            throws DocumentClientException {
        if (StringUtils.isEmpty(documentLink)) {
            throw new IllegalArgumentException("documentLink");
        }
        if (attachment == null) {
            throw new IllegalArgumentException("attachment");          
        }
        
        String path = DocumentClient.joinPath(documentLink, Paths.ATTACHMENTS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Attachment,
                                                                       path,
                                                                       attachment,
                                                                       requestHeaders);
        return new ResourceResponse<Attachment>(this.doCreate(request), Attachment.class);
    }

    /**
     * Replaces an attachment.
     * 
     * @param attachment the attachment to use.
     * @param options the request options.
     * @return the resource response with the replaced attachment.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Attachment> replaceAttachment(Attachment attachment, RequestOptions options)
            throws DocumentClientException {
        if (attachment == null) {
            throw new IllegalArgumentException("attachment");          
        }

        String path = DocumentClient.joinPath(attachment.getSelfLink(), null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Attachment,
                                                                       path,
                                                                       attachment,
                                                                       requestHeaders);
        return new ResourceResponse<Attachment>(this.doReplace(request), Attachment.class);
    }

    /**
     * Deletes an attachment.
     * 
     * @param attachmentLink the attachment link.
     * @param options the request options.
     * @return the resource response.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Attachment> deleteAttachment(String attachmentLink, RequestOptions options)
            throws DocumentClientException {
        if (StringUtils.isEmpty(attachmentLink)) {
            throw new IllegalArgumentException("attachmentLink");
        }
        
        String path = DocumentClient.joinPath(attachmentLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Attachment,
                                                                       path,
                                                                       requestHeaders);
        return new ResourceResponse<Attachment>(this.doDelete(request), Attachment.class);
    }

    /**
     * Reads an attachment.
     * 
     * @param attachmentLink the attachment link.
     * @param options the request options.
     * @return the resource response with the read attachment.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Attachment> readAttachment(String attachmentLink, RequestOptions options)
            throws DocumentClientException {
        if (StringUtils.isEmpty(attachmentLink)) {
            throw new IllegalArgumentException("attachmentLink");
        }
        
        String path = DocumentClient.joinPath(attachmentLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Attachment, path, requestHeaders);
        return new ResourceResponse<Attachment>(this.doRead(request), Attachment.class);
    }

    /**
     * Reads an attachment.
     * 
     * @param documentLink the document link.
     * @param options the feed options.
     * @return the feed response with the read attachments.
     */
    public FeedResponse<Attachment> readAttachments(String documentLink, FeedOptions options) {
        if (StringUtils.isEmpty(documentLink)) {
            throw new IllegalArgumentException("documentLink");
        }
        
        String path = DocumentClient.joinPath(documentLink, Paths.ATTACHMENTS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getFeedHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Attachment,
                                                                       path,
                                                                       requestHeaders);
        return new FeedResponse<Attachment>(new QueryIterable<Attachment>(this,
                                                                          request,
                                                                          ReadType.Feed,
                                                                          Attachment.class));
    }

    /**
     * Query for attachments.
     * 
     * @param documentLink the document link.
     * @param query the query.
     * @param options the feed options.
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
     * @param querySpec the SQL query specification.
     * @param options the feed options.
     * @return the feed response with the obtained attachments.
     */
    public FeedResponse<Attachment> queryAttachments(String documentLink, SqlQuerySpec querySpec, FeedOptions options) {
        if (StringUtils.isEmpty(documentLink)) {
            throw new IllegalArgumentException("documentLink");
        }

        if (querySpec == null) {
            throw new IllegalArgumentException("querySpec");
        }

        String path = DocumentClient.joinPath(documentLink, Paths.ATTACHMENTS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getFeedHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Attachment,
                                                                       path,
                                                                       querySpec,
                                                                       this.queryCompatibilityMode,
                                                                       requestHeaders);
        return new FeedResponse<Attachment>(new QueryIterable<Attachment>(this,
                                                                          request,
                                                                          ReadType.Query,
                                                                          Attachment.class));
    }

    /**
     * Creates an attachment.
     * 
     * @param documentLink the document link.
     * @param mediaStream the media stream for creating the attachment.
     * @param options the media options.
     * @return the resource response with the created attachment.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Attachment> createAttachment(String documentLink,
                                                         InputStream mediaStream,
                                                         MediaOptions options)
            throws DocumentClientException {

        if (StringUtils.isEmpty(documentLink)) {
            throw new IllegalArgumentException("documentLink");
        }
        if (mediaStream == null) {
            throw new IllegalArgumentException("mediaStream");          
        }

        String path = DocumentClient.joinPath(documentLink, Paths.ATTACHMENTS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getMediaHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Attachment,
                                                                       path,
                                                                       mediaStream,
                                                                       requestHeaders);
        request.setIsMedia(true);
        return new ResourceResponse<Attachment>(this.doCreate(request), Attachment.class);
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
        
        String path = DocumentClient.joinPath(mediaLink, null);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Media, path, null);
        request.setIsMedia(true);
        return new MediaResponse(this.doRead(request),
                                 this.connectionPolicy.getMediaReadMode() == MediaReadMode.Buffered);
    }

    /**
     * Updates a media by the media link.
     * 
     * @param mediaLink the media link.
     * @param mediaStream the media stream to upload.
     * @param options the media options.
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
        
        String path = DocumentClient.joinPath(mediaLink, null);
        Map<String, String> requestHeaders = this.getMediaHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Media,
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
     * @param options the request options.
     * @return the resource response with the read conflict.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Conflict> readConflict(String conflictLink, RequestOptions options)
            throws DocumentClientException {
        if (StringUtils.isEmpty(conflictLink)) {
            throw new IllegalArgumentException("conflictLink");
        }

        String path = DocumentClient.joinPath(conflictLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Conflict, path, requestHeaders);
        return new ResourceResponse<Conflict>(this.doRead(request), Conflict.class);
    }

    /**
     * Reads all conflicts in a document collection.
     * 
     * @param collectionLink the collection link.
     * @param options the feed options.
     * @return the feed response with the read conflicts.
     */
    public FeedResponse<Conflict> readConflicts(
        String collectionLink, FeedOptions options) {
        
        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }
        
        String path = DocumentClient.joinPath(collectionLink,
                                              Paths.CONFLICTS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getFeedHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(
            ResourceType.Conflict,
            path,
            requestHeaders);
        return new FeedResponse<Conflict>(new QueryIterable<Conflict>(this, request, ReadType.Feed, Conflict.class));
    }

    /**
     * Query for conflicts.
     * 
     * @param collectionLink the collection link.
     * @param query the query.
     * @param options the feed options.
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
     * @param querySpec the SQL query specification.
     * @param options the feed options.
     * @return the feed response of the obtained conflicts.
     */
    public FeedResponse<Conflict> queryConflicts(String collectionLink, SqlQuerySpec querySpec, FeedOptions options) {

        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }

        if (querySpec == null) {
            throw new IllegalArgumentException("querySpec");
        }

        String path = DocumentClient.joinPath(collectionLink, Paths.CONFLICTS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getFeedHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Conflict,
                                                                       path,
                                                                       querySpec,
                                                                       this.queryCompatibilityMode,
                                                                       requestHeaders);
        return new FeedResponse<Conflict>(new QueryIterable<Conflict>(this, request, ReadType.Query, Conflict.class));
    }

    /**
     * Deletes a conflict.
     * 
     * @param conflictLink the conflict link.
     * @param options the request options.
     * @return the resource response.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Conflict> deleteConflict(String conflictLink, RequestOptions options)
            throws DocumentClientException {

        if (StringUtils.isEmpty(conflictLink)) {
            throw new IllegalArgumentException("conflictLink");
        }

        String path = DocumentClient.joinPath(conflictLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Conflict, path, requestHeaders);
        return new ResourceResponse<Conflict>(this.doDelete(request), Conflict.class);
    }

    /**
     * Creates a user.
     * 
     * @param databaseLink the database link.
     * @param user the user to create.
     * @param options the request options.
     * @return the resource response with the created user.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<User> createUser(String databaseLink, User user, RequestOptions options)
            throws DocumentClientException {

        if (StringUtils.isEmpty(databaseLink)) {
            throw new IllegalArgumentException("databaseLink");
        }
        if (user == null) {
            throw new IllegalArgumentException("user");
        }

        String path = DocumentClient.joinPath(databaseLink, Paths.USERS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.User, path, user, requestHeaders);
        return new ResourceResponse<User>(this.doCreate(request), User.class);
    }

    /**
     * Replaces a user.
     * 
     * @param user the user to use.
     * @param options the request options.
     * @return the resource response with the replaced user.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<User> replaceUser(User user, RequestOptions options) throws DocumentClientException {

        if (user == null) {
            throw new IllegalArgumentException("user");          
        }

        String path = DocumentClient.joinPath(user.getSelfLink(), null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.User, path, user, requestHeaders);
        return new ResourceResponse<User>(this.doReplace(request), User.class);
    }

    /**
     * Deletes a user.
     * 
     * @param userLink the user link.
     * @param options the request options.
     * @return the resource response.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<User> deleteUser(String userLink, RequestOptions options) throws DocumentClientException {

        if (StringUtils.isEmpty(userLink)) {
            throw new IllegalArgumentException("userLink");
        }

        String path = DocumentClient.joinPath(userLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.User, path, requestHeaders);
        return new ResourceResponse<User>(this.doDelete(request), User.class);
    }

    /**
     * Reads a user.
     * 
     * @param userLink the user link.
     * @param options the request options.
     * @return the resource response with the read user.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<User> readUser(String userLink, RequestOptions options) throws DocumentClientException {

        if (StringUtils.isEmpty(userLink)) {
            throw new IllegalArgumentException("userLink");
        }

        String path = DocumentClient.joinPath(userLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.User, path, requestHeaders);
        return new ResourceResponse<User>(this.doRead(request), User.class);
    }

    /**
     * Reads all users in a database.
     * 
     * @param databaseLink the database link.
     * @param options the feed options.
     * @return the feed response with the read users.
     */
    public FeedResponse<User> readUsers(String databaseLink, FeedOptions options) {

        if (StringUtils.isEmpty(databaseLink)) {
            throw new IllegalArgumentException("databaseLink");
        }

        String path = DocumentClient.joinPath(databaseLink, Paths.USERS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getFeedHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.User, path, requestHeaders);
        return new FeedResponse<User>(new QueryIterable<User>(this, request, ReadType.Feed, User.class));
    }

    /**
     * Query for users.
     * 
     * @param databaseLink the database link.
     * @param query the query.
     * @param options the feed options.
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
     * @param querySpec the SQL query specification.
     * @param options the feed options.
     * @return the feed response of the obtained users.
     */
    public FeedResponse<User> queryUsers(String databaseLink, SqlQuerySpec querySpec, FeedOptions options) {

        if (StringUtils.isEmpty(databaseLink)) {
            throw new IllegalArgumentException("databaseLink");
        }

        if (querySpec == null) {
            throw new IllegalArgumentException("querySpec");
        }

        String path = DocumentClient.joinPath(databaseLink, Paths.USERS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getFeedHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.User,
                                                                       path,
                                                                       querySpec,
                                                                       this.queryCompatibilityMode,
                                                                       requestHeaders);
        return new FeedResponse<User>(new QueryIterable<User>(this, request, ReadType.Query, User.class));
    }

    /**
     * Creates a permission.
     * 
     * @param userLink the user link.
     * @param permission the permission to create.
     * @param options the request options.
     * @return the resource response with the created permission.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Permission> createPermission(String userLink, Permission permission, RequestOptions options)
            throws DocumentClientException {

        if (StringUtils.isEmpty(userLink)) {
            throw new IllegalArgumentException("userLink");
        }
        if (permission == null) {
            throw new IllegalArgumentException("permission");          
        }

        String path = DocumentClient.joinPath(userLink, Paths.PERMISSIONS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Permission,
                                                                       path,
                                                                       permission,
                                                                       requestHeaders);
        return new ResourceResponse<Permission>(this.doCreate(request), Permission.class);
    }

    /**
     * Replaces a permission.
     * 
     * @param permission the permission to use.
     * @param options the request options.
     * @return the resource response with the replaced permission.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Permission> replacePermission(Permission permission, RequestOptions options)
    		throws DocumentClientException {

        if (permission == null) {
            throw new IllegalArgumentException("permission");          
        }

        String path = DocumentClient.joinPath(permission.getSelfLink(), null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Permission,
                                                                       path,
                                                                       permission,
                                                                       requestHeaders);
        return new ResourceResponse<Permission>(this.doReplace(request), Permission.class);
    }

    /**
     * Deletes a permission.
     * 
     * @param permissionLink the permission link.
     * @param options the request options.
     * @return the resource response.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Permission> deletePermission(String permissionLink, RequestOptions options)
            throws DocumentClientException {
        
        if (StringUtils.isEmpty(permissionLink)) {
            throw new IllegalArgumentException("permissionLink");
        }
        
        String path = DocumentClient.joinPath(permissionLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Permission, path, requestHeaders);
        return new ResourceResponse<Permission>(this.doDelete(request), Permission.class);
    }

    /**
     * Reads a permission.
     * 
     * @param permissionLink the permission link.
     * @param options the request options.
     * @return the resource response with the read permission.
     * @throws DocumentClientException the document client exception.
     */
    public ResourceResponse<Permission> readPermission(String permissionLink, RequestOptions options)
            throws DocumentClientException {

        if (StringUtils.isEmpty(permissionLink)) {
            throw new IllegalArgumentException("permissionLink");
        }
        
        String path = DocumentClient.joinPath(permissionLink, null);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Permission, path, requestHeaders);
        return new ResourceResponse<Permission>(this.doRead(request), Permission.class);
    }

    /**
     * Reads a permission.
     * 
     * @param permissionLink the permission link.
     * @param options the feed options.
     * @return the feed response with the read permissions.
     */
    public FeedResponse<Permission> readPermissions(String permissionLink, FeedOptions options) {
        if (StringUtils.isEmpty(permissionLink)) {
            throw new IllegalArgumentException("permissionLink");
        }

        String path = DocumentClient.joinPath(permissionLink, Paths.PERMISSIONS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getFeedHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Permission, path, requestHeaders);
        return new FeedResponse<Permission>(new QueryIterable<Permission>(this,
                                                                          request,
                                                                          ReadType.Feed,
                                                                          Permission.class));
    }

    /**
     * Query for permissions.
     * 
     * @param permissionLink the permission link.
     * @param query the query.
     * @param options the feed options.
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
     * @param querySpec the SQL query specification.
     * @param options the feed options.
     * @return the feed response with the obtained permissions.
     */
    public FeedResponse<Permission> queryPermissions(String permissionLink,
                                                     SqlQuerySpec querySpec,
                                                     FeedOptions options) {

        if (StringUtils.isEmpty(permissionLink)) {
            throw new IllegalArgumentException("permissionLink");
        }

        if (querySpec == null) {
            throw new IllegalArgumentException("querySpec");
        }

        String path = DocumentClient.joinPath(permissionLink, Paths.PERMISSIONS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getFeedHeaders(options);
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.Permission,
                                                                       path,
                                                                       querySpec,
                                                                       this.queryCompatibilityMode,
                                                                       requestHeaders);
        return new FeedResponse<Permission>(new QueryIterable<Permission>(this,
                                                                          request,
                                                                          ReadType.Query,
                                                                          Permission.class));
    }

    /**
     * Gets database account information.
     * 
     * @return the database account.
     * @throws DocumentClientException the document client exception.
     */
    public DatabaseAccount getDatabaseAccount() throws DocumentClientException {
        DocumentServiceRequest request = DocumentServiceRequest.create(ResourceType.DatabaseAccount,
                                                                       "",  // path
                                                                       null);
        DocumentServiceResponse response = this.doRead(request);
        DatabaseAccount account = response.getResource(DatabaseAccount.class);

        // read the headers and set to the account
        Map<String, String> responseHeader = response.getResponseHeaders();

        account.setCapacityUnitsConsumed(Long.valueOf(responseHeader.get(
                HttpConstants.HttpHeaders.DATABASE_ACCOUNT_CAPACITY_UNITS_CONSUMED)));
        account.setCapacityUnitsProvisioned(Long.valueOf(responseHeader.get(
                HttpConstants.HttpHeaders.DATABASE_ACCOUNT_CAPACITY_UNITS_PROVISIONED)));
        account.setMaxMediaStorageUsageInMB(Long.valueOf(responseHeader.get(
                HttpConstants.HttpHeaders.MAX_MEDIA_STORAGE_USAGE_IN_MB)));
        account.setMediaStorageUsageInMB(Long.valueOf(responseHeader.get(
                HttpConstants.HttpHeaders.CURRENT_MEDIA_STORAGE_USAGE_IN_MB)));
        account.setConsumedDocumentStorageInMB(Long.valueOf(responseHeader.get(
                HttpConstants.HttpHeaders.DATABASE_ACCOUNT_CONSUMED_DOCUMENT_STORAGE_IN_MB)));
        account.setReservedDocumentStorageInMB(Long.valueOf(responseHeader.get(
                HttpConstants.HttpHeaders.DATABASE_ACCOUNT_RESERVED_DOCUMENT_STORAGE_IN_MB)));
        account.setProvisionedDocumentStorageInMB(Long.valueOf(responseHeader.get(
                HttpConstants.HttpHeaders.DATABASE_ACCOUNT_PROVISIONED_DOCUMENT_STORAGE_IN_MB)));

        return account;
    }
    
    private DocumentServiceResponse doCreate(DocumentServiceRequest request) throws DocumentClientException {
        this.ApplySessionToken(request);

        DocumentServiceResponse response = this.gatewayProxy.doCreate(request);
        this.CaptureSessionToken(request, response);
        return response;
    }
    
    private DocumentServiceResponse doReplace(DocumentServiceRequest request) throws DocumentClientException {
        this.ApplySessionToken(request);

        DocumentServiceResponse response = this.gatewayProxy.doReplace(request);
        this.CaptureSessionToken(request, response);
        return response;
    }
    
    private DocumentServiceResponse doDelete(DocumentServiceRequest request) throws DocumentClientException {
        this.ApplySessionToken(request);

        DocumentServiceResponse response = this.gatewayProxy.doDelete(request);

        if (request.getResourceType() != ResourceType.DocumentCollection) {
            this.CaptureSessionToken(request, response);
        } else {
            this.ClearToken(ResourceId.parse(request.getResourceId()));
        }
        return response;
    }
    
    private DocumentServiceResponse doRead(DocumentServiceRequest request) throws DocumentClientException {
        this.ApplySessionToken(request);

        DocumentServiceResponse response = this.gatewayProxy.doRead(request);
        this.CaptureSessionToken(request, response);
        return response;
    }
    
    DocumentServiceResponse doReadFeed(DocumentServiceRequest request) throws DocumentClientException {
        this.ApplySessionToken(request);

        DocumentServiceResponse response = this.gatewayProxy.doReadFeed(request);
        this.CaptureSessionToken(request, response);
        return response;
    }
    
    DocumentServiceResponse doQuery(DocumentServiceRequest request) throws DocumentClientException {
        this.ApplySessionToken(request);

        DocumentServiceResponse response = this.gatewayProxy.doSQLQuery(request);
        this.CaptureSessionToken(request, response);
        return response;
    }
    
    private void ApplySessionToken(DocumentServiceRequest request) throws DocumentClientException{
        Map<String, String> headers = request.getHeaders();
        if (headers != null && !StringUtils.isEmpty(headers.get(HttpConstants.HttpHeaders.SESSION_TOKEN))) {
            return;  // User is explicitly controlling the session.
        }

        if (this.desiredConsistencyLevel != ConsistencyLevel.Session) {
            return;  // Only apply the session token in case of session consistency
        }

        // Apply the ambient session.
        if (!StringUtils.isEmpty(request.getResourceId())) {
            String sessionToken = this.sessionContainer.resolveSessionToken(ResourceId.parse(request.getResourceId()));

            if (!StringUtils.isEmpty(sessionToken)) {
                headers.put(HttpConstants.HttpHeaders.SESSION_TOKEN, sessionToken);
            }
        }
    }

    private void CaptureSessionToken(DocumentServiceRequest request, DocumentServiceResponse response)
            throws DocumentClientException  {
        String sessionToken = response.getResponseHeaders().get(HttpConstants.HttpHeaders.SESSION_TOKEN);

        if (!StringUtils.isEmpty(sessionToken) && !StringUtils.isEmpty(request.getResourceId())) {
            this.sessionContainer.setSessionToken(ResourceId.parse(request.getResourceId()), sessionToken);
        }
    }

    private void ClearToken(ResourceId resourceId) {
        this.sessionContainer.clearToken(resourceId);
    }

    private Map<String, String> getRequestHeaders(RequestOptions options) {
        if (options == null) return null;

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

        return headers;
    }

    private Map<String, String> getFeedHeaders(FeedOptions options) {
        if (options == null) return null;

        Map<String, String> headers = new HashMap<String, String>();

        if (options.getPageSize() != null) {
            headers.put(HttpConstants.HttpHeaders.PAGE_SIZE, options.getPageSize().toString());
        }

        if (options.getRequestContinuation() != null) {
            headers.put(HttpConstants.HttpHeaders.CONTINUATION, options.getRequestContinuation());
        }

        if (options.getSessionToken() != null) {
            headers.put(HttpConstants.HttpHeaders.SESSION_TOKEN, options.getSessionToken());
        }

        if (options.getEnableScanInQuery() != null) {
            headers.put(HttpConstants.HttpHeaders.ENABLE_SCAN_IN_QUERY, options.getEnableScanInQuery().toString());
        }

        if (options.getEmitVerboseTracesInQuery() != null) {
            headers.put(HttpConstants.HttpHeaders.EMIT_VERBOSE_TRACES_IN_QUERY,
                        options.getEmitVerboseTracesInQuery().toString());
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

    private static String joinPath(String path1, String path2) {
        path1 = DocumentClient.trimBeginingAndEndingSlashes(path1);
        String result = "/" + path1 + "/";

        if (path2 != null && !path2.isEmpty()) {
            path2 = DocumentClient.trimBeginingAndEndingSlashes(path2);
            result += path2 + "/";
        }

        return result;
    }

    private static String trimBeginingAndEndingSlashes(String path) {
        if (path.startsWith("/")) {
            path = path.substring(1);
        }

        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }

        return path;
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
                    e.printStackTrace();
                    throw new IllegalArgumentException("Can't serialize the object into the json string", e);
                }
            }
        }

        return String.format("[%s]", StringUtils.join(stringArray, ","));
    }
}
