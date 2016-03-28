/* 
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 */

package com.microsoft.azure.documentdb;

/**
 * Constants.
 */
class Constants {

    static class Quota {
        // Quota Strings
        static final String DATABASE = "databases";
        static final String COLLECTION = "collections";
        static final String USER = "users";
        static final String PERMISSION = "permissions";
        static final String COLLECTION_SIZE = "collectionSize";
        static final String DOCUMENTS_SIZE = "documentsSize";
        static final String STORED_PROCEDURE = "storedProcedures";
        static final String TRIGGER = "triggers";
        static final String USER_DEFINED_FUNCTION = "functions";
        static final String DELIMITER_CHARS = "=|;";
    }

    static class Properties {
        static final String ID = "id";
        static final String R_ID = "_rid";
        static final String SELF_LINK = "_self";
        static final String LAST_MODIFIED = "_ts";
        static final String COUNT = "_count";
        static final String E_TAG = "_etag";

        static final String CONSISTENCY_POLICY = "consistencyPolicy";
        static final String DEFAULT_CONSISTENCY_LEVEL = "defaultConsistencyLevel";
        static final String MAX_STALENESS_PREFIX = "maxStalenessPrefix";
        static final String MAX_STALENESS_INTERVAL_IN_SECONDS = "maxIntervalInSeconds";
        
        static final String DATABASES_LINK = "_dbs";
        static final String COLLECTIONS_LINK = "_colls";
        static final String USERS_LINK = "_users";
        static final String PERMISSIONS_LINK = "_permissions";
        static final String ATTACHMENTS_LINK = "_attachments";
        static final String STORED_PROCEDURES_LINK = "_sprocs";
        static final String TRIGGERS_LINK = "_triggers";
        static final String USER_DEFINED_FUNCTIONS_LINK = "_udfs";
        static final String CONFLICTS_LINK = "_conflicts";
        static final String DOCUMENTS_LINK = "_docs";
        static final String RESOURCE_LINK = "resource";
        static final String MEDIA_LINK = "media";

        static final String PERMISSION_MODE = "permissionMode";
        static final String RESOURCE_KEY = "key";
        static final String TOKEN = "_token";
        
        //Scripting
        static final String BODY = "body";
        static final String TRIGGER_TYPE = "triggerType";
        static final String TRIGGER_OPERATION = "triggerOperation";

        static final String MAX_SIZE = "maxSize";
        static final String CURRENT_USAGE = "currentUsage";

        static final String CONTENT = "content";

        static final String CONTENT_TYPE = "contentType";

        //ErrorResource.
        static final String CODE = "code";
        static final String MESSAGE = "message";
        static final String ERROR_DETAILS = "errorDetails";
        
        //PartitionInfo.
        static final String RESOURCE_TYPE = "resourceType";
        static final String SERVICE_INDEX = "serviceIndex";
        static final String PARTITION_INDEX = "partitionIndex";

        static final String ADDRESS_LINK = "addresses";
        static final String USER_REPLICATION_POLICY = "userReplicationPolicy";
        static final String USER_CONSISTENCY_POLICY = "userConsistencyPolicy";
        static final String SYSTEM_REPLICATION_POLICY = "systemReplicationPolicy";
        static final String READ_POLICY = "readPolicy";
        
        //Indexing Policy.
        static final String INDEXING_POLICY = "indexingPolicy";            
        static final String AUTOMATIC = "automatic";
        static final String STRING_PRECISION = "StringPrecision";
        static final String NUMERIC_PRECISION = "NumericPrecision";
        static final String MAX_PATH_DEPTH = "maxPathDepth";
        static final String INDEXING_MODE = "indexingMode";
        static final String INDEX_TYPE = "IndexType";
        static final String INDEX_KIND = "kind";
        static final String DATA_TYPE = "dataType";
        static final String PRECISION = "precision";

        static final String PATHS = "paths";
        static final String PATH = "path";
        static final String INCLUDED_PATHS = "includedPaths";
        static final String EXCLUDED_PATHS = "excludedPaths";
        static final String INDEXES = "indexes";

        //Conflict.
        static final String CONFLICT = "conflict";
        static final String OPERATION_TYPE = "operationType";

        //Offer resource
        static final String OFFER_TYPE = "offerType";
        static final String OFFER_VERSION = "offerVersion";
        static final String OFFER_CONTENT = "content";
        static final String OFFER_THROUGHPUT = "offerThroughput";
        static final String OFFER_VERSION_V1 = "V1";
        static final String OFFER_VERSION_V2 = "V2";
        static final String OFFER_RESOURCE_ID = "offerResourceId";
        
        //PartitionKey
        static final String PARTITION_KEY = "partitionKey";
        static final String PARTITION_KEY_PATHS = "paths";
        static final String PARTITION_KIND = "kind";
        static final String RESOURCE_PARTITION_KEY = "resourcePartitionKey";
    }

    static class ResourceKeys {
        static final String ATTACHMENTS = "Attachments";
        static final String CONFLICTS = "Conflicts";
        static final String DATABASES = "Databases";
        static final String DOCUMENTS = "Documents";
        static final String DOCUMENT_COLLECTIONS = "DocumentCollections";
        static final String OFFERS = "Offers"; 
        static final String PERMISSIONS = "Permissions";
        static final String TRIGGERS = "Triggers";
        static final String STOREDPROCEDURES = "StoredProcedures";
        static final String USERS = "Users";
        static final String USER_DEFINED_FUNCTIONS = "UserDefinedFunctions";
    }
    
    static class StreamApi {
        static final int STREAM_LENGTH_EOF = -1;
    }
}
