package com.microsoft.azure.documentdb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.microsoft.azure.documentdb.Attachment;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.Permission;
import com.microsoft.azure.documentdb.SqlParameter;
import com.microsoft.azure.documentdb.SqlParameterCollection;
import com.microsoft.azure.documentdb.SqlQuerySpec;
import com.microsoft.azure.documentdb.StoredProcedure;
import com.microsoft.azure.documentdb.Trigger;
import com.microsoft.azure.documentdb.User;
import com.microsoft.azure.documentdb.UserDefinedFunction;

/**
 * 
 * Base class for creating test cases that run against a production instance of DocumentDB.
 * 
 * IMPORTANT NOTES: 
 * 
 *  Most test cases in this project create collections in your DocumentDB account.
 *  Collections are billing entities.  By running these test cases, you may incur monetary costs on your account.
 * 
 *  To Run the test, replace the two member fields (MASTER_KEY & HOST) with values 
 *  associated with your DocumentDB account.
 */
public class GatewayTestBase {
    // Replace MASTER_KEY and HOST with values from your DocumentDB account.
    protected static final String MASTER_KEY = "[REPLACE WITH YOUR APP MASTER KEY]";
    protected static final String HOST = "[REPLACE WITH YOUR APP ENDPOINT, FOR EXAMPLE https://myapp.documents.azure.com:443]";

    protected static final String DATABASES_PATH_SEGMENT = "dbs";
    protected static final String USERS_PATH_SEGMENT = "users";
    protected static final String PERMISSIONS_PATH_SEGMENT = "permissions";
    protected static final String COLLECTIONS_PATH_SEGMENT = "colls";
    protected static final String DOCUMENTS_PATH_SEGMENT = "docs";
    protected static final String ATTACHMENTS_PATH_SEGMENT = "attachments";
    protected static final String STORED_PROCEDURES_PATH_SEGMENT = "sprocs";
    protected static final String TRIGGERS_PATH_SEGMENT = "triggers";
    protected static final String USER_DEFINED_FUNCTIONS_PATH_SEGMENT = "udfs";
    protected static final String CONFLICTS_PATH_SEGMENT = "conflicts";

    protected static final String databaseForTestId = "GatewayTests_database0";
    protected static final String databaseForTestAlternativeId1 = "GatewayTests_database1";
    protected static final String databaseForTestAlternativeId2 = "GatewayTests_database2";

    protected Database databaseForTest;
    protected DocumentCollection collectionForTest;
    protected DocumentClient client;

    public class Retry implements TestRule {
        private int retryCount;

        public Retry(int retryCount) {
            this.retryCount = retryCount;
        }

        public Statement apply(Statement base, Description description) {
            return statement(base, description);
        }

        private Statement statement(final Statement base, final Description description) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    Throwable caughtThrowable = null;

                    // implement retry logic here
                    for (int i = 0; i < retryCount; i++) {
                        try {
                            base.evaluate();
                            return;
                        } catch (java.lang.IllegalStateException exception) {
                            Throwable cause = exception.getCause();
                            if (cause instanceof javax.net.ssl.SSLPeerUnverifiedException
                                    || cause.getCause() instanceof javax.net.ssl.SSLPeerUnverifiedException) {
                                // We will retry on SSLPeerUnverifiedException.
                                // This is an inconsistent and random
                                // failure that only occasionally happen in
                                // non-production environment.
                                // TODO: remove this hack after figuring out the
                                // reason of this failure.
                                caughtThrowable = exception;
                                System.err.println(description.getDisplayName() + ": run " + (i + 1) + " failed");
                            } else {
                                throw exception;
                            }
                        } catch (Throwable t) {
                            // Everything else is fatal.
                            throw t;
                        }
                    }
                    System.err.println(description.getDisplayName() + ": giving up after " + retryCount + " failures");
                    throw caughtThrowable;
                }
            };
        }
    }

    public class ReadableStream extends InputStream {

        byte[] bytes;
        int index;

        ReadableStream(String content) {
            this.bytes = content.getBytes();
            this.index = 0;
        }

        @Override
        public int read() throws IOException {
            if (this.index < this.bytes.length) {
                return this.bytes[this.index++];
            }
            return -1;
        }
    }
    
    @Rule
    public Retry retry = new Retry(3);

    @Before
    public void setUp() throws DocumentClientException {
        this.client = new DocumentClient(HOST, MASTER_KEY, new ConnectionPolicy(), ConsistencyLevel.Session);

        // Clean up before setting up in case a previous running fails to tear
        // down.
        String[] allDatabaseIds = { GatewayTestBase.databaseForTestId, GatewayTestBase.databaseForTestAlternativeId1,
                GatewayTestBase.databaseForTestAlternativeId2 };
        this.cleanUpGeneratedDatabases(allDatabaseIds);

        // Create the database for test.
        Database databaseDefinition = new Database();
        databaseDefinition.setId(GatewayTestBase.databaseForTestId);
        this.databaseForTest = client.createDatabase(databaseDefinition, null).getResource();
        
        // Create the collection for test.
        DocumentCollection collectionDefinition = new DocumentCollection();
        collectionDefinition.setId(GatewayTestBase.getUID());

        this.collectionForTest = client
                .createCollection(getDatabaseLink(this.databaseForTest, true), collectionDefinition, null)
                .getResource();
    }

    @After
    public void tearDown() throws DocumentClientException {
        String[] allDatabaseIds = { GatewayTestBase.databaseForTestId, GatewayTestBase.databaseForTestAlternativeId1,
                GatewayTestBase.databaseForTestAlternativeId2 };
        
        this.cleanUpGeneratedDatabases(allDatabaseIds);
    }

    protected void cleanUpGeneratedDatabases(String[] databaseIds) throws DocumentClientException {
        for (String id : databaseIds) {
            try {
                Database database = client
                        .queryDatabases(new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
                                new SqlParameterCollection(new SqlParameter("@id", id))), null)
                        .getQueryIterable().iterator().next();
                if (database != null) {
                    client.deleteDatabase(getDatabaseLink(database, true), null);
                }
            } catch (IllegalStateException illegalStateException) {
                Throwable causeException = illegalStateException.getCause();

                // The above code for querying databases has started throwing
                // IOExceptions and causing the Java tests to fail once in a
                // while
                // Capturing the inner stack trace here so that we have this
                // info when it fails next time and analyze it
                // If that is a retryable error, we will add retries here but
                // need to find out the cause first

                if (causeException instanceof IOException) {
                    System.err.println("Detailed message for the exception thrown : " + causeException.toString());
                }
                
                throw illegalStateException;
            }
        }
    }

    protected static String getStringFromInputStream(InputStream is) {
        BufferedReader br = null;
        StringBuilder sb = new StringBuilder();

        String line;
        try {
            br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return sb.toString();
    }

    protected static String getUID() {
        UUID u = UUID.randomUUID();
        return ("" + u.getMostSignificantBits()) + Math.abs(u.getLeastSignificantBits());
    }

    protected static String getDatabaseLink(Database database, boolean isNameBased) {
        if (isNameBased) {
            return DATABASES_PATH_SEGMENT + "/" + database.getId();
        } else {
            return database.getSelfLink();
        }
    }

    protected static String getUserLink(Database database, User user, boolean isNameBased) {
        if (isNameBased) {
            return getDatabaseLink(database, true) + "/" + USERS_PATH_SEGMENT + "/" + user.getId();
        } else {
            return user.getSelfLink();
        }
    }

    protected static String getPermissionLink(Database database, User user, Permission permission,
            boolean isNameBased) {
        if (isNameBased) {
            return getUserLink(database, user, true) + "/" + PERMISSIONS_PATH_SEGMENT + "/" + permission.getId();
        } else {
            return permission.getSelfLink();
        }
    }

    protected static String getDocumentCollectionLink(Database database, DocumentCollection coll, boolean isNameBased) {
        if (isNameBased) {
            return getDatabaseLink(database, true) + "/" + COLLECTIONS_PATH_SEGMENT + "/" + coll.getId();
        } else {
            return coll.getSelfLink();
        }
    }

    protected static String getDocumentLink(Database database, DocumentCollection coll, Document doc,
            boolean isNameBased) {
        if (isNameBased) {
            return getDocumentCollectionLink(database, coll, true) + "/" + DOCUMENTS_PATH_SEGMENT + "/" + doc.getId();
        } else {
            return doc.getSelfLink();
        }
    }

    protected static String getAttachmentLink(Database database, DocumentCollection coll, Document doc,
            Attachment attachment, boolean isNameBased) {
        if (isNameBased) {
            return getDocumentLink(database, coll, doc, true) + "/" + ATTACHMENTS_PATH_SEGMENT + "/"
                    + attachment.getId();
        } else {
            return attachment.getSelfLink();
        }
    }

    protected static String getTriggerLink(Database database, DocumentCollection coll, Trigger trigger,
            boolean isNameBased) {
        if (isNameBased) {
            return getDocumentCollectionLink(database, coll, true) + "/" + TRIGGERS_PATH_SEGMENT + "/"
                    + trigger.getId();
        } else {
            return trigger.getSelfLink();
        }
    }

    protected static String getStoredProcedureLink(Database database, DocumentCollection coll,
            StoredProcedure storedProcedure, boolean isNameBased) {
        if (isNameBased) {
            return getDocumentCollectionLink(database, coll, true) + "/" + STORED_PROCEDURES_PATH_SEGMENT + "/"
                    + storedProcedure.getId();
        } else {
            return storedProcedure.getSelfLink();
        }
    }

    protected static String getUserDefinedFunctionLink(Database database, DocumentCollection coll,
            UserDefinedFunction userDefinedFunction, boolean isNameBased) {
        if (isNameBased) {
            return getDocumentCollectionLink(database, coll, true) + "/" + USER_DEFINED_FUNCTIONS_PATH_SEGMENT + "/"
                    + userDefinedFunction.getId();
        } else {
            return userDefinedFunction.getSelfLink();
        }
    }
}
