package com.microsoft.azure.documentdb;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.microsoft.azure.documentdb.directconnectivity.AddressCache;
import com.microsoft.azure.documentdb.directconnectivity.HttpClientFactory;
import com.microsoft.azure.documentdb.directconnectivity.TransportClient;
import com.microsoft.azure.documentdb.internal.routing.CollectionCache;
import com.microsoft.azure.documentdb.internal.routing.RoutingMapProvider;

/**
 * Base class for creating test cases that run against a production instance of DocumentDB.
 * <p>
 * IMPORTANT NOTES:
 * <p>
 * Most test cases in this project create collections in your DocumentDB account.
 * Collections are billing entities.  By running these test cases, you may incur monetary costs on your account.
 * <p>
 * To Run the test, replace the two member fields (MASTER_KEY & HOST) with values
 * associated with your DocumentDB account.
 */
public abstract class GatewayTestBase {
    // Replace MASTER_KEY and HOST with values from your DocumentDB account.
    //protected static final String MASTER_KEY = "[REPLACE WITH YOUR APP MASTER KEY]";
    //protected static final String HOST = "[REPLACE WITH YOUR APP ENDPOINT, FOR EXAMPLE https://myapp.documents.azure.com:443]";

    protected static final String MASTER_KEY = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
    protected static final String HOST = "https://localhost:443/";

    // protected static final String MASTER_KEY = "q0E1Ty3uNExchQow7l4XQlJr7GCdSEdiPCy2SFLWagPRsS9IdQIxkmFmHpPkITW1pRutVhwJjYcBddvdMhF19A==";
    // protected static final String HOST = "https://javatest.documents.azure.com:443/";

    protected static final String databaseForTestId = "GatewayTests_database0";
    protected static final String databaseForTestAlternativeId1 = "GatewayTests_database1";
    protected static final String databaseForTestAlternativeId2 = "GatewayTests_database2";
    @Rule
    public Retry retry = new Retry(3);
    protected Database databaseForTest;
    protected DocumentCollection collectionForTest;
    protected DocumentClient client;

    public GatewayTestBase() {
        // To run direct connectivity tests we need to disable host name verification because replicas host is not localhost
        HttpClientFactory.DISABLE_HOST_NAME_VERIFICATION = true;

        this.client = new DocumentClient(HOST, MASTER_KEY, new ConnectionPolicy(), ConsistencyLevel.Session);
    }

    public GatewayTestBase(DocumentClient client) {
        // To run direct connectivity tests we need to disable host name verification because replicas host is not localhost
        HttpClientFactory.DISABLE_HOST_NAME_VERIFICATION = true;

        this.client = client;
    }
    
    @Before
    public void setUp() throws DocumentClientException {
        // Clean up before setting up in case a previous running fails to tear
        // down.
        this.cleanUpGeneratedDatabases();

        // Create the database for test.
        Database databaseDefinition = new Database();
        databaseDefinition.setId(GatewayTestBase.databaseForTestId);
        this.databaseForTest = client.createDatabase(databaseDefinition, null).getResource();

        // Create the collection for test.
        DocumentCollection collectionDefinition = new DocumentCollection();
        collectionDefinition.setId(TestUtils.getUID());

        this.collectionForTest = client
                .createCollection(TestUtils.getDatabaseLink(this.databaseForTest, true), collectionDefinition, null)
                .getResource();
    }

    @After
    public void tearDown() throws DocumentClientException {
        this.cleanUpGeneratedDatabases();
    }
    
    protected DocumentClient createDocumentClient(String host, String masterKey, ConnectionPolicy policy,
            ConsistencyLevel consistency, AddressCache addressCache, TransportClient transportClient) {
        return new DocumentClient(host, masterKey, policy, consistency, addressCache, transportClient);
    }
    
    protected CollectionCache getCollectionCache(DocumentClient documentClient) {
        return documentClient.getCollectionCache();
    }
    
    protected RoutingMapProvider getPartitionKeyRangeCache(DocumentClient documentClient) {
        return documentClient.getPartitionKeyRangeCache();
    }

    protected void cleanUpGeneratedDatabases() throws DocumentClientException {
        // If one of databaseForTestId, databaseForTestAlternativeId1, or
        // databaseForTestAlternativeId2 already exists,
        // deletes them.
        String[] allDatabaseIds = {GatewayTestBase.databaseForTestId, GatewayTestBase.databaseForTestAlternativeId1,
                GatewayTestBase.databaseForTestAlternativeId2};

        for (String id : allDatabaseIds) {
            try {
                Database database = client
                        .queryDatabases(new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
                                new SqlParameterCollection(new SqlParameter("@id", id))), null)
                        .getQueryIterable().iterator().next();
                if (database != null) {
                    client.deleteDatabase(TestUtils.getDatabaseLink(database, true), null);
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
                                    || (cause != null
                                    && cause.getCause() instanceof javax.net.ssl.SSLPeerUnverifiedException)) {
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
}
