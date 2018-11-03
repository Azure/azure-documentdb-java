package com.microsoft.azure.documentdb.benchmark;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.LogFactory;
import org.apache.log4j.BasicConfigurator;
import org.codehaus.plexus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.fasterxml.uuid.EthernetAddress;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import com.microsoft.azure.documentdb.ConnectionMode;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.PartitionKey;
import com.microsoft.azure.documentdb.PartitionKeyRange;
import com.microsoft.azure.documentdb.RequestOptions;

import sun.rmi.runtime.Log;

/**
 * CosmosDB SimpleTests
 *
 * Continuous read/write workloads for validation of various scenarios
 *
 * Sample command line:
 *
 * Show usage menu:
 * java -cp simpletests-0.0.1-jar-with-dependencies.jar SimpleTests --help
 *
 * Read throughput:
 * java -cp simpletests-0.0.1-jar-with-dependencies.jar SimpleTests --endpoint https://ENDPOINT.documents.azure.com:443/ --key KEYVALUE --database testdb --collection testcol2 --partitionKey id --connectionPoolSize 12800 --docIdsFilePath khdangoath-testcol2-id4.txt  --operation read-throughput --threads 96 --operations 2000000 --logLatencyPath read-throughput-1852 --warmupRequestCount 2000 --logBatchEntryCount 250000
 *
 * Write throughput:
 * java -cp simpletests-0.0.1-jar-with-dependencies.jar SimpleTests --endpoint https://ENDPOINT.documents.azure.com:443/ --key KEYVALUE --database testdb --collection testcol2 --partitionKey id --connectionPoolSize 12800 --docIdsFilePath khdangoath-testcol2-id4.txt  --operation write-throughput --threads 256 --operations 250000  --logLatencyPath write-throughput-0949 --warmupRequestCount 2000 --logBatchEntryCount 250000
 */
public class SimpleTests {

    private final Logger logger = LoggerFactory.getLogger(SimpleTests.class);
    private final JCommander jCommander;

    private static TimeBasedGenerator TB_GENERATOR = Generators.timeBasedGenerator(EthernetAddress.fromInterface());
    private static long runId;

    private static SimpleDateFormat dateFormat;
    private static DocumentClient client;
    private static final MetricRegistry metricsRegistry = new MetricRegistry();
    private static final ScheduledReporter reporter = ConsoleReporter.forRegistry(metricsRegistry).convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS).build();
    private static Meter successMeter = metricsRegistry.meter("#Successful Operations");
    private static Meter failureMeter = metricsRegistry.meter("#Unsuccessful Operations");

    static {
        dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss.SSS zzz",Locale.US);
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    }

    @Parameter
    private List<String> parameters = new ArrayList<>();

    @Parameter(names = {"--endpoint", "-e"}, description = "CosmosDB endpoint")
    private String endpoint = "";

    @Parameter(names = {"--key", "-k"}, description = "CosmosDB key")
    private String key = "";

    @Parameter(names = {"--database", "-db"}, description = "CosmosDB database ID")
    private String dbName = "testdb";

    @Parameter(names = {"--collection", "-col"}, description = "CosmosDB collection ID")
    private String collectionName = "testcol";

    @Parameter(names = {"--operation", "-o"}, description = "Operation (read, write, write-read, read-throughput)")
    private String operation = "write-read";

    @Parameter(names = {"--partitionKey", "-pk"}, description = "CosmosDB partition key")
    private String pKey = "id";

    @Parameter(names = {"--preferredRegions", "-r"}, description = "CosmosDB preferred regions")
    private String preferredRegions = "West US";

    @Parameter(names = {"--docIdToRead", "-idToRead"}, description = "The Document ID to read")
    private String docId = "";

    @Parameter(names = {"--docIdsFilePath", "-idFile"}, description = "The file containing the Document IDs to read")
    private String docIdFilePath = "";

    @Parameter(names = {"--threads", "-t"}, description = "The number of threads to spawn")
    private int threadCount = 100;

    @Parameter(names = {"--connectionPoolSize", "-cps"}, description = "Connection pool size")
    private int connectionPoolSize = -1;

    @Parameter(names = "--operations", description = "Number of operations for each thread to run")
    private long totalOperations = Long.MAX_VALUE;

    @Parameter(names = "--consistency", description = "Set the consistency level for the client")
    private String consistencyLevel = "Session";

    @Parameter(names = "--connectionMode", description = "Set the connection mode for the client")
    private String connectionMode = "DirectHttps";

    @Parameter(names = "--requestTimeout", description = "Set the request timeout time in seconds for the client")
    private int requestTimeout = -1;

    @Parameter(names = "--idleConnectionTimeout", description = "Set the idle connection timeout time in seconds for the client")
    private int idleConnectionTimeout = -1;

    @Parameter(names = "--logLatencyPath", description = "Latency logging: Log latency from the requests to this path")
    private String logLatencyPath = "";

    @Parameter(names = "--logBatchEntryCount", description = "Latency logging: Number of requests to write in one log file")
    private int logBatchEntryCount = 250000;

    @Parameter(names = "--warmupRequestCount", description = "Latency logging: Number of requests which latency is not logged at the start of the run")
    private int warmupRequestCount = 1000;

    @Parameter(names = "--printLatency", description = "Latency logging: Set to true to print the latency to the console")
    private boolean printLatency = false;

    @Parameter(names = "--help", help = true, description = "Show this program usage")
    private boolean help = false;

    private enum OperationName {
        READ,
        READ_THROUGHPUT,
        WRITE,
        WRITE_THROUGHPUT,
        WRITE_READ,
        INVALID_OPERATION,
        READ_IDS
    }

    private OperationName operationName = OperationName.INVALID_OPERATION;

    private void validateOperation(String operation) {
        if ( operation.equalsIgnoreCase("read") ) {
            operationName = OperationName.READ;
        } else if ( operation.equalsIgnoreCase("write") ) {
            operationName = OperationName.WRITE;
        } else if ( operation.equalsIgnoreCase("write-read") ) {
            operationName = OperationName.WRITE_READ;
        } else if ( operation.equalsIgnoreCase("read-throughput") ) {
            operationName = OperationName.READ_THROUGHPUT;
        } else if ( operation.equalsIgnoreCase("write-throughput") ) {
            operationName = OperationName.WRITE_THROUGHPUT;
        } else if ( operation.equalsIgnoreCase("read-ids") ) {
            operationName = OperationName.READ_IDS;
        } else {
            operationName = OperationName.INVALID_OPERATION;
        }
    }

    public SimpleTests(String[] args) {
        jCommander = new JCommander(this, args);
        reporter.start(10, TimeUnit.SECONDS);
    }

    /**
     * This class is not thread-safe. It's for use within one thread.
     */
    static class LatencyLogger {
        StringBuilder sb = new StringBuilder(1024 * 1024);
        long start = 0;
        long end = 0;
        boolean enabled = false;
        String logFilePath;
        long warmupOperations;
        long operationCount;
        long batchId;
        long logBatchSize;
        boolean printLatency;

        static Logger logger = LoggerFactory.getLogger(LatencyLogger.class);

        public LatencyLogger(boolean logLatency,
                             String logFilePath,
                             long warmupOperations,
                             long logBatchSize,
                             boolean printLantecy) {
            this.enabled = logLatency;
            this.logFilePath = logFilePath;
            this.warmupOperations = warmupOperations;
            this.operationCount = 0;
            this.batchId = 0;
            this.logBatchSize = logBatchSize;
            this.printLatency = printLantecy;
        }

        public void requestStart() {
            start = System.nanoTime();
            if (!this.enabled || this.operationCount < warmupOperations) {
                return;
            }
        }

        public void requestEnd() {
            this.operationCount++;
            end = System.nanoTime();
            String logLine = null;
            if (printLatency) {
                logLine = String.format("%s,%d\n", dateFormat.format(new Date()), end - start);
                logger.debug(logLine);
            }
            if (!this.enabled || this.operationCount < warmupOperations) {
                return;
            }
            sb.append(logLine);
            if (operationCount >= logBatchSize) {
                operationCount = 0;
                writeToLogFile();
                sb.setLength(0);
            }
        }

        public void writeToLogFile() {
            if (!this.enabled || sb.length() == 0) {
               return;
            }
            try {
                FileWriter writer = new FileWriter(String.format("%s-batch%03d.log", logFilePath, batchId));
                batchId++;
                writer.write(sb.toString());
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void writeThroughput() throws InterruptedException {
        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        connectionPolicy.setConnectionMode(ConnectionMode.valueOf(connectionMode));
        if (connectionPoolSize > 0) {
            connectionPolicy.setMaxPoolSize(connectionPoolSize);
        }
        if (requestTimeout > 0) {
            connectionPolicy.setRequestTimeout(requestTimeout);
        }
        if (idleConnectionTimeout > 0) {
            connectionPolicy.setIdleConnectionTimeout(idleConnectionTimeout);
        }
        connectionPolicy.setEnableEndpointDiscovery(false);
        client = new DocumentClient(endpoint, key, connectionPolicy, ConsistencyLevel.valueOf(consistencyLevel));

        Thread[] threads = new Thread[threadCount];

        final String collectionLink = String.format("dbs/%s/colls/%s", dbName, collectionName);

        for (int i = 0; i < threadCount; ++i) {
            final int index = i;
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    LatencyLogger latencyLogger = new LatencyLogger(!StringUtils.isEmpty(logLatencyPath),
                            String.format("%s/%d-thread%03d", logLatencyPath, runId, index),
                            warmupRequestCount,
                            logBatchEntryCount,
                            printLatency);
                    long remainingOperations = totalOperations;

                    while (remainingOperations-- > 0) {
                        try {
                            Document newDoc = new Document();
                            String idString = TB_GENERATOR.generate().toString();
                            newDoc.setId(idString);
                            newDoc.set(pKey, idString);
                            latencyLogger.requestStart();
                            Document createdDoc = client.upsertDocument(collectionLink,
                                    newDoc, null, true).getResource();
                            latencyLogger.requestEnd();
                            successMeter.mark();
                        } catch (Exception e) {
                            failureMeter.mark();
                            logger.error(e.getMessage(), e);
                        }
                    }
                    latencyLogger.writeToLogFile();
                }
            });
            threads[i].start();
        }

        for (Thread t : threads) {
            if (t != null) {
                t.join();
            }
        }
    }

    private void readThroughput() throws InterruptedException {
        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        connectionPolicy.setConnectionMode(ConnectionMode.valueOf(connectionMode));
        if (connectionPoolSize > 0) {
            connectionPolicy.setMaxPoolSize(connectionPoolSize);
        }
        if (requestTimeout > 0) {
            connectionPolicy.setRequestTimeout(requestTimeout);
        }
        if (idleConnectionTimeout > 0) {
            connectionPolicy.setIdleConnectionTimeout(idleConnectionTimeout);
        }
        connectionPolicy.setEnableEndpointDiscovery(false);
        List<String> regions = Arrays.asList(preferredRegions.split(";"));
        connectionPolicy.setPreferredLocations(regions);
        client = new DocumentClient(endpoint, key, connectionPolicy, ConsistencyLevel.valueOf(consistencyLevel));

        List<String> documentIds = new ArrayList<>();

        if (!StringUtils.isEmpty(docIdFilePath)) {
            try {
                File file = new File(docIdFilePath);
                FileReader fileReader = new FileReader(file);
                BufferedReader bufferedReader = new BufferedReader(fileReader);
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    documentIds.add(line);
                }
                fileReader.close();
            } catch (Exception e) {
                logger.error("Failed to read document IDs list at {}", docIdFilePath, e);
            }
        } else if (!StringUtils.isEmpty(docId)) {
            documentIds.add(docId);
        }

        if (documentIds.size() == 0) {
            logger.error("Cannot continue with empty Document IDs list.");
            return;
        }

        Thread[] threads = new Thread[threadCount];

        for (int i = 0; i < threadCount; ++i) {
            final int index = i;
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    LatencyLogger latencyLogger = new LatencyLogger(!StringUtils.isEmpty(logLatencyPath),
                            String.format("%s/%d-thread%03d.log", logLatencyPath, runId, index),
                            warmupRequestCount,
                            logBatchEntryCount,
                            printLatency);
                    long remainingOperations = totalOperations;

                    while (remainingOperations-- > 0) {
                        try {
                            String docId = documentIds.get(index % documentIds.size());
                            RequestOptions options = new RequestOptions();
                            options.setPartitionKey(new PartitionKey(docId));
                            String documentLink = String.format("dbs/%s/colls/%s/docs/%s", dbName, collectionName, docId);
                            latencyLogger.requestStart();
                            Document readDocument = client.readDocument(documentLink, options).getResource();
                            latencyLogger.requestEnd();
                            successMeter.mark();
                        } catch (Exception e) {
                            logger.error(e.getMessage(), e);
                            failureMeter.mark();
                        }
                    }
                    latencyLogger.writeToLogFile();
                }
            });
            threads[i].start();
        }

        for (Thread t : threads) {
            if (t != null) {
                t.join();
            }
        }
    }

    private void readIds() throws IOException {
        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        connectionPolicy.setConnectionMode(ConnectionMode.valueOf(connectionMode));
        List<String> regions = Arrays.asList(preferredRegions.split(";"));
        connectionPolicy.setPreferredLocations(regions);
        client = new DocumentClient(endpoint, key, connectionPolicy, ConsistencyLevel.valueOf(consistencyLevel));

        final String collectionLink = String.format("dbs/%s/colls/%s", dbName, collectionName);

        List<PartitionKeyRange> partitionKeyRanges = client.readPartitionKeyRanges(collectionLink, (FeedOptions) null)
                .getQueryIterable().toList();

        List<String> ids = new ArrayList<>();

        for (PartitionKeyRange r : partitionKeyRanges) {
            FeedOptions feedOptions = new FeedOptions();
            feedOptions.setPartitionKeyRangeIdInternal(r.getId());
            feedOptions.setPageSize(1);
            try {
                List<Document> docs = client.readDocuments(collectionLink, feedOptions).getQueryIterable().fetchNextBlock();
                for (Document doc : docs) {
                    ids.add(doc.getId());
                }
            } catch (DocumentClientException e) {
                logger.error("Failed to read documents from partition {}", r.getId());
            }
        }

        if (StringUtils.isEmpty(docIdFilePath)) {
            logger.error("IDs file path is empty");
            return;
        }

        PrintWriter writer = new PrintWriter(new FileWriter(docIdFilePath));

        for (String idString : ids) {
            writer.println(idString);
        }

        writer.close();
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog");
        org.apache.log4j.Logger logger4j = org.apache.log4j.Logger.getRootLogger();
        logger4j.setLevel(org.apache.log4j.Level.toLevel("INFO"));

        BasicConfigurator.configure();

        SimpleTests test = new SimpleTests(args);

        if (test.help) {
            test.jCommander.usage();
            return;
        }

        test.validateOperation(test.operation);

        runId = System.nanoTime();

        switch ( test.operationName ) {
            case READ_THROUGHPUT:
                test.readThroughput();
                break;
            case WRITE_THROUGHPUT:
                test.writeThroughput();
                break;
            case READ_IDS:
                test.readIds();
                break;
            default:
                System.err.println("Operation name (-o) must be createTable, deleteTable, listTables, or loadTable");
                break;
        }

        reporter.report();
        reporter.close();
        if (client != null) {
            client.close();
        }
    }
}
