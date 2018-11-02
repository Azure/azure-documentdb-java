steps:

```bash
git clone https://github.com/Azure/azure-documentdb-java.git
cd azure-documentdb-java/azure-documentdb-benchmark
mvn clean package
```

To run read operations and log throughput on console, latencies to file:

To create a file of document ids to read from:
```bash
java -jar target/azure-documentdb-benchmark-0.0.1-jar-with-dependencies.jar --endpoint <ENDPOINT> --key <KEY> --database <DB_NAME> --collection <COLLECTON_NAME> --partitionKey <PARTITION_KEY> --connectionPoolSize 12800 --docIdsFilePath <PATH_TO_TEXT_FILE> --operation read-ids
```

To read documents whose ids are present in above file:
```bash
java -jar target/azure-documentdb-benchmark-0.0.1-jar-with-dependencies.jar --endpoint <ENDPOINT> --key <KEY> --database <DB_NAME> --collection <COLLECTION_NAME> --partitionKey <PARTITION_KEY> --connectionPoolSize 12800 --docIdsFilePath <PATH_YO_TEXT_FILE_WITH_IDS> --operation read-throughput --threads 100 --operations 2000  --logLatencyPath <PATH_TO_FOLDER_TO_SAVE_LOGS> --warmupRequestCount 20 --logBatchEntryCount 1000 --printLatency true --connectionMode Gateway
```

To run write operations and log throughput to console, latencies to file:

```bash
java -jar target/azure-documentdb-benchmark-0.0.1-jar-with-dependencies.jar --endpoint <ENDPOINT> --key <KEY> --database <DB_NAME> --collection <COLLECTION_NAME> --partitionKey <PARTITION_KEY> --connectionPoolSize 12800 --docIdsFilePath <PATH_YO_TEXT_FILE_WITH_IDS> --operation write-throughput --threads 100 --operations 2000  --logLatencyPath <PATH_TO_FOLDER_TO_SAVE_LOGS> --warmupRequestCount 20 --logBatchEntryCount 1000 --printLatency true --connectionMode Gateway
```

You can provide ``--help`` to the tool to see the list of other options and their meanings. 

Note: If you want the document insert benchmark to reach its max throughput without throttling failures, you should provision a collection with a large throughput. Otherwise your inserts will be throttled from the server side.
