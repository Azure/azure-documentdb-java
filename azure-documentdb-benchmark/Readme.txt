
steps:
git clone https://github.com/Azure/azure-documentdb-java.git

cd azure-documentdb-java/azure-documentdb-benchmark
mvn clean package
 
Then you can run the tool from command line to insert documents to an existing database and collection with a predefined partition key path.
 
sample invocation:
java -jar target/azure-documentdb-benchmark-0.0.1-SNAPSHOT-jar-with-dependencies.jar -serviceEndpoint "YOUR-ACCOUNT-ENDPOINT" -masterKey "YOUR-MASTER-KEY" -databaseId mydb -collectionId mycol -numberOfDocumentsToInsert 400000 -maxConnectionPoolSize 1000 -partitionKey 'mypk'
 
Note: If you want the document insert benchmark to reach its max throughput without throttling failures, you should provision a collection with a large throughput. Otherwise your inserts will be throttled from the server side.
