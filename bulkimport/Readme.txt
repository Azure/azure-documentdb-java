Document Bulk Import Library can be used to bulk import large number of documents through DocumentDB API of Azure Cosmos DB Service. 

How to use:
add the following dependency to your maven pom file:
<dependency>
  <groupId>com.microsoft.azure</groupId>
  <artifactId>documentdb-bulkimport</artifactId>
  <version>1.0.2</version>
</dependency>


How to Build bulk import library from source:
 mvn clean package

############################################################
############################################################

The following numbers are measured when inserting 1,000,000 Documents each of size 1KB against a collection with 1000K throughput with 100 partitions. Bulk import tool ran in Gateway mode on an Ubuntu Linux with 16 CPU cores in azure portal.

Number of documents inserted in this checkpoint: 1,000,000
Import time for this checkpoint in milli seconds 9,106
Total request unit consumed in this checkpoint: 5,708,431.08
Average RUs/second in this checkpoint: 626,886
Average #Inserts/second in this checkpoint: 109,817

############################################################
############################################################

NOTE: for getting higher throughput:
1) Set JVM heap size to a large enough number to avoid any memory issue in handling large number of documents. 
   Suggested heap size: max(3GB, 3 * sizeof(all documents passed to bulk import in one batch)) 
2) there is a preprocessing and warm up time; due that you will get higher throughput for bulks with larger number of documents. So, if you want to import 10,000,000 documents, running bulk import 10 times on 10 bulk of documents each of size 1,000,000 is more preferable than running bulk import 100 times on 100 bulk of documents each of size 100,000 documents. 

an example for how to use the bulk import:
https://github.com/Azure/azure-documentdb-java/blob/master/bulkimport/src/test/java/com/microsoft/azure/documentdb/bulkimport/Sample.java

sample invocation of command-line tool for benchmarking bulk-import (doing 5 iterations of bulk import and in each iteration it inserts 1,000,000 documents) 
 
java -Xmx6G  -jar documentdb-bulkimport-1.0.2-jar-with-dependencies.jar -serviceEndpoint ACCOUNT_HOST -masterKey ACCOUNT_MASTER_KEY -databaseId DATABASE_NAME -collectionId COLLECTION_NAME -maxConnectionPoolSize 200 -numberOfDocumentsForEachCheckpoint 1000000 -numberOfCheckpoints 5

