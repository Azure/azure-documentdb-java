# Developing a Java app using Azure Cosmos DB
Azure Cosmos DB is a globally distributed multi-model database. One of the supported APIs is the DocumentDB API, which provides a JSON document model with SQL querying and JavaScript procedural logic. This sample shows you how to use the Azure Cosmos DB with the DocumentDB API to store and access data from a Java application.

## Running this sample

* Before you can run this sample, you must have the following prerequisites:

   * An active Azure account. If you don't have one, you can sign up for a [free account](https://azure.microsoft.com/free/). Alternatively, you can use the [Azure Cosmos DB Emulator](https://azure.microsoft.com/documentation/articles/documentdb-nosql-local-emulator) for this tutorial.
   * JDK 1.7+ (Run `apt-get install default-jdk` if you don't have JDK)
   * Maven (Run `apt-get install maven` if you don't have Maven)

* Next, substitute the endpoint and authorization key in Program.java with your Cosmos DB account's values. 

* If Maven dependencies are not added to your project, then you should add it manually in pom.xml file under properties tab using values maven.compiler.source : 1.8 and maven.compiler.target : 1.8.

* From a command prompt or shell, run `mvn package` to compile and resolve dependencies.

* From a command prompt or shell, run `mvn exec:java -D exec.mainClass=GetStarted.Program` to run the application.

## More information
- [Azure Cosmos DB](https://docs.microsoft.com/azure/cosmos-db/introduction)
- [Azure Cosmos DB : DocumentDB API](https://docs.microsoft.com/azure/documentdb/documentdb-introduction)
- [Azure DocumentDB Java SDK](https://docs.microsoft.com/azure/cosmos-db/documentdb-sdk-java)
- [Azure DocumentDB Java SDK Reference Documentation](http://azure.github.io/azure-documentdb-java/)

