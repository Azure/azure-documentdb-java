#Microsoft Azure DocumentDB Java SDK

![](https://img.shields.io/github/release/azure/azure-documentdb-java.svg)
![](https://img.shields.io/maven-central/v/com.microsoft.azure/azure-documentdb.svg)
![](https://img.shields.io/github/issues/azure/azure-documentdb-java.svg)

This project provides a client library in Java that makes it easy to interact with Azure DocumentDB. For documentation please see the Microsoft Azure [Java Developer Center](http://azure.microsoft.com/en-us/develop/java/) and the [JavaDocs](http://dl.windowsazure.com/documentdb/javadoc).

##Disclaimer
The implementation in this project is intended for reference purpose only and does not reflect the latest official Azure DocumentDB Java SDK released on Maven repository.  

##Consuming the official Microsoft Azure DocumentDB Java SDK

To get the binaries of the latest official Microsoft Azure DocumentDB Java SDK as distributed by Microsoft, ready for use within your project, you can use Maven.

    <dependency>
    	<groupId>com.microsoft.azure</groupId>
    	<artifactId>azure-documentdb</artifactId>
    	<version>1.9.6</version>
    </dependency>

##Minimum Requirements
* Java Development Kit 7
* (Optional) Maven

### Dependencies
Dependencies will be added automatically if Maven is used. Otherwise, please download the dependencies from the pom.xml file and add them to your build path. 

##Usage

To use this SDK to call Azure DocumentDB, you need to first [create an account](http://azure.microsoft.com/en-us/documentation/articles/documentdb-create-account/).

You can follow this [tutorial](http://azure.microsoft.com/en-us/documentation/articles/documentdb-java-application/) to help you get started.

```java
import java.io.IOException;
import java.util.List;

import com.google.gson.Gson;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.RequestOptions;

public class HelloWorld {
    // Replace with your DocumentDB end point and master key.
    private static final String END_POINT = "[YOUR_ENDPOINT_HERE]";
    private static final String MASTER_KEY = "[YOUR_KEY_HERE]";
    
    // Define an id for your database and collection
    private static final String DATABASE_ID = "TestDB";
    private static final String COLLECTION_ID = "TestCollection";

    // We'll use Gson for POJO <=> JSON serialization for this sample.
    // Codehaus' Jackson is another great POJO <=> JSON serializer.
    private static Gson gson = new Gson();
    public static void main(String[] args) throws DocumentClientException,
            IOException {
        // Instantiate a DocumentClient w/ your DocumentDB Endpoint and AuthKey.
        DocumentClient documentClient = new DocumentClient(END_POINT,
                MASTER_KEY, ConnectionPolicy.GetDefault(),
                ConsistencyLevel.Session);

        // Start from a clean state (delete database in case it already exists).
        try {
            documentClient.deleteDatabase("dbs/" + DATABASE_ID, null);
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }

        // Define a new database using the id above.
        Database myDatabase = new Database();
        myDatabase.setId(DATABASE_ID);

        // Create a new database.
        myDatabase = documentClient.createDatabase(myDatabase, null)
                .getResource();


        System.out.println("Created a new database:");
        System.out.println(myDatabase.toString());
        System.out.println("Press any key to continue..");
        System.in.read();

        // Define a new collection using the id above.
        DocumentCollection myCollection = new DocumentCollection();
        myCollection.setId(COLLECTION_ID);

        // Set the provisioned throughput for this collection to be 1000 RUs.
        RequestOptions requestOptions = new RequestOptions();
        requestOptions.setOfferThroughput(1000);

        // Create a new collection.
        myCollection = documentClient.createCollection(
                "dbs/" + DATABASE_ID, myCollection, requestOptions)
                .getResource();

        System.out.println("Created a new collection:");
        System.out.println(myCollection.toString());
        System.out.println("Press any key to continue..");
        System.in.read();

        // Create an object, serialize it into JSON, and wrap it into a
        // document.
        SomePojo allenPojo = new SomePojo("123", "Allen Brewer", "allen [at] contoso.com");
        String allenJson = gson.toJson(allenPojo);
        Document allenDocument = new Document(allenJson);

        // Create the 1st document.
        allenDocument = documentClient.createDocument(
                "dbs/" + DATABASE_ID + "/colls/" + COLLECTION_ID, allenDocument, null, false)
                .getResource();

        System.out.println("Created 1st document:");
        System.out.println(allenDocument.toString());
        System.out.println("Press any key to continue..");
        System.in.read();

        // Create another object, serialize it into JSON, and wrap it into a
        // document.
        SomePojo lisaPojo = new SomePojo("456", "Lisa Andrews",
                "lisa [at] contoso.com");
        String somePojoJson = gson.toJson(lisaPojo);
        Document lisaDocument = new Document(somePojoJson);

        // Create the 2nd document.
        lisaDocument = documentClient.createDocument(
                "dbs/" + DATABASE_ID + "/colls/" + COLLECTION_ID, lisaDocument, null, false)
                .getResource();

        System.out.println("Created 2nd document:");
        System.out.println(lisaDocument.toString());
        System.out.println("Press any key to continue..");
        System.in.read();

        // Query documents
        List<Document> results = documentClient
                .queryDocuments(
                        "dbs/" + DATABASE_ID + "/colls/" + COLLECTION_ID,
                        "SELECT * FROM myCollection WHERE myCollection.email = 'allen [at] contoso.com'",
                        null).getQueryIterable().toList();

        System.out.println("Query document where e-mail address = 'allen [at] contoso.com':");
        System.out.println(results.toString());
        System.out.println("Press any key to continue..");
        System.in.read();

        // Replace Document Allen with Percy
        allenPojo = gson.fromJson(results.get(0).toString(), SomePojo.class);
        allenPojo.setName("Percy Bowman");
        allenPojo.setEmail("Percy Bowman [at] contoso.com");

        allenDocument = documentClient.replaceDocument(
                allenDocument.getSelfLink(),
                new Document(gson.toJson(allenPojo)), null)
                .getResource();

        System.out.println("Replaced Allen's document with Percy's contact information");
        System.out.println(allenDocument.toString());
        System.out.println("Press any key to continue..");
        System.in.read();

        // Delete Percy's Document
        documentClient.deleteDocument(allenDocument.getSelfLink(), null);

        System.out.println("Deleted Percy's document");
        System.out.println("Press any key to continue..");
        System.in.read();

        // Delete Database
        documentClient.deleteDatabase("dbs/" + DATABASE_ID, null);

        System.out.println("Deleted database");
        System.out.println("Press any key to continue..");
        System.in.read();

    }
}

```

The sample code above depends on a sample Plain Old Java Object (POJO) class, which is defined as follows:

```java
class SomePojo {
    private String id;
    private String name;
    private String email;

    public SomePojo(String id, String name, String email) {
          super();
          this.id = id;
          this.name = name;
          this.email = email;
    }

    public String getEmail() {
          return email;
    }

    public String getId() {
          return id;
    }

    public String getName() {
          return name;
    }

    public void setEmail(String email) {
          this.email = email;
    }

    public void setId(String id) {
          this.id = id;
    }

    public void setName(String name) {
          this.name = name;
    }
}
```

The following code Illustrates how to create a partitioned collection and use the partition key to access documents:

```java 
// Create a partition key definition that specifies the path to the property
// within a document that is used as the partition key.          
PartitionKeyDefinition partitionKeyDef = new PartitionKeyDefinition();
ArrayList<String> paths = new ArrayList<String>();
paths.add("/id");
partitionKeyDef.setPaths(paths);

// Create a collection with the partition key definition and set the offer throughput
// to 10100 RU per second.
DocumentCollection myPartitionedCollection = new DocumentCollection();
myPartitionedCollection.setId(COLLECTION_ID_PARTITIONED);
myPartitionedCollection.setPartitionKey(partitionKeyDef);
                      
RequestOptions options = new RequestOptions();
options.setOfferThroughput(10100);
myPartitionedCollection = documentClient.createCollection(
    myDatabase.getSelfLink(), myCollection, options).getResource();

// Insert a document into the created collection.
String document = "{ 'id': 'document1', 'description': 'this is a test document.' }";
Document newDocument = new Document(document);
newDocument = documentClient.createDocument(myPartitionedCollection.getSelfLink(),
        newDocument, null, false).getResource();
        
 // Read the created document, specifying the required partition key in RequestOptions.
options = new RequestOptions();
options.setPartitionKey(new PartitionKey("document1"));
newDocument = documentClient.readDocument(newDocument.getSelfLink(), options).getResource();
```

Additional samples are provided in the unit tests.

##Need Help?

Be sure to check out the Microsoft Azure [Developer Forums on MSDN](https://social.msdn.microsoft.com/forums/azure/en-US/home?forum=AzureDocumentDB) or the [Developer Forums on Stack Overflow](http://stackoverflow.com/questions/tagged/azure-documentdb) if you have trouble with the provided code.

##Contribute Code or Provide Feedback

If you would like to become an active contributor to this project please follow the instructions provided in [Azure Projects Contribution Guidelines](http://azure.github.io/guidelines.html).

If you encounter any bugs with the library please file an issue in the [Issues](https://github.com/Azure/azure-documentdb-java/issues) section of the project.

##Learn More

* [Azure Developer Center](http://azure.microsoft.com/en-us/develop/java/)
* [Azure DocumentDB Service](http://azure.microsoft.com/en-us/documentation/services/documentdb/)
* [Azure DocumentDB Team Blog](http://blogs.msdn.com/b/documentdb/)
* [JavaDocs](http://dl.windowsazure.com/documentdb/javadoc)
