#Microsoft Azure DocumentDB Java SDK

![](https://img.shields.io/github/release/azure/azure-documentdb-java.svg)
![](https://img.shields.io/maven-central/v/com.microsoft.azure/azure-documentdb.svg)
![](https://img.shields.io/github/issues/azure/azure-documentdb-java.svg)

This project provides a client library in Java that makes it easy to interact with Azure DocumentDB. For documentation please see the Microsoft Azure [Java Developer Center](http://azure.microsoft.com/en-us/develop/java/) and the [JavaDocs](http://dl.windowsazure.com/documentdb/javadoc).

##Download
###Option 1: Via Maven

To get the binaries of this library as distributed by Microsoft, ready for use within your project, you can use Maven.

    <dependency>
    	<groupId>com.microsoft.azure</groupId>
    	<artifactId>azure-documentdb</artifactId>
    	<version>0.9.0</version>
    </dependency>

###Option 2: Source Via Git

To get the source code of the SDK via git just type:

    git clone git://github.com/Azure/azure-documentdb-java.git

###Option 3: Source Zip

To download a copy of the source code, click "Download ZIP" on the right side of the page or click [here](https://github.com/Azure/azure-documentdb-java/archive/master.zip).


##Minimum Requirements
* Java Development Kit 7
* (Optional) Maven

### Dependencies
* Apache Commons Lang 3.3.2 (org.apache.commons / commons-lang3 / 3.3.2)
* Apache HttpClient 4.2.5 (org.apache.httpcomponents / httpclient / 4.2.5)
* Jackson Data Mapper 1.8 (org.codehaus.jackson / jackson-mapper-asl / 1.8.5)
* JSON 20140107 (org.json / json / 20140107)
* JUnit 4.11 (junit / junit / 4.11)

Dependencies will be added automatically if Maven is used. Otherwise, please download the jars and add them to your build path. 

##Usage

To use this SDK to call Azure DocumentDB, you need to first [create an account](http://azure.microsoft.com/en-us/documentation/articles/documentdb-create-account/).

You can follow this [tutorial](http://azure.microsoft.com/en-us/documentation/articles/documentdb-java-application/) to help you get started.

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
