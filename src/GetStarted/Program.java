package GetStarted;

import java.io.IOException;

import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.DataType;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.FeedResponse;
import com.microsoft.azure.documentdb.Index;
import com.microsoft.azure.documentdb.IndexingPolicy;
import com.microsoft.azure.documentdb.RangeIndex;
import com.microsoft.azure.documentdb.RequestOptions;

public class Program 
{

    private DocumentClient client;

    /**
     * 
     * @param args
     *            command line arguments
     * @throws DocumentClientException
     *             exception
     * @throws IOException 
     */
    public static void main(String[] args) 
    {

        try 
        {
            Program p = new Program();
            p.getStartedDemo();
            System.out.println(String.format("Demo complete, please hold while resources are deleted"));
        } 
        catch (Exception e) 
        {
            System.out.println(String.format("DocumentDB GetStarted failed with %s", e));
        }
    }

    private void getStartedDemo() throws DocumentClientException, IOException 
    {
    	//Replace MY_END_POINT and MY_AUTHORIZATION_KEY values with your AzureCosmosDB Document Account
        this.client = new DocumentClient("MY_END_POINT", "MY_AUTHORIZATION_KEY", new ConnectionPolicy(), ConsistencyLevel.Session);

        this.createDatabaseIfNotExists("ExampleDB");
        this.createDocumentCollectionIfNotExists("ExampleDB", "ExampleCollection");
        
        School InternationalSchool = getInternationalSchoolDocument();
        this.createExampleDocumentIfNotExists("ExampleDB", "ExampleCollection", InternationalSchool);

        School WorldWideSchool = getWorldWideSchoolDocument();
        this.createExampleDocumentIfNotExists("ExampleDB", "ExampleCollection", WorldWideSchool);

        this.executeSimpleQuery("ExampleDB", "ExampleCollection");
        
        this.replaceExampleDocument("ExampleDB", "ExampleCollection", "Hyderabad", WorldWideSchool);

        this.upsertExampleDocument("ExampleDB","ExampleCollection",getInternationalSchoolDocument1());
        
        //this.client.deleteDatabase("/dbs/ExampleDB", null);
        
    }
    
	private School getInternationalSchoolDocument1() {
    	School InternationalSchool1 = new School();
    	InternationalSchool1.setId("Bala Sai International School");
    	InternationalSchool1.setDistrict("Secunderabad");
    	return InternationalSchool1;
    }
    

    private School getInternationalSchoolDocument() {
        School InternationalSchool = new School();
        InternationalSchool.setId("JayaUsha International School");
        InternationalSchool.setLastName("JayaUsha");

        Teacher teacher1 = new Teacher();
        teacher1.setFirstName("Chebrolu");

        Teacher teacher2 = new Teacher();
        teacher2.setFirstName("Sunnapu");

        InternationalSchool.setTeachers(new Teacher[] { teacher1, teacher2 });

        Student student1 = new Student();
        student1.setFirstName("Harika");
        student1.setGender("female");
        student1.setGrade(5);

        Marks mark1 = new Marks();
        mark1.setGivenSubject("Fluffy");

        student1.setMarks(new Marks[] { mark1 });

        InternationalSchool.setDistrict("WA5");
        Address address = new Address();
        address.setCity("Seattle");
        address.setCounty("King");
        address.setState("WA");

        InternationalSchool.setAddress(address);
        InternationalSchool.setRegistered(true);

        return InternationalSchool;
    }

    private School getWorldWideSchoolDocument() {
        School WorldWideSchool = new School();
        WorldWideSchool.setId("Pandu Public School");
        WorldWideSchool.setLastName("Pandu");
        WorldWideSchool.setDistrict("Rangareddy");

        Teacher teacher1 = new Teacher();
        teacher1.setLastName("Yakkali");
        teacher1.setFirstName("Madhavi");
        

        Teacher teacher2 = new Teacher();
        teacher2.setLastName("Yakkali");
        teacher2.setFirstName("Raghava");

        WorldWideSchool.setTeachers(new Teacher[] { teacher1, teacher2 });

        Student student1 = new Student();
        student1.setFirstName("Bala");
        student1.setLastName("Cheb");
        student1.setGrade(8);

        Marks marks1 = new Marks();
        marks1.setGivenSubject("Java");

        Marks marks2 = new Marks();
        marks2.setGivenSubject("Shadow");

        student1.setMarks(new Marks[] { marks1, marks2 });

        Student student2 = new Student();
        student2.setFirstName("Lisa");
        student2.setLastName("Miller");
        student2.setGrade(1);
        student2.setGender("female");

        WorldWideSchool.setStudents(new Student[] { student1, student2 });

        Address address = new Address();
        address.setCity("NY");
        address.setCounty("Manhattan");
        address.setState("NY");

        WorldWideSchool.setAddress(address);
        WorldWideSchool.setDistrict("NY23");
        WorldWideSchool.setRegistered(true);
        return WorldWideSchool;
    }

    private void createDatabaseIfNotExists(String databaseName) throws DocumentClientException, IOException 
    {
        String databaseLink = String.format("/dbs/%s", databaseName);

        // Check to verify a database with the id=ExampleDB does not exist
        try 
        {
            this.client.readDatabase(databaseLink, null);
            this.writeToConsoleAndPromptToContinue(String.format("Found %s", databaseName));
        } 
        catch (DocumentClientException de) 
        {
            // If the database does not exist, create a new database
            if (de.getStatusCode() == 404) 
            {
                Database database = new Database();
                database.setId(databaseName);
                
                this.client.createDatabase(database, null);
                this.writeToConsoleAndPromptToContinue(String.format("Created %s", databaseName));
            } 
            else 
            {
                throw de;
            }
        }
    }

    private void createDocumentCollectionIfNotExists(String databaseName, String collectionName) throws IOException, DocumentClientException 
    {
        String databaseLink = String.format("/dbs/%s", databaseName);
        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);

        try 
        {
            this.client.readCollection(collectionLink, null);
            writeToConsoleAndPromptToContinue(String.format("Found %s", collectionName));
        } 
        catch (DocumentClientException de) 
        {
            // If the document collection does not exist, create a new collection
            if (de.getStatusCode() == 404) 
            {
                DocumentCollection collectionInfo = new DocumentCollection();
                collectionInfo.setId(collectionName);

                // Optionally, you can configure the indexing policy of a collection. 
                // Here we configure collections for maximum query flexibility including string range queries.
                
                RangeIndex index = new RangeIndex(DataType.String);
                index.setPrecision(-1);

                collectionInfo.setIndexingPolicy(new IndexingPolicy(new Index[] { index }));

                // DocumentDB collections can be reserved with throughput specified in request units/second.
                // 1 RU is a normalized request equivalent to the read of a 1KB document. 
                // Here we create a collection with 400 RU/s.
                
                RequestOptions requestOptions = new RequestOptions();
                requestOptions.setOfferThroughput(400);

                this.client.createCollection(databaseLink, collectionInfo, requestOptions);

                this.writeToConsoleAndPromptToContinue(String.format("Created %s", collectionName));
            } 
            else 
            {
                throw de;
            }
        }

    }

    private void createExampleDocumentIfNotExists(String databaseName, String collectionName, School school)
            throws DocumentClientException, IOException {
        try {
            String documentLink = String.format("/dbs/%s/colls/%s/docs/%s", databaseName, collectionName, school.getId());
            this.client.readDocument(documentLink, new RequestOptions());
        } catch (DocumentClientException de) {
            if (de.getStatusCode() == 404) {
                String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
                this.client.createDocument(collectionLink, school, new RequestOptions(), true);
                this.writeToConsoleAndPromptToContinue(String.format("Created School %s", school.getId()));
            } else {
                throw de;
            }
        }
    }
    
    private void createExampleDocumentIfNotExistsForUpsert(String databaseName, String collectionName, School school)
            throws DocumentClientException, IOException {
        try {
            String documentLink = String.format("/dbs/%s/colls/%s/docs/%s", databaseName, collectionName, school.getId());
            this.client.readDocument(documentLink, new RequestOptions());
        } catch (DocumentClientException de) {
            if (de.getStatusCode() == 404) {
                String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
                this.client.createDocument(collectionLink, school, new RequestOptions(), true);
                this.writeToConsoleAndPromptToContinue(String.format("Created School %s", school.getId()));
            } else {
            	replaceExampleDocument("ExampleDB", "ExampleCollection", "Hyderabad", school);
            }
        }
    }

    private void executeSimpleQuery(String databaseName, String collectionName) {
        // Set some common query options
        FeedOptions queryOptions = new FeedOptions();
        queryOptions.setPageSize(-1);
        queryOptions.setEnableCrossPartitionQuery(true);

        String collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
        FeedResponse<Document> queryResults = this.client.queryDocuments(collectionLink,
                "SELECT * FROM School WHERE School.lastName = 'Pandu'", queryOptions);

        System.out.println("Running SQL query...");
        for (Document school : queryResults.getQueryIterable()) {
            System.out.println(String.format("\tRead %s", school));
        }
    }

    @SuppressWarnings("unused")
    private void replaceExampleDocument(String databaseName, String collectionName, String District, School updatedSchool)
            throws IOException, DocumentClientException {
        try {
        	updatedSchool.setDistrict("Mavuri");
            this.client.replaceDocument(
                    String.format("/dbs/%s/colls/%s/docs/%s", databaseName, collectionName, updatedSchool.getId()), updatedSchool,
                    null);
            writeToConsoleAndPromptToContinue(String.format("Replaced School %s", updatedSchool.getId()));
            writeToConsoleAndPromptToContinue(String.format("Replaced %s", updatedSchool.getDistrict()));
        } catch (DocumentClientException de) {
            throw de;
        }
    }
    
    private void upsertExampleDocument(String databaseName, String collectionName, School upsertedSchool)
            throws IOException, DocumentClientException {
    	createExampleDocumentIfNotExistsForUpsert("ExampleDB", "ExampleCollection", upsertedSchool);
    	upsertedSchool.setDistrict("Vijayawada");
    	this.client.replaceDocument(
                String.format("/dbs/%s/colls/%s/docs/%s", databaseName, collectionName, upsertedSchool.getId()), upsertedSchool,
                null);
    	writeToConsoleAndPromptToContinue(String.format("Upserted School %s", upsertedSchool.getId()));
        writeToConsoleAndPromptToContinue(String.format("Upserted %s", upsertedSchool.getDistrict()));
    	}
    	
    


    @SuppressWarnings("unused")
    private void deleteExampleDocument(String databaseName, String collectionName, String documentName) throws IOException,
            DocumentClientException {
        try {
            this.client.deleteDocument(String.format("/dbs/%s/colls/%s/docs/%s", databaseName, collectionName, documentName), null);
            writeToConsoleAndPromptToContinue(String.format("Deleted School %s", documentName));
        } catch (DocumentClientException de) {
            throw de;
        }
    }

    private void writeToConsoleAndPromptToContinue(String text) throws IOException {
        System.out.println(text);
        System.out.println("Press any key to continue ...");
        System.in.read();
    }
}

