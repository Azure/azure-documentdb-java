# Multi Region Strong Consistency Demo

This project is a demo exhibiting the use of the powerful CosmosDB feature - Multi region strong consistency.

## Setting up the environment

1. Create 2 CosmosDB database accounts such that
	* The producer account (producer-acc) writes to region 1 (say North Europe) and has region 2 (say East US) as a read region.
	* The consumer account (consumer-acc) writes to region 2 (East US) and has region 1 (North Europe) as a read region.
	* Set the consistency level of both databases to 'Strong' through the portal.

2. Spin up Virtual Machines in region 1 and region 2. Install the java run time environment in both VMs.

3. In case you are re-running the program, make sure to delete the databases ('producer' in producer-acc and 'consumer' in consumer-acc) created by the program. 

## Executing the program

1. Use the multiregion-strong-demo jar file to run the producer and consumer threads in both VMs.
	* If you have made custom changes, export the project as a runnable jar file with the name multiregion-strong-demo
	* Copy the jar file to both VMs.
	* Use the following commands to run the JAR. **Run the Consumer first**.
	
	Consumer VM :
	```bash
	java -jar multiregion-strong-demo.jar PRODUCER_ACC_ENDPOINT PRODUCER_ACC_KEY CONSUMER_ACC_ENDPOINT CONSUMER_ACC_KEY consumer "East US" 
	```
    since East US (Region 2) is the current location of the VM
	
	Producer VM :
	```bash
	java -jar multiregion-strong-demo.jar PRODUCER_ACC_ENDPOINT PRODUCER_ACC_KEY CONSUMER_ACC_ENDPOINT CONSUMER_ACC_KEY producer "North Europe" 
	```
	since North Europe (Region 1) is the current location of the VM	
	
2. No errors will pop up when using Strong consistency.

3. Set the consistency Level of both databases to Session or Eventual and run the program again. There will be many failures since the payload produced by the producer are not propagated across regions when the consumer reads their corresponding tokens.
