# Data Ingestion and Integration of Streaming Data

In this lab step, we will use the below architectural flow to extract the data from the Postgresql server.

****{architectural diag}****

Let's look into detail about the process followed:

Here, we are using **Postgres** as a source for our data - both batch and streaming data. 
To move the streaming data from Postrges to our ADLS account, we use **Debezium Connector** and **Kafka Connect** as our supporting components. 
Postgres will have **Change Data Capture(CDC)** configured to record any new changes in the database. To expose these changes to the world, we use the Debezium Connector.
The Debezium Connector has a right connectivity with the Kafka Connect, through which all the new records get captured into the Event Hub. 
Once the data is captured in the Event Hub, we create a **stream analytics job** in order to transfer the data to our ADLS account.

