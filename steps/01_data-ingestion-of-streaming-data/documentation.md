# Data Ingestion and Integration (construct raw/bronze layer)

The objective of this lab is to connect to on-premise data source (postgre SQL) containing both batch and streaming data. Extract data onto staging layer (ADLS gen2 storage) and Delta tables(Raw/Bronze layer) via DLT(Delta Live tables) and autoloader. 

We will perform cleansing, quality checking from the data coming from (raw/bronze layer) into other delta table (silver layer) via DLT expectations and transform data to prepare data model with aggregates using synapse dataflows and insert into tables (gold layer) in synapse dedicated SQL DW which will be used for reporting.

Finally, we will query the cleansed data (silver layer) using synapse serverless pool and visualize aggregated data through PowerBI.

Some of the resources like Azure Databricks workspace, compute cluster, Blob storage container, Event hub, Azure Synapse Analytics workspace, etc., will be automated using the ARM template deployment.

Below is the overall architectural flow of the lab. 

![overallDiag](./assets/overall_diag.jpg "Overall diag")

# Data Ingestion of Streaming Data

In this lab step, we will use the below architectural flow to extract the data from the Postgresql server.

![diag](./assets/diag.png "diag")

  Let's look into detail about the process followed:  
  Here, we are using **Postgres** as a source for our data - both batch and streaming data.   
To move the streaming data from Postrges to our ADLS account, we use **Debezium Connector** and **Kafka Connect** as our supporting components. 
Postgres will have **Change Data Capture(CDC)** configured to record any new changes in the database. To expose these changes to the world, we use the Debezium Connector.
The Debezium Connector has a right connectivity with the Kafka Connect, through which all the new records get captured into the Event Hub. 
Once the data is captured in the Event Hub, we create a **stream analytics job** in order to transfer the data to our ADLS account.

### Objectives:
* Connecting to on-premises data source
* Extracting data onto staging layer (ADLS Gen2 Storage)
* Constructing raw/bronze layer

### Pre-requisites:
* An *Azure account* with an active subscription
* Ensure that the following resources are in the Resource group.
    -	Linux Virtual Machine **[linuxvm{randomString}]**,
    -	Windows Virtual Machine **[winvm{randomString}]**,
    -	Eventhub Namespace **[streamdata-{randomString}-ns]**,
    -	ADLS Gen2 Storage **[adls{randomString}]**,
    -	Databricks Workspace with cluster **[dblab-{randomString}], [dbcluster]**
* Users should have *Storage Blob Data Contributor* role for the storage account.

## Exercise 1: Connecting to on-premises data source.
1. Open the Azure Portal. Go to the resource group created.
    
3. Open the **Linux Virtual Machine** which goes by *linuxvm{randomString}*.
    
    ![linuxVm](./assets/1-linux_vm.png "linux vm")
    
5. Make a note of the **DNS Name** which is available on the Overview page, which will be later used in this exercise.
    
7. Select **Connect** at the top on the Overview page.
    
9. Choose **Bastion** from the options listed.
    
    ![vmWindow](./assets/2-vm_window.png "windows vm")
    
11. Click on **Create Azure Bastion using defaults**.
    
    > Note that the creation of Bastion Service will take at least 10 minutes. 
    
13.   Once the Bastion gets updated, Enter **Username**, **Password** and **Connect**.  
      **Username**: `LinuxAdminUser`  
      **Password**: `de22c4!DE22C4@de22c4`  
    
      ![bastionConnect](./assets/4-bastion_connect.png "bastion connect")
    
9. A new Shell window will open. We will type our commands here. First, acquire the ownership of the Kafka connect config file using the following      command.
    
    ```text
    sudo chown $USER /usr/lib/kafka_2.12-3.3.1/config/connect-distributed.properties
    ```
    If prompted for **password**, give `de22c4!DE22C4@de22c4`
    
    ![sudoChown](./assets/6-sudo_chown.png "sudo chown")
    
10. Go back to the Azure Portal. Open the **Windows Virtual Machine** which goes by the name, *winvm{randomString}*.
    
    ![windowsVmScreen](./assets/7-windows_vm_screen.png "windows vm screen")
    
12. Select **Connect**, choose **RDP** from the options in the Overview page.
    
    ![windowsBastion](./assets/2-RDPconnect.jpg "windows bastion")
    
14.  Download the RDP file. Once downloaded, open it.  
    
      ![windowsVmLogin](./assets/3-RDPdownload.jpg "windowsVm login")
      
13. You will see a Remote Desktop pop-up. Click on **Connect**.
    
    ![RDPDownload](./assets/4-connect.jpg "visual studio download")
    
14.   Enter the following Windows VM Credentials by choosing for *More Choices* -> *Use a different Account*  
      **Username**: `windowsVmAdminUser`  
      **Password**: `de22c4!DE22C4@de22c4`
      
      ![credentials](./assets/6a-credentials.jpg "credentials")
      
      ![certificate](./assets/7a_certificate.jpg "certificate")
      
16. In the Windows VM, go to **Microsoft Edge** and download VS Code. The link to download is `https://code.visualstudio.com/Download`
    Click on **Windows** to download. Once downloaded, open the file.
    
    ![VSCodeDownload](./assets/40-vscode.jpg "visual studio download")
    
15. Look for **Extentions** icon in left pane and download the *extension*, **Remote-SSH**.
    
    ![remote-sshInstall](./assets/11-remote-ssh_install.png "remote-ssh install")
    
17. Click on the green-coloured *Remote-SSH icon* on the bottom left and choose **Connect to Host**.
    
    ![connectToHost](./assets/12-connectToHost.png "connect to hst")
    
19. Add the *linux ssh host* and enter.
    
    ```text
    linuxAdminUser@{DNS-Name-of-linux-VM}
    ```
    
    ![sshLogin](./assets/41-linuxuser.jpg "ssh login")
    
17. If prompted for *platforms*, choose **Linux**. If prompted for *“Are you sure you want to continue?”*, choose **yes**. 
    Provide the *password*, `de22c4!DE22C4@de22c4`
    
19.	If the connection is successful, the SSH Host name will be displayed at the bottom. Click **Open File**.
    
    ![connectionSuccessful](./assets/14-connection_successful.png "connection successful")
    
21.	Give the following path:
    
    ```text
    /usr/lib/kafka_2.12-3.3.1/config/connect-distributed.properties
    ```
    
    ![path](./assets/15-path.png "path")
    
20.	In order to edit the properties file, go to the Azure Portal. Go to the *Event Hub Namespace* resource -> Shared Access Policies -> click on
    RootManageSharedAccessKey -> copy the **Connection string-primary key**.
    
    ![eventhubCopy](./assets/16-eventhub_copy.png "eventhub copy")
    
22.	Now, go back to the properties file in the **Windows Bastion Shell**, edit line 1 for *eventhub namespace name* and lines 28, 32, 36 for *connection string*. 
    And **Save** the file.
    
    ![eventhubName](./assets/17-eventhub_name.png "eventhub name")
    
    ![connectionString](./assets/18-connection_string.png "connection string")
    
24.	Go to the **Linux Bastion Shell**, run the *Kafka Connect*.
    
    ```text
    /usr/lib/kafka_2.12-3.3.1/bin/connect-distributed.sh /usr/lib/kafka_2.12-3.3.1/config/connect-distributed.properties
    ```
    
23.	Open another **Linux Bastion shell**. Run the command for creating *Debezium Postgres SQL connection*.
    
    ```text
    curl -X POST -H "Content-Type: application/json" --data @/usr/lib/kafka_2.12-3.3.1/config/pg-source-connector.json http://localhost:8083/connectors
    ```
    
24.	To check the status of the connection.
    
    ```text
    curl -s http://localhost:8083/connectors/retail-connector/status
    ```
    ![curlCommands](./assets/19-curl_commands.png "curl commands")
    
25.	Once it is set, you can go to your **first Bastion shell** and see the Kafka connect sending streams to the event hub.
    
    ![dataExported](./assets/20-data_exported.png "data exported")
    
27. Similarly, we can see the events being processed to the event hubs within the eventhub namespace. 
    **streamdata-{randomString}-ns -> Event Hubs**
    
    ![eventhubEvents](./assets/21-eventhub_events.png "eventhub events")
    
## Exercise 2: Extracting data onto staging layer (ADLS Gen2 Storage)

In order to extract the streaming data to the ADLS Gen2 storage account, let’s create a stream analytics job for any one of the event hubs, which captures the live data from postgres and getting stored in a ADLS Gen2 container.

1. Go to **streamdata-{randomString}-ns** -> **Event Hubs**(under Entities section).
    
3. Open the **retail.public.sales_orders** event hub.
    
    ![salesOrder](./assets/22-sales_order.png "sales order")
    
5. Click on **Process Data** feature.
    
    ![processData](./assets/23-process_data.png "process data")
    
7. Choose **“Start with a blank canvas”** scenario.
    
    ![processDataScenario](./assets/24-pocess_data-scenario.png "process data scenario")
    
9. Give a name for the job, `stream_salesorders`. And click **Create**.
    
    ![jobName](./assets/25-job_name.png "job name")
    
11. You will be landed on to the canvas which has the Event Hub input.
    
13. In the Event Hub configurations pane, Click **Connect** with the default configurations populated.
    
    ![job](./assets/26-job.png "job")
    
15. Once the connection is successful, you can preview your data from the event hub by expanding the below pane.
    
    ![eventhubDataPreview](./assets/27-eventhub_data_preview.png "eventhub data preview")
    
13.	Choose **ADLS Gen2** from **Outputs**. Connect the *Event Hub* input to the *ADLS Gen2* output.
    
    ![jobAdlsConnect](./assets/1a-job_adls_connect.jpg "Job ADLS Connect")
    
15.	Edit the ADLS Gen2 configurations, choose the _subscription, storage account name **[adls{randomString}]**, container **data**, choose the serialization as **json** and **Connect**_.
    
    ![adlsDetails](./assets/2a-adls_details.jpg "ADLS Details")

    ![adlsConnect](./assets/3a-adls_connect.jpg "ADLS Connect")
    
17.	Once the connection is successful, you can preview the sample data in the bottom pane.
    
19.	**Save** and **start** the job by choosing the *“Output start time”* as **Now**.
    
    ![saveStart](./assets/4a-save_start.jpg "save start")
    
    ![startjob](./assets/36-startjob.png "startjob")
    
21.	You can view the job status in Process Data feature -> *Stream Analytics jobs* tab.
    
    ![jobRunning](./assets/37-job_running.png "job running")
    
23.	Now the job is running, we can insert the data in the postgres sql server and witness the stream events getting stored in adls gen2.
    
25.	Open a **linux Bastion shell**. Login into the postgres psql shell.
    
    ```text
    sudo -i -u postgres psql
    ```
    Provide **Password**: `postgrespw`

19.	Go to the database where we have to insert the query.
    
    ```text
    \c retail_org
    ```
    
20.	Insert the below query.
    
    ```sql
    INSERT INTO public.sales_orders (order_number,customer_id,order_datetime,customer_name,clicked_items,number_of_line_items,ordered_products, promo_info) VALUES (317568021,19476261,'1564694921','otbda outside the box digital agency2','[["AVpfPEx61cnluZ0-gyT9", "34"], ["AVpfuJ4pilAPnD_xhDyM", "98"], ["AVpe6jFBilAPnD_xQxO2", "60"], ["AVpfIODe1cnluZ0-eg35", "49"]]',4, '[{"curr": "USD", "id": "AVpfuJ4pilAPnD_xhDyM", "name": "Rony LBT-GPX555 Mini-System with Bluetooth and NFC", "price": 993, "promotion_info": null, "qty": 3, "unit": "pcs"}]','[{"promo_disc": 0.03, "promo_id": 0, "promo_item": "AVpfMVD-ilAPnD_xW6bu", "promo_qty": 2}]');
    ```
    
    ![insertData](./assets/38-insert_data.png "insert data")
    
21.	Once the query is inserted, we can check it out the processed data in our **ADLS Gen2 account** in a minute or two.
    
23.	Go to the Azure Portal. 
    **adls{randomString}** -> **sales-orders-streams** container -> **[date]** folder -> **{randomString}.parquet** file
    
    ![parquetFile](./assets/39-parquetfile1.png "parquet file")
    
**Step-01 : Streaming data is successfully proccessed into the ADLS Gen 2 account**
