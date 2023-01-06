# Data Ingestion of Batch Data

In this lab step, we will use the below architectural flow to extract the data from the Postgresql server.



The datasets which are available in the PostgreSQL server inside the VM is visualized using pgAdmin 4 tool. Using the Self Hosted Integration Runtime and Synapse Pipeline Activity we will ingest the data from the postgres into the adls gen 2 account.

### Objectives:
* Windows Virtual Machine Setup
* Developing Synapse Pipeline to extract batch data

### Pre-requisites:
* An *Azure account* with an active subscription
* Ensure that the following resources are in the Resource group.
    -	Windows Virtual Machine **[azcslabsph2win1]**,
    -	ADLS Gen2 Storage **[adls{randomString}]**,
    -	Azure Synapse Analytics **[dblab-{randomString}-synapse]**
* Users should have *Storage Blob Data Contributor* role for the storage account.

## Exercise 1: Windows Virtual Machine Setup
Let’s begin with setting up the Windows Virtual Machine for the Data Ingestion Process.

We will download the following pre-requisites in the Windows Virtual Machine in order to process the batch data from PostgreSql.
* **pgAdmin 4** – It is the open-source management tool for managing PostgreSQL databases.
* **Open JDK** – Since the files were parsed and written in parquet, it is required to install the Java Runtime Environment on the Self-hosted Integration Runtime machine.
* **Self-Hosted Integration Runtime** – Provides data-integration capabilities across different network environments. Here, it connects our on-premises sql database with synapse.

#### 1. A. Setting up pgAdmin 4
1. Open the **Azure Portal**. Go to the resource group created. 
2. Open the **Windows Virtual Machine** which goes by **azcslabsph2win1**.

    ![windowsVm](./assets/1-windowsvm.jpg "windows vm")
    
4. Select **Connect** at the top on the Overview page.

    ![RDPconnect](./assets/2-RDPconnect.jpg "resources list")
    
6. Choose **Bastion** from the options listed.
7.   Enter **Username** and **Password** and **Connect**.  
     **Username:** `windowsVmAdminUser`  
     **Password:** `de22c4!DE22C4@de22c4`
     
     ![vmcreds](./assets/5-vmcreds.jpg "resources list")
     
6. Once the bastion is opened. Go to **Microsoft Edge** to download **Self-Hosted Integration Runtime, pgAdmin and OpenJDK**.
7. Link to download pgAdmin 4.
    `https://www.pgadmin.org/download/`
    
    ![pgadmin](./assets/9-pgadmindownload.jpg "resources list")
    ![pgadmin](./assets/10-pgadmin.jpg "resources list")
    ![pgadmin](./assets/11-pgadmin1.jpg "resources list")
    
8. After installation, go to pgAdmin. Set the **Master Password** for pgAdmin as `admin`.

    ![pgadminpw](./assets/12-pgadminpw.jpg "resources list")
    
10. Register the retail server in the pgAdmin and view the database. Right click on Servers in the Browser pane on your left. Click **Register** -> **Server**.

    ![registerServer](./assets/12-register_server.jpg "resources list")
    
12. In the 'General' tab of properties, give **Name** of the server as `retail`.

    ![registerServer](./assets/14-reg_server1.jpg "resources list")
    
14. In the 'Connection' tab, give **Hostname/address** as `10.1.0.4` , **Password** as `postgrespw` and toggle on **Save Password**. Click **Save** below. 

     ![registerServer](./assets/15-reg_server2.jpg "resources list")
     
16. Once the connection is successful, you can view the database, **retail_org**, and  tables, **customers.csv, products.csv, sales_orders.csv**.

     ![database](./assets/16-database.jpg "resources list")
     ![tables](./assets/17-tables.jpg "resources list")

#### 1. B. Setting up OpenJDK
13. Link to download OpenJDK.
    `https://openjdk.org/`
    
    Inside this link, click on `jdk.java.net/19`, download windows/64 zip folder.
    
    ![javajdk](./assets/18-javajdk.jpg "resources list")
    ![javajdk](./assets/19-jdkdownload.jpg "resources list")
    
14. Go to **File Explorer**. Go to **C:\Program Files**. Inside the **Program Files** folder, create a **new folder** named `java`.
15. Open the downloaded jdk zip folder. You will find a **jdk-19.0.1** folder inside. Copy the jdk-19.0.1 folder and paste it inside the java folder which you have    created in the previous step. 

    ![javajdk](./assets/20-javafolder.jpg "resources list")
    
17. Once copy pasted, make sure that you find the **jvm.dll** file is present in the following path. **C:\Program Files\java\jdk-19.0.1\bin\server\jvm.dll**

    ![jvmdll](./assets/21-dll_file.jpg "resources list")
    
19. Now, we will add these paths to the environment variables. For that, copy the link until bin folder. 

    ![binPath](./assets/22-bin_path.jpg "resources list")
    
21. Search for **Edit the system environment variables** in your vm. 

    ![envVariables](./assets/23-env_variables.jpg "resources list")
    
23. Click on **Environment variables** in the *System Properties* page.

    ![envVariables](./assets/24-env_variables1.jpg "resources list")
    
25. Under **System variables**, click on **Path** variable and **Edit**.

    ![envVariables](./assets/25-edit_env_var.jpg "resources list")
    
27. In **Edit Environment** Variable, click **New**, paste the path which you have copied at step 17, `C:\Program Files\java\jdk-19.0.1\bin`. Click **Ok**. 

    ![binPath](./assets/26-bin_path.jpg "resources list")
    
29. Under **System variables**, click on **New**.
30. In **Variable Name**, give `JAVA_HOME`. In **Variable value**, give the path. `C:\Program Files\java\jdk-19.0.1\` Click **Ok** in both the ‘New System Variable’ pop-up and ‘Environment Variable’ and ‘System Properties’ page.

    ![javaHome](./assets/27-java_home.jpg "resources list")
    ![javaHome](./assets/28-set_env_vars.jpg "resources list")

#### 1. C. Setting up Self-Hosted Integrated Runtime
24. Link to download Self-Hosted Integration Runtime.
    `https://www.microsoft.com/en-us/download/details.aspx?id=39717`
    
    ![integrationRuntime](./assets/6-downloadmsir.jpg "resources list")
    ![integrationRuntime](./assets/7-IRdownload.jpg "resources list")
    
25. Go to **Azure Portal** and go to the **Azure Synapse Analytics** workspace which goes by the name, **dblab-{randomid}-synapse**.

    ![synapse](./assets/29-synapse.jpg "resources list")
    
27. Click on **Open Synapse Studio** tile.

    ![synapse](./assets/30-synapse-studio.jpg "resources list")
    
29. If you face the below error, click **Ok**.

    ![synapse](./assets/31-synapse-error.jpg "resources list")
    
31. Go back to the **Azure Portal**. Navigate to **Azure Active Directory** service. Click on **Users** under *Manage*.
     
     ![synapse](./assets/31a.jpg "resources list")
     
33. Search for your **Microsoft id**. And click on it to view the details.

    ![synapse](./assets/31b.jpg "resources list")
    
35. You can find the **Object id** under *basic info*. Copy the id.

    ![synapse](./assets/31c.jpg "resources list")
    
37. Go to *Synapse Studio*. In the **Manage** tab, go to **Access Controls** under *Security*. Click **Add**.

    ![synapse](./assets/31d.jpg "resources list")
    
39. Under *Role*, select **Synapse Administrator**. Under **Object ID**, *paste the ID* which you have copied at step 30 and **Apply**. The user will be successfully added to the Synapse Access controls.

    ![synapse](./assets/31e.jpg "resources list")
    
41. Go to **Integration Runtimes** under *Integrations* in the **Manage** tab. Click on **New**

    ![IntegrationRuntime](./assets/32-create_IR.jpg "resources list")
    
43. Select **Azure, Self-Hosted**

    ![IntegrationRuntime](./assets/33-SHIR.jpg "resources list")
   
45. Select **Self-Hosted** in the Network Environment Page.

    ![IntegrationRuntime](./assets/34-SHIR1.jpg "resources list")
   
47. Leave the default name as **IntegrationRuntime1** and **Create**

    ![IntegrationRuntime](./assets/35-SHIR2.jpg "resources list")
   
49. Copy anyone of the **Authentication Keys**

    ![IntegrationRuntime](./assets/36-copy_key.jpg "resources list")
    
51. Go to the **Windows Virtual Machine**, paste the authentication key in the Self-Hosted Integration Runtime. Click **Register**.
52. Once it is registered, **Launch the configuration manager**. And **Finish**

    ![IntegrationRuntime](./assets/38-confih_manager.jpg "resources list")
    
54.   Once the connection is successful, go to Diagnostics tab and test for the postgres connection. Give the following details.  
      **Data Server:** `Postgresql`  
      **Server Name:** `10.1.0.4`  
      **Database Name:** `retail_org`  
      **Authentication Mode:** `Basic`  
      **User Name:** `postgres`  
      **Password:** `postgrespw`  
      
41. The connection should be successful.
 
    ![IntegrationRuntime](./assets/39-test_connection.jpg "resources list")
    
> Once the all the three setup is done. **Restart** the Virtual Machine and keep the **Self Hosted Integration Runtime** opened.

## Exercise 2: Developing Synapse Pipeline to extract batch data
Let’s proceed with creating the Azure Synapse Analytics pipeline to ingest data into adls gen 2. 

Here we will use Lookup activity which can retrieve the datasets from Postgres. It reads and returns the content of the tables by executing a query. The output can be a singleton value or an array of attributes, which can be consumed in a subsequent copy, transformation, or control flow activities like ForEach activity. The ForEach activity with Copy data activity will save the tables in parquet file formats within the adls gen2.

42. Go to the **Azure Synapse Studio**, **Integrate** tab. Create **New pipeline**.

    ![pipeline](./assets/40-new_pipeline.jpg "resources list")
    
44. In the **Activities** list, drag **Lookup activity** under *General* to the pipeline canvas. In the *Properties* pane, give **Name** as `getpostgrestables`. Also, expand the below Configurations pane of lookup activity, and give **name** as `getpostgrestables` in *General* tab.

    ![pipeline](./assets/41-lookup_act.jpg "resources list")
    
46. In *Settings* tab of lookup activity, add a **new source dataset**.

    ![pipeline](./assets/42-source_dataset.jpg "resources list")
    
48. Select **PostgreSql** data store and **continue**

    ![pipeline](./assets/43-sourceDS.jpg "resources list")
    
51. Set **Name** as `DS_Source`, to add a new *Linked Service* select **New**.

    ![pipeline](./assets/44-sourceDSname.jpg "resources list")
    
53.   Fill the following and **Create**.  
      **Name** as `LSpostgres`  
      **Intgration Runtime** as `IntegrationRuntime1`  
      **Server Name** as `10.1.0.4`  
      **Database name** as `retail_org`  
      **Username** as `postgres`  
      **Password** as `postgrespw`  
      
      ![pipeline](./assets/45-ls1.jpg "resources list")
      ![pipeline](./assets/46-ls2.jpg "resources list")
      
49. After creating the new linked service, click **Ok**.

     ![pipeline](./assets/47-tablename.jpg "resources list")
     
51. Choose **Query** option in *Use query*. And give the following query in Query Editor.
    
    ```sql
    select table_name from INFORMATION_SCHEMA.Tables
    where table_schema = ‘public’
    ```
    ![pipeline](./assets/47a.jpg "resources list")
    
    > Make sure First Row Only option is unchecked.

50. Drag and drop **ForEach activity** under `Iteration & Conditionals` from the **Activities pane** to the pipeline canvas. **Connect** the Lookup activity and ForEach activity. Select the *ForEach activity*, expand the below configurations pane, set **Name** as `loopingretailtables` in the *General* tab.

    ![pipeline](./assets/48-foreachloop.jpg "resources list")
    
52. In the *Settings* tab, **enable** the Sequential option. Select the **Items** field and then select the **Add dynamic content link** to open the dynamic content editor pane.

    ![pipeline](./assets/49-foreach_settings.jpg "resources list")
    
54. Select the activity to be executed in the dynamic content editor. In this lab, we select the output value of the previous lookup activity. Give the following,  

    ```text
    @activity(‘getpostgrestables’).output.value
    ```
    
    Click **Ok**.
    
    ![pipeline](./assets/50-foreach.jpg "resources list")
    
54. In the *Activities* tab below, click on the **edit** icon for **ForEach** Case.

    ![pipeline](./assets/51-foreach_act.jpg "resources list")
    
56. Drag and drop the **Copy data** activity under *Move & transform* from the Activities pane to the pipeline canvas. In the *General* tab of configurations pane below, set **Name** as `Copy data`.

    ![pipeline](./assets/52-copy_act.jpg "resources list")
    
58. In the *Source* tab, create a **new Source dataset**.

    ![pipeline](./assets/43-link_dataset.jpg "resources list")
    
60. Select **PostgreSql** data store and **continue**.

    ![pipeline](./assets/54-sinkDS.jpg "resources list")
    
62. In the Set properties window, set **Name** as `DS_sink`, select **Linked service** as **LSpostgres** and click on **Ok**.

    ![pipeline](./assets/55-sinkLS.jpg "resources list")
    ![pipeline](./assets/58-createDS.jpg "resources list")
    
64. **Open** the source dataset.

    ![pipeline](./assets/59-editDs.jpg "resources list")
    
66. Create new parameters in the `Parameters` tab. Give the parameter name as `table_name`. 

    ![pipeline](./assets/60_add_param.jpg "resources list")
    
68. In the `Connection` tab of the DS_sink configurations pane below, give the **table name** as `public`, **enable** the Edit option, and click on **Add dynamic content link** to open the dynamic content editor pane.

    ![pipeline](./assets/61-edit_table.jpg "resources list")
    
70.	Give the expression, 

    ```text
    @dataset().table_name
    ```
    
    Click **Ok**.
    
    ![pipeline](./assets/62-edit_table1.jpg "resources list")
    
66.	Go back to the **Copy data activity** and add value to the table_name using the dynamic content editor.

    ![pipeline](./assets/63-editcopy.jpg "resources list")
    
68.	Give the expression that returns a JSON Array to be iterated over the table_name.
    ```text
    @item().table_name
    ```
    
    Click **Ok**.
    
    ![pipeline](./assets/64-editcopy1.jpg "resources list")
    
66.	In the *Sink* tab, create a **new sink** dataset.

    ![pipeline](./assets/65-copysink.jpg "resources list")
    
68.	Select the **Azure Data Lake Storage Gen2** data store and **continue**.

    ![pipeline](./assets/66-sinkDS.jpg "resources list")
    
70.	Select the file format as **Parquet**, in which the file will be saved in the adls account and **continue**.

    ![pipeline](./assets/67-sinkedit.jpg "resources list")
    
72.	Give **Name** as `DS_Parquet`, choose the **Linked Service** as **dblab-{randomString}-synapse-WorkspaceDefaultStorage** and click **Ok**.

    ![pipeline](./assets/68-sinkDS.jpg "resources list")
    
74.	In the `Parameters` tab, add new parameter as **table_name**.

    ![pipeline](./assets/69-parameters.jpg "resources list")
    
76.	Open the DS_Parquet sink. In the 'Connection' tab, give the File path as **data** / **@dataset().table_name** / [Click on Add Dynamic Content Editor to add file name].

    ![pipeline](./assets/70-sinkDSpath.jpg "resources list")
    
78.	Give the below expression in the editor, 

    ```text
    @concat(dataset().table_name,'.parquet')
    ```
    
    Click **Ok**.
    
    ![pipeline](./assets/71-sinkDSedit.jpg "resources list")
    
73.	Go back to the *Sink* tab of Copy data activity. Give the value for the table_name as `@item().table_name`.

    ![pipeline](./assets/72-copy_sinkDS.jpg "resources list")
    
75.	Validate All and Publish All.

    ![pipeline](./assets/73-validate.jpg "resources list")
    
77.	Once the pipeline is published, select **Add Trigger**. Choose **Trigger Now**. Click **Ok** in the Pipeline Run window. 

    ![pipeline](./assets/75-trigger.jpg "resources list")
    
79.	Once the Pipeline Run is successful, we can view the parquet files of all the three datasets, customers, products and sales_orders in the adls storage account.
81.	In the **Azure Portal**, go to the **adls{uniqueString}** account -> **data** container.

    ![pipeline](./assets/77-data_container.jpg "resources list")
    ![pipeline](./assets/78-folders.jpg "resources list")
    
83.	Open all the three directories customers, products and sales_orders and check the parquet files.

    ![pipeline](./assets/79-customers_folder.jpg "resources list")
    ![pipeline](./assets/80-products_folder.jpg "resources list")
    ![pipeline](./assets/81-sales_orders_folder.jpg "resources list")
