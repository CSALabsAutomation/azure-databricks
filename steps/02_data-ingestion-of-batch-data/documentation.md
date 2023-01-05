In this lab step, we will use the below architectural flow to extract the data from the Postgresql server.



The datasets which are available in the PostgreSQL server inside the VM is visualized using pgAdmin 4 tool. Using the Self Hosted Integration Runtime and Synapse Pipeline Activity we will ingest the data from the postgres into the adls gen 2 account.
Steps:
Let’s begin with setting up the Windows Virtual Machine for the Data Ingestion Process.
We will download the following pre-requisites in the Windows Virtual Machine in order to process the batch data from PostgreSql.
1.	pgAdmin 4 – It is the open-source management tool for managing PostgreSQL databases.
2.	Open JDK – Since the files were parsed and written in parquet, it is required to install the Java Runtime Environment on the Self-hosted Integration Runtime machine.
3.	Self-Hosted Integration Runtime – Provides data-integration capabilities across different network environments. Here, it connects our on-premises sql database with synapse.
1.	Open the Azure Portal. Go to the resource group created. 
2.	Open the Windows Virtual Machine which goes by azcslabsph2win1.
3.	Select Connect at the top on the Overview page.
4.	Choose Bastion from the options listed.
5.	Enter Username and Password and Connect.
Username: windowsVmAdminUser
Password: de22c4!DE22C4@de22c4
6.	Once the bastion is opened. Go to Microsoft Edge to download Self-Hosted Integration Runtime, pgAdmin and OpenJDK.
7.	Link to download pgAdmin 4.
https://www.pgadmin.org/download/
8.	Go to pgAdmin. Set the Master Password for pgAdmin as admin.
9.	Register the retail server in the pgAdmin and view the database. Right click on Servers in the Browser pane on your left. Click Register -> Server.
10.	In the General tab of properties, give Name of the server as retail.
11.	In the Connection tab, give Hostname/address as 10.1.0.4 , Password as postgrespw and toggle on Save Password. Click Save below. 
12.	Once the connection is successful, you can view the database, retail_org, and  tables, customers.csv, products.csv, sales_orders.csv.
13.	Link to download OpenJDK.
https://openjdk.org/
Inside this link, click on jdk.java.net/19, download windows/64 zip folder.
14.	Go to File Explorer. Go to C:\Program Files. Inside the Program Files folder, create a new folder named java.
15.	Open the downloaded jdk zip folder. You will find a jdk-19.0.1 folder inside. Copy the jdk-19.0.1 folder and paste it inside the java folder which you have created in step 15. 
16.	Once copy pasted, make sure that you find the jvm.dll file is present in the following path. C:\Program Files\java\jdk-19.0.1\bin\server\jvm.dll
17.	Now, we will add these paths to the environment variables. For that, copy the link until bin folder. 
18.	Search for Edit the system environment variables in your vm. 
19.	Click on Environment variables in the System Properties page.
20.	Under System variables, click on Path variable and Edit.
21.	In Edit Environment Variable, click New, paste the path which you have copied at step 18, C:\Program Files\java\jdk-19.0.1\bin. Click Ok. 
22.	Under System variables, click on New.
23.	In Variable Name, give JAVA_HOME. In Variable value, give the path. C:\Program Files\java\jdk-19.0.1\ Click Ok in both the ‘New System Variable’ pop-up and ‘Environment Variable’ and ‘System Properties’ page.
24.	Link to download Self-Hosted Integration Runtime.
https://www.microsoft.com/en-us/download/details.aspx?id=39717
25.	Go to Azure Portal and go to the Azure Synapse Analytics workspace which goes by the name, dblab-{randomid}-synapse.
26.	Click on Open Synapse Studio tile.
27.	If you face the below error, click Ok.
28.	Go to Azure Portal. Navigate to Azure Active Directory service. Click on Users under Manage.
29.	Search for your Microsoft id. And click on it to view the details.
30.	You can find the Object id under basic info. Copy the id.
31.	Go to Synapse Studio. In the Manage tab, go to Access Controls under Security. Click Add.
32.	Under Role, select Synapse Administrator. Under Object ID, paste the ID which you have copied at step 30 and Apply. The user will be successfully added to the Synapse Access controls.
33.	Go to Integration Runtimes under Integrations in the Manage tab. Click on New.
34.	Select Azure, Self-Hosted.
35.	Select Self-Hosted in the Network Environment Page.
36.	Leave the default name as IntegrationRuntime1 and Create.
37.	Copy anyone of the Authentication Key.
38.	Go to the Windows Virtual Machine, paste the authentication key in the Self-Hosted Integration Runtime. Click Register.
39.	Once it is registered, Launch the configuration manager. And Finish. 
40.	Once the connection is successful, go to Diagnostics tab and test for the postgres connection. Give the following details.
41.	The connection should be successful.
Let’s proceed with creating the Azure Synapse Analytics pipeline to ingest data into adls gen 2. Here we will use Lookup activity which can retrieve the datasets from Postgres. It reads and returns the content of the tables by executing a query. The output can be a singleton value or an array of attributes, which can be consumed in a subsequent copy, transformation, or control flow activities like ForEach activity. The ForEach activity with Copy data activity will save the tables in parquet file formats within the adls gen2.
42.	Go to the Azure Synapse Studio, Integrate tab. Create New pipeline.
43.	In the Activities list, drag Lookup activity under ‘General’ to the pipeline canvas. In the ‘Properties’ pane, give Name as getpostgrestables. Also, expand the below Configurations pane of lookup activity, and give name as getpostgrestables in ‘General’ tab.
44.	In Settings pane of lookup activity, add a new source dataset.
45.	Select PostgreSql data store and continue.
46.	Set Name as DS_Source, to add a new ‘Linked Service’ select New.
47.	Give Name as LSpostgres. Choose the IntgrationRuntime1, set the Server Name to 10.1.0.4, Database name as retail_org, Username as postgres, Password as postgrespw and Create.
48.	After creating the new linked service, click Ok.
49.	Choose Query option in Use query. And give the following query in Query Editor.
select table_name from INFORMATION_SCHEMA.Tables
where table_schema = ‘public’
Make sure First Row Only option is unchecked.
50.	Drag and drop ForEach activity under ‘Iteration & Conditionals’ from the Activities pane to the pipeline canvas. Connect the Lookup activity and ForEach activity. Select the ForEach activity, expand the below configurations pane, set Name as loopingretailtables in the ‘General’ tab.
51.	In the ‘Settings’ tab, enable the Sequential option. Select the Items field and then select the Add dynamic content link to open the dynamic content editor pane.
52.	Select the activity to be executed in the dynamic content editor. In this lab, we select the output value of the previous lookup activity. Give the following, @activity(‘getpostgrestables’).output.value and click Ok.
53.	In the Activities tab below, click on the edit icon for ForEach Case.
54.	Drag and drop the Copy data activity under Move & transform from the Activities pane to the pipeline canvas. In the ‘General’ tab of configurations pane below, set Name as Copy data.
55.	In the ‘Source’ tab, create a new Source dataset.
56.	Select PostgreSql data store and continue.
57.	In the Set properties window, set Name as DS_sink, create new Linked service.
58.	Set Name as lspostgresparam, choose the IntegrationRuntime1, give server name as 10.1.0.4, Database name as retail_org, User name as postgres, give Password as postgrespw and Create.
59.	Once the Linked service is created, click Ok.
60.	Open the source dataset.
61.	Give Name for the source dataset as DS_sink in the Properties pane. In the ‘Connection’ tab of the DS_sink configurations pane below, give the table name as public, enable the Edit option, and click on Add dynamic content link to open the dynamic content editor pane.
62.	Give the expression, @dataset().table_name and click Ok.
63.	Create new parameters in the ‘Parameters’ tab. Give the parameter name as table_name.
64.	Go back to the Copy data activity and add value to the table_name using the dynamic content editor.
65.	Give the expression that returns a JSON Array to be iterated over the table_name.
@item().table_name
Click Ok.
66.	In the ‘Sink’ tab, create a new sink dataset.
67.	Select the Azure Data Lake Storage Gen2 data store and continue.
68.	Select the file format as Parquet, in which the file will be saved in the adls account. and continue.
69.	Give Name as DS_Parquet, choose the Linked Service as dblab-{randomString}-synapse-WorkspaceDefaultStorage and click Ok.
70.	Open the DS_Parquet sink. In the ‘Connection’ tab, give the File path as batch / @dataset().table_name / [Click on Add Dynamic Content Editor to add file name].
71.	Give the below expression in the editor,
@concat(dataset().table_name,’.parquet’)
Click Ok.
72.	In the ‘Parameters’ tab, add new parameter as table_name.
73.	Go back to the ‘Sink’ tab of Copy data activity. Give the value for the table_name as @item().table_name.
74.	Validate All and Publish All.
75.	Once the pipeline is published, select Add Trigger. Choose Trigger Now. Click Ok in the Pipeline Run window. 
76.	Once the Pipeline Run is successful, we can view the parquet files of all the three datasets, customers, products and sales_orders in the adls storage account.
77.	In the Azure Portal, go to the adls{uniqueString} account -> batch container.
78.	Open all the three directories customers, products and sales_orders and check the parquet files.


## Exercise 3 : Construct raw/bronze layer

1.	Go to Azure Active Directory and click on **App Registrations** under **Manage** on the left pane.

    sc

3.	Now click on **"+ New Registration"** to create a new **App Registration**.

    sc

5.	Give the name as ``app_registration_lab1`` and click on **Register**.

    sc

7.	Once created, copy the **Client ID** and **Directory ID** from the overview page to a notepad for future reference.

    sc

9.	Now click on **Certificates and secrets** under the **Manage** in the left pane.

    sc

11.	Create a new **Client Secret** by naming it ``client_secret_lab1`` and click on **Add**.

    sc

15.	Once done, copy the **Client Secret Value** for future reference.

    sc
    
17.	Go to your storage account **adls-{Random-String}** --> **Access Control** in the left pane -- > **Add** -- > Search for **Storage Blob Data Contributor** --> Next --> Select **Members** --> Search for ``app_registration_lab1`` --> Select --> Review and Assign

    sc
    
19.	Go to your Key Vault which does by the name ``keyvaultdb`` present in your resource group and click on **Secrets** under **Objects** in the left pane.

    sc
    
21.	Click on **+Generate/Import** and fill in the following details: 
**Name** – ``secret``
**Secret value** – The Client Secret Value that you had copied earlier
And click create.

    sc
    
11.	Go to your resource group and open the **Databricks Workspace** which goes by the name {}

    sc
    
13.	Edit the URL of the workspace as ``https://adb.......azuredatabricks.net/#secrets/createScope``

    sc 
    
15.	You should see the **Create Secret Scope** page. Fill in the following details and click on Create.
**Scope Name** – ``keyvaultdb``
**DNS Name** – Go to your resource group and find the **Key Vault** --> Click on **properties** under Settings in the left pane --> copy the **Vault URI** and paste it.
**Resource** -  Go to your resource group and find the **Key Vault** --> Click on **properties** under Settings in the left pane --> copy the **Resource ID** and paste it.

    sc
    
14.	Go to the **Databricks Workspace** and go to **Compute** in the left pane. Start the cluster ``dbcluster``

    sc
   
16.	Click on **+New** in the left pane, select **Notebook**. Provide the name of the Notebook as ``retailorg``. Choose the language as ``Python`` and Cluster as ``dbcluster``.

    sc
    
18.	Run the following code to mount the files on the **DBFS** and **ADLS**:

**Cmd1 :**
```text
configs ={"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id":"{Client ID}",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="{keyvault name}",key="{client secret name}"),
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/{Directory ID}/oauth2/token" }
try:
    dbutils.fs.mount(
    source="abfss://data@{Storage Account Name}.dfs.core.windows.net/",
    mount_point="/mnt/data/",
    extra_configs = configs)
except Exception as e:
    print ("Error: {} already mounted.Run unmount first")
 ```

**Replace the Client ID, Key Vault Name, Client Secret Name, Directory ID, Storage Account Name in the above code with yours. **

**Cmd2:**
```text
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime #create streaming delta live table now = datetime.datetime.now()
currentdate =now.strftime("%Y-%m-%d") salesorderstreampath='/mnt/data/'+currentdate+'/' @dlt.table(
    comment="the streaming raw dataset."
    )
def sales_orders_stream_raw():
    return (spark.read.parquet(salesorderstreampath,header=True))
```

**Cmd3:**
```text
# customers raw delta live table customerpath='/mnt/data/customers/customers.parquet' @dlt.table(
    comment="the customers raw dataset."
    )
def customers_raw():
    return (spark.read.parquet(customerpath,header=True))

**Cmd4:**
#products raw delta live table
productpath ='/mnt/data/products/products.parquet' @dlt.table(
    comment="the prdduct raw dataset."
    )
def products_raw():
    return (spark.read.parquet(productpath,header=True))
```

**Cmd5:**
```text
# sales order batch raw delta live table
salesorderbatchpath='/mnt/data/sales_orders/sales_orders.parquet' @dlt.table(
    comment="the sales order batch raw dataset.."
    )
def sales_orders_batch_raw():
    return (spark.read.parquet(salesorderbatchpath,header=True))
```

**Cmd6:**
```text
products_schema = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("product_category", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField(
            "sales_price",
            StructType(
                [
                    StructField("scale", IntegerType(), False),
                    StructField("value", StringType(), False),
                ]
            ),
            False,
        ),
        StructField("ean13", DoubleType(), False),
        StructField("ean5", StringType(), False),
        StructField("product_unit", StringType(), False),
    ]
) 
@dlt.table(
    comment="Load data to a products cleansed table",
    table_properties={"pipelines.reset.allowed": "true"},
    spark_conf={"pipelines.trigger.interval": "60 seconds"},
    temporary=False,
)
def products_cleansed():
    return (
        dlt.read_stream("products_raw")
        #         spark.readStream.format("delta").table("retail_org.products_raw")
        #         spark.read.format("delta").table("retail_org.products_raw")
        .select(get_json_object(col("value"), "$.payload.after").alias("row"))
        .select(from_json(col("row"), products_schema).alias("row"))
        .select("row.*")
    )
```

**Cmd7:**
```text
%sql
select * from retail_org.products_raw
```

