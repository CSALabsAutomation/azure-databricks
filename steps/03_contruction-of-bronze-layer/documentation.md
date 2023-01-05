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
```python
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

**Replace the Client ID, Key Vault Name, Client Secret Name, Directory ID, Storage Account Name in the above code with yours.**

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
```python
# customers raw delta live table customerpath='/mnt/data/customers/customers.parquet' @dlt.table(
    comment="the customers raw dataset."
    )
def customers_raw():
    return (spark.read.parquet(customerpath,header=True))
```

**Cmd4:**
```python
#products raw delta live table
productpath ='/mnt/data/products/products.parquet' @dlt.table(
    comment="the prdduct raw dataset."
    )
def products_raw():
    return (spark.read.parquet(productpath,header=True))
```

**Cmd5:**
```python
# sales order batch raw delta live table
salesorderbatchpath='/mnt/data/sales_orders/sales_orders.parquet' @dlt.table(
    comment="the sales order batch raw dataset.."
    )
def sales_orders_batch_raw():
    return (spark.read.parquet(salesorderbatchpath,header=True))
```

**Cmd6:**
```python
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
```python
%sql
select * from retail_org.products_raw
```
