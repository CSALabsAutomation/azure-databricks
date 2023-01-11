1. Go to **Synapse Studio** which goes my the name ``dblab-{random-string}-synapse``.
2. Under **Develop** tab, create a **new Notebook** with the name ``Notebook1``.
3. Paste the following commands in the notebook.

```text
   import com.microsoft.spark.sqlanalytics
   from com.microsoft.spark.sqlanalytics.Constants import Constants

   df=spark.read.format('parquet').load('abfss://data@adlsrzrwjwoong4im.dfs.core.windows.net/retailorg/tables/dim_products/')
   (df.write.option(Constants.SERVER, "dblab-rzrwjw-synapse.sql.azuresynapse.net")
   .option(Constants.TEMP_FOLDER, "abfss://data@adlsrzrwjwoong4im.dfs.core.windows.net/staging").mode("overwrite")
   .synapsesql("dedicatedPool.dbo.dim_products"))
```

```text
   df1=spark.read.format('parquet').load('abfss://data@adlsrzrwjwoong4im.dfs.core.windows.net/retailorg/tables/dim_customers/')
   (df1.write.option(Constants.SERVER, "dblab-rzrwjw-synapse.sql.azuresynapse.net")
   .option(Constants.TEMP_FOLDER, "abfss://data@adlsrzrwjwoong4im.dfs.core.windows.net/staging")
   .mode("overwrite").synapsesql("dedicatedPool.dbo.dim_customers"))
```

```text
   df2=spark.read.format('parquet').load('abfss://data@adlsrzrwjwoong4im.dfs.core.windows.net/retailorg/tables/fact_sales_orders/')
   (df2.write.option(Constants.SERVER, "dblab-rzrwjw-synapse.sql.azuresynapse.net")
   .option(Constants.TEMP_FOLDER, "abfss://data@adlsrzrwjwoong4im.dfs.core.windows.net/staging")
   .mode("overwrite").synapsesql("dedicatedPool.dbo.fact_sales_orders"))
```

```text
   df3=spark.read.format('parquet').load('abfss://data@adlsrzrwjwoong4im.dfs.core.windows.net/retailorg/tables/fact_customer_sales/')
   (df3.write.option(Constants.SERVER, "dblab-rzrwjw-synapse.sql.azuresynapse.net")
   .option(Constants.TEMP_FOLDER, "abfss://data@adlsrzrwjwoong4im.dfs.core.windows.net/staging")
   .mode("overwrite").synapsesql("dedicatedPool.dbo.fact_customer_sales"))
```

4. After pasting all commands, click on **Run All**. Then proceed to **Validate** and **Publish** your Notebook.
