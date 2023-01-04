# Databricks notebook source
def mountDataLake(clientId, clientSecret, tokenEndPoint, storageAccountName, containerName):    
    mountPoint = f"/mnt/{containerName}"    
    if all(mount.mountPoint != mountPoint for mount in dbutils.fs.mounts()):      
        configs = {"fs.azure.account.auth.type": "OAuth",
                  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                  "fs.azure.account.oauth2.client.id": clientId,
                  "fs.azure.account.oauth2.client.secret": clientSecret,
                  "fs.azure.account.oauth2.client.endpoint": tokenEndPoint}
        dbutils.fs.mount(
          source = f"abfss://{containerName}@{storageAccountName}.dfs.core.windows.net/",
          mount_point = f"/mnt/{containerName}",
          extra_configs = configs)   


# COMMAND ----------

mountDataLake(clientId="2k970b0a-73f1-45b8-813a-7b87016fa2d8",
              clientSecret=dbutils.secrets.get(
                  scope="adlsgen2", key="adlsmountkey"),
              tokenEndPoint="https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token",
              storageAccountName="azcslabsph2adlsgen",
              containerName="1data")
