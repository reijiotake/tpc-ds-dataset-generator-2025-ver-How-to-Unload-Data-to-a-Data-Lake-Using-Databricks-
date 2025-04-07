# Databricks notebook source
# MAGIC %md
# MAGIC ## step 0 : Create a cluster
# MAGIC
# MAGIC In my case, the following configuration was able to unload the TPC-DS sf100000 scale without any issues.
# MAGIC
# MAGIC ![Cluster Settings](https://raw.githubusercontent.com/reijiotake/tpc-ds-dataset-generator-2025-ver-How-to-Unload-Data-to-a-Data-Lake-Using-Databricks-/main/img/Cluster%20Settings.png)
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Add the spark-sql-perf library jar to your Databricks Cluster
# MAGIC %md
# MAGIC ## step 1
# MAGIC
# MAGIC This step mounts an Azure Data Lake Storage Gen2 container to Databricks, allowing access to files using a path like `/mnt/datalake`.
# MAGIC
# MAGIC - Uses OAuth authentication with client credentials
# MAGIC - Checks if the mount already exists before creating it

# COMMAND ----------

# DBTITLE 1,Mount Azure Data Lake Gen2

# Use Azure KeyVault for storing & retrieving sensitive information
clientId = "yourClientId"
clientSecret = "yourclientSecret"
tenantId = "yourtenantId"
storageAccountName = "yourStorageAccountName"
fileSystemName = "datalake" #Container name
mountPoint = f"/mnt/{fileSystemName}"

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": clientId,
           "fs.azure.account.oauth2.client.secret": clientSecret,
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenantId}/oauth2/token"}

# determine if not already mounted
for m in dbutils.fs.mounts():
  mount_exists = (m.mountPoint==mountPoint)
  if mount_exists:
    print(f"Mount point {mountPoint} already exists")
    break

# create mount if not exists
if not mount_exists:
  
  print(f"Creating mount point {mountPoint}")

  dbutils.fs.mount(
    source = f"abfss://{fileSystemName}@{storageAccountName}.dfs.core.windows.net/",
    mount_point = mountPoint,
    extra_configs = configs)


# COMMAND ----------

# DBTITLE 1,Install the Databricks TPC-DS Benchmark Kit
# MAGIC %md
# MAGIC ## step2
# MAGIC
# MAGIC Run the following command to temporarily save the script to DBFS:

# COMMAND ----------

# Create a folder in DBFS
dbutils.fs.mkdirs("dbfs:/databricks/scripts")

# COMMAND ----------

# Save a shell script to DBFS that installs dependencies and builds the TPC-DS toolkit from source.

dbutils.fs.put("/databricks/scripts/tpcds-install.sh","""
#!/bin/bash
sudo apt-get --assume-yes install gcc make flex bison byacc git

cd /usr/local/bin
git clone https://github.com/databricks/tpcds-kit.git
cd tpcds-kit/tools
make OS=LINUX""", True)

# COMMAND ----------

# Confirm the presence of the script in DBFS
display(dbutils.fs.ls("dbfs:/databricks/scripts/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## step 3
# MAGIC
# MAGIC To make the shell script downloadable via a web browser, the script is copied to /FileStore and a download link is displayed in the notebook.

# COMMAND ----------

# This command copies the shell script from /databricks/scripts in DBFS to the /FileStore directory, which allows it to be accessed via a public URL.

dbutils.fs.cp("/databricks/scripts/tpcds-install.sh", "/FileStore/tpcds-install.sh")


# COMMAND ----------

# This displays a download link in the notebook so users can easily download the tpcds-install.sh script from /FileStore

displayHTML("<a href='/files/tpcds-install.sh' download>Download tpcds-install.sh</a>")

# COMMAND ----------

# MAGIC %md
# MAGIC ## step 4
# MAGIC
# MAGIC Open the downloaded tpcds-install.sh file in VS Code or another text editor and modify the script as needed.
# MAGIC <br>The contents of the modified script are as follows

# COMMAND ----------

#!/bin/bash

# Update package list
apt-get update

# Install required packages for building the TPC-DS toolkit
apt-get --assume-yes install gcc make flex bison byacc git

# Clone the tpcds-kit repository and build the toolkit
cd /usr/local/bin
git clone https://github.com/databricks/tpcds-kit.git
cd tpcds-kit/tools
make OS=LINUX

# COMMAND ----------

# MAGIC %md
# MAGIC ## step5 
# MAGIC * Upload the script from your local machine to a desired workspace folder in Databricks.
# MAGIC * Register the uploaded script as a cluster init script by specifying its workspace path.
# MAGIC ![Registering an Init Script](https://github.com/reijiotake/tpc-ds-dataset-generator-2025-ver-How-to-Unload-Data-to-a-Data-Lake-Using-Databricks-/blob/main/img/Registering%20an%20Init%20Script.png?raw=true)
# MAGIC
# MAGIC * Restart the cluster
# MAGIC
# MAGIC ▽Reference<br>
# MAGIC [What are init scripts?](https://learn.microsoft.com/ja-jp/azure/databricks/init-scripts/#cluster-scoped-init-script) <br><br>

# COMMAND ----------

# DBTITLE 1,Generate Data
# MAGIC %md
# MAGIC ## step 6
# MAGIC
# MAGIC After the cluster restarts, you can run the "data generator notebook".