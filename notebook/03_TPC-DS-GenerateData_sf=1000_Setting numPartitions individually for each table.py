# Databricks notebook source
# DBTITLE 1,Generate TPC-DS data
# MAGIC %md
# MAGIC Generating data at larger scales can take hours to run, and you may want to run the notebook as a job.
# MAGIC
# MAGIC The cell below generates the data. Read the code carefully, as it contains many parameters to control the process. See the <a href="https://github.com/databricks/spark-sql-perf" target="_blank">Databricks spark-sql-perf repository README</a> for more information.

# COMMAND ----------

# MAGIC
# MAGIC %scala
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "1000" // scaleFactor defines the size of the dataset to generate (in GB).
# MAGIC val scaleFactoryInt = scaleFactor.toInt
# MAGIC
# MAGIC val scaleName = if(scaleFactoryInt < 1000){
# MAGIC     f"${scaleFactoryInt}%03d" + "GB"
# MAGIC   } else {
# MAGIC     f"${scaleFactoryInt / 1000}%03d" + "TB"
# MAGIC   }
# MAGIC
# MAGIC val fileFormat = "parquet" // valid spark file format like parquet, csv, json.
# MAGIC val rootDir = s"/mnt/datalake/raw/tpc-ds/source_files_${scaleName}_${fileFormat}"
# MAGIC val databaseName = "tpcds" + scaleName // name of database to create.
# MAGIC
# MAGIC // Run:
# MAGIC val tables = new TPCDSTables(sqlContext,
# MAGIC     dsdgenDir = "/usr/local/bin/tpcds-kit/tools", // location of dsdgen 
# MAGIC     scaleFactor = scaleFactor,
# MAGIC     useDoubleForDecimal = false, // true to replace DecimalType with DoubleType 
# MAGIC     useStringForDate = false) // true to replace DateType with StringType
# MAGIC
# MAGIC tables.genData(
# MAGIC     location = rootDir,
# MAGIC     format = fileFormat,
# MAGIC     overwrite = true, // overwrite the data that is already there
# MAGIC     partitionTables = false, // create the partitioned fact tables 
# MAGIC     clusterByPartitionColumns = false, // shuffle to get partitions coalesced into single files. 
# MAGIC     filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
# MAGIC     tableFilter = "", // "" means generate all tables
# MAGIC     numPartitions = 1000) // how many dsdgen partitions to run - number of input tasks.

# COMMAND ----------

# DBTITLE 1,View TPC-DS data
# examine data
df = spark.read.parquet("/mnt/datalake/raw/tpc-ds/source_files_001TB_parquet/customer")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Sample Results
# MAGIC Below are a few sample results from generating data at the 1 and 1000 scale.
# MAGIC
# MAGIC | File Format | Generate Column Stats | Number of dsdgen Tasks | Partition Tables | TPC-DS Scale | Databricks Cluster Config               | Duration | Storage Size |
# MAGIC | ----------- | --------------------- | ---------------------- | ---------------- | ------------ | --------------------------------------- | -------- | ------------ |
# MAGIC | csv | no | 4 | no | 1 | 1 Standard_DS3_v2 worker, 4 total cores | 4.79 min | 1.2 GB |
# MAGIC | parquet     | yes                   | 4                      | no               | 1            | 1 Standard_DS3_v2 worker, 4 total cores  | 5.88 min | 347 MB |
# MAGIC | json | no | 4 | no | 1 | 1 Standard_DS3_v2 worker, 4 total cores | 7.35 min | 5.15 GB |
# MAGIC | parquet | yes | 1000 | yes | 1000 | 4 Standard_DS3_v2 worker, 16 total cores | 4 hours | 333 GB |

# COMMAND ----------

# MAGIC %md
# MAGIC # 以下のテーブルについてはファイル数を指定して生成

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "1000" // scaleFactor defines the size of the dataset to generate (in GB).
# MAGIC val scaleFactoryInt = scaleFactor.toInt
# MAGIC
# MAGIC val scaleName = if(scaleFactoryInt < 1000){
# MAGIC     f"${scaleFactoryInt}%03d" + "GB"
# MAGIC   } else {
# MAGIC     f"${scaleFactoryInt / 1000}%03d" + "TB"
# MAGIC   }
# MAGIC
# MAGIC val fileFormat = "parquet" // valid spark file format like parquet, csv, json.
# MAGIC val tableName = "store_sales"
# MAGIC val rootDir = s"/mnt/datalake/raw/tpc-ds/source_files_${scaleName}_${fileFormat}/${tableName}"
# MAGIC val databaseName = "tpcds" + scaleName // name of database to create.
# MAGIC
# MAGIC // Run:
# MAGIC val tables = new TPCDSTables(sqlContext,
# MAGIC     dsdgenDir = "/usr/local/bin/tpcds-kit/tools", // location of dsdgen 
# MAGIC     scaleFactor = scaleFactor,
# MAGIC     useDoubleForDecimal = false, // true to replace DecimalType with DoubleType 
# MAGIC     useStringForDate = false) // true to replace DateType with StringType
# MAGIC
# MAGIC // Generate data
# MAGIC tables.genData(
# MAGIC     location = rootDir,
# MAGIC     format = fileFormat,
# MAGIC     overwrite = true, // overwrite the data that is already there
# MAGIC     partitionTables = false, // create the partitioned fact tables 
# MAGIC     clusterByPartitionColumns = false, // shuffle to get partitions coalesced into single files. 
# MAGIC     filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
# MAGIC     tableFilter = tableName, // "" means generate all tables
# MAGIC     numPartitions = 250) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "1000" // scaleFactor defines the size of the dataset to generate (in GB).
# MAGIC val scaleFactoryInt = scaleFactor.toInt
# MAGIC
# MAGIC val scaleName = if(scaleFactoryInt < 1000){
# MAGIC     f"${scaleFactoryInt}%03d" + "GB"
# MAGIC   } else {
# MAGIC     f"${scaleFactoryInt / 1000}%03d" + "TB"
# MAGIC   }
# MAGIC
# MAGIC val fileFormat = "parquet" // valid spark file format like parquet, csv, json.
# MAGIC val tableName = "catalog_returns"
# MAGIC val rootDir = s"/mnt/datalake/raw/tpc-ds/source_files_${scaleName}_${fileFormat}/${tableName}"
# MAGIC val databaseName = "tpcds" + scaleName // name of database to create.
# MAGIC
# MAGIC // Run:
# MAGIC val tables = new TPCDSTables(sqlContext,
# MAGIC     dsdgenDir = "/usr/local/bin/tpcds-kit/tools", // location of dsdgen 
# MAGIC     scaleFactor = scaleFactor,
# MAGIC     useDoubleForDecimal = false, // true to replace DecimalType with DoubleType 
# MAGIC     useStringForDate = false) // true to replace DateType with StringType
# MAGIC
# MAGIC // Generate data
# MAGIC tables.genData(
# MAGIC     location = rootDir,
# MAGIC     format = fileFormat,
# MAGIC     overwrite = true, // overwrite the data that is already there
# MAGIC     partitionTables = false, // create the partitioned fact tables 
# MAGIC     clusterByPartitionColumns = false, // shuffle to get partitions coalesced into single files. 
# MAGIC     filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
# MAGIC     tableFilter = tableName, // "" means generate all tables
# MAGIC     numPartitions = 20) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "1000" // scaleFactor defines the size of the dataset to generate (in GB).
# MAGIC val scaleFactoryInt = scaleFactor.toInt
# MAGIC
# MAGIC val scaleName = if(scaleFactoryInt < 1000){
# MAGIC     f"${scaleFactoryInt}%03d" + "GB"
# MAGIC   } else {
# MAGIC     f"${scaleFactoryInt / 1000}%03d" + "TB"
# MAGIC   }
# MAGIC
# MAGIC val fileFormat = "parquet" // valid spark file format like parquet, csv, json.
# MAGIC val tableName = "catalog_sales"
# MAGIC val rootDir = s"/mnt/datalake/raw/tpc-ds/source_files_${scaleName}_${fileFormat}/${tableName}"
# MAGIC val databaseName = "tpcds" + scaleName // name of database to create.
# MAGIC
# MAGIC // Run:
# MAGIC val tables = new TPCDSTables(sqlContext,
# MAGIC     dsdgenDir = "/usr/local/bin/tpcds-kit/tools", // location of dsdgen 
# MAGIC     scaleFactor = scaleFactor,
# MAGIC     useDoubleForDecimal = false, // true to replace DecimalType with DoubleType 
# MAGIC     useStringForDate = false) // true to replace DateType with StringType
# MAGIC
# MAGIC // Generate data
# MAGIC tables.genData(
# MAGIC     location = rootDir,
# MAGIC     format = fileFormat,
# MAGIC     overwrite = true, // overwrite the data that is already there
# MAGIC     partitionTables = false, // create the partitioned fact tables 
# MAGIC     clusterByPartitionColumns = false, // shuffle to get partitions coalesced into single files. 
# MAGIC     filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
# MAGIC     tableFilter = tableName, // "" means generate all tables
# MAGIC     numPartitions = 200) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "1000" // scaleFactor defines the size of the dataset to generate (in GB).
# MAGIC val scaleFactoryInt = scaleFactor.toInt
# MAGIC
# MAGIC val scaleName = if(scaleFactoryInt < 1000){
# MAGIC     f"${scaleFactoryInt}%03d" + "GB"
# MAGIC   } else {
# MAGIC     f"${scaleFactoryInt / 1000}%03d" + "TB"
# MAGIC   }
# MAGIC
# MAGIC val fileFormat = "parquet" // valid spark file format like parquet, csv, json.
# MAGIC val tableName = "customer_address"
# MAGIC val rootDir = s"/mnt/datalake/raw/tpc-ds/source_files_${scaleName}_${fileFormat}/${tableName}"
# MAGIC val databaseName = "tpcds" + scaleName // name of database to create.
# MAGIC
# MAGIC // Run:
# MAGIC val tables = new TPCDSTables(sqlContext,
# MAGIC     dsdgenDir = "/usr/local/bin/tpcds-kit/tools", // location of dsdgen 
# MAGIC     scaleFactor = scaleFactor,
# MAGIC     useDoubleForDecimal = false, // true to replace DecimalType with DoubleType 
# MAGIC     useStringForDate = false) // true to replace DateType with StringType
# MAGIC
# MAGIC // Generate data
# MAGIC tables.genData(
# MAGIC     location = rootDir,
# MAGIC     format = fileFormat,
# MAGIC     overwrite = true, // overwrite the data that is already there
# MAGIC     partitionTables = false, // create the partitioned fact tables 
# MAGIC     clusterByPartitionColumns = false, // shuffle to get partitions coalesced into single files. 
# MAGIC     filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
# MAGIC     tableFilter = tableName, // "" means generate all tables
# MAGIC     numPartitions = 1) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "1000" // scaleFactor defines the size of the dataset to generate (in GB).
# MAGIC val scaleFactoryInt = scaleFactor.toInt
# MAGIC
# MAGIC val scaleName = if(scaleFactoryInt < 1000){
# MAGIC     f"${scaleFactoryInt}%03d" + "GB"
# MAGIC   } else {
# MAGIC     f"${scaleFactoryInt / 1000}%03d" + "TB"
# MAGIC   }
# MAGIC
# MAGIC val fileFormat = "parquet" // valid spark file format like parquet, csv, json.
# MAGIC val tableName = "customer_demographics"
# MAGIC val rootDir = s"/mnt/datalake/raw/tpc-ds/source_files_${scaleName}_${fileFormat}/${tableName}"
# MAGIC val databaseName = "tpcds" + scaleName // name of database to create.
# MAGIC
# MAGIC // Run:
# MAGIC val tables = new TPCDSTables(sqlContext,
# MAGIC     dsdgenDir = "/usr/local/bin/tpcds-kit/tools", // location of dsdgen 
# MAGIC     scaleFactor = scaleFactor,
# MAGIC     useDoubleForDecimal = false, // true to replace DecimalType with DoubleType 
# MAGIC     useStringForDate = false) // true to replace DateType with StringType
# MAGIC
# MAGIC // Generate data
# MAGIC tables.genData(
# MAGIC     location = rootDir,
# MAGIC     format = fileFormat,
# MAGIC     overwrite = true, // overwrite the data that is already there
# MAGIC     partitionTables = false, // create the partitioned fact tables 
# MAGIC     clusterByPartitionColumns = false, // shuffle to get partitions coalesced into single files. 
# MAGIC     filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
# MAGIC     tableFilter = tableName, // "" means generate all tables
# MAGIC     numPartitions = 1) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "1000" // scaleFactor defines the size of the dataset to generate (in GB).
# MAGIC val scaleFactoryInt = scaleFactor.toInt
# MAGIC
# MAGIC val scaleName = if(scaleFactoryInt < 1000){
# MAGIC     f"${scaleFactoryInt}%03d" + "GB"
# MAGIC   } else {
# MAGIC     f"${scaleFactoryInt / 1000}%03d" + "TB"
# MAGIC   }
# MAGIC
# MAGIC val fileFormat = "parquet" // valid spark file format like parquet, csv, json.
# MAGIC val tableName = "inventory"
# MAGIC val rootDir = s"/mnt/datalake/raw/tpc-ds/source_files_${scaleName}_${fileFormat}/${tableName}"
# MAGIC val databaseName = "tpcds" + scaleName // name of database to create.
# MAGIC
# MAGIC // Run:
# MAGIC val tables = new TPCDSTables(sqlContext,
# MAGIC     dsdgenDir = "/usr/local/bin/tpcds-kit/tools", // location of dsdgen 
# MAGIC     scaleFactor = scaleFactor,
# MAGIC     useDoubleForDecimal = false, // true to replace DecimalType with DoubleType 
# MAGIC     useStringForDate = false) // true to replace DateType with StringType
# MAGIC
# MAGIC // Generate data
# MAGIC tables.genData(
# MAGIC     location = rootDir,
# MAGIC     format = fileFormat,
# MAGIC     overwrite = true, // overwrite the data that is already there
# MAGIC     partitionTables = false, // create the partitioned fact tables 
# MAGIC     clusterByPartitionColumns = false, // shuffle to get partitions coalesced into single files. 
# MAGIC     filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
# MAGIC     tableFilter = tableName, // "" means generate all tables
# MAGIC     numPartitions = 6) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "1000" // scaleFactor defines the size of the dataset to generate (in GB).
# MAGIC val scaleFactoryInt = scaleFactor.toInt
# MAGIC
# MAGIC val scaleName = if(scaleFactoryInt < 1000){
# MAGIC     f"${scaleFactoryInt}%03d" + "GB"
# MAGIC   } else {
# MAGIC     f"${scaleFactoryInt / 1000}%03d" + "TB"
# MAGIC   }
# MAGIC
# MAGIC val fileFormat = "parquet" // valid spark file format like parquet, csv, json.
# MAGIC val tableName = "store_returns"
# MAGIC val rootDir = s"/mnt/datalake/raw/tpc-ds/source_files_${scaleName}_${fileFormat}/${tableName}"
# MAGIC val databaseName = "tpcds" + scaleName // name of database to create.
# MAGIC
# MAGIC // Run:
# MAGIC val tables = new TPCDSTables(sqlContext,
# MAGIC     dsdgenDir = "/usr/local/bin/tpcds-kit/tools", // location of dsdgen 
# MAGIC     scaleFactor = scaleFactor,
# MAGIC     useDoubleForDecimal = false, // true to replace DecimalType with DoubleType 
# MAGIC     useStringForDate = false) // true to replace DateType with StringType
# MAGIC
# MAGIC // Generate data
# MAGIC tables.genData(
# MAGIC     location = rootDir,
# MAGIC     format = fileFormat,
# MAGIC     overwrite = true, // overwrite the data that is already there
# MAGIC     partitionTables = false, // create the partitioned fact tables 
# MAGIC     clusterByPartitionColumns = false, // shuffle to get partitions coalesced into single files. 
# MAGIC     filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
# MAGIC     tableFilter = tableName, // "" means generate all tables
# MAGIC     numPartitions = 32) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "1000" // scaleFactor defines the size of the dataset to generate (in GB).
# MAGIC val scaleFactoryInt = scaleFactor.toInt
# MAGIC
# MAGIC val scaleName = if(scaleFactoryInt < 1000){
# MAGIC     f"${scaleFactoryInt}%03d" + "GB"
# MAGIC   } else {
# MAGIC     f"${scaleFactoryInt / 1000}%03d" + "TB"
# MAGIC   }
# MAGIC
# MAGIC val fileFormat = "parquet" // valid spark file format like parquet, csv, json.
# MAGIC val tableName = "web_returns"
# MAGIC val rootDir = s"/mnt/datalake/raw/tpc-ds/source_files_${scaleName}_${fileFormat}/${tableName}"
# MAGIC val databaseName = "tpcds" + scaleName // name of database to create.
# MAGIC
# MAGIC // Run:
# MAGIC val tables = new TPCDSTables(sqlContext,
# MAGIC     dsdgenDir = "/usr/local/bin/tpcds-kit/tools", // location of dsdgen 
# MAGIC     scaleFactor = scaleFactor,
# MAGIC     useDoubleForDecimal = false, // true to replace DecimalType with DoubleType 
# MAGIC     useStringForDate = false) // true to replace DateType with StringType
# MAGIC
# MAGIC // Generate data
# MAGIC tables.genData(
# MAGIC     location = rootDir,
# MAGIC     format = fileFormat,
# MAGIC     overwrite = true, // overwrite the data that is already there
# MAGIC     partitionTables = false, // create the partitioned fact tables 
# MAGIC     clusterByPartitionColumns = false, // shuffle to get partitions coalesced into single files. 
# MAGIC     filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
# MAGIC     tableFilter = tableName, // "" means generate all tables
# MAGIC     numPartitions = 12) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "1000" // scaleFactor defines the size of the dataset to generate (in GB).
# MAGIC val scaleFactoryInt = scaleFactor.toInt
# MAGIC
# MAGIC val scaleName = if(scaleFactoryInt < 1000){
# MAGIC     f"${scaleFactoryInt}%03d" + "GB"
# MAGIC   } else {
# MAGIC     f"${scaleFactoryInt / 1000}%03d" + "TB"
# MAGIC   }
# MAGIC
# MAGIC val fileFormat = "parquet" // valid spark file format like parquet, csv, json.
# MAGIC val tableName = "web_sales"
# MAGIC val rootDir = s"/mnt/datalake/raw/tpc-ds/source_files_${scaleName}_${fileFormat}/${tableName}"
# MAGIC val databaseName = "tpcds" + scaleName // name of database to create.
# MAGIC
# MAGIC // Run:
# MAGIC val tables = new TPCDSTables(sqlContext,
# MAGIC     dsdgenDir = "/usr/local/bin/tpcds-kit/tools", // location of dsdgen 
# MAGIC     scaleFactor = scaleFactor,
# MAGIC     useDoubleForDecimal = false, // true to replace DecimalType with DoubleType 
# MAGIC     useStringForDate = false) // true to replace DateType with StringType
# MAGIC
# MAGIC // Generate data
# MAGIC tables.genData(
# MAGIC     location = rootDir,
# MAGIC     format = fileFormat,
# MAGIC     overwrite = true, // overwrite the data that is already there
# MAGIC     partitionTables = false, // create the partitioned fact tables 
# MAGIC     clusterByPartitionColumns = false, // shuffle to get partitions coalesced into single files. 
# MAGIC     filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
# MAGIC     tableFilter = tableName, // "" means generate all tables
# MAGIC     numPartitions = 90) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 