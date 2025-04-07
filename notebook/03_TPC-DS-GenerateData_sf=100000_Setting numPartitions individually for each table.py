# Databricks notebook source
# DBTITLE 1,Generate TPC-DS data
# MAGIC %md
# MAGIC Generating data at larger scales can take hours to run, and you may want to run the notebook as a job.
# MAGIC
# MAGIC The cell below generates the data. Read the code carefully, as it contains many parameters to control the process. See the <a href="https://github.com/databricks/spark-sql-perf" target="_blank">Databricks spark-sql-perf repository README</a> for more information.

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "100000" // scaleFactor defines the size of the dataset to generate (in GB).
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
# MAGIC     numPartitions = 25000) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC call_center
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "100000" // scaleFactor defines the size of the dataset to generate (in GB).
# MAGIC val scaleFactoryInt = scaleFactor.toInt
# MAGIC
# MAGIC val scaleName = if(scaleFactoryInt < 1000){
# MAGIC     f"${scaleFactoryInt}%03d" + "GB"
# MAGIC   } else {
# MAGIC     f"${scaleFactoryInt / 1000}%03d" + "TB"
# MAGIC   }
# MAGIC
# MAGIC val fileFormat = "parquet" // valid spark file format like parquet, csv, json.
# MAGIC val tableName = "call_center"
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
# MAGIC     numPartitions = 10) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC catalog_page
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "100000" // scaleFactor defines the size of the dataset to generate (in GB).
# MAGIC val scaleFactoryInt = scaleFactor.toInt
# MAGIC
# MAGIC val scaleName = if(scaleFactoryInt < 1000){
# MAGIC     f"${scaleFactoryInt}%03d" + "GB"
# MAGIC   } else {
# MAGIC     f"${scaleFactoryInt / 1000}%03d" + "TB"
# MAGIC   }
# MAGIC
# MAGIC val fileFormat = "parquet" // valid spark file format like parquet, csv, json.
# MAGIC val tableName = "catalog_page"
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
# MAGIC     numPartitions = 10) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC customer_address
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "100000" // scaleFactor defines the size of the dataset to generate (in GB).
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
# MAGIC     numPartitions = 10) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC customer_demographics
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "100000" // scaleFactor defines the size of the dataset to generate (in GB).
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
# MAGIC     numPartitions = 10) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC date_dim
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "100000" // scaleFactor defines the size of the dataset to generate (in GB).
# MAGIC val scaleFactoryInt = scaleFactor.toInt
# MAGIC
# MAGIC val scaleName = if(scaleFactoryInt < 1000){
# MAGIC     f"${scaleFactoryInt}%03d" + "GB"
# MAGIC   } else {
# MAGIC     f"${scaleFactoryInt / 1000}%03d" + "TB"
# MAGIC   }
# MAGIC
# MAGIC val fileFormat = "parquet" // valid spark file format like parquet, csv, json.
# MAGIC val tableName = "date_dim"
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
# MAGIC     numPartitions = 10) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC household_demographics
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "100000" // scaleFactor defines the size of the dataset to generate (in GB).
# MAGIC val scaleFactoryInt = scaleFactor.toInt
# MAGIC
# MAGIC val scaleName = if(scaleFactoryInt < 1000){
# MAGIC     f"${scaleFactoryInt}%03d" + "GB"
# MAGIC   } else {
# MAGIC     f"${scaleFactoryInt / 1000}%03d" + "TB"
# MAGIC   }
# MAGIC
# MAGIC val fileFormat = "parquet" // valid spark file format like parquet, csv, json.
# MAGIC val tableName = "household_demographics"
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
# MAGIC     numPartitions = 10) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC income_band
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "100000" // scaleFactor defines the size of the dataset to generate (in GB).
# MAGIC val scaleFactoryInt = scaleFactor.toInt
# MAGIC
# MAGIC val scaleName = if(scaleFactoryInt < 1000){
# MAGIC     f"${scaleFactoryInt}%03d" + "GB"
# MAGIC   } else {
# MAGIC     f"${scaleFactoryInt / 1000}%03d" + "TB"
# MAGIC   }
# MAGIC
# MAGIC val fileFormat = "parquet" // valid spark file format like parquet, csv, json.
# MAGIC val tableName = "income_band"
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
# MAGIC     numPartitions = 10) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC item
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "100000" // scaleFactor defines the size of the dataset to generate (in GB).
# MAGIC val scaleFactoryInt = scaleFactor.toInt
# MAGIC
# MAGIC val scaleName = if(scaleFactoryInt < 1000){
# MAGIC     f"${scaleFactoryInt}%03d" + "GB"
# MAGIC   } else {
# MAGIC     f"${scaleFactoryInt / 1000}%03d" + "TB"
# MAGIC   }
# MAGIC
# MAGIC val fileFormat = "parquet" // valid spark file format like parquet, csv, json.
# MAGIC val tableName = "item"
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
# MAGIC     numPartitions = 10) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC promotion
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "100000" // scaleFactor defines the size of the dataset to generate (in GB).
# MAGIC val scaleFactoryInt = scaleFactor.toInt
# MAGIC
# MAGIC val scaleName = if(scaleFactoryInt < 1000){
# MAGIC     f"${scaleFactoryInt}%03d" + "GB"
# MAGIC   } else {
# MAGIC     f"${scaleFactoryInt / 1000}%03d" + "TB"
# MAGIC   }
# MAGIC
# MAGIC val fileFormat = "parquet" // valid spark file format like parquet, csv, json.
# MAGIC val tableName = "promotion"
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
# MAGIC     numPartitions = 10) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC reason
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "100000" // scaleFactor defines the size of the dataset to generate (in GB).
# MAGIC val scaleFactoryInt = scaleFactor.toInt
# MAGIC
# MAGIC val scaleName = if(scaleFactoryInt < 1000){
# MAGIC     f"${scaleFactoryInt}%03d" + "GB"
# MAGIC   } else {
# MAGIC     f"${scaleFactoryInt / 1000}%03d" + "TB"
# MAGIC   }
# MAGIC
# MAGIC val fileFormat = "parquet" // valid spark file format like parquet, csv, json.
# MAGIC val tableName = "reason"
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
# MAGIC     numPartitions = 10) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ship_mode
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "100000" // scaleFactor defines the size of the dataset to generate (in GB).
# MAGIC val scaleFactoryInt = scaleFactor.toInt
# MAGIC
# MAGIC val scaleName = if(scaleFactoryInt < 1000){
# MAGIC     f"${scaleFactoryInt}%03d" + "GB"
# MAGIC   } else {
# MAGIC     f"${scaleFactoryInt / 1000}%03d" + "TB"
# MAGIC   }
# MAGIC
# MAGIC val fileFormat = "parquet" // valid spark file format like parquet, csv, json.
# MAGIC val tableName = "ship_mode"
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
# MAGIC     numPartitions = 10) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC store
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "100000" // scaleFactor defines the size of the dataset to generate (in GB).
# MAGIC val scaleFactoryInt = scaleFactor.toInt
# MAGIC
# MAGIC val scaleName = if(scaleFactoryInt < 1000){
# MAGIC     f"${scaleFactoryInt}%03d" + "GB"
# MAGIC   } else {
# MAGIC     f"${scaleFactoryInt / 1000}%03d" + "TB"
# MAGIC   }
# MAGIC
# MAGIC val fileFormat = "parquet" // valid spark file format like parquet, csv, json.
# MAGIC val tableName = "store"
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
# MAGIC     numPartitions = 10) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC time_dim
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "100000" // scaleFactor defines the size of the dataset to generate (in GB).
# MAGIC val scaleFactoryInt = scaleFactor.toInt
# MAGIC
# MAGIC val scaleName = if(scaleFactoryInt < 1000){
# MAGIC     f"${scaleFactoryInt}%03d" + "GB"
# MAGIC   } else {
# MAGIC     f"${scaleFactoryInt / 1000}%03d" + "TB"
# MAGIC   }
# MAGIC
# MAGIC val fileFormat = "parquet" // valid spark file format like parquet, csv, json.
# MAGIC val tableName = "time_dim"
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
# MAGIC     numPartitions = 10) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC warehouse
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "100000" // scaleFactor defines the size of the dataset to generate (in GB).
# MAGIC val scaleFactoryInt = scaleFactor.toInt
# MAGIC
# MAGIC val scaleName = if(scaleFactoryInt < 1000){
# MAGIC     f"${scaleFactoryInt}%03d" + "GB"
# MAGIC   } else {
# MAGIC     f"${scaleFactoryInt / 1000}%03d" + "TB"
# MAGIC   }
# MAGIC
# MAGIC val fileFormat = "parquet" // valid spark file format like parquet, csv, json.
# MAGIC val tableName = "warehouse"
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
# MAGIC     numPartitions = 10) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC web_page
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "100000" // scaleFactor defines the size of the dataset to generate (in GB).
# MAGIC val scaleFactoryInt = scaleFactor.toInt
# MAGIC
# MAGIC val scaleName = if(scaleFactoryInt < 1000){
# MAGIC     f"${scaleFactoryInt}%03d" + "GB"
# MAGIC   } else {
# MAGIC     f"${scaleFactoryInt / 1000}%03d" + "TB"
# MAGIC   }
# MAGIC
# MAGIC val fileFormat = "parquet" // valid spark file format like parquet, csv, json.
# MAGIC val tableName = "web_page"
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
# MAGIC     numPartitions = 10) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC web_site
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "100000" // scaleFactor defines the size of the dataset to generate (in GB).
# MAGIC val scaleFactoryInt = scaleFactor.toInt
# MAGIC
# MAGIC val scaleName = if(scaleFactoryInt < 1000){
# MAGIC     f"${scaleFactoryInt}%03d" + "GB"
# MAGIC   } else {
# MAGIC     f"${scaleFactoryInt / 1000}%03d" + "TB"
# MAGIC   }
# MAGIC
# MAGIC val fileFormat = "parquet" // valid spark file format like parquet, csv, json.
# MAGIC val tableName = "web_site"
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
# MAGIC     numPartitions = 10) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC customer

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "100000" // scaleFactor defines the size of the dataset to generate (in GB).
# MAGIC val scaleFactoryInt = scaleFactor.toInt
# MAGIC
# MAGIC val scaleName = if(scaleFactoryInt < 1000){
# MAGIC     f"${scaleFactoryInt}%03d" + "GB"
# MAGIC   } else {
# MAGIC     f"${scaleFactoryInt / 1000}%03d" + "TB"
# MAGIC   }
# MAGIC
# MAGIC val fileFormat = "parquet" // valid spark file format like parquet, csv, json.
# MAGIC val tableName = "customer"
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
# MAGIC     numPartitions = 5) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC inventory

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "100000" // scaleFactor defines the size of the dataset to generate (in GB).
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
# MAGIC     numPartitions = 10) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC web_returns
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "100000" // scaleFactor defines the size of the dataset to generate (in GB).
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
# MAGIC     numPartitions = 400) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC store_returns
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "100000" // scaleFactor defines the size of the dataset to generate (in GB).
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
# MAGIC     numPartitions = 800) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC catalog_returns
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "100000" // scaleFactor defines the size of the dataset to generate (in GB).
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
# MAGIC     numPartitions = 1000) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC web_sales
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "100000" // scaleFactor defines the size of the dataset to generate (in GB).
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
# MAGIC     numPartitions = 5000) // how many dsdgen partitions to run - number of input tasks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC catalog_sales

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC
# MAGIC // Set:
# MAGIC val scaleFactor = "100000" // scaleFactor defines the size of the dataset to generate (in GB).
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
# MAGIC     numPartitions = 10000) // how many dsdgen partitions to run - number of input tasks.
# MAGIC