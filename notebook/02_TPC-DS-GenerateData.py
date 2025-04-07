-- Databricks notebook source
-- MAGIC %md
-- MAGIC The following cell generates the data.<br>
-- MAGIC Since it contains many parameters that control the process, please read the code carefully.<br><br>
-- MAGIC
-- MAGIC For example, if you run the cell as-is, it will generate the entire TPC-DS dataset with sf=100000 (all 24 tables).<br>
-- MAGIC If you want to generate a 1TB dataset, reduce sf to 1000.<br><br>
-- MAGIC
-- MAGIC The numPartitions parameter is especially important—it determines how many Parquet files will be created.<br>
-- MAGIC In some cases, specifying numPartitions for each table can be helpful later during the data loading process.<br>
-- MAGIC
-- MAGIC ▽If you want to do that, please refer to the following article（）<br>
-- MAGIC https://qiita.com/ReijiOtake/items/3d6bc52f4c78afee9269<br>
-- MAGIC ※Sorry, the following article is written in Japanese.
-- MAGIC If you would like to read it in English, please use your web browser's translation feature.
-- MAGIC Alternatively, by running the 03 notebook, you can generate each table so that each Parquet file is approximately 500MB in size.
Feel free to make use of it!


-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
-- MAGIC
-- MAGIC // Set:
-- MAGIC val scaleFactor = "100000" // scaleFactor defines the size of the dataset to generate (in GB).
-- MAGIC val scaleFactoryInt = scaleFactor.toInt
-- MAGIC
-- MAGIC val scaleName = if(scaleFactoryInt < 1000){
-- MAGIC     f"${scaleFactoryInt}%03d" + "GB"
-- MAGIC   } else {
-- MAGIC     f"${scaleFactoryInt / 1000}%03d" + "TB"
-- MAGIC   }
-- MAGIC
-- MAGIC val fileFormat = "parquet" // valid spark file format like parquet, csv, json.
-- MAGIC val rootDir = s"/mnt/datalake/raw/tpc-ds/source_files_${scaleName}_${fileFormat}"
-- MAGIC val databaseName = "tpcds" + scaleName // name of database to create.
-- MAGIC
-- MAGIC // Run:
-- MAGIC val tables = new TPCDSTables(sqlContext,
-- MAGIC     dsdgenDir = "/usr/local/bin/tpcds-kit/tools", // location of dsdgen 
-- MAGIC     scaleFactor = scaleFactor,
-- MAGIC     useDoubleForDecimal = false, // true to replace DecimalType with DoubleType 
-- MAGIC     useStringForDate = false) // true to replace DateType with StringType
-- MAGIC
-- MAGIC tables.genData(
-- MAGIC     location = rootDir,
-- MAGIC     format = fileFormat,
-- MAGIC     overwrite = true, // overwrite the data that is already there
-- MAGIC     partitionTables = false, // create the partitioned fact tables 
-- MAGIC     clusterByPartitionColumns = false, // shuffle to get partitions coalesced into single files. 
-- MAGIC     filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
-- MAGIC     tableFilter = "", // "" means generate all tables
-- MAGIC     numPartitions = 1000) // how many dsdgen partitions to run - number of input tasks.
