# tpc-ds-dataset-generator-2025-ver-How-to-Unload-Data-to-a-Data-Lake-Using-Databricks-

# TPC-DS Dataset Generator 2025: How to Unload Data to a Data Lake Using Databricks

✅ I successfully generated a **TPC-DS dataset at scale factor 100,000 (i.e., 100TB)** using this notebook.

---

## Overview

This project is a refined and updated version of the excellent work by [BlueGranite](https://github.com/BlueGranite/tpc-ds-dataset-generator).

While their repository served as an invaluable reference, I encountered a few challenges when using it as-is, mainly due to the passage of time and some expired configurations. This notebook was created to address those issues and provide a more seamless experience for generating TPC-DS datasets on Databricks.

---

## Key Differences and Improvements

- 🚀 **Init Script Location**  
  The original DBFS-based init script had expired. In this version, the init script is uploaded and registered in the Databricks Workspace for longer-term stability and clarity.

- ⚙️ **Cluster Configuration Guidance**  
  This notebook includes a sample cluster configuration that successfully handled `sf=100000`, which is not included in the original reference.

- 📦 **Per-table numPartitions Configuration**  
  To optimize data loading and performance, each table's `numPartitions` is adjusted so that each generated Parquet file is approximately **500MB**.  
  If you're interested in why this matters or how to customize it, see my Qiita article linked below.

---

## Acknowledgments

🙏 I have deep respect for [cjkoester](https://github.com/cjkoester), who authored the original notebook.  
Thanks to their work, I was able to successfully generate and work with TPC-DS datasets on Databricks.

---

## Further Reading

📝 These articles (written in Japanese) provide further details and context about the process:

- [TPC-DS データ生成の実践（sf100000）](https://qiita.com/Reiji
