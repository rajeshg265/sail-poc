#!/usr/bin/env python3
import os, time, glob

os.environ["SAIL_MODE"] = "local"
os.environ["SAIL_CATALOG__LIST"] = '[{name="memory_catalog",type="memory",initial_database=["default"]}]'
os.environ["SAIL_CATALOG__DEFAULT_CATALOG"] = "memory_catalog"

from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession

srv = SparkConnectServer(port=50056)
srv.start(background=True)
time.sleep(3)

spark = (SparkSession.builder
    .remote("sc://localhost:50056")
    .config("spark.sql.warehouse.dir", "memory:///warehouse")
    .appName("test")
    .getOrCreate())

print("Step 1: CREATE TABLE ...", flush=True)
spark.sql("CREATE TABLE memory_catalog.default.frtb_test AS SELECT 42 AS x, 'hello' AS y")
print("Step 1: done", flush=True)

files = glob.glob("/Users/rajeshganta/work/sail_parquet/spark-warehouse/frtb_test*/")
print(f"Disk files: {files}", flush=True)

print("Step 2: DESCRIBE ...", flush=True)
for row in spark.sql("DESCRIBE EXTENDED memory_catalog.default.frtb_test").collect():
    if row['col_name'] in ('Location', 'Provider', 'Type'):
        print(f"  {row['col_name']}: {row['data_type']}", flush=True)
print("Step 2: done", flush=True)

print("Step 3: SELECT ...", flush=True)
result = spark.sql("SELECT * FROM memory_catalog.default.frtb_test").collect()
print(f"Step 3: done — {result}", flush=True)

srv.stop()
