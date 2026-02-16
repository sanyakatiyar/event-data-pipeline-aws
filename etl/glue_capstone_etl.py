import sys
import time

import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql.functions import (
    col,
    to_timestamp,
    date_format,
    when,
    trim,
    lower
)

def parse_s3_path(s3_path: str):
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path: {s3_path}")
    no_scheme = s3_path[5:]
    bucket, _, key = no_scheme.partition("/")
    return bucket, key

def build_athena_output_from_output_path(output_path: str) -> str:
    bucket, _ = parse_s3_path(output_path)
    return f"s3://{bucket}/athena-results/"

def run_athena_query(
    athena_client,
    query: str,
    result_location: str,
    database: str = None,
    poll_interval: int = 3
):
    print(f"\n[ATHENA] Running query (db={database}):\n{query}\n")

    params = {
        "QueryString": query,
        "ResultConfiguration": {"OutputLocation": result_location},
        "WorkGroup": "primary"
    }

    # Only set QueryExecutionContext when a database is provided
    if database:
        params["QueryExecutionContext"] = {
            "Database": database
        }

    response = athena_client.start_query_execution(**params)

    qid = response["QueryExecutionId"]
    while True:
        status = athena_client.get_query_execution(QueryExecutionId=qid)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            print(f"[ATHENA] Query {qid} finished with state: {state}")
            if state != "SUCCEEDED":
                reason = status["QueryExecution"]["Status"].get("StateChangeReason", "")
                raise RuntimeError(f"Athena query failed: {state} - {reason}")
            break
        time.sleep(poll_interval)

    return qid


args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "INPUT_PATH", "OUTPUT_PATH", "STUDENT_ID"]
)

input_path = args["INPUT_PATH"].rstrip("/")
output_path = args["OUTPUT_PATH"].rstrip("/") + "/"
student_id_raw = args["STUDENT_ID"]

student_id = student_id_raw.replace("-", "_")

staging_db = f"capstone_{student_id}_staging_db"
analytics_db = f"capstone_{student_id}_db"
staging_table = "events_raw"
analytics_table = "events_parquet"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

print(f"Reading raw events from: {input_path}")
print(f"Writing transformed events to: {output_path}")
print(f"Student ID (sanitized): {student_id}")
print(f"Staging DB: {staging_db}, Analytics DB: {analytics_db}")

raw_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [input_path],
        "recurse": True
    },
    transformation_ctx="read_raw_events"
)

print(f"Raw DynamicFrame count (before toDF): {raw_dyf.count()}")

df = raw_dyf.toDF()
df.printSchema()

df = df.where(
    col("timestamp").isNotNull()
    & col("user_id").isNotNull()
    & col("session_id").isNotNull()
    & col("event_type").isNotNull()
)

df = (
    df
    .withColumn("event_type", lower(trim(col("event_type"))))
    .withColumn("category", trim(col("category")))
)

df = df.withColumn("event_ts", to_timestamp(col("timestamp")))

df = (
    df
    .withColumn("event_date", date_format(col("event_ts"), "yyyy-MM-dd"))
    .withColumn("year", date_format(col("event_ts"), "yyyy"))
    .withColumn("month", date_format(col("event_ts"), "MM"))
    .withColumn("day", date_format(col("event_ts"), "dd"))
    .withColumn("hour", date_format(col("event_ts"), "HH"))
)

df = df.withColumn("quantity", col("quantity").cast("int"))
df = df.withColumn("price", col("price").cast("double"))

df = df.withColumn(
    "quantity",
    when(col("quantity") < 0, None).otherwise(col("quantity"))
)
df = df.withColumn(
    "price",
    when(col("price") < 0, None).otherwise(col("price"))
)

df = df.withColumn(
    "revenue",
    when(col("event_type") == "purchase", col("price") * col("quantity"))
    .otherwise(None)
)

df.printSchema()
df.show(5, truncate=False)

output_dyf = DynamicFrame.fromDF(df, glueContext, "output_events")

print(f"Writing Parquet to: {output_path}")

glueContext.write_dynamic_frame.from_options(
    frame=output_dyf,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": output_path,
        "partitionKeys": ["year", "month", "day", "hour"]
    },
    transformation_ctx="write_events_parquet"
)

print("Parquet write completed")

region = "us-west-2"
athena = boto3.client("athena", region_name=region)
athena_results = build_athena_output_from_output_path(output_path)

staging_location = input_path.rstrip("/") + "/"
analytics_location = output_path

print(f"Athena results location: {athena_results}")
print(f"Staging table LOCATION: {staging_location}")
print(f"Analytics table LOCATION: {analytics_location}")

create_staging_db_sql = f"""
CREATE DATABASE IF NOT EXISTS {staging_db}
"""

run_athena_query(
    athena_client=athena,
    query=create_staging_db_sql,
    result_location=athena_results
)

create_staging_table_sql = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {staging_db}.{staging_table} (
  timestamp    string,
  user_id      string,
  session_id   string,
  event_type   string,
  product_id   string,
  quantity     int,
  price        double,
  category     string,
  search_query string
)
PARTITIONED BY (
  year   string,
  month  string,
  day    string,
  hour   string,
  minute string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'true'
)
LOCATION '{staging_location}'
TBLPROPERTIES (
  'has_encrypted_data' = 'false'
)
"""

run_athena_query(
    athena_client=athena,
    query=create_staging_table_sql,
    result_location=athena_results,
    database=staging_db
)

msck_staging_sql = f"MSCK REPAIR TABLE {staging_db}.{staging_table}"

run_athena_query(
    athena_client=athena,
    query=msck_staging_sql,
    result_location=athena_results,
    database=staging_db
)

create_analytics_db_sql = f"""
CREATE DATABASE IF NOT EXISTS {analytics_db}
"""

run_athena_query(
    athena_client=athena,
    query=create_analytics_db_sql,
    result_location=athena_results
)

create_analytics_table_sql = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {analytics_db}.{analytics_table} (
  event_ts      timestamp,
  event_date    date,
  user_id       string,
  session_id    string,
  event_type    string,
  product_id    string,
  quantity      int,
  price         double,
  category      string,
  search_query  string,
  revenue       double
)
PARTITIONED BY (
  year   string,
  month  string,
  day    string,
  hour   string
)
STORED AS PARQUET
LOCATION '{analytics_location}'
"""

run_athena_query(
    athena_client=athena,
    query=create_analytics_table_sql,
    result_location=athena_results,
    database=analytics_db
)

msck_analytics_sql = f"MSCK REPAIR TABLE {analytics_db}.{analytics_table}"

run_athena_query(
    athena_client=athena,
    query=msck_analytics_sql,
    result_location=athena_results,
    database=analytics_db
)

print("Athena databases, tables, and partitions are set up")

job.commit()
print("Glue job completed successfully")
