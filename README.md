# Scalable Event Data Pipeline on AWS

A production-style event data pipeline built on AWS to process
high-volume e-commerce user activity streams using a Medallion
(Bronze--Silver--Gold) architecture. The system ingests semi-structured
event data, performs incremental transformations with AWS Glue (Spark),
and exposes analytics-ready datasets through Amazon Athena.

------------------------------------------------------------------------

## Overview

Modern applications generate large volumes of event data continuously.
This project simulates an e-commerce platform producing user interaction
events every five minutes and demonstrates how to transform raw logs
into a structured analytics dataset that supports business intelligence
queries efficiently.

The pipeline processes approximately 500K--750K events every 5 minutes
and converts raw JSON data into optimized Parquet format with
partitioning for scalable querying.

------------------------------------------------------------------------

## Architecture

The system follows a Medallion Architecture (Bronze → Silver → Gold):

Bronze Layer\
Raw gzipped JSON events stored in Amazon S3 with minute-level
partitions.

Silver Layer\
Cleaned and transformed data written as partitioned Parquet using AWS
Glue (Spark).

Gold Layer\
Analytics-ready dataset exposed through Amazon Athena for SQL querying.

High-Level Flow\
EventBridge → Lambda → S3 (Bronze) → AWS Glue → S3 (Silver/Parquet) →
Athena (Gold)

------------------------------------------------------------------------

## Tech Stack

-   AWS Lambda\
-   Amazon S3\
-   AWS Glue (Spark / PySpark)\
-   Amazon Athena\
-   AWS CloudFormation\
-   Python\
-   SQL

------------------------------------------------------------------------

## Repository Structure

event-data-pipeline-aws/ ├── README.md ├── architecture/ │ └──
architecture_diagram.png ├── infrastructure/ │ └──
capstone-starter-extended.cfn.yaml ├── etl/ │ └── glue_capstone_etl.py
├── queries/ │ └── queries.sql ├── data-generator/ │ ├──
event_generator.py │ └── generate_sample_data.py ├── sample-data/ │ └──
sample_events.jsonl.gz └── docs/ └── blog_post.pdf

------------------------------------------------------------------------

## Key Features

Incremental Processing\
Uses AWS Glue job bookmarks to process only newly arrived files,
avoiding reprocessing of historical data.

Partition Optimization\
Data is partitioned by year / month / day / hour to improve query
performance and reduce Athena scan costs.

Columnar Storage\
Raw JSON data is converted into Parquet format for compression and
faster analytical queries.

Infrastructure as Code\
AWS resources are provisioned using CloudFormation, enabling
reproducible deployments.

Schema Validation & Transformations\
The ETL job enforces schema consistency, parses timestamps, and derives
metrics such as revenue.

------------------------------------------------------------------------

## Analytical Queries Supported

1.  Conversion Funnel Analysis\
2.  Hourly Revenue Trends\
3.  Top Viewed Products\
4.  Category Performance Metrics\
5.  Daily User Activity Statistics

See queries/queries.sql for implementation.

------------------------------------------------------------------------

## Local Data Generation (Optional)

The repository includes scripts to simulate event data locally:

-   event_generator.py → Generates realistic event records\
-   generate_sample_data.py → Creates sample files for testing without
    AWS

These scripts help validate ETL logic before deploying to the cloud.

------------------------------------------------------------------------

## Deployment Instructions

1.  Deploy Infrastructure using CloudFormation\
2.  Upload Glue ETL script to the configured S3 location\
3.  Run the AWS Glue job\
4.  Query results using Amazon Athena

------------------------------------------------------------------------

## Design Decisions

-   Hour-level partitions instead of minute-level to avoid small file
    problems\
-   Explicit schema definitions instead of crawlers to prevent schema
    drift\
-   Parquet format for performance and cost efficiency\
-   Single analytics table to reduce complexity given one event stream

------------------------------------------------------------------------

## Validation

Pipeline correctness was validated by:

-   Running incremental Glue jobs multiple times\
-   Comparing record counts across runs\
-   Executing analytical SQL queries in Athena\
-   Inspecting partition discovery using MSCK REPAIR TABLE

------------------------------------------------------------------------

## Author

Sanya Katiyar\
MS Data Science, University of Washington

------------------------------------------------------------------------

## License

This project is for educational and portfolio purposes.

