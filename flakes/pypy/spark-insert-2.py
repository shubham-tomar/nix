#!/usr/bin/env python3
"""
Spark script to interact with Nessie/Iceberg tables
This script can be run directly without using the pyspark shell command
"""

import os
import sys
import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext

def parse_args():
    parser = argparse.ArgumentParser(description='PySpark script to interact with Nessie/Iceberg tables')
    parser.add_argument('--ref', default='dummy', help='Nessie branch reference (default: dummy)')
    parser.add_argument('--table', default='test4', help='Table name to query (default: test)')
    parser.add_argument('--counter', default='1', help='counter (default: 1)')
    return parser.parse_args()

def main():
    args = parse_args()
    
    # Configuration
    CATALOG_URI = "http://localhost:19120/api/v1"  # Nessie Server URI
    WAREHOUSE = "s3a://warehouse/"                # MinIO Address (Use s3a://)
    STORAGE_URI = "http://127.0.0.1:9000"
    
    print(f"Starting Spark with Nessie branch: {args.ref}")
    
    # Configure Spark with necessary packages and Iceberg/Nessie settings
    conf = (
        pyspark.SparkConf()
            .setAppName('nessie_iceberg_app')
            # Include necessary packages
            .set('spark.jars.packages', 'org.slf4j:slf4j-api:2.0.7,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.102.5,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,software.amazon.awssdk:bundle:2.24.8,software.amazon.awssdk:url-connection-client:2.24.8')
            # Add maven repositories
            .set('spark.jars.repositories', 'https://repo1.maven.org/maven2/,https://packages.confluent.io/maven/')
            # Enable Iceberg and Nessie extensions
            .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions')
            # Configure Nessie catalog
            .set('spark.sql.catalog.nessie', 'org.apache.iceberg.spark.SparkCatalog')
            .set('spark.sql.catalog.nessie.uri', CATALOG_URI)
            .set('spark.sql.catalog.nessie.ref', args.ref)  # Use the branch from arguments
            .set('spark.sql.catalog.nessie.authentication.type', 'NONE')
            .set('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
            # Set MinIO as the S3 endpoint for Iceberg storage
            .set('spark.sql.catalog.nessie.warehouse', WAREHOUSE)
            .set('spark.sql.catalog.nessie.s3.endpoint', STORAGE_URI)
            .set('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
            # S3A Specific Configuration (Fixes No FileSystem for scheme "s3")
            .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .set("spark.hadoop.fs.s3a.endpoint", STORAGE_URI)  # MinIO S3 Endpoint
            .set("spark.hadoop.fs.s3a.access.key", "admin")
            .set("spark.hadoop.fs.s3a.secret.key", "password")
            .set("spark.hadoop.fs.s3a.path.style.access", "true")  # Required for MinIO
            .set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    )
    
    # Start Spark session
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    print("Spark Session Started")
    
    # Set additional Hadoop configurations
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", "admin")
    hadoop_conf.set("fs.s3a.secret.key", "password")
    hadoop_conf.set("fs.s3a.endpoint", STORAGE_URI)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.attempts.maximum", "1")
    hadoop_conf.set("fs.s3a.connection.establish.timeout", "5000")
    hadoop_conf.set("fs.s3a.connection.timeout", "10000")
    
    try:
        # Verify if Nessie tables are available
        print("\nAvailable tables in nessie.flink namespace:")
        spark.sql("SHOW TABLES IN nessie.flink").show()
        
        # Query the specified table
        table_name = f"nessie.flink.{args.table}"
        print(f"\nCounting records in {table_name}:")
        spark.sql(f"SELECT COUNT(*) FROM {table_name}").show()

        print(f'Inserting 1 row')
        spark.sql(f"INSERT INTO {table_name} (id, name, clicks, created_at, status) VALUES ('{args.counter}', 'Zack_raw_v{args.counter}', 23, '2025-03-06 15:52:00', 'TEST');")
        
        print(f"\nSample data from {table_name}:")
        spark.sql(f"SELECT * FROM {table_name} LIMIT 10").show()

        print(f"\nCounting records in {table_name}:")
        spark.sql(f"SELECT COUNT(*) FROM {table_name}").show()
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Clean up
        spark.stop()

if __name__ == "__main__":
    main()

