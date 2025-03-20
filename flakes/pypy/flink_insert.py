#!/usr/bin/env python
"""
Simple PyFlink job to demonstrate connectivity
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
import os
import sys

def main():
    # Print Flink environment variables
    print(f"FLINK_CONF_DIR: {os.getenv('FLINK_CONF_DIR')}")
    print(f"FLINK_HOME: {os.getenv('FLINK_HOME')}")
    
    # Create execution environment
    print("Creating Stream Execution Environment...")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    # Create table environment
    print("Creating Table Environment...")
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = StreamTableEnvironment.create(env, settings)
    
    # Print available catalogs
    print("\nListing available catalogs:")
    catalogs = table_env.execute_sql("SHOW CATALOGS").collect()
    for catalog in catalogs:
        print(f"- {catalog}")
    
    # Create a simple in-memory source table
    print("\nCreating a temporary table...")
    table_env.execute_sql("""
    CREATE TEMPORARY TABLE source_table (
        id STRING,
        name STRING,
        value_amount INT
    ) WITH (
        'connector' = 'datagen',
        'rows-per-second' = '1',
        'fields.id.kind' = 'random',
        'fields.name.kind' = 'random',
        'fields.value_amount.kind' = 'random',
        'fields.value_amount.min' = '1',
        'fields.value_amount.max' = '100'
    )
    """)
    
    # Execute a simple query
    print("\nExecuting a simple query:")
    result = table_env.execute_sql("SELECT * FROM source_table LIMIT 5")
    print("\nQuery results:")
    print(result.get_table_schema())
    for row in result.collect():
        print(row)
    
    print("\nPyFlink job completed successfully")

if __name__ == "__main__":
    main()
