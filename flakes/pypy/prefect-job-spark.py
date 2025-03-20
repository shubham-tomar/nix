from prefect import flow, task
import random
import subprocess
import sys
import os
import time
import logging
import signal
import psutil
import argparse
# from redis_lock import RedisLock, with_redis_lock, print_current_locks
from redis_lock_v2 import RedisQueueLock, with_redis_queue_lock, print_current_queue, clear_queue

os.chdir("/Users/shubhamtomar/Downloads/flink-1.20.0")
PATH = "/Users/shubhamtomar/pixis-projects/aalearning/nix/flakes/kafka/kafka-flink-job/target/kafka-flink-job-1.0-SNAPSHOT.jar"
ENV = os.environ.copy()
ENV["FLINK_CONF_DIR"] = "/Users/shubhamtomar/Downloads/flink-1.20.0/conf"
ENV["AWS_ACCESS_KEY_ID"] = "admin"
ENV["AWS_SECRET_ACCESS_KEY"] = "password"
ENV["AWS_REGION"] = "us-east-1"
ENV["AWS_DEFAULT_REGION"] = "us-east-1"

FLINK_HOME = "/Users/shubhamtomar/Downloads/flink-1.20.0"
FLINK_BIN = "./bin/flink"
FLINK_JOB_JAR = "/Users/shubhamtomar/pixis-projects/aalearning/nix/flakes/kafka/kafka-flink-job/target/kafka-flink-job-1.0-SNAPSHOT.jar"

@task
def start_flink_job(table_name: str = "kafka_table_v2"):
    """Start the Flink job and capture the JOB ID."""

    sleep_time = random.randint(5, 30)  # Random delay between 5 to 30 seconds
    print(f"Sleeping for {sleep_time} seconds before starting the Flink job...")
    time.sleep(sleep_time)

    print("Starting Flink job...")
    process = subprocess.Popen(
        [FLINK_BIN, "run", FLINK_JOB_JAR, table_name],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=ENV
    )

    # Extract JOB ID from output
    job_id = None
    for line in process.stdout:
        print(line.strip())  # Print Flink logs
        if "Job has been submitted with JobID" in line:
            job_id = line.split()[-1]  # Extract last word as JOB ID
            break

    if not job_id:
        raise RuntimeError("Failed to extract JOB ID from Flink output.")

    print(f"Flink job started with JOB ID: {job_id}")
    return job_id

@task
def monitor_flink_job(job_id):
    """Monitor the Flink job to ensure it's running."""
    print(f"Monitoring Flink job with JOB ID: {job_id}...")
    time.sleep(2500) # Wait for 41 min

@task
def stop_flink_job(job_id):
    """Cancel the Flink job using its JOB ID."""
    print(f"Cancelling Flink job with JOB ID: {job_id}...")
    
    process = subprocess.run([FLINK_BIN, "cancel", job_id],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True,
                            env=ENV)

    if process.returncode == 0:
        print("Flink job cancelled successfully.")
    else:
        print("Failed to cancel Flink job:", process.stderr)


@with_redis_queue_lock("my_job")
def my_function():
    # This function will only execute when it's first in the queue
    # Other jobs calling this function will wait in line
    pass

# Configure root logger to show all log levels
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# Create console handler that prints to stdout
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

# Add the handler to the root logger
root_logger.addHandler(console_handler)

# Set specific logger levels
logging.getLogger("redis_lock").setLevel(logging.INFO)

@task
def run_spark_1(table_name: str = "test1", branch: str = "dummy", counter: str = "1"):
    """Run Spark script 1 against Nessie tables with specified branch."""
    
    logger = logging.getLogger("spark_script_1")
    
    # Apply Redis queue lock using decorator pattern for cleaner code
    @with_redis_queue_lock(
        job_name=f"spark1_{table_name}_{branch}_{counter}",
        redis_host="localhost",
        redis_port=6379,
        lock_name="spark_job_lock",
        lock_timeout=300,
        retry_interval=2,
        max_retries=30
    )
    def run_spark_job():
        # Get the script path
        script_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                                'pypy', 'spark-insert.py')
        
        logger.info(f"Running Spark script 1 to query/insert to table {table_name} on branch {branch}")
        
        # Make script executable
        subprocess.run(["chmod", "+x", script_path], check=True)
        
        # Run the script with specified arguments
        process = subprocess.run(
            [sys.executable, script_path, "--ref", branch, "--table", table_name, "--counter", counter],
            capture_output=True,
            text=True
        )
        
        if process.returncode == 0:
            logger.info("Spark script 1 completed successfully")
            for line in process.stdout.splitlines():
                logger.info(f"SPARK1: {line}")
        else:
            logger.error("Spark script 1 failed:")
            for line in process.stderr.splitlines():
                logger.error(f"SPARK1_ERR: {line}")
        
        return process.returncode == 0
    
    # Execute the job with Redis locking
    return run_spark_job()

@task
def run_spark_2(table_name: str = "test2", branch: str = "dummy", counter: str = "1"):
    """Run Spark script 2 against Nessie tables with specified branch."""
    
    logger = logging.getLogger("spark_script_2")
    
    # Apply Redis queue lock using decorator pattern for cleaner code
    @with_redis_queue_lock(
        job_name=f"spark2_{table_name}_{branch}_{counter}",
        redis_host="localhost",
        redis_port=6379,
        lock_name="spark_job_lock",  # Using the same lock name ensures only one job runs at a time
        lock_timeout=300,
        retry_interval=2,
        max_retries=30
    )
    def run_spark_job():
        # Get the script path
        script_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                                'pypy', 'spark-insert-2.py')
        
        logger.info(f"Running Spark script 2 to query/insert to table {table_name} on branch {branch}")
        
        # Make script executable
        subprocess.run(["chmod", "+x", script_path], check=True)
        
        # Run the script with specified arguments
        process = subprocess.run(
            [sys.executable, script_path, "--ref", branch, "--table", table_name, "--counter", counter],
            capture_output=True,
            text=True
        )
        
        if process.returncode == 0:
            logger.info("Spark script 2 completed successfully")
            for line in process.stdout.splitlines():
                logger.info(f"SPARK2: {line}")
        else:
            logger.error("Spark script 2 failed:")
            for line in process.stderr.splitlines():
                logger.error(f"SPARK2_ERR: {line}")
        
        return process.returncode == 0
    
    # Execute the job with Redis locking
    return run_spark_job()

@flow
def flink_prefect_flow(table_name: str = "kafka_table_v2", branch: str = "dummy", counter: str = "1"):
    """Prefect Flow to Run and Cancel Flink Job and verify with concurrent Spark queries."""
    # job_id = start_flink_job(table_name)
    # monitor_flink_job(job_id)
    # stop_flink_job(job_id)
    
    # Wait to ensure all data is committed
    # time.sleep(10)
    
    # Run both Spark scripts concurrently to verify/modify data in Nessie
    # The .submit() method runs the tasks asynchronously
    spark1_future = run_spark_1.submit(table_name="test1", branch=branch, counter=counter)
    spark2_future = run_spark_2.submit(table_name="test2", branch=branch, counter=counter)
    
    # Wait for both tasks to complete and get their results
    spark1_result = spark1_future.result()
    spark2_result = spark2_future.result()
    
    return {
        "flink_job_id": job_id, 
        "spark1_success": spark1_result,
        "spark2_success": spark2_result
    }

@flow
def run_spark_concurrent(counter, branch):
    # Print lock information directly to console
    print("\n" + "=" * 80)
    print(f"REDIS LOCK INFO: Running concurrent Spark jobs with lock 'spark_job_lock'")
    print(f"REDIS LOCK INFO: Job parameters - Branch: {branch}, Counter: {counter}")
    print("=" * 80 + "\n")
    
    # Clear any zombie jobs from previous runs
    clear_queue(redis_host='localhost', redis_port=6379, lock_name='spark_job_lock')
    
    # Check current lock status before starting
    print_current_queue(redis_host='localhost', redis_port=6379, lock_name='spark_job_lock')
    
    # The .submit() method runs the tasks asynchronously
    print(f"REDIS LOCK INFO: Submitting spark_job_1 for table 'test1' - Will wait for lock")
    spark1_future = run_spark_1.submit(table_name="test1", branch=branch, counter=counter)
    
    # Check lock status after first job is submitted
    # time.sleep(1)  # Brief pause to let the first job acquire the lock
    print_current_queue(redis_host='localhost', redis_port=6379, lock_name='spark_job_lock')
    
    print(f"REDIS LOCK INFO: Submitting spark_job_2 for table 'test2' - Will wait for lock")
    print(f"REDIS LOCK INFO: Jobs are submitted concurrently but will execute one at a time due to Redis lock")
    spark2_future = run_spark_2.submit(table_name="test2", branch=branch, counter=counter)

    # Wait for both tasks to complete and get their results
    print("\nREDIS LOCK INFO: Waiting for both jobs to complete...")
    spark1_result = spark1_future.result()
    print(f"REDIS LOCK INFO: spark_job_1 completed with result: {spark1_result}")
    
    # Check lock status after first job is completed
    print_current_queue(redis_host='localhost', redis_port=6379, lock_name='spark_job_lock')
    
    spark2_result = spark2_future.result()
    print(f"REDIS LOCK INFO: spark_job_2 completed with result: {spark2_result}")
    
    # Check final lock status
    print_current_queue(redis_host='localhost', redis_port=6379, lock_name='spark_job_lock')
    
    print("\n" + "=" * 80)
    print(f"REDIS LOCK INFO: All jobs completed - Results: spark_job_1={spark1_result}, spark_job_2={spark2_result}")
    print("=" * 80 + "\n")

    return {
        "spark1_success": spark1_result,
        "spark2_success": spark2_result
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run Spark jobs concurrently using Prefect')
    parser.add_argument('--counter', default='1', help='Counter value for data insertion (default: 1)')
    parser.add_argument('--run-mode', choices=['flink-and-spark', 'spark-only', 'full-flow'], 
                      default='spark-only', help='Execution mode (default: spark-only)')
    parser.add_argument('--branch', default='dummy', help='Nessie branch to use (default: dummy)')
    args = parser.parse_args()

    results = run_spark_concurrent(counter=args.counter, branch=args.branch)
    print(f"Concurrent execution results: {results}")

