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
PATH_A = "/Users/shubhamtomar/pixis-projects/aalearning/nix/flakes/kafka/kafka-flink-job-a/target/kafka-flink-job-a-1.0-SNAPSHOT.jar"
PATH_B = "/Users/shubhamtomar/pixis-projects/aalearning/nix/flakes/kafka/kafka-flink-job-b/target/kafka-flink-job-b-1.0-SNAPSHOT.jar"
ENV = os.environ.copy()
ENV["FLINK_CONF_DIR"] = "/Users/shubhamtomar/Downloads/flink-1.20.0/conf"
ENV["AWS_ACCESS_KEY_ID"] = "admin"
ENV["AWS_SECRET_ACCESS_KEY"] = "password"
ENV["AWS_REGION"] = "us-east-1"
ENV["AWS_DEFAULT_REGION"] = "us-east-1"
FLINK_BIN = "./bin/flink"

@task
@with_redis_queue_lock(
        job_name=f"flink_",
        redis_host="localhost",
        redis_port=6379,
        lock_name="flink_job_lock",  # Using the same lock name ensures only one job runs at a time
        lock_timeout=300,
        retry_interval=2,
        max_retries=30
    )
def start_flink_job(table_name: str = "test_1", branch: str = "dummy", counter: str = "1", path: str = PATH_A):
    """Start the Flink job and capture the JOB ID."""
    sleep_time = random.randint(2, 7)
    print(f"Sleeping for {sleep_time} seconds before starting the Flink job...")
    time.sleep(sleep_time)

    print("Starting Flink job...")
    process = subprocess.Popen(
        [FLINK_BIN, "run", path, table_name, branch, counter],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=ENV
    )

    job_id = None
    for line in process.stdout:
        print(line.strip())
        if "Job has been submitted with JobID" in line:
            job_id = line.split()[-1]
            break

    if not job_id:
        raise RuntimeError("Failed to extract JOB ID from Flink output.")

    print(f"Flink job started with JOB ID: {job_id}")
    return job_id

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

@flow
def run_flink_concurrent():
    start_flink_job.submit("test_1", "dummy", "1", PATH_A)
    start_flink_job.submit("test_1", "dummy", "2", PATH_B)
    
    # Uncomment these lines if you want to automatically cancel jobs after they run
    # stop_flink_job(job_id_a)
    # stop_flink_job(job_id_b)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run Flink job concurrently using Prefect')
    parser.add_argument('--run-mode', choices=['flink-only', 'full-flow'], 
                      default='flink-only', help='Execution mode (default: flink-only)')
    parser.add_argument('--clear-queue', action='store_true', 
                      help='Clear the Redis queue before running')
    args = parser.parse_args()
    
    if args.clear_queue:
        print("Clearing Redis queue...")
        clear_queue("flink_job_queue")
        print("Queue cleared.")
    
    print("Current queue status:")
    print_current_queue("flink_job_queue")
    
    run_flink_concurrent()


