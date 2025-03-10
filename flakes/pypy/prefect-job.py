from prefect import flow, task
import random
import subprocess
import time
import signal
import psutil
import os

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

@flow
def flink_prefect_flow(table_name: str = "kafka_table_v2"):
    """Prefect Flow to Run and Cancel Flink Job."""
    job_id = start_flink_job(table_name)
    monitor_flink_job(job_id)
    stop_flink_job(job_id)

if __name__ == "__main__":
    # import sys
    # table_name = sys.argv[1] if len(sys.argv) > 1 else "kafka_table_v2"
    # flink_prefect_flow(table_name)

    jobs = []

    for i in range(2, 50):  # Loop from 1 to 300
        table_name = f"kafka_table_{i}"
        job_id = start_flink_job(table_name)
        jobs.append(job_id)
    
    time.sleep(1000)  # Sleep for 10 seconds

    for job_id in jobs:
        stop_flink_job(job_id)
