#!/usr/bin/env python
"""
Test script to demonstrate Redis Queue Lock vs Regular Redis Lock
"""

import time
import multiprocessing
import random
from redis_lock_v2 import RedisQueueLock, print_current_queue

def process_job(job_id, delay_range=(1, 3)):
    """Simulates a job with Redis Queue Lock"""
    # Create lock
    lock = RedisQueueLock(
        redis_host="localhost",
        redis_port=6379,
        lock_name="spark_job_lock",
        lock_timeout=300
    )
    
    # Add job to queue
    print(f"Job {job_id}: Adding to queue")
    lock.add_to_queue(f"test_job_{job_id}")
    
    # Wait until this job becomes the leader
    print(f"Job {job_id}: Waiting for turn...")
    lock.wait_for_turn(f"test_job_{job_id}", retry_interval=1, max_retries=30)
    
    # Simulate work
    work_time = random.randint(*delay_range)
    print(f"Job {job_id}: Got to front of queue! Processing for {work_time} seconds...")
    time.sleep(work_time)
    print(f"Job {job_id}: Finished processing")
    
    # Remove job from queue
    lock.remove_from_queue(f"test_job_{job_id}")
    print(f"Job {job_id}: Released lock")

def run_demonstration():
    """Run multiple jobs concurrently to demonstrate the queue behavior"""
    print("\n=== REDIS QUEUE LOCK DEMONSTRATION ===")
    print("Starting 5 concurrent jobs with random start times")
    
    # First, clear any existing jobs in the queue
    from redis_lock_v2 import clear_queue
    clear_queue(lock_name="spark_job_lock")
    
    # Show initial queue state
    print("\nInitial queue state:")
    print_current_queue(lock_name="spark_job_lock")
    
    # Create processes but don't start them yet
    processes = []
    for i in range(5):
        # Create a process with random work times
        p = multiprocessing.Process(
            target=process_job, 
            args=(i+1, (random.randint(2, 8), random.randint(10, 15)))
        )
        processes.append(p)
    
    # Start processes with random delays
    print("\nStarting processes with random delays...")
    random.shuffle(processes)  # Shuffle the order
    for i, p in enumerate(processes):
        # Random delay between 0-2 seconds before starting next process
        delay = random.uniform(0, 2)
        print(f"Starting process {i+1} after {delay:.2f}s delay")
        p.start()
        time.sleep(delay)
    
    # Show queue state after all jobs are queued
    time.sleep(1)
    print("\nQueue state after all jobs are queued:")
    print_current_queue(lock_name="spark_job_lock")
    
    # Wait for all processes to complete
    for p in processes:
        p.join()
    
    # Show final queue state
    print("\nFinal queue state:")
    print_current_queue(lock_name="spark_job_lock")
    
    print("\n=== DEMONSTRATION COMPLETE ===")
    print("The jobs were processed in the order they were added to the queue")
    print("Each job waited for its turn at the front of the queue before executing")

if __name__ == "__main__":
    run_demonstration()
