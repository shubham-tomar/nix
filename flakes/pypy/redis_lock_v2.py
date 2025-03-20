import redis
import time
import uuid
import logging
import sys
from functools import wraps

# Create a custom logger for this module with minimal logging
logger = logging.getLogger('redis_lock')
logger.setLevel(logging.INFO)

# Create a console handler that outputs to stdout
if not logger.handlers:
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Prevent log propagation to avoid duplicate logs
    logger.propagate = False

class RedisQueueLock:
    """
    A distributed locking mechanism using Redis sorted sets as a queue.
    This ensures jobs are executed in the order they are submitted.
    """
    
    def __init__(self, redis_host='localhost', redis_port=6379, redis_db=0, 
                 lock_name='spark_job_queue', lock_timeout=300):
        """
        Initialize the RedisQueueLock with connection details and lock parameters.
        
        Args:
            redis_host (str): Redis server hostname
            redis_port (int): Redis server port
            redis_db (int): Redis database number
            lock_name (str): Name of the lock queue (sorted set) in Redis
            lock_timeout (int): Lock timeout in seconds
        """
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
        self.lock_name = lock_name
        self.lock_timeout = lock_timeout
        self.lock_id = str(uuid.uuid4())
        self.current_lock = None
        
    def is_leader(self, job_name):
        """
        Check if the current job is at the front of the queue (first in line).
        
        Args:
            job_name (str): Name of the job to check
            
        Returns:
            bool: True if job is first in queue, False otherwise
        """
        lock_key = f"{job_name}-{self.lock_id}"
        
        # Get the first job in the queue
        first_job = self.redis_client.zrange(self.lock_name, 0, 0)
        
        # Check if our job is the first in line
        return first_job and first_job[0].decode('utf-8') == lock_key
        
    def add_to_queue(self, job_name):
        """
        Add a job to the queue with a timestamp-based score.
        
        Args:
            job_name (str): Name of the job to queue
            
        Returns:
            bool: True if added successfully, False otherwise
        """
        lock_key = f"{job_name}-{self.lock_id}"
        # Use nanosecond precision to ensure unique ordering
        score = time.time_ns()
        
        # First, clean up expired locks
        self._clean_expired_locks()
        
        # Add job to the queue with its timestamp score
        added = self.redis_client.zadd(self.lock_name, {lock_key: score})
        
        if added:
            # Store current job information for future reference
            self.current_lock = lock_key
            logger.info(f"Job added to queue: '{lock_key}', position score: {score}")
            return True
        else:
            logger.info(f"Failed to add job to queue: '{lock_key}'")
            return False
            
    def remove_from_queue(self, job_name):
        """
        Remove a job from the queue.
        
        Args:
            job_name (str): Name of the job to remove
            
        Returns:
            bool: True if removed successfully, False otherwise
        """
        lock_key = f"{job_name}-{self.lock_id}"
        self.current_lock = lock_key
        
        # Remove job from the queue
        removed = self.redis_client.zrem(self.lock_name, lock_key)
        
        if removed:
            logger.info(f"Job removed from queue: '{lock_key}'")
            return True
        else:
            return False
            
    def wait_for_turn(self, job_name, retry_interval=1, max_retries=30):
        """
        Wait until this job is at the front of the queue.
        
        Args:
            job_name (str): Name of the job waiting for its turn
            retry_interval (int): Seconds to wait between checks
            max_retries (int): Maximum number of retries, None means infinite
            
        Returns:
            bool: True if job becomes the leader, False if max retries exceeded
        """
        lock_key = f"{job_name}-{self.lock_id}"
        retry_count = 0
        
        # First, clean expired locks more aggressively
        self._clean_expired_locks()
        
        while True:
            # Check if we're first in line
            if self.is_leader(job_name):
                position_score = self.redis_client.zscore(self.lock_name, lock_key)
                logger.info(f"Job is now first in queue: '{lock_key}', position score: {position_score}")
                return True
                
            # Get our position in queue
            position = self.redis_client.zrank(self.lock_name, lock_key)
            if position is None:
                logger.warning(f"Job not found in queue: '{lock_key}'")
                return False
                
            # Get the leader (job ahead of us)
            leader = self.redis_client.zrange(self.lock_name, 0, 0, withscores=True)
            if leader:
                leader_key = leader[0][0].decode('utf-8')
                leader_score = leader[0][1]
                logger.info(f"Waiting for job's turn, current leader: '{leader_key}', position: {position+1}, score: {leader_score}")
                
                # Check if leader is potentially stuck/abandoned
                # If we're not making progress after half our retries, try to clean
                if retry_count > max_retries // 2 and retry_count % 5 == 0:
                    self._clean_expired_locks()
            
            retry_count += 1
            if max_retries is not None and retry_count >= max_retries:
                logger.warning(f"Exceeded maximum wait retries ({max_retries}) for job: '{lock_key}'")
                # Remove the job from the queue to avoid orphaned entries
                self.remove_from_queue(job_name)
                logger.warning(f"Removed job '{job_name}' from queue after exceeding max retries")
                return False
                
            time.sleep(retry_interval)
    
    def _clean_expired_locks(self):
        """
        Clean up expired entries from the queue.
        Uses job timeout to determine expiration.
        Avoids removing jobs at the front of the queue (leader) that might still be running.
        """
        current_time = time.time()
        oldest_valid_time = current_time - self.lock_timeout
        oldest_valid_score = oldest_valid_time * 1_000_000_000  # Convert to nanoseconds
        
        # Get all queue entries
        all_jobs = self.redis_client.zrange(self.lock_name, 0, -1, withscores=True)
        
        if not all_jobs:
            return
        
        # Get the current leader (first job in queue)
        leader = self.redis_client.zrange(self.lock_name, 0, 0)
        leader_key = leader[0].decode('utf-8') if leader else None
            
        # Find expired jobs (older than timeout)
        expired_jobs = []
        for job_data, score in all_jobs:
            job_key = job_data.decode('utf-8')
            
            # For older implementations that might not use nanoseconds
            # We check if score is smaller than current time in seconds
            # This assumes scores from old version used seconds
            if score < oldest_valid_score and score > current_time:
                continue  # Skip, likely using nanosecond timing
                
            # Special handling for leader - we don't expire the current leader
            # even if it's past timeout to avoid removing running jobs
            if job_key == leader_key:
                logger.info(f"Not expiring leader job: '{job_key}' even though it's old")
                continue
                
            if score < oldest_valid_time:
                expired_jobs.append(job_data)
                logger.info(f"Marking as expired: '{job_key}'")
        
        # Remove expired jobs
        if expired_jobs:
            removed = self.redis_client.zrem(self.lock_name, *expired_jobs)
            if removed > 0:
                logger.info(f"Removed {removed} expired job(s) from queue")

def clear_queue(redis_host='localhost', redis_port=6379, redis_db=0, lock_name='spark_job_lock'):
    """
    Helper function to clear all jobs from the queue.
    Useful for cleaning up after failed runs or when starting fresh.
    
    Args:
        redis_host (str): Redis server hostname
        redis_port (int): Redis server port
        redis_db (int): Redis database number
        lock_name (str): Name of the lock queue in Redis
        
    Returns:
        int: Number of jobs removed from the queue
    """
    try:
        client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
        # Get all jobs in the queue
        jobs = client.zrange(lock_name, 0, -1, withscores=True)
        
        if not jobs:
            print("Queue is already empty")
            return 0
            
        # Remove all jobs from the queue
        removed = client.delete(lock_name)
        print(f"Cleared {removed} jobs from queue '{lock_name}'")
        return removed
    except Exception as e:
        print(f"Error clearing queue: {e}")
        return 0

def print_current_queue(redis_host='localhost', redis_port=6379, redis_db=0, lock_name='spark_job_lock'):
    """
    Helper function to print the current state of the job queue in Redis.
    
    Args:
        redis_host (str): Redis server hostname
        redis_port (int): Redis server port
        redis_db (int): Redis database number
        lock_name (str): Name of the lock queue in Redis
    """
    try:
        client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
        jobs = client.zrange(lock_name, 0, -1, withscores=True)
        
        print("\nCurrent job queue:")
        if not jobs:
            print("Queue is empty")
        else:
            for i, (job_data, score) in enumerate(jobs):
                job_str = job_data.decode('utf-8')
                print(f"Position {i+1}: '{job_str}', score: {score}")
        print()
    except Exception as e:
        print(f"Error getting queue status: {e}")

def with_redis_queue_lock(job_name, redis_host='localhost', redis_port=6379, redis_db=0, 
                    lock_name='spark_job_queue', lock_timeout=300, 
                    retry_interval=1, max_retries=None):
    """
    Decorator to apply Redis queue locking to a function.
    Ensures functions are executed in the order they are called.
    
    Args:
        job_name (str): Name of the job to queue
        redis_host (str): Redis server hostname
        redis_port (int): Redis server port
        redis_db (int): Redis database number
        lock_name (str): Name of the lock queue in Redis
        lock_timeout (int): Lock timeout in seconds
        retry_interval (int): Seconds to wait between queue position checks
        max_retries (int): Maximum number of retries, None means infinite
        
    Returns:
        function: Decorated function with Redis queue locking
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Create a descriptive job name if args or kwargs contain valuable information
            dynamic_job_name = job_name
            if args or kwargs:
                # Try to add more context to the job name based on args/kwargs
                try:
                    arg_str = ", ".join([str(arg) for arg in args[:2]] if args else [])
                    kwarg_str = ", ".join([f"{k}={v}" for k, v in list(kwargs.items())[:2]] if kwargs else [])
                    context = [arg_str, kwarg_str]
                    context = [c for c in context if c]  # Remove empty strings
                    if context:
                        dynamic_job_name = f"{job_name}({'; '.join(context)})"
                except Exception:
                    pass
            
            # Initialize lock and add job to queue
            lock = RedisQueueLock(redis_host, redis_port, redis_db, lock_name, lock_timeout)
            
            try:
                # Add job to the queue
                if not lock.add_to_queue(dynamic_job_name):
                    logger.error(f"Failed to add job to queue: '{dynamic_job_name}'")
                    return None
                    
                # Wait for our turn (until we're at the front of the queue)
                if not lock.wait_for_turn(dynamic_job_name, retry_interval, max_retries):
                    logger.error(f"Job '{dynamic_job_name}' never reached the front of the queue")
                    return None
                    
                # Execute the function now that we're at the front
                logger.info(f"Executing job '{dynamic_job_name}'")
                result = func(*args, **kwargs)
                return result
                
            except Exception as e:
                logger.error(f"Error during execution: {e}")
                raise
            finally:
                # Always remove job from queue when done
                lock.remove_from_queue(dynamic_job_name)
                
        return wrapper
    return decorator
