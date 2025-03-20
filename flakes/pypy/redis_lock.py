
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

class RedisLock:
    """
    A distributed locking mechanism using Redis sorted sets.
    This ensures that only one job can run at a time.
    """
    
    def __init__(self, redis_host='localhost', redis_port=6379, redis_db=0, 
                 lock_name='spark_job_lock', lock_timeout=300):
        """
        Initialize the RedisLock with connection details and lock parameters.
        
        Args:
            redis_host (str): Redis server hostname
            redis_port (int): Redis server port
            redis_db (int): Redis database number
            lock_name (str): Name of the lock (sorted set) in Redis
            lock_timeout (int): Lock timeout in seconds
        """
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
        self.lock_name = lock_name
        self.lock_timeout = lock_timeout
        self.lock_id = str(uuid.uuid4())
        
    def acquire_lock(self, job_name, blocking=True, retry_interval=1, max_retries=None):
        """
        Attempt to acquire a lock for the specified job.
        
        Args:
            job_name (str): Name of the job to lock
            blocking (bool): Whether to block until lock is acquired
            retry_interval (int): Seconds to wait between retries
            max_retries (int): Maximum number of retries, None means infinite
            
        Returns:
            bool: True if lock was acquired, False otherwise
        """
        current_time = time.time()
        retry_count = 0
        lock_key = f"{job_name}:{self.lock_id}"
        lock_expiry = current_time + self.lock_timeout
        
        # First, clean up expired locks
        self._clean_expired_locks()
        
        while True:
            # Use a Redis transaction to check and set the lock atomically
            # This prevents race conditions when multiple processes try to acquire locks simultaneously
            pipeline = self.redis_client.pipeline()
            
            # Check for existing locks - use watch to monitor the sorted set
            pipeline.watch(self.lock_name)
            active_locks = pipeline.zrange(self.lock_name, 0, -1, withscores=True).execute()[0]
            
            if not active_locks:
                # No active locks, try to acquire with a transaction
                try:
                    pipeline.multi()
                    pipeline.zadd(self.lock_name, {lock_key: lock_expiry})
                    # Execute the transaction
                    pipeline.execute()
                    # If we get here, we acquired the lock
                    logger.info(f"Lock acquired: '{lock_key}', value: {lock_expiry} ({time.ctime(lock_expiry)})")
                    return True
                except redis.exceptions.WatchError:
                    # Someone modified the sorted set between our watch and execute
                    # Another process likely acquired a lock first, retry
                    pass
            else:
                # Locks exist, unwatch to proceed
                pipeline.unwatch()
                
                if not blocking:
                    return False
                    
                retry_count += 1
                if max_retries is not None and retry_count >= max_retries:
                    return False
                    
                # Show which lock is blocking us
                existing_lock = active_locks[0][0].decode('utf-8')
                existing_lock_expiry = active_locks[0][1]
                logger.info(f"Waiting for lock, lock already exists: '{existing_lock}', expires at: {existing_lock_expiry} ({time.ctime(existing_lock_expiry)})")
                time.sleep(retry_interval)
                current_time = time.time()
                lock_expiry = current_time + self.lock_timeout
    
    def release_lock(self, job_name):
        """
        Release the lock for the specified job.
        
        Args:
            job_name (str): Name of the job to unlock
            
        Returns:
            bool: True if lock was released, False otherwise
        """
        lock_key = f"{job_name}:{self.lock_id}"
        self.current_lock = lock_key
        
        # Attempt to remove the lock
        removed = self.redis_client.zrem(self.lock_name, lock_key)
        
        if removed:
            logger.info(f"Lock released: '{self.current_lock}' from set '{self.lock_name}'")
            return True
        else:
            return False
    
    def _clean_expired_locks(self):
        """
        Clean up expired locks from the sorted set.
        """
        current_time = time.time()
        
        # Remove expired locks without verbose logging
        removed = self.redis_client.zremrangebyscore(self.lock_name, 0, current_time)
        
        # Only log if we actually removed something
        if removed > 0:
            logger.info(f"Removed {removed} expired lock(s)")

def print_current_locks(redis_host='localhost', redis_port=6379, redis_db=0, lock_name='spark_job_lock'):
    """
    Helper function to print the current state of locks in Redis.
    Useful for debugging and monitoring lock status.
    
    Args:
        redis_host (str): Redis server hostname
        redis_port (int): Redis server port
        redis_db (int): Redis database number
        lock_name (str): Name of the lock in Redis
    """
    try:
        client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
        locks = client.zrange(lock_name, 0, -1, withscores=True)
        
        print("\nCurrent locks:")
        if not locks:
            print("No active locks")
        else:
            for lock_data, expiry in locks:
                lock_str = lock_data.decode('utf-8')
                print(f"Lock: '{lock_str}' until {time.ctime(expiry)}")
        print()
    except Exception as e:
        print(f"Error getting lock status: {e}")

def with_redis_lock(job_name, redis_host='localhost', redis_port=6379, redis_db=0, 
                    lock_name='spark_job_lock', lock_timeout=300, blocking=True, 
                    retry_interval=1, max_retries=None):
    """
    Decorator to apply Redis locking to a function.
    
    Args:
        job_name (str): Name of the job to lock
        redis_host (str): Redis server hostname
        redis_port (int): Redis server port
        redis_db (int): Redis database number
        lock_name (str): Name of the lock in Redis
        lock_timeout (int): Lock timeout in seconds
        blocking (bool): Whether to block until lock is acquired
        retry_interval (int): Seconds to wait between retries
        max_retries (int): Maximum number of retries, None means infinite
        
    Returns:
        function: Decorated function with Redis locking
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
            
            lock = RedisLock(redis_host, redis_port, redis_db, lock_name, lock_timeout)
            start_time = time.time()
            
            try:
                if lock.acquire_lock(dynamic_job_name, blocking, retry_interval, max_retries):
                    # Lock acquired - function will execute
                    result = func(*args, **kwargs)
                    return result
                else:
                    # Failed to acquire lock
                    return None
            except Exception as e:
                logger.error(f"Error during execution: {e}")
                raise
            finally:
                lock.release_lock(dynamic_job_name)
        return wrapper
    return decorator
