import multiprocessing
import time
import os
import redis

# Connect to Redis
redis_url = os.getenv('REDIS_URL')
if redis_url is None:
    raise Exception("Please set the REDIS_URL environment variable")
r = redis.Redis.from_url(redis_url)

# Define the worker function
def worker(laptop_label):
    while True:
        # Check if there's a task in the processing queue
        task = r.zrange('task_queue', 0, 0)
        if not task:
            break
        task = task[0]

        # Check if the task is already processed
        task_state = r.get(task)
        if task_state == b'processed':
            # Remove the task from the task queue
            r.zrem('task_queue', task)
            continue
