import multiprocessing
import threading
import time
import os
import redis

# Connect to Redis
redis_url = 'rediss://default:AVNS_6NSpId2vs7NJtw1T1JB@redis-3e1b6ad1-shadabkalim375-47b0.a.aivencloud.com:11899'
r = redis.Redis.from_url(redis_url)

# Define the worker function
def worker():
    while True:
        task = r.lpop('task_queue')
        if task is None:
            break
        # Perform a long-running task
        for i in range(1, 200000001):  # Count from 1 to 20 million
            if i % 10000000 == 0:  # Print a message every million iterations
                print(f"Task {task}: Reached {i}")
        print(f"Finished task: {task}")
        # Save the state to Redis
        r.set(task, "processed")

# Define the scheduler function
def scheduler():
    # Put numbers from 1 to a million into the task queue
    for i in range(1, 1000001):
        r.rpush('task_queue', i)

if __name__ == "__main__":
    # Start the worker and scheduler in separate processes
    worker_process = multiprocessing.Process(target=worker)
    worker_process.start()

    scheduler_process = multiprocessing.Process(target=scheduler)
    scheduler_process.start()

    # Wait for the worker and scheduler to finish
    worker_process.join()
    scheduler_process.join()
