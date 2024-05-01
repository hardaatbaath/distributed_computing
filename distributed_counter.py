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
def worker(laptop_label, worker_index, total_workers):
    while True:
        # Get all tasks in the processing queue
        tasks = r.zrange('task_queue', 0, -1)
        if not tasks:
            break

        # Only process tasks that are assigned to this worker
        tasks = [task for i, task in enumerate(tasks) if i % total_workers == worker_index]

        for task in tasks:
            # Check if the task is already processed
            task_state = r.get(task)
            if task_state == b'processed':
                # Remove the task from the task queue
                r.zrem('task_queue', task)
                continue

            # Check if there's a checkpoint for this task
            checkpoint = r.get(f"{task}_checkpoint")
            if checkpoint is not None:
                start = int(checkpoint)
                if start < 200000000:
                    print(f"Laptop {laptop_label}, Resuming task {task} from {start}")
            else:
                start = 1

            # Perform a long-running task
            for i in range(start, 200000001):  # Count from start to 200 million
                if i % 10000000 == 0:  # Print a message every 10 million iterations
                    print(f"Laptop {laptop_label}, Task {task}: Reached {i}")
                    # Save a checkpoint to Redis
                    r.set(f"{task}_checkpoint", i)
            print(f"Laptop {laptop_label}, Finished task: {task}")
            # Save the state to Redis
            r.set(task, "processed")
            # Remove the task from the task queue
            r.zrem('task_queue', task)

# Define the scheduler function
def scheduler():
    # Put numbers from 1 to a million into the task queue
    for i in range(1, 1000001):
        r.zadd('task_queue', {i: i})

if __name__ == "__main__":
    # Get the laptop label from an environment variable
    laptop_label = os.getenv('LAPTOP_LABEL')
    if laptop_label is None:
        raise Exception("Please set the LAPTOP_LABEL environment variable")

    # Delete the 'task_queue' key from Redis
    r.delete('task_queue')

    # Start the scheduler and worker in separate processes
    scheduler_process = multiprocessing.Process(target=scheduler)
    scheduler_process.start()

    # Allow some time for the scheduler to add tasks to the queue
    time.sleep(1)

    # Start three worker processes
    total_workers = 3
    worker_processes = []
    for i in range(total_workers):
        worker_process = multiprocessing.Process(target=worker, args=(f"{laptop_label}_{i+1}", i, total_workers))
        worker_process.start()
        worker_processes.append(worker_process)

    # Wait for the worker and scheduler to finish
    for worker_process in worker_processes:
        worker_process.join()
    scheduler_process.join()