import ray
import math
import time

# Initialize Ray
ray.init()

# Global variables to store the results
total_checked = 0
result_string = ''

# Define a function to check if a number is prime
@ray.remote
def is_prime(n):
    if n < 2:
        return 0  # 0 represents 'not prime'
    for i in range(2, int(math.sqrt(n)) + 1):
        if n % i == 0:
            return 1  # 1 represents 'checked and not prime'
    return 2  # 2 represents 'checked and prime'

# Define an actor class to handle a range of numbers
@ray.remote
class RangeWorker:
    def __init__(self, start, end):
        self.start = start
        self.end = end
        self.checked = 0
        self.result = ['0'] * (end - start + 1)

    def process_range(self):
        for i in range(self.start, self.end + 1):
            self.result[i - self.start] = str(ray.get(is_prime.remote(i)))
            self.checked += 1
        return self.checked, ''.join(self.result)

    def update_globals(self):
        global total_checked, result_string
        total_checked += self.checked
        result_string += ''.join(self.result)

# Function to divide the range and create workers
def check_primes(start, end):
    range_length = end - start + 1
    chunk_size = range_length // 3
    workers = [RangeWorker.remote(start + i * chunk_size, start + (i + 1) * chunk_size - 1) for i in range(3)]
    results = ray.get([worker.process_range.remote() for worker in workers])

    # Update global variables with the initial results
    global total_checked, result_string
    total_checked = sum(result[0] for result in results)
    result_string = ''.join(result[1] for result in results)

    # Update global variables every 5 seconds
    while True:
        ray.get([worker.update_globals.remote() for worker in workers])
        print(f"Total numbers checked: {total_checked}")
        print(f"Result string: {result_string}")
        time.sleep(5)

# Example usage
if __name__ == '__main__':
    start = int(input("Enter the start of the range: "))
    end = int(input("Enter the end of the range: "))
    check_primes(start, end)