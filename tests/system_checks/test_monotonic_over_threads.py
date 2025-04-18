import queue
import threading
import time
from typing import Callable

# Number of threads
NUM_THREADS = 64

# Define the clocks to test
clocks = {
	"time           ": time.time,
	"time_ns        ": time.time_ns,
	"monotonic      ": time.monotonic,
	"monotonic_ns   ": time.monotonic_ns,
	"perf_counter   ": time.perf_counter,
	"perf_counter_ns": time.perf_counter_ns,
}

# Queue to store timestamps in order
timestamps = queue.Queue()


def capture_time(i, clock_func: Callable) -> None:
	# Capture time using the given clock function and put it in the queue
	for _ in range(1000):
		# print(f"Thread {i} capturing time")
		timestamps.put(clock_func())


def check_monotonicity_for_clock(clock_name: str, clock_func: Callable) -> None:
	# Clear the queue for the next clock
	while not timestamps.empty():
		timestamps.get()

	# Create and start threads
	threads = []
	for i in range(NUM_THREADS):
		thread = threading.Thread(
			target=capture_time,
			args=(
				i,
				clock_func,
			),
		)
		thread.start()
		threads.append(thread)

	# Wait for all threads to complete
	for thread in threads:
		thread.join()

	# Extract timestamps from the queue
	captured_times = []
	while not timestamps.empty():
		captured_times.append(timestamps.get())

	# Check if the clock is monotonic
	is_monotonic = all(captured_times[i] <= captured_times[i + 1] for i in range(len(captured_times) - 1))

	if is_monotonic:
		print(f"Clock: {clock_name} is monotonic over {NUM_THREADS} threads ✅")
	else:
		print(f"Clock: {clock_name} is not monotonic over {NUM_THREADS} threads ❌")
	print("-" * 40)


if __name__ == "__main__":
	# Check monotonicity for each clock
	for clock_name, clock_func in clocks.items():
		check_monotonicity_for_clock(clock_name, clock_func)
