import multiprocessing
import threading
import time


def print_clocks(label: str) -> None:
    print(f"--- {label} ---")
    print("time_ns()        :", time.time_ns())
    print("monotonic_ns()   :", time.monotonic_ns())
    print("perf_counter_ns():", time.perf_counter_ns())
    print("\n")


def thread_function(thread_name: str) -> None:
    print_clocks(f"Thread-{thread_name}")


def process_function(process_name: str) -> None:
    print_clocks(f"Process-{process_name}")


if __name__ == "__main__":
    print_clocks("Main Thread")

    threads = []
    for i in range(3):
        thread = threading.Thread(target=thread_function, args=(i,))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    processes = []
    for i in range(3):
        process = multiprocessing.Process(target=process_function, args=(i,))
        process.start()
        processes.append(process)

    for process in processes:
        process.join()
