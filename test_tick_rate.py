import time


def get_tick_rate(clock_func, duration=1.0):
    start_time = time.time()
    end_time = start_time + duration

    # Initial reading
    prev_value = clock_func()
    ticks = 0

    while time.time() < end_time:
        current_value = clock_func()
        if current_value < prev_value:
            raise RuntimeError("Clock function is not monotonic")
        if current_value != prev_value:
            ticks += 1
        prev_value = current_value

    return ticks / duration  # ticks per second


if __name__ == "__main__":
    clock_funcs = {
        "time           ": time.time,
        "time_ns        ": time.time_ns,
        "monotonic      ": time.monotonic,
        "monotonic_ns   ": time.monotonic_ns,
        "perf_counter   ": time.perf_counter,
        "perf_counter_ns": time.perf_counter_ns,
    }

    for name, func in clock_funcs.items():
        print(f"Tick rate for {name}: {get_tick_rate(func) / 1_000_000.0:.3f}M ticks/second")
