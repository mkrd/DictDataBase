import asyncio
import os
import shutil
import time

from utils import incrementor, print_and_assert_results

import dictdatabase as DDB


async def thread_job(i, n, file_count):
	DDB.locking.SLEEP_TIMEOUT = 0.001
	incrementor(i, n, file_count)


async def threaded_stress(file_count=2, thread_count=10, per_thread=500):
	# Create file_count json files
	for t in range(file_count):
		DDB.at(f"incr{t}").create({"counter": 0}, force_overwrite=True)

	# Create tasks for concurrent execution
	tasks = [(incrementor, (i, per_thread, file_count)) for i in range(thread_count)]

	# Execute process pool running incrementor as the target task
	t1 = time.monotonic()
	await asyncio.gather(*[thread_job(i, per_thread, file_count) for i in range(thread_count)])
	t2 = time.monotonic()

	print_and_assert_results(thread_count, per_thread, file_count, t1, t2)


if __name__ == "__main__":
	DDB.config.storage_directory = ".ddb_bench_async"
	try:
		shutil.rmtree(".ddb_bench_async", ignore_errors=True)
		os.mkdir(".ddb_bench_async")
		loop = asyncio.get_event_loop()
		loop.run_until_complete(threaded_stress())
		loop.close()
	finally:
		shutil.rmtree(".ddb_bench_async", ignore_errors=True)
