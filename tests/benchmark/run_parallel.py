import dictdatabase as DDB
from multiprocessing import Pool
import shutil
import time
import os

from utils import incrementor, print_and_assert_results


def proc_job(i, n, tables, sd, uc, uo, id, sk):
	DDB.config.storage_directory = sd
	DDB.config.use_compression = uc
	DDB.config.use_orjson = uo
	DDB.config.indent = id
	DDB.config.sort_keys = sk
	DDB.locking.SLEEP_TIMEOUT = 0.0 if i % 4 < 2 else 0.001
	incrementor(i, n, tables)


def parallel_stress(tables=2, proc_count=8, per_process=512):
	# Create Tables
	for t in range(tables):
		DDB.at(f"incr{t}").create({"counter": 0}, force_overwrite=True)

	# Execute process pool running incrementor as the target task
	t1 = time.monotonic()
	pool = Pool(processes=proc_count)
	for i in range(proc_count):
		# Each process will enter this file again, but not as __main__
		# So only the outside context is executed, and then the incrementor function
		# This means we need to pass the config since the process is "fresh"
		pool.apply_async(proc_job, args=(i, per_process, tables,
			DDB.config.storage_directory,
			DDB.config.use_compression,
			DDB.config.use_orjson,
			DDB.config.indent,
			DDB.config.sort_keys,
		))
	pool.close()
	pool.join()
	t2 = time.monotonic()
	print_and_assert_results(proc_count, per_process, tables, t1, t2)


if __name__ == "__main__":
	DDB.config.storage_directory = ".ddb_bench_parallel"
	try:
		shutil.rmtree(".ddb_bench_parallel", ignore_errors=True)
		os.mkdir(".ddb_bench_parallel")
		parallel_stress()
	finally:
		shutil.rmtree(".ddb_bench_parallel", ignore_errors=True)
