from calendar import c
import json
import dictdatabase as DDB
from multiprocessing import Pool
import shutil
import time
import os
from pyinstrument import Profiler

from utils import print_and_assert_results, db_job, make_table


def proc_job(mode, tables, n, sd, uc, uo, id, sk):
	DDB.config.storage_directory = sd
	DDB.config.use_compression = uc
	DDB.config.use_orjson = uo
	DDB.config.indent = id
	DDB.config.sort_keys = sk
	DDB.locking.SLEEP_TIMEOUT = 0.001
	db_job(mode, tables, n)


def parallel_stressor(file_count, readers, writers, operations_per_process, big_file, compression):
	# Create Tables
	for t in range(file_count):
		if big_file:
			with open(os.path.join(os.getcwd(), "test_db/production_database/tasks.json"), "r") as f:
				db = json.loads(f.read())
				db["counter"] = {"counter": 0}
		else:
			db = {"counter": {"counter": 0}}

		DDB.at(f"incr{t}").create(db, force_overwrite=True)

	# Execute process pool running incrementor as the target task
	t1 = time.monotonic()
	pool = Pool(processes=readers + writers)
	for mode in "w" * writers + "r" * readers:
		pool.apply_async(proc_job, args=(mode, file_count, operations_per_process,
			DDB.config.storage_directory,
			compression,
			DDB.config.use_orjson,
			DDB.config.indent,
			DDB.config.sort_keys,
		))
	pool.close()
	pool.join()
	t2 = time.monotonic()
	print_and_assert_results(readers, writers, operations_per_process, file_count, big_file, compression, t1, t2)



scenarios = {
	# file_count, readers, writers, operations_per_process, big_file, compression
	# Let's try to break shit
	(1, 20, 20): [
		(30, False, False)
	],
	(1, 8, 0): [
		(1500, False, False),
		(1500, False, True),
		(1500, True, False),
	],
	(1, 8, 1): [
		(200, False, False),
		(25, True, False),
	],
	(1, 1, 8): [
		(200, False, False),
		(10, True, False),
	],
	(1, 8, 8): [
		(100, False, False),
		(8, True, False),
	],
}




if __name__ == "__main__":
	DDB.config.storage_directory = ".ddb_bench_parallel"
	for (file_count, readers, writers), scenario_params in scenarios.items():
		print("")
		print(f"✨ Scenario: {file_count} files, {readers} readers, {writers} writers")
		for ops_per_proc, big_file, compression in scenario_params:
			# p = Profiler(interval=0.0001)
			# p.start()
			try:
				shutil.rmtree(".ddb_bench_parallel", ignore_errors=True)
				os.mkdir(".ddb_bench_parallel")
				parallel_stressor(file_count, readers, writers, ops_per_proc, big_file, compression)
			finally:
				shutil.rmtree(".ddb_bench_parallel", ignore_errors=True)
			# p.stop()
			# p.open_in_browser()
