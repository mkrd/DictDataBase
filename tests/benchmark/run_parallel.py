from calendar import c
import json
import dictdatabase as DDB
from multiprocessing import Pool
import shutil
import time
import os

from utils import print_and_assert_results, db_job, make_table


def proc_job(mode, n, tables, sd, uc, uo, id, sk):
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
		pool.apply_async(proc_job, args=(mode, operations_per_process, file_count,
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




if __name__ == "__main__":
	DDB.config.storage_directory = ".ddb_bench_parallel"
	operations_per_process = 4
	for file_count, readers, writers in [(1, 4, 4), (1, 8, 1), (1, 1, 8), (4, 8, 8)]:
		print("")
		print(f"✨ Scenario: {file_count} files, {readers} readers, {writers} writers, {operations_per_process} operations per process")
		for big_file, compression in [(False, False), (False, True), (True, False), (True, True)]:
			try:
				shutil.rmtree(".ddb_bench_parallel", ignore_errors=True)
				os.mkdir(".ddb_bench_parallel")
				parallel_stressor(file_count, readers, writers, operations_per_process, big_file, compression)
			finally:
				shutil.rmtree(".ddb_bench_parallel", ignore_errors=True)



# Test Matrix Rows (Scenarios)
# 1: 1 file, 4 reading 4 writing 200 times each
# 2: 1 file, 8 reading 200 times each
# 3: 1 file, 8 writing 200 times each
# 4: 4 files, 8 reading 8 writing 200 times each

# Test Matrix Columns (Configurations)
# 1: Big File (50mb), compression
# 2: Small File, compression
# 3: Big File (50mb), no compression
# 4: Small File, no compression
