import dictdatabase as DDB
import super_py as sp
import shutil
import time
import os
import json

from utils import print_and_assert_results, db_job


def threaded_stressor(file_count, readers, writers, operations_per_thread, big_file, compression):
	# Create Tables
	for t in range(file_count):
		if big_file:
			with open(os.path.join(os.getcwd(), "test_db/production_database/tasks.json"), "r") as f:
				db = json.loads(f.read())
				db["counter"] = {"counter": 0}
		else:
			db = {"counter": {"counter": 0}}
		DDB.at(f"incr{t}").create(db, force_overwrite=True)

	tasks = [(db_job, (mode, file_count, operations_per_thread)) for mode in "w" * writers + "r" * readers]

	# Execute process pool running incrementor as the target task
	t1 = time.monotonic()
	sp.concurrency.run_threaded(tasks, max_threads=writers + readers)
	t2 = time.monotonic()
	print_and_assert_results(readers, writers, operations_per_process, file_count, big_file, compression, t1, t2)



if __name__ == "__main__":
	DDB.config.storage_directory = ".ddb_bench_threaded"
	operations_per_process = 4
	for file_count, readers, writers in [(1, 4, 4), (1, 8, 1), (1, 1, 8), (4, 8, 8)]:
		print("")
		print(f"✨ Scenario: {file_count} files, {readers} readers, {writers} writers, {operations_per_process} operations per process")
		for big_file, compression in [(False, False), (False, True), (True, False), (True, True)]:
			try:
				shutil.rmtree(".ddb_bench_threaded", ignore_errors=True)
				os.mkdir(".ddb_bench_threaded")
				threaded_stressor(file_count, readers, writers, operations_per_process, big_file, compression)
			finally:
				shutil.rmtree(".ddb_bench_threaded", ignore_errors=True)
