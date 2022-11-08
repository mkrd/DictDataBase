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



def benchmark(iterations, setup: callable = None):
	def decorator(function):
		def wrapper(*args, **kwargs):
			if setup:
				setup()
			t1 = time.monotonic()
			for _ in range(iterations):
				function(*args, **kwargs)
			t2 = time.monotonic()
			print(f"⏱️ {iterations / (t2 - t1):.1f} op/s for {function.__name__} ({(t2 - t1):.1f} seconds)")
		return wrapper
	return decorator




@benchmark(iterations=9000, setup=lambda: DDB.at("db").create({"data": {"counter": 0}}, force_overwrite=True))
def sequential_full_read_small_file():
	DDB.at("db").read()


@benchmark(iterations=8000, setup=lambda: DDB.at("db").create({"data": {"counter": 0}}, force_overwrite=True))
def sequential_partial_read_small_file():
	DDB.at("db", key="data").read()


@benchmark(iterations=8000, setup=lambda: DDB.at("db").create({"data": {"counter": 0}}, force_overwrite=True))
def sequential_full_write_small_file():
	with DDB.at("db").session() as (session, db):
		db["data"]["counter"] += 1
		session.write()


@benchmark(iterations=6000, setup=lambda: DDB.at("db").create({"data": {"counter": 0}}, force_overwrite=True))
def sequential_partial_write_small_file():
	with DDB.at("db", key="data").session() as (session, db):
		db["counter"] += 1
		session.write()















if __name__ == "__main__":
	DDB.config.storage_directory = ".ddb_bench_multi"

	# Sequential benchmarks
	print("✨ Simple sequential benchmarks")
	sequential_full_read_small_file()
	sequential_partial_read_small_file()
	sequential_full_write_small_file()
	sequential_partial_write_small_file()

	# Parallel benchmarks
	for (file_count, readers, writers), scenario_params in scenarios.items():
		print(f"\n✨ Scenario: {readers} readers, {writers} writers")
		for ops_per_proc, big_file, compression in scenario_params:

			try:
				shutil.rmtree(".ddb_bench_multi", ignore_errors=True)
				os.mkdir(".ddb_bench_multi")
				parallel_stressor(file_count, readers, writers, ops_per_proc, big_file, compression)
			finally:
				shutil.rmtree(".ddb_bench_multi", ignore_errors=True)
