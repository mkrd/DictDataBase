import dictdatabase as DDB
from path_dict import pd
import time
from multiprocessing import Pool
from pyinstrument import profiler
import shutil
import os



def incr_db(n, tables, sd, uc, uo, id, sk):
	DDB.config.storage_directory = sd
	DDB.config.use_compression = uc
	DDB.config.use_orjson = uo
	DDB.config.indent = id
	DDB.config.sort_keys = sk
	DDB.locking.SLEEP_TIMEOUT = 0.001
	for _ in range(n):
		for t in range(tables):
			with DDB.at(f"incr{t}").session(as_type=pd) as (session, d):
				d["counter"] = lambda x: (x or 0) + 1
				session.write()
	return True


def parallel_stress(tables=4, processes=16, per_process=128):
	# Create Tables
	for t in range(tables):
		# DDB.at(f"incr{t}").create(utils.get_tasks_json())
		DDB.at(f"incr{t}").create({"counter": 0}, force_overwrite=True)

	# Execute process pool running incr_db as the target task
	t1 = time.time()
	pool = Pool(processes=processes)
	for _ in range(processes):
		# Each process will enter this file again, but not as __main__
		# So only the outside context is executed, and then the incr_db function
		# This means we need to pass the config since the process is "fresh"
		pool.apply_async(incr_db, args=(
			per_process,
			tables,
			DDB.config.storage_directory,
			DDB.config.use_compression,
			DDB.config.use_orjson,
			DDB.config.indent,
			DDB.config.sort_keys,
		))
	pool.close()
	pool.join()
	t2 = time.time()

	ops = processes * per_process * tables
	ops_sec = f"{(ops / (t2 - t1)):.2f}"
	print(f"{ops = }, {ops_sec = }, {tables = }, {processes = }")

	for t in range(tables):
		db = DDB.at(f"incr{t}").read()
		print(f"✅ {db['counter'] = } == {per_process * processes = }")
		assert DDB.at(f"incr{t}").read()["counter"] == processes * per_process




if __name__ == "__main__":
	DDB.config.storage_directory = "ddb_parallel_benchmark_storage"
	shutil.rmtree("ddb_parallel_benchmark_storage", ignore_errors=True)
	os.mkdir("ddb_parallel_benchmark_storage")
	p = profiler.Profiler(interval=0.001)
	p.start()
	parallel_stress()
	p.stop()
	p.open_in_browser(timeline=True)
	shutil.rmtree("ddb_parallel_benchmark_storage", ignore_errors=True)
