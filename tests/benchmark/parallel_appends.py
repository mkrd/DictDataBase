from calendar import c
import json
import dictdatabase as DDB
from multiprocessing import Pool
import shutil
import time
import os
from pyinstrument import Profiler

from utils import print_and_assert_results, db_job, make_table


def proc_job(id, n):
	DDB.config.storage_directory = "./.ddb_bench_parallel"
	DDB.locking.SLEEP_TIMEOUT = 0.001
	for _ in range(n):
		t1 = time.monotonic_ns()
		with DDB.at("append_here").session() as (session, db):
			if len(db) == 0:
				db += [{
					"counter": 0,
					"firstname": "John",
					"lastname": "Doe",
					"age": 42,
					"address": "1234 Main St",
					"city": "Anytown",
					"state": "CA",
					"zip": "12345",
					"phone": "123-456-7890",
					"interests": ["Python", "Databases", "DDB", "DDB-CLI", "DDB-Web", "Google"],
				}] * 50000
			else:
				db.append({**db[-1], "counter": db[-1]["counter"] + 1})
			session.write()
		time.sleep(0.5)

		vis = "🔴" * (id + 1)
		print(f"{(time.monotonic_ns() - t1) / 1e6:.2f} ms {vis}")


def proc_read_job(id, n):
	DDB.config.storage_directory = "./.ddb_bench_parallel"
	DDB.locking.SLEEP_TIMEOUT = 0.001
	for _ in range(n):
		t1 = time.monotonic_ns()
		DDB.at("append_here").read()
		vis = "🟢" * (id + 1)
		print(f"{(time.monotonic_ns() - t1) / 1e6:.2f} ms {vis}")





if __name__ == "__main__":
	proc_count = 2
	per_proc = 100
	DDB.config.storage_directory = "./.ddb_bench_parallel"
	# Create Tables
	DDB.at("append_here").create([], force_overwrite=True)
	# Execute process pool running incrementor as the target task
	t1 = time.monotonic()
	pool = Pool(processes=proc_count * 2)
	for i in range(proc_count):
		pool.apply_async(proc_job, args=(i, per_proc,))
		pool.apply_async(proc_read_job, args=(i, per_proc,))
	pool.close()
	pool.join()
	print(f"⏱️ {time.monotonic() - t1} seconds")
