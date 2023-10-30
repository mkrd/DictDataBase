import json
import os
import shutil
import threading
import time
from calendar import c
from multiprocessing import Pool

from pyinstrument import Profiler
from utils import print_and_assert_results

import dictdatabase as DDB
from dictdatabase.configuration import Confuguration


def proc_job(n, cfg):
	DDB.config = cfg
	DDB.locking.SLEEP_TIMEOUT = 0.001

	for _ in range(n):
		with DDB.at("incr/*").session() as (session, d):
			for k, v in d.items():
				v["counter"] += 1
			session.write()


def parallel_stressor(file_count):
	# Create Tables
	for t in range(11):
		DDB.at("incr", t).create({"counter": 0}, force_overwrite=True)

	# Execute process pool running incrementor as the target task
	t1 = time.monotonic()
	res = []
	pool = Pool(processes=file_count)
	for _ in range(file_count):
		r = pool.apply_async(proc_job, args=(1000, DDB.config))
		res.append(r)
	pool.close()
	pool.join()
	t2 = time.monotonic()
	for r in res:
		print(r.get())


if __name__ == "__main__":
	DDB.config.storage_directory = ".ddb_bench_parallel"
	try:
		shutil.rmtree(".ddb_bench_parallel", ignore_errors=True)
		os.mkdir(".ddb_bench_parallel")
		parallel_stressor(4)
	finally:
		pass
		# shutil.rmtree(".ddb_bench_parallel", ignore_errors=True)
