import dictdatabase as DDB
import super_py as sp
import time
import os
from testing import utils, test_scenes, orjson_encode, orjson_decode
import orjson
from multiprocessing import Pool
import cProfile
import subprocess


def incr_db(n, tables, sd, uc, uo, id, sk):
	print("parallel_runner incr_db")
	DDB.config.storage_directory = sd
	DDB.config.use_compression = uc
	DDB.config.use_orjson = uo
	DDB.config.indent = id
	DDB.config.sort_keys = sk
	for _ in range(n):
		for t in range(tables):
			with DDB.session(f"incr{t}", as_PathDict=True) as (session, d):
				d["counter"] = lambda x: (x or 0) + 1
				session.write()
	return True


def parallel_stress(tables=1, processes=8, per_process=8):
	# Create Tables
	for t in range(tables):
		DDB.create(f"incr{t}", db=utils.get_tasks_json())

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
		db = DDB.read(f"incr{t}")
		print(f"✅ {db['counter'] = } == {per_process * processes = }")
		assert DDB.read(f"incr{t}")["counter"] == processes * per_process


if __name__ == "__main__":
	with cProfile.Profile() as pr:
		pr.enable()

		scene = "(🔴 pretty) (🔴 compression) (🟢 orjson)"
		print(scene)
		test_scenes[scene](parallel_stress)

		pr.disable()
		pr.dump_stats("test.prof")
		pr.print_stats("tottime")

	command = "poetry run snakeviz test.prof"
	subprocess.call(command.split())
