from concurrent.futures import ThreadPoolExecutor, wait

from path_dict import pd

import dictdatabase as DDB


def increment_counters(n, tables):
	for _ in range(n):
		for t in range(tables):
			# Perform a counter increment
			with DDB.at(f"test_stress_threaded{t}").session(as_type=pd) as (session, d):
				d["counter"] = lambda x: (x or 0) + 1
				session.write()
	return True


def read_counters(n, tables):
	for _ in range(n):
		for t in range(tables):
			DDB.at(f"test_stress_threaded{t}").read()
	return True


def test_stress_threaded(use_compression, use_orjson):
	per_thread = 15
	tables = 1
	threads = 3
	# Create tables
	for t in range(tables):
		DDB.at(f"test_stress_threaded{t}").create({}, force_overwrite=True)

	results = []
	with ThreadPoolExecutor(max_workers=threads) as pool:
		for _ in range(threads):
			future = pool.submit(increment_counters, per_thread, tables)
			results.append(future)
			future = pool.submit(read_counters, per_thread, tables)
			results.append(future)
	wait(results)

	# Check correctness of results
	assert [r.result() for r in results] == [True] * threads * 2
	for t in range(tables):
		db = DDB.at(f"test_stress_threaded{t}").read()
		assert db["counter"] == threads * per_thread


def test_heavy_threading():
	per_thread = 50
	tables = 1
	threads = 20
	# Create tables
	for t in range(tables):
		DDB.at(f"test_stress_threaded{t}").create({}, force_overwrite=True)

	results = []
	with ThreadPoolExecutor(max_workers=threads) as pool:
		for _ in range(threads):
			future = pool.submit(increment_counters, per_thread, tables)
			results.append(future)
			future = pool.submit(read_counters, per_thread, tables)
			results.append(future)
	wait(results)

	# Check correctness of results
	assert [r.result() for r in results] == [True] * threads * 2
	for t in range(tables):
		db = DDB.at(f"test_stress_threaded{t}").read()
		assert db["counter"] == threads * per_thread
