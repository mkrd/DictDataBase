from multiprocessing.pool import Pool

from path_dict import pd

import dictdatabase as DDB


def increment_counters(n, tables, cfg):
	DDB.config.storage_directory = cfg.storage_directory
	DDB.config.use_compression = cfg.use_compression
	DDB.config.use_orjson = cfg.use_orjson

	for _ in range(n):
		for t in range(tables):
			# Perform a counter increment
			with DDB.at(f"test_stress_parallel{t}").session(as_type=pd) as (session, d):
				d["counter"] = lambda x: (x or 0) + 1
				session.write()
	return True


def read_counters(n, tables, cfg):
	DDB.config.storage_directory = cfg.storage_directory
	DDB.config.use_compression = cfg.use_compression
	DDB.config.use_orjson = cfg.use_orjson
	for _ in range(n):
		for t in range(tables):
			DDB.at(f"test_stress_parallel{t}").read()
	return True


def test_stress_multiprocessing(use_compression, use_orjson):
	per_thread = 15
	tables = 1
	threads = 3
	# Create tables
	for t in range(tables):
		DDB.at(f"test_stress_parallel{t}").create({}, force_overwrite=True)

	results = []
	pool = Pool(processes=threads)
	for _ in range(threads):
		r = pool.apply_async(increment_counters, args=(per_thread, tables, DDB.config))
		results.append(r)
		r = pool.apply_async(read_counters, args=(per_thread, tables, DDB.config))
		results.append(r)
	pool.close()
	pool.join()

	# Check correctness of results
	assert [r.get() for r in results] == [True] * threads * 2
	for t in range(tables):
		db = DDB.at(f"test_stress_parallel{t}").read()
		assert db["counter"] == threads * per_thread


def test_heavy_multiprocessing():
	per_thread = 50
	tables = 1
	threads = 20
	# Create tables
	for t in range(tables):
		DDB.at(f"test_stress_parallel{t}").create({}, force_overwrite=True)

	results = []
	pool = Pool(processes=threads)
	for _ in range(threads):
		r = pool.apply_async(increment_counters, args=(per_thread, tables, DDB.config))
		results.append(r)
		r = pool.apply_async(read_counters, args=(per_thread, tables, DDB.config))
		results.append(r)
	pool.close()
	pool.join()

	# Check correctness of results
	assert [r.get() for r in results] == [True] * threads * 2
	for t in range(tables):
		db = DDB.at(f"test_stress_parallel{t}").read()
		assert db["counter"] == threads * per_thread


def read_partial(n, cfg):
	DDB.locking.SLEEP_TIMEOUT = 0
	DDB.config = cfg
	for _ in range(n):
		DDB.at("test_stress_parallel0", key="key").read()
	return True


def test_induce_indexer_except(use_compression):
	DDB.at("test_stress_parallel0").create({}, force_overwrite=True)

	pool = Pool(processes=2)
	for _ in range(2):
		pool.apply_async(read_partial, args=(1000, DDB.config))
	pool.close()
	pool.join()
