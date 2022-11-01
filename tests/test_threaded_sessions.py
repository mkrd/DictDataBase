import dictdatabase as DDB
from path_dict import pd
from concurrent.futures import ThreadPoolExecutor, wait


def increment_counters(n, tables):
	for _ in range(n):
		for t in range(tables):
			# Perform a useless read operation
			d = DDB.at(f"test_stress_threaded{t}").read()
			# Perform a counter increment
			with DDB.at(f"test_stress_threaded{t}").session(as_type=pd) as (session, d):
				d["counter"] = lambda x: (x or 0) + 1
				session.write()
	return True



def run_threaded(fns_args: list, max_threads=None):
	"""
		Run a list of tasks concurrently, and return their results as
		a list in the same order. A task is a 2-tuple of the function and an
		n-tuple of the function's n arguments.
		Remember: A 1-tuple needs a trailing comma, eg. (x,)
		Return: A list of results, in the order of the input tasks.
	"""
	if max_threads is None:
		max_threads = len(fns_args)
	results = []
	with ThreadPoolExecutor(max_threads) as pool:
		for fn, args in fns_args:
			future = pool.submit(fn, *args)
			results.append(future)
	wait(results)
	return [r.result() for r in results]


def test_stress_threaded(env):
	per_thread = 12
	tables = 2
	threads = 4
	# Create tables
	for t in range(tables):
		DDB.at(f"test_stress_threaded{t}").create({}, force_overwrite=True)

	# Create tasks for concurrent execution
	tasks = [(increment_counters, (per_thread, tables)) for _ in range(threads)]

	# Run tasks concurrently
	results = run_threaded(tasks, max_threads=threads)

	# Check correctness of results
	assert results == [True] * threads
	for t in range(tables):
		db = DDB.at(f"test_stress_threaded{t}").read()
		assert db["counter"] == threads * per_thread
