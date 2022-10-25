import dictdatabase as DDB
import super_py as sp
import time
import cProfile
import subprocess
from tests import test_scenes, utils



def increment_counters(n, tables):
	for _ in range(n):
		for t in range(tables):
			# Perform a useless read operation
			d = DDB.read(f"incr{t}")
			# Perform a counter increment
			with DDB.session(f"incr{t}", as_PathDict=True) as (session, d):
				d["counter"] = lambda x: (x or 0) + 1
				session.write()
	return True


def test_stress_threaded(tables=1, threads=4, per_thread=3):
	# Create tables
	for t in range(tables):
		DDB.at(f"incr{t}").create(utils.make_table())

	# Create tasks for concurrent execution
	tasks = [(increment_counters, (per_thread, tables)) for _ in range(threads)]

	# Run tasks concurrently
	t1 = time.time()
	results = sp.concurrency.run_threaded(tasks, max_threads=threads)
	t2 = time.time()

	# Print performance
	ops = threads * per_thread * tables
	ops_sec = int(ops / (t2 - t1))
	print(f"{ops = }, {ops_sec = }, {tables = }, {threads = }")

	# Check correctness of results
	assert results == [True] * threads
	for t in range(tables):
		db = DDB.read(f"incr{t}")
		assert db["counter"] == threads * per_thread
		print(f"✅ {db['counter'] = } == {per_thread * threads = }")



with cProfile.Profile() as pr:
	pr.enable()

	scene = "(🔴 pretty) (🔴 compression) (🔴 orjson)"
	print(scene)
	test_scenes[scene](test_stress_threaded)

	pr.disable()
	pr.dump_stats("test.prof")
	pr.print_stats("tottime")

command = "poetry run snakeviz test.prof"
subprocess.call(command.split())
