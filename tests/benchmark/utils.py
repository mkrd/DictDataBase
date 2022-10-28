import dictdatabase as DDB
from path_dict import pd
import random
import time



def print_stats(i, durations):
	avg = f"{sum(durations) / len(durations):.0f}"
	median = f"{sorted(durations)[len(durations) // 2]:.0f}"
	min_t = f"{min(durations):.0f}"
	max_t = f"{max(durations):.0f}"
	type = "read" if i % 2 == 0 else "write"
	print(f"{type}: total: {len(durations)}, avg: {avg}ms (med: {median}), {min_t}-{max_t}ms")


def print_and_assert_results(proc_count, per_proc, tables, t1, t2):
	ops = proc_count * per_proc * tables
	ops_sec = f"{(ops / (t2 - t1)):.0f}"
	print(f"⏱️ {ops_sec}op/s ({ops} in {t2 - t1:.2f}s), {tables = }, {proc_count = }")
	for t in range(tables):
		db = DDB.at(f"incr{t}").read()
		assert db["counter"] == per_proc * (proc_count // 2)
		print(f"✅ counter={db['counter']}")


def random_reads(file_count):
	""" Read the n tables in random order """
	for t in sorted(range(file_count), key=lambda _: random.random()):
		DDB.at(f"incr{t}").read()


def random_writes(file_count):
	""" Iterated the n tables in random order and increment the counter """
	for t in sorted(range(file_count), key=lambda _: random.random()):
		with DDB.at(f"incr{t}").session(as_type=pd) as (session, d):
			d["counter"] = lambda x: (x or 0) + 1
			session.write()


def incrementor(i, iterations, file_count):
	durations = []
	for _ in range(iterations):
		t_start = time.monotonic_ns()
		random_reads(file_count) if i % 2 == 0 else random_writes(file_count)
		t_end = time.monotonic_ns()
		durations.append((t_end - t_start) / 1e6)
	print_stats(i, durations)
