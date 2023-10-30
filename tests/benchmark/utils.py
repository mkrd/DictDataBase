import random
import time

from path_dict import pd

import dictdatabase as DDB


def make_table(recursion_depth=3, keys_per_level=50):
	d = {"key1": "val1", "key2": 2, "key3": [1, "2", [3, 3]]}
	for i in range(recursion_depth):
		d = {f"key{i}{j}": d for j in range(keys_per_level)}
	# print(f"Made table of size {len(json.dumps(d)) // 1e6}mb")
	return {"counter": {"counter": 0}, "big": d}


def print_stats(i, durations):
	avg = f"{sum(durations) / len(durations):.0f}"
	median = f"{sorted(durations)[len(durations) // 2]:.0f}"
	min_t = f"{min(durations):.0f}"
	max_t = f"{max(durations):.0f}"

	# print(f"{i}: total: {len(durations)}, avg: {avg}ms (med: {median}), {min_t}-{max_t}ms")


def print_and_assert_results(readers, writers, per_proc, tables, big_file, compression, t1, t2):
	ops = (writers + readers) * per_proc * tables
	ops_sec = f"{(ops / (t2 - t1)):.0f}"
	print(f"⏱️  {ops_sec} op/s ({ops} in {t2 - t1:.2f}s), {big_file = }, {compression = }")
	for t in range(tables):
		db = DDB.at(f"incr{t}").read()
		# print(db["counter"]["counter"], "==", per_proc * writers)
		assert db["counter"]["counter"] == per_proc * writers
		# print(f"✅ counter={db['counter']}")
