from __future__ import annotations

import json
import os
import random
import shutil
import time
from dataclasses import dataclass
from multiprocessing import Pool
from typing import Callable

from path_dict import PathDict

import dictdatabase as DDB

DDB.config.storage_directory = ".ddb_bench_multi"


def benchmark(iterations, setup: Callable | None = None):
	def decorator(function):
		def wrapper(*args, **kwargs):
			f_name = function.__name__
			kwargs["name"] = f_name
			if setup:
				setup(kwargs)
			t1 = time.monotonic()
			for _ in range(iterations):
				function(*args, **kwargs)
			t2 = time.monotonic()
			print(f"⏱️ {iterations / (t2 - t1):.1f} op/s for {f_name} ({(t2 - t1):.1f} seconds)")

		return wrapper

	return decorator


@benchmark(iterations=9000, setup=lambda kw: DDB.at(kw["name"]).create({"data": {"counter": 0}}, force_overwrite=True))
def sequential_full_read_small_file(name):
	DDB.at(name).read()


@benchmark(iterations=8000, setup=lambda kw: DDB.at(kw["name"]).create({"data": {"counter": 0}}, force_overwrite=True))
def sequential_partial_read_small_file(name):
	DDB.at(name, key="data").read()


@benchmark(iterations=8000, setup=lambda kw: DDB.at(kw["name"]).create({"data": {"counter": 0}}, force_overwrite=True))
def sequential_full_write_small_file(name):
	with DDB.at(name).session() as (session, db):
		db["data"]["counter"] += 1
		session.write()


@benchmark(iterations=6000, setup=lambda kw: DDB.at(kw["name"]).create({"data": {"counter": 0}}, force_overwrite=True))
def sequential_partial_write_small_file(name):
	with DDB.at(name, key="data").session() as (session, db):
		db["counter"] += 1
		session.write()


@dataclass
class Scenario:
	files: int = 1
	readers: int = 0
	writers: int = 0
	big_file: bool = False
	use_compression: bool = False
	ops: int = 10

	def print(self):
		res = f"✨ Scenario: {'🔹' * self.readers}{'🔻' * self.writers} ({self.readers}r{self.writers}w)"
		res += ", 🔸 compression" if self.use_compression else ""
		res += ", 💎 big file" if self.big_file else ""
		print(res)


def print_and_assert_results(scenario: Scenario, t):
	ops = (scenario.writers + scenario.readers) * scenario.ops * scenario.files
	ops_sec = f"{(ops / t):.0f}"
	s = f"⏱️ {ops_sec} op/s ({ops} in {t:.2f}s)"
	print(str.ljust(s, 32), end="")
	for t in range(scenario.files):
		db = DDB.at(f"incr{t}").read()
		if db["counter"]["counter"] != scenario.ops * scenario.writers:
			print("❌", db["counter"]["counter"], "!=", scenario.ops * scenario.writers)
		assert db["counter"]["counter"] == scenario.ops * scenario.writers


def process_job(mode, scenario, cfg):
	DDB.config = cfg
	DDB.locking.SLEEP_TIMEOUT = 0.001

	t1 = time.monotonic()
	for _ in range(scenario.ops):
		for t in sorted(range(scenario.files), key=lambda _: random.random()):
			if mode == "r":
				DDB.at(f"incr{t}", key="counter").read()

			elif mode == "w":
				with DDB.at(f"incr{t}", key="counter").session(as_type=PathDict) as (session, d):
					d.at("counter").set(d.at("counter").get() + 1)
					session.write()
	t2 = time.monotonic()
	return t2 - t1


def parallel_stressor(scenario: Scenario):
	DDB.config.use_compression = scenario.use_compression
	# Create Tables
	for t in range(scenario.files):
		if scenario.big_file:
			with open(os.path.join(os.getcwd(), "test_db/production_database/tasks.json")) as f:
				db = json.loads(f.read())
				db["counter"] = {"counter": 0}
		else:
			db = {"counter": {"counter": 0}}
		DDB.at(f"incr{t}").create(db, force_overwrite=True)

	# Execute process pool running incrementor as the target task
	res = []
	pool = Pool(processes=scenario.readers + scenario.writers)
	for mode in "w" * scenario.writers + "r" * scenario.readers:
		res.append(pool.apply_async(process_job, args=(mode, scenario, DDB.config)))
	pool.close()
	pool.join()

	total_time = sum(r.get() for r in res) / (scenario.readers + scenario.writers)
	print_and_assert_results(scenario, total_time)


scenarios = [
	Scenario(readers=1, ops=6000),
	Scenario(readers=2, ops=6000),
	Scenario(readers=4, ops=6000),
	Scenario(readers=8, ops=3000),
	Scenario(writers=1, ops=6000),
	Scenario(writers=2, ops=1000),
	Scenario(writers=4, ops=800),
	Scenario(writers=8, ops=200),
	Scenario(readers=20, writers=20, ops=30),
	Scenario(readers=8, ops=1500),
	Scenario(readers=8, ops=1500, use_compression=True),
	Scenario(readers=8, ops=1500, big_file=True),
	Scenario(readers=8, writers=1, ops=200),
	Scenario(readers=8, writers=1, ops=25, big_file=True),
	Scenario(readers=1, writers=8, ops=200),
	Scenario(readers=1, writers=8, ops=10, big_file=True),
	Scenario(readers=8, writers=8, ops=100),
	Scenario(readers=8, writers=8, ops=8, big_file=True),
]

if __name__ == "__main__":
	# print("✨ Simple sequential benchmarks")
	# sequential_full_read_small_file()
	# sequential_partial_read_small_file()
	# sequential_full_write_small_file()
	# sequential_partial_write_small_file()

	# Parallel benchmarks
	for scenario in scenarios:
		try:
			shutil.rmtree(".ddb_bench_multi", ignore_errors=True)
			os.mkdir(".ddb_bench_multi")
			parallel_stressor(scenario)
			scenario.print()
		finally:
			shutil.rmtree(".ddb_bench_multi", ignore_errors=True)
