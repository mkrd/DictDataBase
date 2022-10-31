from calendar import c
import json
import dictdatabase as DDB
from multiprocessing import Pool
import shutil
import time
import os
from pyinstrument import Profiler


def seq_job(n):
	DDB.at("db").create([{
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
	}] * 50000, force_overwrite=True)
	for _ in range(n):
		t1 = time.monotonic_ns()
		with DDB.at("db").session() as (session, db):
			db.append({**db[-1], "counter": db[-1]["counter"] + 1})
			session.write()
		print(f"{(time.monotonic_ns() - t1) / 1e6:.2f} ms")


if __name__ == "__main__":
	DDB.config.storage_directory = "./.ddb_bench_sequential"
	DDB.locking.SLEEP_TIMEOUT = 0.001
	DDB.config.use_orjson = True
	DDB.config.indent = 2
	DDB.config.sort_keys = True

	p = Profiler(interval=0.00001)
	p.start()
	# Execute process pool running incrementor as the target task
	seq_job(20)
	p.stop()
	p.open_in_browser()
