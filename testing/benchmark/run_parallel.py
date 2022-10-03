import dictdatabase as DDB
import super_py as sp
import time
import os
from testing import utils


def parallel_stress(tables=1, processes=4, per_process=8):
	for t in range(tables):
		DDB.create(f"incr{t}", db=utils.make_table())

	ddb_sd = DDB.config.storage_directory
	ddb_pj = DDB.config.pretty_json_files
	ddb_uc = DDB.config.use_compression
	args = f"{tables} {processes} {per_process} {ddb_sd} {ddb_pj} {ddb_uc}"

	t1 = time.time()
	os.system(f"python3 testing/test_parallel_runner.py {args}")
	t2 = time.time()

	ops = processes * per_process * tables
	ops_sec = int(ops / (t2 - t1))
	print(f"{ops = }, {ops_sec = }, {tables = }, {processes = }")

	for t in range(tables):
		db = DDB.read(f"incr{t}")
		print(f"✅ {db['counter'] = } == {per_process * processes = }")
		assert DDB.read(f"incr{t}")["counter"] == processes * per_process



print("lol")
if __name__ == "__main__":
	print("🔥 parallel_stress")
