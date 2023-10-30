import os
import sqlite3
import time

import super_py as sp


def teardown():
	os.remove("test.db")


@sp.test(teardown=teardown)
def parallel_stress(tables=4, processes=16, per_process=128):
	# Create the database with all tables
	con = sqlite3.connect("test.db")
	for t in range(tables):
		cur = con.cursor()
		cur.execute(f"CREATE TABLE IF NOT EXISTS incr{t} (counter INTEGER)")
		cur.execute(f"INSERT INTO incr{t} (counter) VALUES (0)")
		con.commit()
	con.close()

	# Run the incr_db function in parallel
	args = f"{tables} {processes} {per_process}"
	t1 = time.time()
	os.system(f"python3 test_parallel_runner.py {args}")
	t2 = time.time()

	ops = processes * per_process * tables
	ops_sec = int(ops / (t2 - t1))
	print(f"{ops = }, {ops_sec = }, {tables = }, {processes = } {per_process = }")
	print(f"{t2 - t1 = }")

	for t in range(tables):
		con = sqlite3.connect("test.db")
		cur = con.cursor()
		cur.execute(f"SELECT counter FROM incr{t}")
		t_counter = cur.fetchone()[0]
		con.close()
		print(f"{t_counter = }, should be {processes * per_process}")
		assert t_counter == processes * per_process
