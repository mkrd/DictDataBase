import os
import shutil
import dictdatabase as DDB
import super_py as sp
import sys
import time


def setup():
	DDB.config.storage_directory = ".ddb_storage_testing"
	DDB.config.pretty_json_files = True
	DDB.config.use_compression = False
	os.makedirs(DDB.config.storage_directory, exist_ok=True)


def teardown():
	shutil.rmtree(".ddb_storage_testing")
	# Reset defaults
	DDB.config.storage_directory = "./ddb_storage"
	DDB.config.use_compression = False
	DDB.config.pretty_json_files = True
















@sp.test(setup, teardown)
def test_file_creation():
	n = DDB.read("Non_existent")
	assert n is None

	DDB.create("db1")
	db = DDB.read("db1")
	assert db == {}

	with DDB.session("db1", as_PathDict=True) as (session, d):
		d["a", "b", "c"] = "dee"
		assert d["a", "b", "c"] == "dee"
		session.write()
	assert DDB.read("db1") == {"a": {"b": {"c": "dee"}}}




@sp.test(setup, teardown)
def test_nested_file_creation():
	n = DDB.read("blobbles/bla/blub")
	assert n is None
	DDB.create("blobbles/osna/efforts", db={"val": [1, 2]})
	assert DDB.read("blobbles/osna/efforts") == {"val": [1, 2]}





# @sp.test(setup, teardown)
# def create_read_write_lock():
# 	DDB.create("test", db={"test": "value"})
# 	ReadLock("test")
# 	WriteLock("test")
# 	ReadLock("test")
# 	ReadLock("test")
# 	WriteLock("test")
# 	WriteLock("test")

# 	with DDB.session("test", as_PathDict=True) as (session, t):
# 		assert True

# import sys
# sys.exit()


@sp.test(setup, teardown)
def except_during_open():
	d = {"test": "value"}
	DDB.create("test", db=d)
	try:
		with DDB.session("test", as_PathDict=True) as (session, test):
			raise Exception("Any Exception")
	except Exception:
		pass






@sp.test(setup, teardown)
def save_unserializable():
	try:
		d = {"test": "value"}
		DDB.create("test", db=d)
		with DDB.session("test", as_PathDict=True) as (session, test):
			test["test"] = {"key": set([1, 2, 2])}
			session.write()
		assert False
	except TypeError:
		assert True



@sp.test(setup, teardown)
def test_session_in_session():
	d = {"test": "value"}
	DDB.create("test", db=d)
	try:
		with DDB.session("test", as_PathDict=True) as (session, test):
			with DDB.session("test", as_PathDict=True) as (session2, test2):
				assert False
	except RuntimeError:
		assert True


@sp.test_class(each_setup=setup, each_teardown=teardown)
class TestReadWrite:

	def test_read_non_existent_json(self):
		DDB.config.use_compression = False
		d = DDB.read("nonexistent")
		assert d is None

	def test_read_non_existent_ddb(self):
		DDB.config.use_compression = True
		d = DDB.read("nonexistent")
		assert d is None

	def test_open_non_existent_json(self):
		DDB.config.use_compression = False
		try:
			with DDB.session("nonexistent", as_PathDict=True) as (session, d):
				assert False
		except Exception:
			assert True

	def test_open_non_existent_ddb(self):
		DDB.config.use_compression = True
		try:
			with DDB.session("nonexistent", as_PathDict=True) as (session, d):
				assert False
		except Exception:
			assert True

	def test_write_json_read_json(self):
		DDB.config.use_compression = False
		d = {"test": "value"}
		DDB.create("test", db=d)
		dd = DDB.read("test")
		assert d == dd

	def test_write_ddb_read_ddb(self):
		DDB.config.use_compression = True
		d = {"test": "value"}
		DDB.create("test", db=d)
		dd = DDB.read("test")
		assert d == dd

	def test_write_json_read_ddb(self):
		DDB.config.use_compression = False
		d = {"test": "value"}
		DDB.create("test", db=d)
		DDB.config.use_compression = True
		dd = DDB.read("test")
		assert d == dd

	def test_write_ddb_read_json(self):
		DDB.config.use_compression = True
		d = {"test": "value"}
		DDB.create("test", db=d)
		DDB.config.use_compression = False
		dd = DDB.read("test")
		assert d == dd

	def test_write_json_write_json(self):
		DDB.config.use_compression = False
		d = {"test": "value"}
		DDB.create("test", db=d)
		with DDB.session("test", as_PathDict=True) as (session, dd):
			assert d == dd.dict
			session.write()

	def test_write_ddb_write_ddb(self):
		DDB.config.use_compression = True
		d = {"test": "value"}
		DDB.create("test", db=d)
		with DDB.session("test", as_PathDict=True) as (session, dd):
			assert d == dd.dict
			session.write()

	def test_write_ddb_write_json(self):
		DDB.config.use_compression = True
		d = {"test": "value"}
		DDB.create("test", db=d)
		DDB.config.use_compression = False
		with DDB.session("test", as_PathDict=True) as (session, dd):
			assert d == dd.dict
			session.write()

	def test_write_json_write_ddb(self):
		DDB.config.use_compression = False
		d = {"test": "value"}
		DDB.create("test", db=d)
		DDB.config.use_compression = True
		with DDB.session("test", as_PathDict=True) as (session, dd):
			assert d == dd.dict
			session.write()

################################################################################
######## Stress Testing ########################################################
################################################################################

def incr_db(n, tables):
	for _ in range(n):
		for t in range(tables):
			d = DDB.read(f"incr{t}")
			with DDB.session(f"incr{t}", as_PathDict=True) as (session, d):
				d["counter"] = lambda x: (x or 0) + 1
				session.write()
	return True



@sp.test(setup, teardown)
def test_stress_threaded(tables=4, threads=20, per_thread=200):
	for t in range(tables):
		incr = {}
		for i in range(1_000):
			incr[f"key{i}"] = {"someval": "val", "some_list": [1,3,4,5,524,32], "some_dict": {"k1": "v1", "k2": "v2"}}
		DDB.create(f"incr{t}", db={})

	tasks = []
	for _ in range(threads):
		tasks.append((incr_db, (per_thread, tables)))

	t1 = time.time()
	results = sp.concurrency.run_threaded(tasks, max_threads=threads)
	t2 = time.time()

	ops = threads * per_thread * tables
	ops_sec = int(ops / (t2 - t1))
	print(f"{ops=}, {ops_sec=}, {tables=}, {threads=}")


	assert results == [True] * threads
	for t in range(tables):
		assert DDB.read(f"incr{t}")["counter"] == threads * per_thread







@sp.test(setup, teardown)
def parallel_stress(tables=4, processes=20, per_process=20):
	for t in range(tables):
		incr = {}
		for i in range(1_000):
			incr[f"key{i}"] = {"someval": "val", "some_list": [1,3,4,5,524,32], "some_dict": {"k1": "v1", "k2": "v2"}}
		DDB.create(f"incr{t}", db={})

	file = "test_parallel_runner.py"
	args = f"{tables} {processes} {per_process} {DDB.config.storage_directory}"
	t1 = time.time()
	os.system(f"python3 {file} {args}")
	t2 = time.time()

	ops = processes * per_process * tables
	ops_sec = int(ops / (t2 - t1))
	print(f"{ops=}, {ops_sec=}, {tables=}, {processes=}")

	for t in range(tables):
		t_counter = DDB.read(f"incr{t}")["counter"]
		assert t_counter == processes * per_process






@sp.test_class(class_setup=setup, class_teardown=teardown)
class TestBigDB:
	def test_a_create(self):
		d = {"key1": "val1", "key2": 2, "key3": [1, "2", [3, 3]]}
		for i in range(4):
			d_new = {}
			for j in range(20):
				d_new[f"key{i}{j}"] = d
			d = d_new
		# About 22MB
		DDB.create("_test_big_db", db=d)

	def test_b_read(self):
		d = DDB.read("_test_big_db")

	def test_c_open_session(self):
		with DDB.session("_test_big_db") as (session, d):
			assert True



	def test_d_open_session_and_write(self):
		with DDB.session("_test_big_db") as (session, d):
			session.write()
