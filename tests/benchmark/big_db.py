import dictdatabase as DDB
from pyinstrument import profiler


def a_create():
	d = {"key1": "val1", "key2": 2, "key3": [1, "2", [3, 3]]}
	for i in range(4):
		d = {f"key{i}{j}": d for j in range(20)}
	# About 22MB
	DDB.create("_test_big_db", db=d, force_overwrite=True)


def b_read():
	d = DDB.read("_test_big_db")


def c_session():
	with DDB.session("_test_big_db") as (session, d):
		session.write()





p = profiler.Profiler(interval=0.00001)
p.start()

for f in [a_create, b_read, c_session]:
	for uc in [False, True]:
		for uo in [False, True]:
			for sc in [False, True]:
				for id in [None, 0, 2, "\t"]:
					f()

p.stop()

p.open_in_browser(timeline=True)
