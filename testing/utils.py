import dictdatabase as DDB
import json


def incr_db(n, tables):
	for _ in range(n):
		for t in range(tables):
			d = DDB.read(f"incr{t}")
			with DDB.session(f"incr{t}", as_PathDict=True) as (session, d):
				d["counter"] = lambda x: (x or 0) + 1
				session.write()
	return True




def make_table():

	incr = {"counter": 0}
	d = {"key1": "val1", "key2": 2, "key3": [1, "2", [3, 3]]}
	for i in range(4):
		d = {f"key{i}{j}": d for j in range(20)}
	incr["big"] = d
	return incr
