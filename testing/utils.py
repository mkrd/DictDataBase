import dictdatabase as DDB
import json
import os

def incr_db(n, tables):
	for _ in range(n):
		for t in range(tables):
			d = DDB.read(f"incr{t}")
			with DDB.session(f"incr{t}", as_PathDict=True) as (session, d):
				d["counter"] = lambda x: (x or 0) + 1
				session.write()
	return True




def make_table(recursion_depth=4, keys_per_level=20):

	incr = {"counter": 0}
	d = {"key1": "val1", "key2": 2, "key3": [1, "2", [3, 3]]}
	for i in range(recursion_depth):
		d = {f"key{i}{j}": d for j in range(keys_per_level)}
	incr["big"] = d
	return incr


def get_tasks_json():
	print(os.getcwd())
	with open("test_db/production_database/tasks.json", "rb") as f:
		return json.load(f)
