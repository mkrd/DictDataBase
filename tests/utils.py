import dictdatabase as DDB
import random
import string
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
	d = {"key1": "val1", "key2": 2, "key3": [1, "2", [3, 3]]}
	for i in range(recursion_depth):
		d = {f"key{i}{j}": d for j in range(keys_per_level)}
	return {"counter": 0, "big": d}


def get_tasks_json():
	print(os.getcwd())
	with open("test_db/production_database/tasks.json", "rb") as f:
		return json.load(f)


def make_complex_nested_random_dict(max_width, max_depth):

	def random_string(choices, md):
		length = random.randint(0, max_width)
		letters = string.ascii_letters + "".join(["\\", " ", "🚀", '"'])
		return "".join(random.choices(letters, k=length))

	def random_int(choices, md):
		return random.randint(-1000, 1000)

	def random_float(choices, md):
		return random.uniform(-1000, 1000)

	def random_bool(choices, md):
		return random.choice([True, False])

	def random_none(choices, md):
		return None

	def random_list(choices, md):
		if md == 0:
			return []
		res = []
		for _ in range(random.randint(0, max_width)):
			v = random.choice(choices)(choices, md - 1)
			res += [v]
		return res

	def random_dict(choices, md):
		if md == 0:
			return {}
		res = {}
		for _ in range(random.randint(0, max_width)):
			k = random_string(choices, md)
			v = random.choice(choices)(choices, md - 1)
			res[k] = v
		return res

	return random_dict([
		random_string,
		random_int,
		random_float,
		random_bool,
		random_none,
		random_list,
		random_dict
	], max_depth)


import json

d = make_complex_nested_random_dict(10, 5)

print(json.dumps(d, indent=2))
