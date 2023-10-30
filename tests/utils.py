import json
import os
import random
import string


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

	return random_dict(
		[random_string, random_int, random_float, random_bool, random_none, random_list, random_dict], max_depth
	)
