import random
import time
from multiprocessing import Manager, Process

import dictdatabase as DDB


def do_create(name_of_test: str, return_dict: dict, id_counter: dict, operations: dict) -> None:
	with DDB.at(name_of_test).session() as (session, db):
		key = f"{id_counter['id']}"
		db[key] = {"counter": 0}
		id_counter["id"] += 1
		operations["create"] += 1
		session.write()
		return_dict["created_ids"] += [key]


def do_update(name_of_test: str, return_dict: dict, operations: dict) -> None:
	# increment a random counter
	with DDB.at(name_of_test).session() as (session, db):
		key = random.choice(return_dict["created_ids"])
		db[key]["counter"] += 1
		operations["increment"] += 1
		session.write()


def do_delete(name_of_test: str, return_dict: dict, operations: dict) -> None:
	# Delete a counter
	with DDB.at(name_of_test).session() as (session, db):
		key = random.choice(return_dict["created_ids"])
		operations["increment"] -= db[key]["counter"]
		operations["delete"] += 1
		db.pop(key)
		return_dict["created_ids"] = [i for i in return_dict["created_ids"] if i != key]
		session.write()


def do_read(name_of_test: str, return_dict: dict, operations: dict) -> None:
	# read a counter
	key = random.choice(return_dict["created_ids"])
	DDB.at(name_of_test, key=key).read()
	operations["read"] += 1


def worker_process(name_of_test: str, i: int, return_dict: dict, id_counter: dict) -> None:
	# Random seed to ensure each process gets different random numbers
	random.seed(i)
	DDB.config.storage_directory = ".ddb_bench_threaded"
	operations = {
		"create": 0,
		"increment": 0,
		"read": 0,
		"delete": 0,
	}

	for _ in range(1000):
		choice = random.random()
		if choice < 0.05:  # 5% chance
			do_create(name_of_test, return_dict, id_counter, operations)
		elif choice < 0.30:  # 25% chance
			do_update(name_of_test, return_dict, operations)
		elif choice < 0.33:  # 3% chance
			do_delete(name_of_test, return_dict, operations)
		else:  # 67% chance
			do_read(name_of_test, return_dict, operations)

	# Return the operations for this worker
	return_dict[i] = operations


def test_multiprocessing_crud(name_of_test, use_compression, use_orjson):
	pre_fill_count = 500
	DDB.at(name_of_test).create({f"{i}": {"counter": 0} for i in range(pre_fill_count)}, force_overwrite=True)

	manager = Manager()
	return_dict = manager.dict()
	id_counter = manager.dict()
	id_counter["id"] = pre_fill_count
	return_dict["created_ids"] = [f"{i}" for i in range(pre_fill_count)]

	start_time = time.time()
	processes = []
	for i in range(8):  # Spawn 4 processes
		p = Process(target=worker_process, args=(name_of_test, i, return_dict, id_counter))
		processes.append(p)
		p.start()

	for p in processes:
		p.join()

	print(return_dict)
	print("Duration", time.time() - start_time)

	db_state = DDB.at(name_of_test).read()

	logged_increment_ops = sum(x["increment"] for k, x in return_dict.items() if k != "created_ids")
	assert logged_increment_ops == sum(x["counter"] for x in db_state.values())

	logged_create_ops = sum(x["create"] for k, x in return_dict.items() if k != "created_ids")
	logged_delete_ops = sum(x["delete"] for k, x in return_dict.items() if k != "created_ids")
	assert pre_fill_count + logged_create_ops - logged_delete_ops == len(db_state.keys())
