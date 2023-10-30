import random
import time
from pathlib import Path

from pyinstrument import profiler

import dictdatabase as DDB

DDB.config.storage_directory = ".ddb_scenario_comparison"
Path(DDB.config.storage_directory).mkdir(exist_ok=True)


# Create a database with 10_000 entries
all_users = {}
for i in range(10_000):
	print(i)
	user = {
		"id": "".join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=8)),
		"name": "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=5)),
		"surname": "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=20)),
		"description": "".join(random.choices('abcdefghij"klmnopqrst😁uvwxyz\\ ', k=5000)),
		"age": random.randint(0, 100),
	}
	all_users[user["id"]] = user
	DDB.at("users_dir", user["id"]).create(user)
DDB.at("users").create(all_users)


################################################################################
#### Test read from directory


# 06.11.22: 2695ms
t1 = time.monotonic()
with profiler.Profiler() as p:
	DDB.at("users_dir/*").read()
p.open_in_browser()
print("Read all users from directory:", time.monotonic() - t1)


################################################################################
#### Test read from single file


# 06.11.22: 181ms
t1 = time.monotonic()
DDB.at("users").read()
print("Read all users from single file:", time.monotonic() - t1)
