# Case 1: Thounsands of keys with users by their id in a single file
# Case 2: Thousands of files with users by their id in a directory
# Test for both cases: Read every user, read one user, write one user, write every user


from distutils.command.config import config
import dictdatabase as DDB
from dictdatabase import io_unsafe
from path_dict import PathDict
from pyinstrument import profiler
from pathlib import Path
import random
import time

DDB.config.storage_directory = ".ddb_scenario_comparison"
Path(DDB.config.storage_directory).mkdir(exist_ok=True)


def make_fake_user():
    return {
        "id": "".join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=8)),
        "name": "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=5)),
        "surname": "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=20)),
        "description": "".join(random.choices("abcdefghij\"klmnopqrst😁uvwxyz\\ ", k=5000)),
        "age": random.randint(0, 100),
    }


all_users = {}
for i in range(10000):
    print(i)
    user = make_fake_user()
    all_users[user["id"]] = user
    DDB.at("users_dir", user["id"]).create(user)
DDB.at("users").create(all_users)


t1 = time.monotonic()
with profiler.Profiler() as p:
    DDB.at("users_dir/*").read()
p.open_in_browser()

print("Read all users from directory:", time.monotonic() - t1)

t1 = time.monotonic()
DDB.at("users").read()
print("Read all users from single file:", time.monotonic() - t1)
