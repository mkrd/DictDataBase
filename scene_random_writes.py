
import dictdatabase as DDB
import random
from pyinstrument.profiler import Profiler


user_count = 100_000

# all_users = {}
# for i in range(user_count):

#     user = {
#         "id": str(i),
#         "pref": "".join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=8)),
#         "name": "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=5)),
#         "surname": "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=20)),
#         "description": "".join(random.choices("abcdefghij\"klmnopqrst😁uvwxyz\\ ", k=5000)),
#         "age": random.randint(0, 100),
#     }
#     all_users[str(i)] = user
# DDB.at("users").create(all_users, force_overwrite=True)

print("Users created")

p = Profiler(interval=0.0001)
p.start()
for it in range(50):
    print(it)
    user_id = str(random.randint(user_count - 100, user_count - 1))
    with DDB.at("users", key=user_id).session() as (session, user):
        user["age"] += 1
        session.write()
p.stop()
p.open_in_browser(timeline=False)
