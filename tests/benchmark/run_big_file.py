import dictdatabase as DDB
import random
import time


def make_random_posts(count):
    posts = {}
    for _ in range(count):
        id = str(random.randint(0, 999_999_999))
        title_length = random.randint(10, 100)
        content_length = random.randint(200, 500)
        posts[id] = {
            'id': id,
            'title': "".join(random.choices("  abcdefghijklmnopqrstuvwxyz,.", k=title_length)),
            'content': "".join(random.choices("  abcdefghijklmnopqrstuvwxyz,.", k=content_length)),
        }
    return posts



def make_users(count):
    all_users = {}
    for i in range(count):
        all_users[str(i)] = {
            "id": str(i),
            "name": "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=5)),
            "surname": "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=20)),
            "age": random.randint(20, 80),
            "posts": make_random_posts(random.randint(200, 300)),
        }
    return all_users




def read_specific_users():
    accessed_users = sorted([str(i * 100) for i in range(100)], key=lambda x: random.random())
    t1 = time.monotonic()
    for user_id in accessed_users:
        print(f"Accessing user {user_id}")
        u = DDB.at("big_users", key=user_id).read()
        print(f"User {user_id} has {len(u['posts'])} posts and is {u['age']} years old")
    t2 = time.monotonic()
    print(f"Time taken: {(t2 - t1) * 1000}ms")



def write_specific_users():
    accessed_users = sorted([str(i * 100) for i in range(100)], key=lambda x: random.random())
    t1 = time.monotonic()
    for user_id in accessed_users:
        print(f"Accessing user {user_id}")

        with DDB.at("big_users", key=user_id).session() as (session, user):
            user["surname"] = "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=random.randint(3, 50)))
            session.write()
    t2 = time.monotonic()
    print(f"Time taken: {(t2 - t1) * 1000}ms")




def random_access_users(write_read_ratio=0.1, count=500):
    accessed_users = [str(i * 100) for i in [random.randint(0, 99) for _ in range(count)]]
    t1 = time.monotonic()
    for user_id in accessed_users:

        if random.random() < write_read_ratio:
            with DDB.at("big_users", key=user_id).session() as (session, user):
                user["surname"] = "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=random.randint(3, 50)))
                session.write()
            print(f"Accessed user {user_id} for writing")
        else:
            u = DDB.at("big_users", key=user_id).read()
            print(f"User {user_id} has {len(u['posts'])} posts and is {u['age']} years old")

    t2 = time.monotonic()
    print(f"Time taken: {t2 - t1}s")






# DDB.at("big_users").create(make_users(20_000), force_overwrite=True)  # 2500MB

# random_access_users()
# write_specific_users()
read_specific_users()
