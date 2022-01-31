import dictdatabase as DDB
import time

DDB.config.storage_directory = "./test_db"


t1 = time.time()

db = DDB.multiread("cups/*")
print(len(db))

t2 = time.time()

print((t2 - t1) * 1000, "ms")

