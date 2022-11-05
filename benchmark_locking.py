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
from dictdatabase import locking

DDB.config.storage_directory = "./.benchmark_locking"
Path(DDB.config.storage_directory).mkdir(exist_ok=True)



t1 = time.monotonic()
with profiler.Profiler() as p:
    for _ in range(10_000):
        l = locking.ReadLock("db")
        l._lock()
        l._unlock()


p.open_in_browser()
