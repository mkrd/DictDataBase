import dictdatabase as DDB
from pyinstrument import profiler
from pathlib import Path
from dictdatabase import locking

DDB.config.storage_directory = "./.benchmark_locking"
Path(DDB.config.storage_directory).mkdir(exist_ok=True)

with profiler.Profiler() as p:
    for _ in range(10_000):
        l = locking.ReadLock("db")
        l._lock()
        l._unlock()
p.open_in_browser()
