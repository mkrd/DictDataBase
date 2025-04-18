import shutil
from pathlib import Path

from pyinstrument import profiler

import dictdatabase as DDB
from dictdatabase import locking

DDB.config.storage_directory = "./.benchmark_locking"
path = Path(DDB.config.storage_directory)
path.mkdir(exist_ok=True, parents=True)


# 05.11.22: 4520ms
# 25.11.22: 4156ms
with profiler.Profiler() as p:
	for _ in range(25_000):
		read_lock = locking.ReadLock("db")
		read_lock._lock()  # noqa: SLF001
		read_lock._unlock()  # noqa: SLF001
p.open_in_browser()


# 05.11.22: 4884ms
# 25.11.22: 4159ms
with profiler.Profiler() as p:
	for _ in range(25_000):
		write_lock = locking.WriteLock("db")
		write_lock._lock()  # noqa: SLF001
		write_lock._unlock()  # noqa: SLF001
p.open_in_browser()


write_lock = locking.WriteLock("db/test.some")
write_lock._lock()  # noqa: SLF001


shutil.rmtree(DDB.config.storage_directory)
