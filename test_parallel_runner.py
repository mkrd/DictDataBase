import dictdatabase as DDB
from multiprocessing import Pool
import sys

def incr_db(n, tables, storage_directory):
	DDB.config.storage_directory = storage_directory
	DDB.config.pretty_json_files = False
	for _ in range(n):
		for t in range(tables):
			with DDB.session(f"incr{t}", as_PathDict=True) as (session, d):
				d["counter"] = lambda x: (x or 0) + 1
				session.write()
	return True




def test_stress(tables, processes, per_process, storage_directory):
	DDB.config.storage_directory = storage_directory
	pool = Pool(processes=processes)
	for _ in range(processes):
		pool.apply_async(incr_db, args=(per_process, tables, storage_directory,))
	pool.close()
	pool.join()



if __name__ == "__main__":
	test_stress(
		tables=int(sys.argv[1]),
		processes=int(sys.argv[2]),
		per_process=int(sys.argv[3]),
		storage_directory=sys.argv[4]
	)
