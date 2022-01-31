import dictdatabase as DDB
from multiprocessing import Pool
import sys



def incr_db(n, tables, ddb_sd, ddb_pj, ddb_uc):
	DDB.config.storage_directory = ddb_sd
	DDB.config.pretty_json_files = ddb_pj
	DDB.config.use_compression = ddb_uc

	for _ in range(n):
		for t in range(tables):
			with DDB.session(f"incr{t}", as_PathDict=True) as (session, d):
				d["counter"] = lambda x: (x or 0) + 1
				session.write()
	return True



if __name__ == "__main__":
	tables = int(sys.argv[1])
	processes = int(sys.argv[2])
	per_process = int(sys.argv[3])
	ddb_sd = str(sys.argv[4])
	ddb_pj = bool(int(sys.argv[5]))
	ddb_uc = bool(int(sys.argv[6]))

	print(sys.argv)
	print(f"{tables = }, {processes = }, {per_process = }, {ddb_sd = }, {ddb_pj = }, {ddb_uc = }")

	pool = Pool(processes=processes)
	for _ in range(processes):
		pool.apply_async(incr_db, args=(per_process, tables, ddb_sd, ddb_pj, ddb_uc,))
	pool.close()
	pool.join()
