import dictdatabase as DDB
from multiprocessing import Pool
import sys
import orjson


def orjson_decode(data_str):
	return orjson.loads(data_str)


def orjson_encode(data_dict):
	return orjson.dumps(
		data_dict,
		option=orjson.OPT_SORT_KEYS | orjson.OPT_INDENT_2,
	)




def incr_db(n, tables, ddb_sd, ddb_pj, ddb_uc):
	DDB.config.storage_directory = ddb_sd
	DDB.config.pretty_json_files = ddb_pj
	DDB.config.use_compression = ddb_uc
	DDB.config.custom_json_encoder = orjson_encode
	DDB.config.custom_json_decoder = orjson_decode
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
	ddb_sd = sys.argv[4]
	ddb_pj = sys.argv[5] == "True"
	ddb_uc = sys.argv[6] == "True"

	pool = Pool(processes=processes)
	for _ in range(processes):
		pool.apply_async(incr_db, args=(per_process, tables, ddb_sd, ddb_pj, ddb_uc,))
	pool.close()
	pool.join()
