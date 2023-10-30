import sqlite3
import sys
from multiprocessing import Pool


def incr_db(n, tables):
	for _ in range(n):
		for t in range(tables):
			con = sqlite3.connect("test.db")
			cur = con.cursor()
			cur.execute(f"UPDATE incr{t} SET counter = counter + 1")
			con.commit()
			con.close()
	return True


if __name__ == "__main__":
	tables = int(sys.argv[1])
	processes = int(sys.argv[2])
	per_process = int(sys.argv[3])

	pool = Pool(processes=processes)
	for _ in range(processes):
		pool.apply_async(
			incr_db,
			args=(
				per_process,
				tables,
			),
		)
	pool.close()
	pool.join()
