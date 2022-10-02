import dictdatabase as DDB


def except_during_open_session():
	d = {"test": "value"}
	DDB.create("test", db=d)
	try:
		with DDB.session("test", as_PathDict=True) as (session, test):
			raise Exception("Any Exception")
	except Exception:
		pass


def except_on_save_unserializable():
	try:
		d = {"test": "value"}
		DDB.create("test", db=d)
		with DDB.session("test", as_PathDict=True) as (session, test):
			test["test"] = {"key": set([1, 2, 2])}
			session.write()
		assert False
	except TypeError:
		assert True


def except_on_session_in_session():
	d = {"test": "value"}
	DDB.create("test", db=d)
	try:
		with DDB.session("test", as_PathDict=True) as (session, test):
			with DDB.session("test", as_PathDict=True) as (session2, test2):
				assert False
	except RuntimeError:
		assert True
