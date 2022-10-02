import dictdatabase as DDB


def read_non_existent_json():
	DDB.config.use_compression = False
	d = DDB.read("nonexistent")
	assert d is None


def read_non_existent_ddb():
	DDB.config.use_compression = True
	d = DDB.read("nonexistent")
	assert d is None


def open_non_existent_json():
	DDB.config.use_compression = False
	try:
		with DDB.session("nonexistent", as_PathDict=True) as (session, d):
			assert False
	except Exception:
		assert True


def open_non_existent_ddb():
	DDB.config.use_compression = True
	try:
		with DDB.session("nonexistent", as_PathDict=True) as (session, d):
			assert False
	except Exception:
		assert True


def write_json_read_json():
	DDB.config.use_compression = False
	d = {"test": "value"}
	DDB.create("test", db=d)
	dd = DDB.read("test")
	assert d == dd


def write_ddb_read_ddb():
	DDB.config.use_compression = True
	d = {"test": "value"}
	DDB.create("test", db=d)
	dd = DDB.read("test")
	assert d == dd


def write_json_read_ddb():
	DDB.config.use_compression = False
	d = {"test": "value"}
	DDB.create("test", db=d)
	DDB.config.use_compression = True
	dd = DDB.read("test")
	assert d == dd


def write_ddb_read_json():
	DDB.config.use_compression = True
	d = {"test": "value"}
	DDB.create("test", db=d)
	DDB.config.use_compression = False
	dd = DDB.read("test")
	assert d == dd


def write_json_write_json():
	DDB.config.use_compression = False
	d = {"test": "value"}
	DDB.create("test", db=d)
	with DDB.session("test", as_PathDict=True) as (session, dd):
		assert d == dd.dict
		session.write()


def write_ddb_write_ddb():
	DDB.config.use_compression = True
	d = {"test": "value"}
	DDB.create("test", db=d)
	with DDB.session("test", as_PathDict=True) as (session, dd):
		assert d == dd.dict
		session.write()


def write_ddb_write_json():
	DDB.config.use_compression = True
	d = {"test": "value"}
	DDB.create("test", db=d)
	DDB.config.use_compression = False
	with DDB.session("test", as_PathDict=True) as (session, dd):
		assert d == dd.dict
		session.write()


def write_json_write_ddb():
	DDB.config.use_compression = False
	d = {"test": "value"}
	DDB.create("test", db=d)
	DDB.config.use_compression = True
	with DDB.session("test", as_PathDict=True) as (session, dd):
		assert d == dd.dict
		session.write()
