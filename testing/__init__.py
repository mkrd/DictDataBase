import dictdatabase as DDB
import super_py as sp
import shutil
import os
import orjson


def orjson_decode(data_str):
	return orjson.loads(data_str)


def orjson_encode(data_dict):
	return orjson.dumps(
		data_dict,
		option=orjson.OPT_SORT_KEYS | orjson.OPT_INDENT_2,
	)


def config_orjson():
	DDB.config.custom_json_encoder = orjson_encode
	DDB.config.custom_json_decoder = orjson_decode



def make_test_dir():
	DDB.config.storage_directory = ".ddb_storage_testing"
	os.makedirs(DDB.config.storage_directory, exist_ok=True)





def teardown():
	shutil.rmtree(".ddb_storage_testing")
	DDB.config.custom_json_encoder = None
	DDB.config.custom_json_decoder = None



def setup():
	make_test_dir()
	DDB.config.pretty_json_files = False
	DDB.config.use_compression = False


def setup_pretty():
	setup()
	DDB.config.pretty_json_files = True


def setup_compress():
	setup()
	DDB.config.use_compression = True


def setup_orjson():
	setup()
	config_orjson()


def setup_pretty_orjson():
	setup_pretty()
	config_orjson()


def setup_compress_orjson():
	setup_compress()
	config_orjson()




test_scenes = {
	"(🔴 pretty) (🔴 compression) (🔴 orjson)": sp.test(setup, teardown),
	"(🟢 pretty) (🔴 compression) (🔴 orjson)": sp.test(setup_pretty, teardown),
	"(🔴 pretty) (🟢 compression) (🔴 orjson)": sp.test(setup_compress, teardown),
	"(🔴 pretty) (🔴 compression) (🟢 orjson)": sp.test(setup_orjson, teardown),
	"(🟢 pretty) (🔴 compression) (🟢 orjson)": sp.test(setup_pretty_orjson, teardown),
	"(🔴 pretty) (🟢 compression) (🟢 orjson)": sp.test(setup_compress_orjson, teardown),
}

test_scenes_no_teardown = {
	"(🔴 pretty) (🔴 compression) (🔴 orjson)": sp.test(setup),
	"(🟢 pretty) (🔴 compression) (🔴 orjson)": sp.test(setup_pretty),
	"(🔴 pretty) (🟢 compression) (🔴 orjson)": sp.test(setup_compress),
	"(🔴 pretty) (🔴 compression) (🟢 orjson)": sp.test(setup_orjson),
	"(🟢 pretty) (🔴 compression) (🟢 orjson)": sp.test(setup_pretty_orjson),
	"(🔴 pretty) (🟢 compression) (🟢 orjson)": sp.test(setup_compress_orjson),
}
