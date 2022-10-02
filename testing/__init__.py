import dictdatabase as DDB
import super_py as sp
import shutil
import os
import orjson
from inspect import getmembers, isfunction



def orjson_decode(data_str):
	print("orjson_decode")
	return orjson.loads(data_str)


def orjson_encode(data_dict):
	print("orjson_encode")
	return orjson.dumps(
		data_dict,
		option=orjson.OPT_SORT_KEYS | orjson.OPT_INDENT_2,
	)


def config_orjson():
	DDB.config.custom_json_encoder = orjson_encode
	DDB.config.custom_json_decoder = orjson_decode



def make_test_dir():
	# shutil.rmtree(".ddb_storage_testing", ignore_errors=True)
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
	# "(🔴 pretty) (🟢 compression) (🔴 orjson)": sp.test(setup_compress, teardown),
	"(🔴 pretty) (🔴 compression) (🟢 orjson)": sp.test(setup_orjson, teardown),
	"(🟢 pretty) (🔴 compression) (🟢 orjson)": sp.test(setup_pretty_orjson, teardown),
	# "(🔴 pretty) (🟢 compression) (🟢 orjson)": sp.test(setup_compress_orjson, teardown),
}

test_scenes_no_teardown = {
	"(🔴 pretty) (🔴 compression) (🔴 orjson)": sp.test(setup),
	"(🟢 pretty) (🔴 compression) (🔴 orjson)": sp.test(setup_pretty),
	# "(🔴 pretty) (🟢 compression) (🔴 orjson)": sp.test(setup_compress),
	"(🔴 pretty) (🔴 compression) (🟢 orjson)": sp.test(setup_orjson),
	"(🟢 pretty) (🔴 compression) (🟢 orjson)": sp.test(setup_pretty_orjson),
	# "(🔴 pretty) (🟢 compression) (🟢 orjson)": sp.test(setup_compress_orjson),
}





# print("🚧 Test create")
# import test_create
# for scene, run_scene in test_scenes.items():
# 	print(scene)
# 	for _, fn in getmembers(test_create, isfunction):
# 		run_scene(fn)


# print("🚧 Test exceptions")
# import test_excepts
# for scene, run_scene in test_scenes.items():
# 	print(scene)
# 	[run_scene(f) for _, f in getmembers(test_excepts, isfunction)]


# print("🚧 Test read and write")
# import test_read_write
# for scene, run_scene in test_scenes.items():
# 	print(scene)
# 	[run_scene(f) for _, f in getmembers(test_read_write, isfunction)]


# print("🚧 Test big db")
# import test_big_db
# for scene, run_scene in test_scenes_no_teardown.items():
# 	print(scene)
# 	[run_scene(f) for _, f in getmembers(test_big_db, isfunction)]
# 	teardown()


print("🚧 Test stress")
import test_stress
for scene, run_scene in test_scenes.items():
	print(scene)
	[run_scene(f) for _, f in getmembers(test_stress, isfunction)]
