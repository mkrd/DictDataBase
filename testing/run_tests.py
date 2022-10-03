from inspect import getmembers, isfunction
from testing import test_scenes, test_scenes_no_teardown, teardown


print("🚧 Test create")
from testing import test_create
for scene, run_scene in test_scenes.items():
	print(scene)
	for _, fn in getmembers(test_create, isfunction):
		run_scene(fn)


print("🚧 Test exceptions")
from testing import test_excepts
for scene, run_scene in test_scenes.items():
	print(scene)
	[run_scene(f) for _, f in getmembers(test_excepts, isfunction)]


print("🚧 Test read and write")
from testing import test_read_write
for scene, run_scene in test_scenes.items():
	print(scene)
	[run_scene(f) for _, f in getmembers(test_read_write, isfunction)]


print("🚧 Test big db")
from testing import test_big_db
for scene, run_scene in test_scenes_no_teardown.items():
	print(scene)
	[run_scene(f) for _, f in getmembers(test_big_db, isfunction)]
	teardown()
