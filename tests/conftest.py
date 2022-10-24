import dictdatabase as DDB
import pytest
import shutil



@pytest.fixture(scope="session")
def env(request):
	dir = "./.ddb_pytest_storage"
	DDB.config.storage_directory = dir
	request.addfinalizer(lambda: shutil.rmtree(dir))



@pytest.fixture(params=[False, True])
def use_compression(request):
	DDB.config.use_compression = request.param



@pytest.fixture(params=[False, True])
def use_orjson(request):
	DDB.config.use_orjson = request.param



@pytest.fixture(params=[False, True])
def sort_keys(request):
	DDB.config.sort_keys = request.param



@pytest.fixture(params=[None, 0, 2, "\t"])
def indent(request):
	DDB.config.indent = request.param
