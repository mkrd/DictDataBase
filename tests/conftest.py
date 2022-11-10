import dictdatabase as DDB
from tests import TEST_DIR
import pytest
import shutil


@pytest.fixture(scope="session")
def use_test_dir(request):
	DDB.config.storage_directory = TEST_DIR
	request.addfinalizer(lambda: shutil.rmtree(TEST_DIR))



@pytest.fixture(params=[True, False])
def use_compression(request):
	DDB.config.use_compression = request.param
	return request.param



@pytest.fixture(params=[True, False])
def use_orjson(request):
	DDB.config.use_orjson = request.param
	return request.param



@pytest.fixture(params=[False, True])
def sort_keys(request):
	DDB.config.sort_keys = request.param
	return request.param



@pytest.fixture(params=[None, 0, 2, "\t"])
def indent(request):
	DDB.config.indent = request.param
	return request.param
