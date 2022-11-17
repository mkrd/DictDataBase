import dictdatabase as DDB
from tests import TEST_DIR
import pytest
import shutil
import os


@pytest.fixture(scope="session")
def use_test_dir(request):
	DDB.config.storage_directory = TEST_DIR
	os.makedirs(TEST_DIR, exist_ok=True)
	request.addfinalizer(lambda: shutil.rmtree(TEST_DIR))



@pytest.fixture(scope="function")
def name_of_test(request):
	return request.function.__name__



@pytest.fixture(params=[True, False])
def use_compression(request):
	DDB.config.use_compression = request.param
	return request.param



@pytest.fixture(params=[True, False])
def use_orjson(request):
	DDB.config.use_orjson = request.param
	return request.param



@pytest.fixture(params=[None, 0, 2, "\t"])
def indent(request):
	DDB.config.indent = request.param
	return request.param
