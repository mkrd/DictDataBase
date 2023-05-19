from pathlib import Path
import dictdatabase as DDB
import pytest


@pytest.fixture(autouse=True)
def isolate_database_files(tmp_path: Path):
	DDB.config.storage_directory = str(tmp_path)



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
