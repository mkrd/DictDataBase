import dictdatabase as DDB
import pytest


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
