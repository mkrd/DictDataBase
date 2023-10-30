from dictdatabase.models import at


def test_at():
	assert at("x").path == "x"
	assert at("x", "y", "z").path == "x/y/z"
	assert at(["x", "y", "z"]).path == "x/y/z"
	assert at("x", ["y", "z"]).path == "x/y/z"
	assert at(["x", "y"], "z").path == "x/y/z"
	assert at(["x"], "y", "z").path == "x/y/z"
	assert at("x", ["y"], "z").path == "x/y/z"
	assert at("x", "y", ["z"]).path == "x/y/z"
	assert at("x", ["y"], ["z"]).path == "x/y/z"
	assert at(["x"], "y", ["z"]).path == "x/y/z"
	assert at(["x"], ["y"], "z").path == "x/y/z"
	assert at(["x"], ["y"], ["z"]).path == "x/y/z"
