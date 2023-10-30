import dictdatabase as DDB


def test_indexer(use_compression, use_orjson, indent):
	DDB.at("test_indexer").create(force_overwrite=True, data={"a": {"e": 4}, "b": 2})

	# Trigger create index entry for key "a"
	assert DDB.at("test_indexer", key="a").read() == {"e": 4}

	# Retrieve the index entry for key "a" by using the indexer
	with DDB.at("test_indexer", key="a").session() as (session, d):
		d["e"] = 5
		session.write()

	# Check that the index entry for key "a" has been updated
	assert DDB.at("test_indexer").read() == {"a": {"e": 5}, "b": 2}
