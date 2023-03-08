from dictdatabase.object_mapper import DictModel, FileDictItemModel, FileDictModel
import dictdatabase as DDB
import pytest


def test_object_mapper_docs_example(use_test_dir):
	name = "object_mapper_docs_example_users"
	DDB.at(name).create({
		"u1": {
			"first_name": "John",
			"last_name": "Doe",
			"age": 21
		},
		"u2": {
			"first_name": "Jane",
			"last_name": "Smith",
			"age": 30,
			"phone": "0123456"
		},
	}, force_overwrite=True)

	class User(FileDictItemModel):
		first_name: str
		last_name: str
		age: int
		phone: str | None

		def full_name(self):
			return f"{self.first_name} {self.last_name}"

	class Users(FileDictModel[User]):
		__file__ = name

	u1: User = Users.get_at_key("u1")
	assert u1.full_name() == "John Doe"
	assert u1.age == 21
	assert u1.phone is None

	with pytest.raises(AttributeError):
		u1.no

	u2: User = Users.get_at_key("u2")
	assert u2.full_name() == "Jane Smith"
	assert u2.age == 30
	assert u2.phone == "0123456"



	for uid, user in Users.get_all().items():
		assert user.age in [21, 30]
