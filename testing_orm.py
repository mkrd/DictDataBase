


class WorkTime(DictModel):
	start: str
	end: str


class User(FileDictItemModel):
	first_name: str
	last_name: str
	email: str | None

	work_times: list[WorkTime]


	def full_name(self):
		return f"{self.first_name} {self.last_name}"


class Users(FileDictModel[User]):
	__file__ = "users"



u = User.from_key_value("uid1", {
	"first_name": "John",
	"last_name": "Doe",
	"none": "no",
	"work_times": [
		{"start": "08:00", "end": "12:00"},
		{"start": "13:00", "end": "17:00"},
	]
})


assert u.first_name == "John"
assert u.last_name == "Doe"
assert u.full_name() == "John Doe"
assert u.work_times[0].start == "08:00"
assert u.work_times[0].end == "12:00"
assert u.work_times[1].start == "13:00"
assert u.work_times[1].end == "17:00"
assert len(u.work_times) == 2


print("u type:", type(u))




DDB.at("users").create({
	"uid1": {
		"first_name": "John",
		"last_name": "Doe",
		"none": "no",
		"work_times": [
			{"start": "08:00", "end": "12:00"},
			{"start": "13:00", "end": "17:00"},
		]
	},
	"uid2": {
		"first_name": "Jane",
		"last_name": "Smith",
		"none": "no",
		"work_times": [
			{"start": "08:00", "end": "12:00"},
			{"start": "13:00", "end": "17:00"},
		]
	},
	"uid3": {
		"first_name": "Pete",
		"last_name": "Griffin",
		"none": "no",
		"work_times": [
			{"start": "08:00", "end": "12:00"},
			{"start": "13:00", "end": "17:00"},
		]
	}
}, force_overwrite=True)


u1 = Users.get_at_key("uid1")
assert u1.first_name == "John"
assert u1.last_name == "Doe"
assert u1.full_name() == "John Doe"
assert u1.work_times[0].start == "08:00"
assert u1.work_times[0].end == "12:00"
assert u1.work_times[1].start == "13:00"
assert u1.work_times[1].end == "17:00"
assert len(u1.work_times) == 2



u2 = Users.get_at_key("uid2")



for uid, u in Users.items():
	print(u.full_name())



# # Iterate FileDictModel
# for user_id, user in Users.items():
# 	print(user_id, user.first_name, user.last_name, user.email)

# # Get one item
# user: User = Users.get_at_key("user_id")


# # Get by lambda
# users: Users = Users.where(lambda user: user.first_name != "John")


# with Users.session_at_key(user_id) as (session, user):
# 	...

# with Users.session() as (session, users): Dict[str, User]
# 	...
