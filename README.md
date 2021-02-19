# DictDataBase

[![Downloads](https://pepy.tech/badge/dictdatabase)](https://pepy.tech/project/dictdatabase)
[![Downloads](https://pepy.tech/badge/dictdatabase/month)](https://pepy.tech/project/dictdatabase)
[![Downloads](https://pepy.tech/badge/dictdatabase/week)](https://pepy.tech/project/dictdatabase)

DictDataBase is a simple but fast and secure database for handling dicts (or PathDicts for more advanced features), that uses json files as the underlying storage mechanism.
It is also multiprocessind and multithreading safe, due to the employed locking mechanisms.

## Import

```python
	import DictDataBase as DDB
```


## Configuration

There are 3 configuration options.
Set storage_directory to the path of the directory that will contain your database files:

```python
	DDB.config.storage_directory = "./ddb_storage" # Default value
```

If you want to use compressed files, set use_compression to True.
This will make the db files significantly smaller and might improve performance if your disk is slow.
However, the files will not be human readable.
```python
	DDB.config.use_compression = False # Default value
```

If you set pretty_json_files to True, the json db files will be indented and the keys will be sorted.
It won't affect compressed files, since the are not human-readable anyways.
```python
	DDB.config.pretty_json_files = True # Default value
```




## Create dicts
Before you can access dicts, you need to explicitly create them.

Do create ones that already exist, this would raise an exception.
Also do not access ones that do not exist, this will also raise an exception.

```python
	user_data_dict = {
		"users": {
			"Ben": {
				"age": 30,
				"job": "Software Engineer"
			},
			"Sue": {
				"age": 21:
				"job": "Student"
			},
			"Joe": {
				"age": 50,
				"job": "Influencer"
			}
		},
		"follows": [["Ben", "Sue"], ["Joe", "Ben"]]
	})
	DDB.create("user_data", db=user_data_dict)
	# There is now a file called user_data.json (or user_data.ddb if you use compression)
	# in your specified storage directory.
```


## Read dicts
```python
	d = DDB.read("user_data")
	# You now have a copy of the dict named "user_data"
	print(d == user_data_dict) # True
```

## Write dicts

```python
	import DictDataBase as DDB
	with DDB.session("user_data") as (session, d):
		# You now have a handle on the dict named "user_data"
		# Inside the with statement, the file of user_data will be locked, and no other
		# processes will be able to interfere.
		d["follows"].append(["Sue", "Ben"])
		session.save_changes()
		# Now the changes to d are written to the database

	print(DDB.read("user_data")["follows"])
	# -> [["Ben", "Sue"], ["Joe", "Ben"], ["Sue", "Ben"]]
```

If you do not call session.save_changes(), the database file will not be modified.
