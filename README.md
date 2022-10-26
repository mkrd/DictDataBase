![Logo](https://github.com/mkrd/DictDataBase/blob/master/assets/logo.png?raw=true)

[![Downloads](https://pepy.tech/badge/dictdatabase)](https://pepy.tech/project/dictdatabase)
[![Downloads](https://pepy.tech/badge/dictdatabase/month)](https://pepy.tech/project/dictdatabase)
[![Downloads](https://pepy.tech/badge/dictdatabase/week)](https://pepy.tech/project/dictdatabase)
![Tests](https://github.com/mkrd/DictDataBase/actions/workflows/test.yml/badge.svg)
![Coverage](https://github.com/mkrd/DictDataBase/blob/master/assets/coverage.svg?raw=1)

DictDataBase is a simple and fast database for handling json or compressed json as the underlying storage mechanism. Features:
- **Multi threading and multi processing safe**. Multiple processes on the same machine can simultaneously read and write to dicts without data getting lost.
- **ACID** compliant. Unlike TinyDB, it is suited for concurrent environments.
- **No database server** required. Simply import DictDataBase in your project and use it.
- **Compression**. Configure if the files should be stored as raw json or as json compressed with zlib.
- **Fast**. A dict can be accessed partially without having to parse the entire file, making the read and writes very efficient.
- **Tested** with over 400 test cases.

### Why use DictDataBase
- For example have a webserver dispatches database read and writes concurrently.
- If spinning up a database server is overkill for your application.
	- But you need [ACID](https://en.wikipedia.org/wiki/ACID) guarantees.
- You have a big database, only want to access single key-value pairs repeatedly. DictDataBase can do this efficiently and quickly.
- Your use case is suited for working with json data, or you have to work with a lot of json data.

### Why not DictDataBase
- If you need document indexes.
- If your use case is better suited for a SQL database.


# Configuration
There are 5 configuration options:

### Storage directory
Set storage_directory to the path of the directory that will contain your json files:
```python
DDB.config.storage_directory = "./ddb_storage" # Default value
```

### Compression
If you want to use compressed files, set use_compression to `True`.
This will make the db files significantly smaller and might improve performance if your disk is slow.
However, the files will not be human readable.
```python
DDB.config.use_compression = False # Default value
```

### Indentation
Set the way how written json files should be indented. Behaves exactly like `json.dumps(indent=...)`.
It can be an `int` for the number of spaces, the tab character, or `None` if you don't want the files to be indented.
```python
DDB.config.indent = "\t" # Default value
```

### Sort keys
Specify if you want the dict keys to be sorted when writing to a file.Behaves exactly like `json.dumps(sort_keys=...)`.
```python
DDB.config.sort_keys = True # Default value
```

### Use orjson
You can specify the orjson encoder and decoder if you need to.
The standard library json module is sufficient most of the time.
However, orjson is a lot more performant in virtually all cases.
```python
DDB.config.use_orjson = True # Default value
```


# Usage

## Import

```python
import dictdatabase as DDB
```


## Create dict
This library is called DictDataBase, but you can actually use any json serializable object.
```python
user_data_dict = {
	"users": {
		"Ben": { "age": 30, "job": "Software Engineer" },
		"Sue": { "age": 21, "job": "Architect" },
		"Joe": { "age": 50, "job": "Manager" }
	},
	"follows": [["Ben", "Sue"], ["Joe", "Ben"]]
}
DDB.at("users").create(user_data_dict)

# There is now a file called users.json (or user_data.ddb if you use compression)
# in your specified storage directory.
```

## Check file or sub-key exists
```python
DDB.at("users").exists()  # True
DDB.at("users").exists("Ben")  # True
DDB.at("users").exists("Sam")  # False
```

## Read dicts
```python
d = DDB.at("users").read()
# You now have a copy of the json file named "users"
print(d == user_data_dict) # True

# Only partially read Joe
joe = DDB.at("users").read("Joe")
print(joe == user_data_dict["Joe"])
```

## Write dicts
```python
with DDB.at("users").session() as (session, users):
	# You now have a copy of the json file users as the variable users
	# Inside the with statement, the file of user_data will be locked, and no other
	# processes will be able to interfere.
	users["follows"].append(["Sue", "Ben"])
	session.write()
	# session.write() must be called to save the changes!
print(DDB.at("user_data").read()["follows"])
# -> [["Ben", "Sue"], ["Joe", "Ben"], ["Sue", "Ben"]]
```

If you do not call session.write(), changes will not be written to disk!


## Partial reading and writing
Imaging you have a huge json file with many transactions.
The json file looks like this: `{<id>: <transaction>, <id>: <transaction>, ...}`.
Normally, you would have to read and parse the entire file to get a specific key.
After modifying the transaction, you would also have to serialize and wirte the entire file again.
With DDB, you can do it more efficiently:
```python
with DDB.at("transactions").session(key="134425") as (session, transaction):
	transaction["status"] = "cancelled"
	session.write()
```
Afterwards, the status is updated in the json file.
However, DDB did only efficiently gather the one transaction with id 134425, parsed only its value, and only serialized that value before writing again.
This is several orders of magnitude faster than the naive approach when working with big files.


# API Reference

### `at(pattern) -> DDBMethodChooser`
`pattern` can be multiple parameters, which will be joined with a "`/"` to a path.
The file at the given path is then selected, and further operations can be performed using the `DDBMethodChooser`

## DDBMethodChooser

### `exists(key: str = None) -> bool`
Efficiently checks if a database exists.
If it contains a wildcard, it will return True if at least one exists.
If the key is passed, check if it exists in a database.
The key can be anywhere in the database, even deeply nested.
As long it exists as a key in any dict, it will be found.

### `create(db=None, force_overwrite=False)`
It creates a database file at the given path, and writes the given database to
it
:param db: The database to create. If not specified, an empty database is
created.
:param force_overwrite: If True, will overwrite the database if it already
exists, defaults to False (optional).

### `delete()`
Delete the database at the selected path.

### `read(key: str = None, as_type=None) -> dict | Any`
Reads a database and returns it. If a key is given, return the value at that key, more info in Args.

Args:
- `key`: If provided, only return the value of the given key. The key
	can be anywhere in the database, even deeply nested. If multiple
	identical keys exist, the one at the outermost indentation will
	be returned. This is very fast, as it does not read the entire
	database, but only the key - value pair.
- `as_type`: If provided, return the value as the given type. Eg. as=str will return str(value).

### `session(key: str = None, as_type=None) -> DDBSession | DDBMultiSession | DDBSubSession`
Open multiple files at once using a glob pattern, like "user/*".
Mutliple arguments are allowed to access folders,
so session(f"users/{user_id}") is equivalent
to session("users", user_id).
