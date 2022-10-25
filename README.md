# DictDataBase



[![Downloads](https://pepy.tech/badge/dictdatabase)](https://pepy.tech/project/dictdatabase)

[![Downloads](https://pepy.tech/badge/dictdatabase/month)](https://pepy.tech/project/dictdatabase)

[![Downloads](https://pepy.tech/badge/dictdatabase/week)](https://pepy.tech/project/dictdatabase)

DictDataBase is a simple but fast and secure database for handling dicts (or PathDicts for more advanced features), that uses json or compressed json as the underlying storage mechanism. It is:
- **Multi threading and multi processing safe**. Multiple processes on the same machine can simultaneously read and write to dicts without writes getting lost.
- **No database server** required. Simply import DictDataBase in your project and use it.
- **ACID** compliant. Unlike TinyDB, it is suited for concurrent environments.
- **Fast**. A dict can be accessed partially without having to parse the entire file, making the read and writes very efficient.
- **Tested** with over 400 test cases.

### Why use DictDataBase
- For example have a webserver dispatches database read and writes concurrently.
- If spinning up a database server is overkill for your app.
	- But you still need [ACID](https://en.wikipedia.org/wiki/ACID) guarantees
- You have a big database, only want to access one key-value pair. DictDataBase can do this efficiently and fast.
- Your use case is suited for working with json data, or you have to work with a lot of json data.

### Why not DictDataBase
- If you need document indexes
- If your use case is better suited for a sql database


# Configuration
There are 5 configuration options:

### Storage directory
Set storage_directory to the path of the directory that will contain your database files:
```python

DDB.config.storage_directory = "./ddb_storage" # Default value
```

### Compression
If you want to use compressed files, set use_compression to True.
This will make the db files significantly smaller and might improve performance if your disk is slow. However, the files will not be human readable.
```python

DDB.config.use_compression = False # Default value

```

### Indentation
Set the way how written json files should be indented. Behaves exactly like json.dumps(indent=...). It can be an `int` for the number of spaces, the tab character, or `None` if you don't want the files to be indented.
```python

DDB.config.indent = "\t" # Default value

```


### Sort keys
Specify if you want the dict keys to be sorted when writing to a file.Behaves exactly like json.dumps(sort_keys=...).
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

import DictDataBase as DDB

```


## Create dict
This library is called DictDataBase, but you can actually use any json serializable object.
```python

user_data_dict = {
	"users": {
		"Ben": { "age": 30, "job": "Software Engineer" },
		"Sue": { "age": 21, "job": "Student" },
		"Joe": { "age": 50, "job": "Influencer" }
	},
	"follows": [["Ben", "Sue"], ["Joe", "Ben"]]
})

DDB.at("user_data").create(user_data_dict)

# There is now a file called user_data.json
# (or user_data.ddb if you use compression)
# in your specified storage directory.
```

## Check if exists



## Read dicts

```python

d = DDB.at("user_data").read()
# You now have a copy of the dict named "user_data"
print(d == user_data_dict) # True


# Only partially read Joe
joe = DDB.subread("user_data", key="Joe")
print(joe == user_data_dict["Joe"])

```


## Write dicts
```python

import DictDataBase as DDB

with DDB.session("user_data") as (session, user_data):

# You now have a handle on the dict named "user_data"

# Inside the with statement, the file of user_data will be locked, and no other

# processes will be able to interfere.

user_data["follows"].append(["Sue", "Ben"])

session.write()

# Now the changes to d are written to the database



print(DDB.at("user_data").read()["follows"])

# -> [["Ben", "Sue"], ["Joe", "Ben"], ["Sue", "Ben"]]

```

If you do not call session.write(), the database file will not be modified.


# API Reference

### at()

## DDBMethodChooser

### exists

### haskey

### create

### delete

### read

### session
