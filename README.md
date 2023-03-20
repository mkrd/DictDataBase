![Logo](https://github.com/mkrd/DictDataBase/blob/main/assets/logo.png?raw=true)

[![Downloads](https://pepy.tech/badge/dictdatabase)](https://pepy.tech/project/dictdatabase)
![Tests](https://github.com/mkrd/DictDataBase/actions/workflows/test.yml/badge.svg)
![Coverage](https://github.com/mkrd/DictDataBase/blob/main/assets/coverage.svg?raw=1)

DictDataBase is a fast document-based database that uses json files or compressed json files for storage.
- **Multi threading and multi processing safe**. Multiple processes on the same machine
can simultaneously read and write to dicts without losing data.
- **ACID** compliant. Unlike TinyDB, it is suited for concurrent environments.
- **No Conflict resolution** required. Unlike with ZODB, lock-based access control is used, such that conflicts never occur.
- **No database server** required. Simply import DictDataBase in your project and use
it.
- **Compression**. Configure if the files should be stored as raw json or as json
compressed with zlib.
- **Fast**. Key-value pairs inside a json file can be accessed quickly and efficiently because the keys are indexed.
- **Tested** with 99%+ coverage.

### Why use DictDataBase
- Your application concurrently reads and writes data from multiple processes or threads.
- Using database server is a bit too much for your application.
    - But you need [ACID](https://en.wikipedia.org/wiki/ACID) guarantees.
- Your use case requires reading key-value pairs from very large json files repeatedly. (For example, DictDataBase can handle about 2000 reads per second when reading single key-value pairs from a 2.5GB json file with 20000 key-value pairs.)
- You need to repeatedly read and write many smaller json files.
- Your use case is suited for working with json data, or you have to work with a lot of
json data.

### Why not DictDataBase
- If your storage is slow.
- Your use cases requires repeatedly modifying or writing data in a single very large json file
- If a relational database is better suited for your use case.
- If you need to read files that are larger than your system's RAM.

Install
========================================================================================

```sh
pip install dictdatabase
```

Configuration
========================================================================================
The following configuration parameters can be modified using `DDB.config`:

### Storage directory
Set storage_directory to the path of the directory that will contain your json files:
```python
DDB.config.storage_directory = "./ddb_storage" # Default value
```

### Compression
If you want to use compressed files, set use_compression to `True`.
This will make the db files significantly smaller and might improve performance if your
disk is slow. However, the files will not be human readable.
```python
DDB.config.use_compression = False # Default value
```

### Indentation
Set the way how written json files should be indented. Behaves exactly like
`json.dumps(indent=...)`. It can be an `int` for the number of spaces, the tab
character, or `None` if you don't want the files to be indented.
```python
DDB.config.indent = "\t" # Default value
```
Notice: If `DDB.config.use_orjson = True`, then the value can only be 2 (spaces) or
0/None for no indentation.

### Use orjson
You can use the orjson encoder and decoder if you need to.
The standard library json module is sufficient most of the time.
However, orjson is a lot more performant in virtually all cases.
```python
DDB.config.use_orjson = True # Default value
```

Usage
========================================================================================

Import
----------------------------------------------------------------------------------------

```python
import dictdatabase as DDB
```

Create a file
----------------------------------------------------------------------------------------
This library is called DictDataBase, but you can actually use any json serializable object.
```python
users_dict = {
   "u1": { "name" : "Ben", "age": 30, "job": "Software Engineer" },
   "u2": { "name" : "Sue", "age": 21, "job": "Architect" },
   "u3": { "name" : "Joe", "age": 50, "job": "Manager" },
}
DDB.at("users").create(user_data_dict)
```
There is now a file called `users.json` or `users.ddb` in your specified storage
directory depending on if you use compression.


Check if file or sub-key exists
----------------------------------------------------------------------------------------
```python
DDB.at("users").exists()
>>> True  # File exists
DDB.at("users", key="u10").exists()
>>> False # Key "u10" not in users
DDB.at("users", key="u2").exists()
>>> True
```

Read dicts
----------------------------------------------------------------------------------------

```python
d = DDB.at("users").read()
d == users_dict # True

# Only partially read Joe
joe = DDB.at("users", key="u3").read()
joe == users_dict["Joe"] # True
```

> Note: Doing a partial read like with `DDB.at("users", key="Joe").read()` will only
> return the value of the key if the key is at the root indentation level.
> Example: You can get "a" from {"a" : 3}, but not from {"b": {"a": 3}}.

It is also possible to only read a subset of keys based on a filter callback:

```python
DDB.at("numbers").create({"a", 1, "b", 2, "c": 3})

above_1 = DDB.at("numbers", where=lambda k, v: v > 1).read()
>>> above_1 == {"b", 2, "c": 3}
```
> The `where` callback is a function that takes two parameters, the key and the value.


Write dicts
----------------------------------------------------------------------------------------

```python
with DDB.at("users").session() as (session, users):
   users["u3"]["age"] = 99
print(DDB.at("users", key="u3").read()["age])
>>> 99
```
> If you do not call session.write(), changes will not be written to disk!

Partial writing
----------------------------------------------------------------------------------------
Imagine you have a huge json file with many purchases.
The json file looks like this: `{<id>: <purchase>, <id>: <purchase>, ...}`.
Normally, you would have to read and parse the entire file to get a specific key.
After modifying the purchase, you would also have to serialize and write the
entire file again. With DDB, you can do it more efficiently:
```python
with DDB.at("purchases", key="3244").session() as (session, purchase):
    purchase["status"] = "cancelled"
    session.write()
```
Afterwards, the status is updated in the json file.
However, DDB did only efficiently gather the one purchase with id 134425, parsed
its value, and serialized that value alone before writing again. This is several
orders of magnitude faster than the naive approach when working with big files.


Folders
----------------------------------------------------------------------------------------

You can also read and write to folders of files. Consider the same example as
before, but now we have a folder called `purchases` that contains many files
`<id>.json`. If you want to open a session or read a specific one, you can do:

```python
DDB.at("purchases/<id>").read()
# Or equivalently:
DDB.at("purchases", "<id>").read()
```

To open a session or read all, do the following:
```python
DDB.at("purchases/*").read()
# Or equivalently:
DDB.at("purchases", "*").read()
```

### Select from folder

If you have a folder containing many json files, you can read them selectively
based on a function. The file is included if the provided function returns true
when it get the file dict as input:

To open a session or read all, do the following:
```python
for i in range(10):
    DDB.at("folder", i).create({"a": i})
# Now in the directory "folder", 10 files exist
res = DDB.at("folder/*", where=lambda x: x["a"] > 7).read() # .session() also possible
assert ress == {"8": {"a": 8}, "9": {"a": 9}} # True
```



Performance
========================================================================================

In preliminary testing, DictDataBase showed promising performance.

### SQLite vs DictDataBase
In each case, `16` parallel processes were spawned to perform `128` increments
of a counter in `4` tables/files. SQLite achieves `2435 operations/s` while
DictDataBase managed to achieve `3143 operations/s`.

### More tests
It remains to be tested how DictDatabase performs in different scenarios, for
example when multiple processes want to perform full writes to one big file.


Advanced
========================================================================================

Sleep Timeout
----------------------------------------------------------------------------------------
DictDataBase uses a file locking protocol to coordinate concurrent file accesses.
While waiting for a file where another thread or process currently has exclusive
access rights, the status of the file lock is periodically checked. You can set
the timout between the checks:

```python
DDB.locking.SLEEP_TIMEOUT = 0.001 # 1ms, default value
```

A value of 1 millisecond is good and it is generally not recommended to change it,
but you can still tune it to optimize performance in your use case.

Lock Timeout
----------------------------------------------------------------------------------------
When a lock file is older than the lock timeout, it is considered orphaned and will
be removed. This could be the case when your operating terminates a thread or process
while it holds a lock. The timeout can be adjusted:

```python
DDB.locking.LOCK_TIMEOUT = 30.0 # 30s, default value
```

Chose a value that is long enough where you know that your database operations will
less than it.


API Reference
========================================================================================

### `at(path) -> DDBMethodChooser:`
Select a file or folder to perform an operation on.
If you want to select a specific key in a file, use the `key` parameter,
e.g. `DDB.at("file", key="subkey")`. The key value is only returned if the key
is at the root level of the json object.

If you want to select an entire folder, use the `*` wildcard,
eg. `DDB.at("folder", "*")`, or `DDB.at("folder/*")`. You can also use
the `where` callback to select a subset of the file or folder.

If the callback returns `True`, the item will be selected. The callback
needs to accept a key and value as arguments.

Args:
- `path`: The path to the file or folder. Can be a string, a
comma-separated list of strings, or a list.
- `key`: The key to select from the file.
- `where`: A function that takes a key and value and returns `True` if the
key should be selected.

Beware: If you select a folder with the `*` wildcard, you can't use the `key`
parameter.
Also, you cannot use the `key` and `where` parameters at the same time.

DDBMethodChooser
----------------------------------------------------------------------------------------

### `exists() -> bool:`
Create a new file with the given data as the content. If the file
already exists, a FileExistsError will be raised unless
`force_overwrite` is set to True.

Args:
- `data`: The data to write to the file. If not specified, an empty dict
will be written.
- `force_overwrite`: If `True`, will overwrite the file if it already
exists, defaults to False (optional).


### `create(data=None, force_overwrite: bool = False):`
It creates a database file at the given path, and writes the given database to
it
:param db: The database to create. If not specified, an empty database is
created.
:param force_overwrite: If True, will overwrite the database if it already
exists, defaults to False (optional).

### `delete()`
Delete the file at the selected path.

### `read(self, as_type: T = None) -> dict | T | None:`
Reads a file or folder depending on previous `.at(...)` selection.

Args:
- `as_type`: If provided, return the value as the given type.
Eg. as_type=str will return str(value).

### `session(self, as_type: T = None) -> DDBSession[T]:`
Opens a session to the selected file(s) or folder, depending on previous
`.at(...)` selection. Inside the with block, you have exclusive access
to the file(s) or folder.
Call `session.write()` to write the data to the file(s) or folder.

Args:
- `as_type`: If provided, cast the value to the given type.
Eg. as_type=str will return str(value).
