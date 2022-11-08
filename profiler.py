from distutils.command.config import config
import dictdatabase as DDB
from dictdatabase import io_unsafe
from path_dict import PathDict
from pyinstrument import profiler



DDB.config.storage_directory = "./test_db/production_database"
DDB.config.use_orjson = True
DDB.config.indent = 2


p = profiler.Profiler(interval=0.0001)
with p:
    # fM44 is small
    # a2lU has many annotations
    # DDB.at("tasks", key="fM44").read(key="fM44", as_type=PathDict)
    for _ in range(10):
        with DDB.at("tasks", key="a2lU").session(as_type=PathDict) as (session, task):
            task["jay"] = lambda x: (x or 0) + 1
            session.write()
    # DDB.at("tasks_as_dir/*").read()


p.open_in_browser(timeline=False)
