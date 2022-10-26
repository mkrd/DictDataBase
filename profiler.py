import dictdatabase as DDB
from path_dict import PathDict
from pyinstrument import profiler



DDB.config.storage_directory = "./test_db/production_database"
DDB.config.use_orjson = True

p = profiler.Profiler(interval=0.00001)
with p:
    # fM44 is small
    # a2lU has many annotations
    # DDB.at("tasks").read(key="fM44", as_type=PathDict)
    with DDB.at("tasks").session(key="a2lU", as_type=PathDict) as (session, task):
        task["jay"] = lambda x: (x or 0) + 1
        session.write()
p.open_in_browser(timeline=True)
