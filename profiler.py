import dictdatabase as DDB
from pyinstrument import profiler



DDB.config.storage_directory = "./test_db/production_database"
DDB.config.use_orjson = True

p = profiler.Profiler(interval=0.00001)
with p:
    with DDB.at("tasks").session(key="fM44", as_PathDict=True) as (session, task):
        task["jay"] = lambda x: (x or 0) + 1
        session.write()
p.open_in_browser()
