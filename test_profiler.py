import dictdatabase as DDB
import subprocess
from scalene import scalene_profiler
from pyinstrument import profiler



DDB.config.storage_directory = "./test_db/production_database"
DDB.config.use_orjson = True

p = profiler.Profiler(interval=0.00001)
with p:
    with DDB.subsession("tasks", key="fM44", as_PathDict=True) as (session, task):
        task["jay"] = lambda x: (x or 0) + 1
        session.write()
p.open_in_browser()

# command = "poetry run snakeviz test.prof"
# subprocess.call(command.split())
