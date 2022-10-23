import dictdatabase as DDB
import subprocess
from scalene import scalene_profiler



DDB.config.storage_directory = "./test_db/production_database"
DDB.config.use_orjson = True


scalene_profiler.start()
with DDB.subsession("tasks", key="fM44", as_PathDict=True) as (session, task):
    task["jay"] = lambda x: x + 1
    session.write()
scalene_profiler.stop()

# command = "poetry run snakeviz test.prof"
# subprocess.call(command.split())
