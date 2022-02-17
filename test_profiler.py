import cProfile
import dictdatabase as DDB
import subprocess

DDB.config.storage_directory = "./test_db/ddb_storage"


with cProfile.Profile() as pr:
    pr.enable()
    with DDB.multisession("cups/*") as (session, cups):
        pass

    pr.disable()
    pr.dump_stats("test.prof")
    pr.print_stats("tottime")

command = "poetry run snakeviz test.prof"
subprocess.call(command.split())
