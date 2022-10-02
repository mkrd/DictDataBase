import cProfile
import dictdatabase as DDB
import subprocess
import orjson

DDB.config.storage_directory = "./test_db/production_database"


def orjson_decode(data_str):
    return orjson.loads(data_str)


def orjson_encode(data_dict):
    return orjson.dumps(
        data_dict,
        option=orjson.OPT_SORT_KEYS | orjson.OPT_INDENT_2,
    )


DDB.config.custom_json_encoder = orjson_encode
DDB.config.custom_json_decoder = orjson_decode


with cProfile.Profile() as pr:
    pr.enable()

    for _ in range(200):
        with DDB.session("tasks", as_PathDict=True) as (session, tasks):
            tasks["00000"] = lambda x: (x or 0) + 1
            session.write()

    pr.disable()
    pr.dump_stats("test.prof")
    pr.print_stats("tottime")

command = "poetry run snakeviz test.prof"
subprocess.call(command.split())
