import cProfile
import dictdatabase as DDB
import subprocess
import orjson
from scalene import scalene_profiler


DDB.config.storage_directory = "./test_db/production_database"






def orjson_decode(data_str, select_key: str = "fM44"):

    return orjson.loads(data_str)
    # select_start = f'"{select_key}": '
    # key_index = data_str.find(select_start)
    # print(key_index)

    # in_str = False
    # in_lst = 0
    # in_dct = 0
    # for i in range(key_index + len(select_start), len(data_str)):
    #     if data_str[i] == '"' and data_str[i-1] != "\\":
    #         in_str = not in_str
    #         continue
    #     if in_str:
    #         continue
    #     if data_str[i] == "[":
    #         in_lst += 1
    #     elif data_str[i] == "]":
    #         in_lst -= 1
    #     elif data_str[i] == "{":
    #         in_dct += 1
    #     elif data_str[i] == "}":
    #         in_dct -= 1
    #     if in_lst == 0 and in_dct == 0:
    #         i += 1
    #         if data_str[i] == ",":
    #             i += 1
    #         break

    # load_str = data_str[key_index + len(select_start):i]
    # if load_str[-1] == ",":
    #     load_str = load_str[:-1]
    # print(load_str)
    # loaded = orjson.loads(load_str)
    # return loaded





def orjson_encode(data_dict):
    return orjson.dumps(
        data_dict,
        option=orjson.OPT_SORT_KEYS | orjson.OPT_INDENT_2,
    )


DDB.config.custom_json_encoder = orjson_encode
DDB.config.custom_json_decoder = orjson_decode


scalene_profiler.start()


with DDB.session("tasks") as (session, tasks):
    print("sess", len(tasks))
    session.write()
scalene_profiler.stop()

# command = "poetry run snakeviz test.prof"
# subprocess.call(command.split())
