import json

from dictdatabase import utils

test_dict = {
    "b": 2,
    "c": {
        "a": 1,
        "b": 2,
    },
    "d": {
        "a": 1,
        "b": 2,
    },
    "a": 1,

}

json_str = json.dumps(test_dict, indent=2, sort_keys=False)
json_bytes = json_str.encode()

index = utils.find_outermost_key_in_json_bytes(json_bytes, "a")

print("lel")
print(index)
print(json_bytes[index[0]:index[1]])
