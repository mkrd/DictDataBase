#include <stdio.h>
#include <string.h>

int square(int i) {
	return i * i;
}



// def seek_index_through_value_bytes(data: bytes, index: int) -> int:
// 	"""
// 	Finds the index of the next comma or closing bracket/brace, but only if
// 	it is at the same indentation level as at the start index.

// 	Args:
// 	- `data`: A vaild JSON string
// 	- `index`: The start index in data

// 	Returns:
// 	- The end index of the value.
// 	"""

// 	# See https://www.json.org/json-en.html for the JSON syntax

// 	skip_next, in_str, list_depth, dict_depth = False, False, 0, 0

// 	for i in range(index, len(data)):
// 		if skip_next:
// 			skip_next = False
// 			continue
// 		current = data[i]
// 		if current == byte_codes.BACKSLASH:
// 			skip_next = True
// 			continue
// 		if current == byte_codes.QUOTE:
// 			in_str = not in_str
// 		if in_str or current == byte_codes.SPACE:
// 			continue
// 		if current == byte_codes.OPEN_SQUARE:
// 			list_depth += 1
// 		elif current == byte_codes.CLOSE_SQUARE:
// 			list_depth -= 1
// 		elif current == byte_codes.OPEN_CURLY:
// 			dict_depth += 1
// 		elif current == byte_codes.CLOSE_CURLY:
// 			dict_depth -= 1
// 		if list_depth == 0 and dict_depth == 0:
// 			return i + 1
// 	raise TypeError("Invalid JSON syntax")
// The above is the python implementation. Here is the exact same code but written in C
int seek_index_through_value_bytes(char* data, int index) {
    int skip_next = 0, in_str = 0, list_depth = 0, dict_depth = 0;
	int data_len = strlen(data);
    for (int i = index; i < data_len; i++) {
        if (skip_next) {
            skip_next = 0;
            continue;
        }
        char current = data[i];
        if (current == '\\') {
            skip_next = 1;
            continue;
        }
        if (current == '"') {
            in_str = !in_str;
        }
        if (in_str || current == ' ') {
            continue;
        }
        if (current == '[') {
            list_depth++;
        } else if (current == ']') {
            list_depth--;
        } else if (current == '{') {
            dict_depth++;
        } else if (current == '}') {
            dict_depth--;
        }
        if (list_depth == 0 && dict_depth == 0) {
            return i + 1;
        }
    }
    return -1;
}


// def count_nesting_bytes(data: bytes, start: int, end: int) -> int:
// 	"""
// 	Returns the number of nesting levels between the start and end indices.

// 	:param data: The string to be parsed
// 	"""
// 	skip_next, in_str, nesting = False, False, 0

// 	for i in range(start, end):
// 		if skip_next:
// 			skip_next = False
// 			continue
// 		current = data[i]
// 		if current == byte_codes.BACKSLASH:
// 			skip_next = True
// 			continue
// 		if current == byte_codes.QUOTE:
// 			in_str = not in_str
// 		if in_str or current == byte_codes.SPACE:
// 			continue
// 		elif current == byte_codes.OPEN_CURLY:
// 			nesting += 1
// 		elif current == byte_codes.CLOSE_CURLY:
// 			nesting -= 1
// 	return nesting
// This is the python implementation of the above function. Here is the exact same code but written in C
int count_nesting(char* data, int start, int end) {
	int skip_next = 0, in_str = 0, nesting = 0;
	for (int i = start; i < end; i++) {
		if (skip_next) {
			skip_next = 0;
			continue;
		}
		char current = data[i];
		if (current == '\\') {
			skip_next = 1;
			continue;
		}
		if (current == '"') {
			in_str = !in_str;
		}
		if (in_str || current == ' ') {
			continue;
		} else if (current == '{') {
			nesting++;
		} else if (current == '}') {
			nesting--;
		}
	}
	return nesting;
}


// def find_outermost_json_key_index_bytes(data: bytes, key: bytes):
// 	"""
// 		Returns the index of the key that is at the outermost nesting level.
// 		If the key is not found, return -1.
// 		If the key you are looking for is `some_key`, then you should pass
// 		`"some_key":` as the `key` argument to this function.
// 		Args:
// 		- `data`: Correct JSON as a string
// 		- `key`: The key of an object in `data` to search for
// 	"""
// 	if (curr_i := data.find(key, 0)) == -1:
// 		return -1

// 	key_nest = [(curr_i, 0)]  # (key, nesting)

// 	while (next_i := data.find(key, curr_i + len(key))) != -1:
// 		nesting = count_nesting_bytes(data, curr_i + len(key), next_i)
// 		key_nest.append((next_i, nesting))
// 		curr_i = next_i

// 	# Early exit if there is only one key
// 	if len(key_nest) == 1:
// 		return key_nest[0][0]

// 	# Relative to total nesting
// 	for i in range(1, len(key_nest)):
// 		key_nest[i] = (key_nest[i][0], key_nest[i - 1][1] + key_nest[i][1])
// 	return min(key_nest, key=lambda x: x[1])[0]
// This is the python implementation of the above function. Here is the exact same code but written in C
