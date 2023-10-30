import pytest

from dictdatabase import io_bytes


def test_write_bytes(name_of_test, use_compression):
	# No partial writing to compressed file allowed
	if use_compression:
		with pytest.raises(RuntimeError):
			io_bytes.write(name_of_test, b"test", start=5)
		return
	# Write shorter content at index
	io_bytes.write(name_of_test, b"0123456789")
	io_bytes.write(name_of_test, b"abc", start=2)
	assert io_bytes.read(name_of_test) == b"01abc"
	# Overwrite with shorter content
	io_bytes.write(name_of_test, b"xy")
	assert io_bytes.read(name_of_test) == b"xy"
	# Overwrite with longer content
	io_bytes.write(name_of_test, b"0123456789")
	io_bytes.write(name_of_test, b"abcdef", start=8)
	assert io_bytes.read(name_of_test) == b"01234567abcdef"
	# Write at index out of range
	io_bytes.write(name_of_test, b"01")
	io_bytes.write(name_of_test, b"ab", start=4)
	assert io_bytes.read(name_of_test) == b"01\x00\x00ab"


def test_read_bytes(name_of_test, use_compression):
	io_bytes.write(name_of_test, b"0123456789")
	# In range
	assert io_bytes.read(name_of_test, start=2, end=5) == b"234"
	# Normal ranges
	assert io_bytes.read(name_of_test, start=0, end=10) == b"0123456789"
	assert io_bytes.read(name_of_test, start=2) == b"23456789"
	assert io_bytes.read(name_of_test, end=2) == b"01"
	assert io_bytes.read(name_of_test) == b"0123456789"
	# End out of range
	assert io_bytes.read(name_of_test, start=9, end=20) == b"9"
	# Completely out of range
	assert io_bytes.read(name_of_test, start=25, end=30) == b""
	# Start negative
	if use_compression:
		assert io_bytes.read(name_of_test, start=-5, end=3) == b""
	else:
		with pytest.raises(OSError):
			io_bytes.read(name_of_test, start=-5, end=3)
