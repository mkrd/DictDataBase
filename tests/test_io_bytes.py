import pytest
from dictdatabase import io_bytes



def test_write_bytes(use_test_dir, name_of_test):
    # Write shorter content at index
    io_bytes.write(name_of_test, b"0123456789")
    io_bytes.write(name_of_test, b"abc", 2)
    assert io_bytes.read(name_of_test) == b"01abc"
    # Overwrite with shorter content
    io_bytes.write(name_of_test, b"xy")
    assert io_bytes.read(name_of_test) == b"xy"
    # Overwrite with longer content
    io_bytes.write(name_of_test, b"0123456789")
    io_bytes.write(name_of_test, b"abcdef", 8)
    assert io_bytes.read(name_of_test) == b"01234567abcdef"
    # Write at index out of range
    io_bytes.write(name_of_test, b"01")
    io_bytes.write(name_of_test, b"ab", 4)
    assert io_bytes.read(name_of_test) == b'01\x00\x00ab'



def test_read_bytes(use_test_dir, name_of_test, use_compression):
    io_bytes.write(name_of_test, b"0123456789")
    # In range
    assert io_bytes.read(name_of_test, 2, 5) == b"234"
    # Complete range
    assert io_bytes.read(name_of_test, 0, 10) == b"0123456789"
    assert io_bytes.read(name_of_test, 0, None) == b"0123456789"
    assert io_bytes.read(name_of_test) == b"0123456789"
    # End out of range
    assert io_bytes.read(name_of_test, 9, 20) == b"9"
    # Completely out of range
    assert io_bytes.read(name_of_test, 25, 30) == b""
    # Start negative
    if use_compression:
        assert io_bytes.read(name_of_test, -5, 3) == b""
    else:
        with pytest.raises(OSError):
            io_bytes.read(name_of_test, -5, 3)
