from ea_driver.modbus import crc16_modbus, pack_float_be, unpack_float_be


def test_crc16_modbus_known_value():
    frame = bytes.fromhex("01030000000A")
    assert crc16_modbus(frame) == 0xCDC5


def test_float_pack_unpack_round_trip():
    registers = pack_float_be(80.0)
    assert unpack_float_be(list(registers)) == 80.0
