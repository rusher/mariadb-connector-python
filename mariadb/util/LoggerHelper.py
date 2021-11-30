import copy

HEX_ARRAY = list("0123456789ABCDEF")


def hex(raw, offset, data_length, trunk_length=1024 * 1024 * 1024):
    if raw == None or len(raw) == 0:
        return ""
    hexa_value = [' '] * 16
    hexa_value[8] = ' '
    pos = offset
    pos_hexa = 0
    log_length = min(data_length, trunk_length)
    sb = "+--------------------------------------------------+\n" \
         + "|  0  1  2  3  4  5  6  7   8  9  a  b  c  d  e  f |\n" \
         + "+--------------------------------------------------+------------------+\n| "

    while pos < log_length + offset:
        byte_value = raw[pos] & 0xFF
        sb += str(HEX_ARRAY[byte_value >> 4])
        sb += str(HEX_ARRAY[byte_value & 0x0F]) + " "
        hexa_value[pos_hexa] = chr(byte_value) if 31 < byte_value < 127 else '.'
        pos_hexa += 1

        if pos_hexa == 8:
            sb += " "

        if pos_hexa == 16:
            sb += "| "
            sb += ''.join(hexa_value)
            sb += " |\n"
            if pos + 1 != log_length + offset:
                sb += "| "
            pos_hexa = 0

        pos += 1

    remaining = pos_hexa
    if remaining > 0:
        if remaining < 8:
            for i in range(remaining, 8):
                sb += "   "
            sb += " "
            remaining = 8

        for i in range(remaining, 16):
            sb += "   "
        remaining = 16
        for i in range(pos_hexa, 16):
            hexa_value[i] = ' '
        sb += "| "
        sb += ''.join(hexa_value)
        sb += " |\n"

    if data_length > trunk_length:
        sb += "+-------------------truncated----------------------+------------------+\n"
    else:
        sb += "+--------------------------------------------------+------------------+\n"
    return sb


def hex_header(header, raw, offset, data_length, trunk_length):
    complete = bytearray(data_length + len(header))
    complete[0:len(header)] = header
    complete[len(header):len(header) + data_length] = raw[offset:offset + data_length]
    return hex(complete, 0, data_length + len(header), trunk_length)
