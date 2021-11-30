
BUFFER_SIZE=32767

class ReadAheadBufferedStream:

    __slots__ = ('socket', 'buf', 'view', 'end', 'pos')

    def __init__(self, socket):
        self.socket = socket
        self.buf = bytearray(BUFFER_SIZE)
        self.view = memoryview(self.buf)
        self.end = 0
        self.pos = 0

    def read(self, length):
        if length == 0:
            return 0
        total_reads = 0
        while True:
            if self.end - self.pos <= 0:
                if length - total_reads >= BUFFER_SIZE:
                    # buf length is less than asked byte and buf is empty
                    # => filling directly into external buf
                    external_buf = bytearray(length)
                    view = memoryview(external_buf)

                    while total_reads < length:
                        chunk = view[total_reads: length]
                        read = self.socket.recv_into(chunk)
                        total_reads += read
                    return external_buf, 0, length
                else:
                    chunk = self.view[self.end:BUFFER_SIZE]
                    read = self.socket.recv_into(chunk)
                    self.end += read
                    if self.end - self.pos < length:
                        continue
            elif length > self.end - self.pos:
                # some data have been buffered, but not enough
                if length >= BUFFER_SIZE:
                    external_buf = bytearray(length)
                    view = memoryview(external_buf)
                    view[0:self.end - self.pos] = self.view[self.pos:self.end]
                    total_reads = self.end - self.pos
                    self.pos = 0
                    self.end = 0
                    while total_reads < length:
                        chunk = view[total_reads: length]
                        read = self.socket.recv_into(chunk)
                        total_reads += read
                    return external_buf, 0, length
                else:
                    self.buf[0:self.end - self.pos] = self.view[self.pos:self.end]
                    self.end = self.end - self.pos
                    self.pos = 0
                    chunk = self.view[self.end:BUFFER_SIZE]
                    read = self.socket.recv_into(chunk)
                    self.end += read
                    if self.end - self.pos < length:
                        continue

            len_to_copy = min(length, self.end - self.pos)
            begin = self.pos
            self.pos += len_to_copy
            if self.pos >= self.end:
                self.pos = 0
                self.end = 0
            return self.buf, begin, begin + len_to_copy
