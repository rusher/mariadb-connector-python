from mariadb.client.Context import Context
from mariadb.client.PacketReader import PacketReader
from mariadb.client.result.Result import Result


class CompleteResult(Result):

    __slots__ = ('data', 'data_len', 'pos')

    def __init__(self, binary_protocol: bool, metadata_list: list, reader: PacketReader, context: Context):
        super(CompleteResult, self).__init__(binary_protocol, metadata_list, reader, context)

        res = []
        next = super().read_next
        append = res.append

        tup = next()
        while tup is not None:
            append(tup)
            tup = next()

        self.data = tuple(res)
        self.data_len = len(self.data)
        self.pos = 0

    def fetchone(self) -> tuple:
        if self.pos >= self.data_len:
            return None
        self.pos += 1
        return self.data[self.pos - 1]

    def fetchmany(self, arraysize: int = -1) -> tuple:
        if arraysize <= 0:
            raise Exception("Wrong arraysize value {}", arraysize)
        if self.pos >= self.data_len:
            return None
        self.pos += arraysize
        return self.data[self.pos - arraysize:self.pos]

    def fetchall(self) -> tuple:
        return self.data

    def streaming(self) -> bool:
        return False
