import hashlib

from mariadb.client.Context import Context
from mariadb.client.PacketReader import PacketReader
from mariadb.client.PacketWriter import PacketWriter
from mariadb.client.ReadableByteBuf import ReadableByteBuf
from mariadb.message.authentication.AuthenticationPlugin import AuthenticationPlugin


class NativePasswordPlugin(AuthenticationPlugin):

    def __init__(self):
        self.info = {}

    @staticmethod
    def encrypt_password(password: str, pwd_seed: bytearray) -> bytearray:
        if password is None:
            return bytearray(0)

        stage1 = hashlib.sha1(password.encode('utf-8')).digest()
        stage2 = hashlib.sha1(stage1).digest()
        s = hashlib.sha1()
        s.update(pwd_seed)
        s.update(stage2)
        digest = s.digest()
        return_bytes = bytearray(len(digest))
        for i in range(len(digest)):
            return_bytes[i] = stage1[i] ^ digest[i]
        return return_bytes

    def get_type(self) -> str:
        return "mysql_native_password"

    def initialize(self, user: str, password: str, seed: bytearray, conf) -> None:
        self.info = {
            "user": user,
            "password": password,
            "seed": seed
        }

    def process(self, writer: PacketWriter, reader: PacketReader, context: Context) -> ReadableByteBuf:
        if self.info['password'] is None:
            writer.write_empty_packet()
        else:
            truncated_seed = self.info['seed'][0: len(self.info['seed']) - 1]
            b = self.encrypt_password(self.info['password'], truncated_seed)
            writer.write_bytes(b, len(b))
            writer.flush()
        return reader.read_packet()
